import asyncio
import logging
from collections import deque
import time


class CancellingTaskCtx:
    def __init__(self, fut):
        """
        Creates a task from a future/coroutine and cancels at during __exit__ if task not done
        :param fut: Future or coroutine
        """
        self._task = asyncio.Task(fut)

    def __enter__(self):
        return self._task

    def __exit__(self, exc_type, exc_val, exc_tb):
        if not self._task.done():
            self._task.cancel()


# This is a moving-window rate limiter.  The theory
# behind this limiter is that it will guarantee that at most `max_rate` hits will be allowed during
# any window of `period_s`. To be clear since the window is sliding there will be many windows this
# pertains to. To do this it needs to keep track of the window allotted to each request and only
# expire items which end a `period_s` after it finished, and with each expiration allow a new request.
class RateLimiter:
    class Error(Exception):
        pass

    def __init__(self, max_rate: int, period_s: float or int, logger: logging.Logger):
        """
        Allows `max_rate` per `period_s`.

        :param max_rate: number of hits allowed per `period_s`
        :param period_s: period in seconds
        :param logger: logger to use
        """

        assert isinstance(max_rate, int) and max_rate > 0
        assert period_s > 0

        self._max_rate = max_rate
        self._period_s = period_s
        self._loop = asyncio.get_event_loop()
        self._logger = logger
        self._release_task = None
        self._release_worker_exception = None
        self._broken_event = asyncio.Event()  # will get set if limiter is broken
        self._broken_evt_wait_fut = asyncio.ensure_future(self._broken_event.wait())
        self._waiters = 0

        # We'll initially allow `max_rate` to happen in parallel, and then release
        # the semaphores as new tasks can be started
        self._sema = asyncio.Semaphore(max_rate)

        # we'll push the task end-time to this queue during `__aexit__`
        self._end_time_q = deque()

    @property
    def is_broken(self):
        return self._broken_event.is_set()

    async def join(self):
        """ Will wait until all waiters have finished """
        while self._waiters:
            await asyncio.sleep(1)

        if not self._broken_evt_wait_fut.done():
            # normal operation
            if self._release_task and not self._release_task.done():
                await self._release_task

            self._broken_evt_wait_fut.cancel()
        else:
            # exceptional operation
            if not self._release_task.done():
                self._release_task.cancel()

    async def __aenter__(self):
        self._waiters += 1

        try:
            # Wait on which happens first: we acquire the semaphore or the rate-limiter breaks
            with CancellingTaskCtx(self._sema.acquire()) as acquire_fut:
                await asyncio.wait((self._broken_evt_wait_fut, acquire_fut), return_when=asyncio.FIRST_COMPLETED)

                if self._broken_evt_wait_fut.done():
                    raise self.Error("Error while acquiring semaphore") from self._release_worker_exception
        finally:
            self._waiters -= 1

    async def _release_worker(self, sleep_s):
        try:
            # swapping back/forth at this point is ok because __aexit__ will not swap as it does not yield will detect we're already running
            await asyncio.sleep(sleep_s)

            now = time.time()  # cache as this call is not cheap

            # Here we'll release each semaphore that expired its period from when it finished
            # We have a loop as an optimization against having multipler timers since the timer may be called later
            # than when wanted, and thus we may have multiple semaphores that we can release.
            while len(self._end_time_q):
                oldest_finished_ts = self._end_time_q[0]
                time_since_finished_ts = now - oldest_finished_ts
                if time_since_finished_ts >= self._period_s:
                    # if either of these fail, the ratelimiter will be marked as broken and all current and future acquires will raise
                    self._end_time_q.popleft()
                    self._sema.release()
                else:
                    # swapping here is ok for same reason as above
                    await asyncio.sleep(self._period_s - time_since_finished_ts)
                    now = time.time()  # we need to update time after we sleep
        except BaseException as e:
            self._logger.exception("Failed while attempting to release semaphores")
            self._release_worker_exception = e  # must set this before we set the event
            self._broken_event.set()
            # NOTE: theoretically we could try to "reset" the limiter after flushing the semas
            raise
        finally:
            self._release_task = None  # only clear this when we're actually exiting

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        # NOTE: Even if there's a pending exception we have to assume the call counted

        # It's important no yields occur because this and _release_worker modify self._end_time_q.  We're
        # relying on asyncio behavior of only allowing one task to run at a time as a lock.
        try:
            # If this fails you'll permanently decrease your available max_rate by one, and if max_rate == 1 deadlock
            self._end_time_q.append(time.time())

            if not self._release_task:
                # If there's already a timer we don't need to register a new one because the existing
                # timer will iterate through all the pending events and re-register if they're not yet releasable.
                # If this fails you'll deadlock if your max_rate == 1
                self._release_task = asyncio.ensure_future(self._release_worker(self._period_s))
        except BaseException:
            self._logger.exception("Error registering rate limiter hit, potential for deadlock!!!")
            raise

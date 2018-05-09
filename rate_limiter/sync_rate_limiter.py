import logging
import time
import threading
import queue


# This is a moving-window rate limiter.  The theory behind this limiter is that it will guarantee that at most
# `max_rate` hits will be allowed during any window of `period_s`. To be clear since the window is sliding
# there will be many windows this pertains to. To do this it needs to keep track of the window allotted to each
# request and only expire items which end a `period_s` after it finished, and with each expiration allow a new request.
class RateLimiter:
    class Error(Exception):
        pass

    class Terminator:
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
        self._logger = logger

        self._lock = threading.Lock()
        self._waiters = 0

        # We'll initially allow `max_rate` to happen in parallel, and then release
        # the semaphores as new tasks can be started
        self._sema = threading.Semaphore(max_rate)

        # we'll push the task end-time to this queue during `__aexit__`
        self._end_time_q = queue.Queue()

        self._release_worker_exception = None
        self._release_thread: threading.Thread = None

    @property
    def max_rate(self):
        return self._max_rate

    @property
    def is_broken(self):
        return self._release_worker_exception is not None

    def join(self):
        """ Will wait until all waiters have finished.

         This method is public for access by unittests. """
        while self._waiters:
            time.sleep(1)

        if self._release_thread:
            self._end_time_q.put(self.Terminator())

            # wait until the helper thread exits, this could cause an extra `self._period_s` seconds of waiting
            # we could add some more code to exit sooner if we wanted
            while self._release_thread:
                time.sleep(1)

    def __enter__(self):
        self.acquire()
        return self

    def acquire(self, timeout=None):
        assert not self.is_broken

        with self._lock:
            if not self._release_thread:
                self._release_thread = threading.Thread(target=self._semaphore_releaser_thread_func, daemon=True)
                self._release_thread.start()
            self._waiters += 1

        try:
            # Wait on which happens first: we acquire the semaphore or the rate-limiter breaks
            # Since there's no equivalent to asyncio.wait for threading primitives we need to always have a timeout to ensure
            # we see `self._release_worker_exception`
            acquired = False
            while not acquired and not self._release_worker_exception:
                sleep_s = timeout or 10
                acquired = self._sema.acquire(timeout=sleep_s)  # worst case scenario, if the _release_worker fails, we wait for sleep_s

                if not acquired and timeout:
                    raise TimeoutError("Timed out acquiring rate limiter")
        finally:
            with self._lock:
                self._waiters -= 1

    def _semaphore_releaser_thread_func(self):
        try:
            while True:
                # wait until we have items to release
                # NOTE this code uses a few undocumented but non-private attributes (not_empty, etc)
                #      We could either add another condition variable or continue using this. Given that we have unittests
                #      which will ensure these attributes behave correctly we should be ok
                # not_empty is a condition variable that is notified when the queue is not empty
                # This is the beginning of a "peek" method that follows the implementation of Queue.get, see: https://github.com/python/cpython/blob/master/Lib/queue.py#L153
                with self._end_time_q.not_empty:  # only "get" acquires this lock which is only used below
                    while not len(self._end_time_q.queue):
                        self._end_time_q.not_empty.wait()

                # Here we'll release each semaphore that expired its period from when it finished
                while len(self._end_time_q.queue):  # ok not to have lock as the enclosed code is the only thing that can remove items
                    now = time.time()  # cache as this call is not cheap
                    sleep_s = 0

                    while len(self._end_time_q.queue):  # ok not to have lock as the enclosed code is the only thing that can remove items
                        oldest_finished_ts = self._end_time_q.queue[0]
                        if isinstance(oldest_finished_ts, self.Terminator):
                            return

                        time_since_finished_ts = now - oldest_finished_ts
                        if time_since_finished_ts >= self._period_s:
                            # if either of these fail, the ratelimiter will be marked as broken and all current and future acquires will raise
                            self._end_time_q.get(block=False)
                            self._end_time_q.task_done()

                            self._sema.release()
                        else:
                            sleep_s = self._period_s - time_since_finished_ts
                            break

                    time.sleep(sleep_s)  # sleep outside lock
        except BaseException as e:
            self._logger.exception("Failed while attempting to release semaphores")
            self._release_worker_exception = e
            # NOTE: theoretically we could try to "reset" the limiter after flushing the semas
            raise
        finally:
            self._release_thread = None  # only clear this when we're actually exiting

    def __exit__(self, exc_type, exc_val, exc_tb):
        # NOTE: Even if there's a pending exception we have to assume the __enter__ call counted
        self.release()

    def release(self):
        assert not self.is_broken

        try:
            # If this fails you'll permanently decrease your available max_rate by one, and if max_rate == 1 deadlock
            self._end_time_q.put(time.time())
        except:
            if self._max_rate == 1:
                self._logger.exception("Error registering rate limiter hit, deadlocked!")
            else:
                self._logger.exception("Error registering rate limiter hit, max_rate decreased by 1, potential for eventual deadlock")
            raise

    def __del__(self):
        self.join()

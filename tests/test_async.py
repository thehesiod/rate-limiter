import asyncio
import logging
import time

import asynctest

import common
from rate_limiter import ASyncRateLimiter as RateLimiter


class TestRateLimiter(asynctest.TestCase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        logging.basicConfig(level=logging.INFO)
        self._logger = logging.getLogger(self.__class__.__name__)
        self._rl = None

    async def validate_elapsed(self, coro, num_seconds: float or int):
        start_s = time.time()
        try:
            await asyncio.wait_for(coro, num_seconds)
        except:
            print("Error elapsed: {}".format(time.time() - start_s))
            raise

        elapsed_s = time.time() - start_s
        self.assertAlmostEqual(num_seconds, elapsed_s, delta=0.1)

    async def tearDown(self):
        await self._rl.join()

    @staticmethod
    async def acquire(rl2, sleep_s=0):
        start = time.time()
        async with rl2:
            wait_s = time.time() - start
            await asyncio.sleep(sleep_s)

        return wait_s

    async def test_rate_limiter1(self):
        # test sequential
        self._rl = rl = RateLimiter(3, 2, self._logger)

        await asyncio.wait_for(self.acquire(rl), 0.01)  # 1
        await asyncio.wait_for(self.acquire(rl), 0.01)  # 2
        await asyncio.wait_for(self.acquire(rl), 0.01)  # 3

        fut = asyncio.Task(self.acquire(rl))  # 4

        with self.assertRaises(asyncio.TimeoutError):
            await asyncio.wait_for(asyncio.shield(fut), 0.1)  # 4 (fail)

        await self.validate_elapsed(fut, 1.95)  # 4 (complete)
        await asyncio.wait_for(self.acquire(rl), 0.01)  # 5
        await asyncio.wait_for(self.acquire(rl), 0.01)  # 6

        fut = asyncio.Task(self.acquire(rl))  # 7

        with self.assertRaises(asyncio.TimeoutError):
            await asyncio.wait_for(asyncio.shield(fut), 0.01)  # 7 (fail)

        await self.validate_elapsed(fut, 2)  # 7 (complete)

    async def test_rate_limiter2(self):
        # test parallel
        self._rl = rl = RateLimiter(3, 2, self._logger)

        await asyncio.gather(*[asyncio.wait_for(self.acquire(rl), 0.1) for _ in range(3)])  # 1, 2, 3

        fut = asyncio.Task(self.acquire(rl))  # 4

        # this one should timeout but count
        with self.assertRaises(asyncio.TimeoutError):
            await asyncio.wait_for(asyncio.shield(fut), 0.1)  # 4 (fail)

        # these should take 2 seconds and pass
        await asyncio.gather(self.validate_elapsed(fut, 1.95), *[self.validate_elapsed(self.acquire(rl), 1.95) for _ in range(2)])

    async def test_rate_limiter3(self):
        self._rl = rl = RateLimiter(1, 1, self._logger)
        await asyncio.wait_for(self.acquire(rl), 0.01)

        await asyncio.sleep(1.1)

        await asyncio.wait_for(self.acquire(rl), 0.01)

    async def test_rate_limiter4(self):
        self._rl = rl = RateLimiter(3, 2, self._logger)

        # these won't register the call as having happened 1, 2, 3 seconds after
        await asyncio.gather(*[self.validate_elapsed(self.acquire(rl, i), i + 0.1) for i in range(1, 4)])

        # we've waited 3s, so 1s one is released, 2nd has 1s wait, 3rd has 2s wait
        times = sorted(await asyncio.gather(*[asyncio.wait_for(self.acquire(rl), 3) for _ in range(3)]))
        self.assertRecursiveAlmostEqual(times, [0, 1, 2], delta=0.1)

    async def test_rate_limiter5(self):
        self._rl = rl = RateLimiter(3, 2, self._logger)

        for i in range(1, 4):
            asyncio.ensure_future(self.acquire(rl, i))

        # in this scenario we'll have to wait 1 + 2, 2 + 2, 3 + 2 seconds
        times = sorted(await asyncio.gather(*[asyncio.wait_for(self.acquire(rl), 5.1) for _ in range(3)]))
        self.assertRecursiveAlmostEqual(times, [3, 4, 5], delta=0.1)

    # TODO: add test where we break _release_worker

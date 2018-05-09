from contextlib import contextmanager, ExitStack
from enum import Enum
import logging
from functools import partial
import threading
from typing import Union, Callable, ContextManager, Dict, List, Tuple

from sync_rate_limiter import RateLimiter


class StrEnum(str, Enum):
    pass


SECONDS_IN_MINUTE = 60
SECONDS_IN_HOUR = SECONDS_IN_MINUTE * 60

# This represents a contextmanager that takes a rate limit unit parameter and waits against one or many RateLimiters
# It's a partial that is the second type
API_LIMITER_CONTEXT_TYPE = Union[partial, Callable[[int], ContextManager]]


# For now these are Google API limiters
class LimiterServices(StrEnum):
    Calendar = "calendar"
    AdminSDK = "admin_sdk"
    Gmail = "gmail"
    People = 'people'


# The following two structures document the known limits for Google APIs.  There are two types of limiters, a per request
# limiter, and a quota per request.  Some requests have a quota cost.  Further there are global limiters, and per user limiters.
# The format of the two structures is: {limiter_type: {LimiterServices: {period_in_seconds: rate_in_period}}}
# NOTE: we're often reducing the periods of the actual limits to avoid blowing memory with the rate limiter's queue
# NOTE: batch requests will count as multiple requests in terms of limits if there are multiple items in the batch
GLOBAL_LIMITERS = {
    "request": {
        # https://developers.google.com/calendar/pricing
        LimiterServices.Calendar: {
            SECONDS_IN_HOUR: 41_666,  # 1,000,000 / day ~= 41,666 / hr
        },

        LimiterServices.AdminSDK: {
            SECONDS_IN_HOUR: 6250,  # 150,000 / day = 6260 / hr
        },

        LimiterServices.Gmail: {
            100: 2_000_000
        }
    },
    "quota": {
        # Neither Calendar (https://developers.google.com/calendar/pricing) nor Admin SDK (https://developers.google.com/admin-sdk/directory/v1/limits) seem to have "quota" units
        LimiterServices.Gmail: {
            # https://developers.google.com/gmail/api/v1/reference/quota
            SECONDS_IN_MINUTE: 694_4444  # 1,000,000,000 / day ~= 694,4444 / min
        }
    }
}

# For some strange reason the non-daily quotas costs are multiplied by this multiple, so we reduce
# the limits appropriately
_NON_DAILY_QUOTA_MULTIPLIER = 1 + (2 / 3)


PER_USER_LIMITERS = {
    "request": {
        LimiterServices.Calendar: {
            100: int(500 // _NON_DAILY_QUOTA_MULTIPLIER),  # we still hit this
        },
        LimiterServices.AdminSDK: {
            100: int(1500 // _NON_DAILY_QUOTA_MULTIPLIER),
        },
        LimiterServices.Gmail: {
            100: int(25_000 // _NON_DAILY_QUOTA_MULTIPLIER),
        },
        # In reality this is split between read/write/critical write, but we take the lowest for now
        LimiterServices.People: {
            SECONDS_IN_MINUTE: int(30 // _NON_DAILY_QUOTA_MULTIPLIER),
        }
    },
    "quota": {
        LimiterServices.Gmail: {
            1: int(250 // _NON_DAILY_QUOTA_MULTIPLIER)
        }
    }
}

# These locks are to ensure that a group of limiters for a given service acquire a set of locks atomically
# so that groups don't contend with each other and cause deadlocks.  They need to be RLocks to ensure that
# you can acquire "request" and "quota" limiters on a given thread for a given service.  If we enforced rate limiters
# go be acquired in particular orders this would not be needed.
_LIMITERS_LOCKS = {
    LimiterServices.Calendar: threading.RLock(),
    LimiterServices.AdminSDK: threading.RLock(),
    LimiterServices.Gmail: threading.RLock(),
    LimiterServices.People: threading.RLock(),
}


GET_LIMITERS_RET_TYPE = Dict[str, Dict[str, API_LIMITER_CONTEXT_TYPE]]


def get_limiters(logger: logging.Logger) -> GET_LIMITERS_RET_TYPE:
    """
    Returns a dictionary of service name to a callable to acquire `num` API requests from a
    set of limiters associated with said service.

    NOTE: You should only call this method once as the set of limiters should last length of life of the application.

    :param logger: logger to use
    :return: dict of: {service_name: limiter_context_callable}
    """
    global _GET_LIMITERS_CALLED
    assert not _GET_LIMITERS_CALLED

    ret_value = {
        limiter_type: {
            svc: partial(limiters_context, limiters=[RateLimiter(units, period, logger) for period, units in period_units_dict.items()], service=svc)
            for svc, period_units_dict in limiters.items()
        }
        for limiter_type, limiters in GLOBAL_LIMITERS.items()
    }

    _GET_LIMITERS_CALLED = True
    return ret_value


# limiters + service params have defaults because we want `num` to be positional and have a default
@contextmanager
def limiters_context(num: int=1, limiters: List[RateLimiter]=None, service: LimiterServices=None) -> ContextManager:
    """
    Context class which will acquire `num` semaphores from all `limiters`

    :param num: number of API queries which will be called
    :param limiters: list of limiters to acquire from
    :param service: service of limiters
    """
    with ExitStack() as exit_stack:
        # We only want one thread to acquire a group of locks at a time to avoid deadlocks.
        # We want to release the lock on the service before we yield to allow others to begin acquiring from the limiters
        with _LIMITERS_LOCKS[service]:
            # now we can acquire the semaphores
            for limiter in limiters:
                assert num <= limiter.max_rate
                for _ in range(num):
                    # NOTE: If we block on a semaphore here, we will be holding the lock as well, but it's ok because if someone comes
                    #       and blocks on the lock they would have blocked on the semaphore if the lock were not present.
                    exit_stack.enter_context(limiter)

        yield


def get_limiters_context_with_added_limiters(method: API_LIMITER_CONTEXT_TYPE, limiters: List[RateLimiter]) -> API_LIMITER_CONTEXT_TYPE:
    """
    Will create a new rate limiter context from the exist context `method` with a new list of limiters which contains the existing limiters
    from `method` and the new list of `limiters`.

    :param method: limiters partial context to obtain existing limiters from
    :param limiters: list of limiters to add
    :return: new partial
    """
    assert method.func == limiters_context
    assert not method.args
    assert method.keywords.keys() == {'limiters', 'service'}

    # we are creating a new list because we do not want to modify the old context
    new_limiters = method.keywords['limiters'] + limiters
    return partial(method.func, *method.args, limiters=new_limiters, service=method.keywords["service"])


_PER_USER_LIMITER_CONTEXT_RETURN_TYPE = Dict[str, API_LIMITER_CONTEXT_TYPE]

# cache of get_per_user_limiter_context return values
_PER_USER_LIMITER_CACHE: Dict[Tuple[str, str], _PER_USER_LIMITER_CONTEXT_RETURN_TYPE] = dict()  # dict of: {(service_name, user_name): return of get_per_user_limiter_context}

# This lock protects `_PER_USER_LIMITER_CACHE`
_PER_USER_LIMITER_LOCK = threading.Lock()


def get_per_user_limiter_context(logger: logging.Logger, limiters: GET_LIMITERS_RET_TYPE, service_name: LimiterServices, user_name: str) -> _PER_USER_LIMITER_CONTEXT_RETURN_TYPE:
    """
    Will return the per user request and quota limiters for `service_name` by appending any relevant per user limiters to the global limiters
    for said service.

    :param logger: logger to use
    :param limiters: limiters for all services (value from `get_limiters`)
    :param service_name: service name from which to get limiters
    :param user_name: user name associated with this set of limiters
    :return: dictionary of: {'request': per_user_request_limiter_context, 'quota': per_user_quota_limiter_context}.  If a given `service_name`
            does not have limits the context will be None
    """
    cache_key = (service_name, user_name)

    with _PER_USER_LIMITER_LOCK:
        return_value = _PER_USER_LIMITER_CACHE.get(cache_key)
        if return_value:
            return return_value

        # request limiters
        request_context = limiters["request"].get(service_name)
        named_user_req_limiters = PER_USER_LIMITERS["request"].get(service_name)
        if named_user_req_limiters:
            named_user_req_limiters = [RateLimiter(units, period, logger) for period, units in named_user_req_limiters.items()]

        if request_context and named_user_req_limiters:
            request_context = get_limiters_context_with_added_limiters(request_context, named_user_req_limiters)
        elif named_user_req_limiters:
            request_context = partial(limiters_context, limiters=named_user_req_limiters, service=service_name)
        else:
            request_context = None

        # quota limiters
        quota_context = limiters["quota"].get(service_name)
        named_user_quota_limiters = PER_USER_LIMITERS["quota"].get(service_name)
        if named_user_quota_limiters:
            named_user_quota_limiters = [RateLimiter(units, period, logger) for period, units in named_user_quota_limiters.items()]

        if quota_context and named_user_quota_limiters:
            quota_context = get_limiters_context_with_added_limiters(quota_context, named_user_quota_limiters)
        elif named_user_quota_limiters:
            quota_context = partial(limiters_context, limiters=named_user_quota_limiters, service=service_name)
        else:
            quota_context = None

        return_value = {
            'request': request_context,
            'quota': quota_context
        }

        _PER_USER_LIMITER_CACHE[cache_key] = return_value

        return return_value

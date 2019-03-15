import logging
from functools import partial, wraps
from typing import Union, Type, Container


logger = logging.getLogger(__name__)


def retryable(
        func=None,
        expected_exception: Union[Type[Exception], Container[Type[Exception]]]=(),
        *,
        retry_count: int=1,
        callback=None,
):
    if func is None:
        return partial(
            retryable,
            expected_exception=expected_exception,
            retry_count=retry_count,
            callback=callback
        )

    @wraps(func)
    def wrapper(*args, **kwargs):
        i = 0

        prev_exceptions = []

        while True:
            try:
                return func(*args, **kwargs)
            except expected_exception as e:
                prev_exceptions.append(e)
                if i >= retry_count:
                    logger.error(
                        'Got too many exceptions: %s\nRaising the last one.',
                        prev_exceptions
                    )
                    raise e

                logger.info(
                    'Got expected exception %s while running %s. %s attempts left.',
                    type(e).__name__,
                    func.__name__ if hasattr(func, '__name__') else func,
                    retry_count - i
                )

                if callback is not None:
                    callback(*args, **kwargs)

                i += 1

    return wrapper


def retryable_generator(
        func=None,
        expected_exception: Union[Type[Exception], Container[Type[Exception]]]=(),
        *,
        retry_count: int=1,
        callback=None,
):
    if func is None:
        return partial(
            retryable,
            expected_exception=expected_exception,
            retry_count=retry_count,
            callback=callback
        )

    @wraps(func)
    def wrapper(*args, **kwargs):
        i = 0

        prev_exceptions = []

        while True:
            try:
                object_to_return = func(*args, **kwargs)

                try:
                    yield next(object_to_return)
                except StopIteration:
                    return
                except GeneratorExit:
                    pass
            except expected_exception as e:
                prev_exceptions.append(e)
                if i >= retry_count:
                    logger.error(
                        'Got too many exceptions: %s\nRaising the last one.',
                        prev_exceptions
                    )
                    raise e

                logger.info(
                    'Got expected exception %s while running %s. %s attempts left.',
                    type(e).__name__,
                    func.__name__ if hasattr(func, '__name__') else func,
                    retry_count - i
                )

                if callback is not None:
                    callback(*args, **kwargs)

                i += 1
            else:
                yield from object_to_return
                return

    return wrapper
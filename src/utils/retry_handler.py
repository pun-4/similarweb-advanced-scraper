thonimport functools
import logging
import time
from typing import Any, Callable, Iterable, Tuple, TypeVar

logger = logging.getLogger("similarweb_scraper.retry")

F = TypeVar("F", bound=Callable[..., Any])

def retry(
    retries: int = 3,
    backoff_in_seconds: float = 1.0,
    exceptions: Iterable[type[BaseException]] = (Exception,),
) -> Callable[[F], F]:
    """
    Simple retry decorator with exponential backoff.

    Example:
        @retry(retries=3, backoff_in_seconds=2)
        def fetch(...):
            ...
    """

    exception_tuple: Tuple[type[BaseException], ...] = tuple(exceptions)

    def decorator(func: F) -> F:
        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            attempt = 0
            delay = backoff_in_seconds
            while True:
                try:
                    return func(*args, **kwargs)
                except exception_tuple as exc:  # type: ignore[misc]
                    attempt += 1
                    if attempt > retries:
                        logger.error(
                            "Function %s failed after %d attempts: %s",
                            func.__name__,
                            retries,
                            exc,
                        )
                        raise
                    logger.warning(
                        "Function %s failed on attempt %d/%d: %s. Retrying in %.2fs",
                        func.__name__,
                        attempt,
                        retries,
                        exc,
                        delay,
                    )
                    time.sleep(delay)
                    delay *= 2

        return wrapper  # type: ignore[return-value]

    return decorator
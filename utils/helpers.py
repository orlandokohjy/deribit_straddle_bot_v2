from __future__ import annotations

import functools
import time
from datetime import datetime, timezone
from typing import Any, Callable, TypeVar

import structlog

F = TypeVar("F", bound=Callable[..., Any])

log = structlog.get_logger("helpers")


def retry(
    max_attempts: int = 3,
    delay_seconds: float = 2.0,
    backoff_factor: float = 1.0,
    exceptions: tuple[type[BaseException], ...] = (Exception,),
) -> Callable[[F], F]:
    def decorator(fn: F) -> F:
        @functools.wraps(fn)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            last_exc: BaseException | None = None
            for attempt in range(1, max_attempts + 1):
                try:
                    return fn(*args, **kwargs)
                except exceptions as exc:
                    last_exc = exc
                    if attempt < max_attempts:
                        wait = delay_seconds * (backoff_factor ** (attempt - 1))
                        log.warning("retry_scheduled", fn=fn.__qualname__, attempt=attempt, wait_s=wait, error=str(exc))
                        time.sleep(wait)
            raise last_exc  # type: ignore[misc]
        return wrapper  # type: ignore[return-value]
    return decorator


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def ms_to_utc(timestamp_ms: int) -> datetime:
    return datetime.fromtimestamp(timestamp_ms / 1000, tz=timezone.utc)

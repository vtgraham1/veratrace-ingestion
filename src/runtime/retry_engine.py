"""
Retry engine with decorrelated jitter and circuit breaker.

- Decorrelated jitter prevents thundering herd on rate limit recovery.
- Circuit breaker stops retrying when a service is genuinely down.
- Never retries non-idempotent operations blindly.
"""
import time
import random
import logging
from dataclasses import dataclass, field
from typing import Callable, TypeVar

logger = logging.getLogger(__name__)

T = TypeVar("T")

MAX_RETRIES = 7
BASE_BACKOFF_S = 1.0
MAX_BACKOFF_S = 300.0  # 5 minutes cap
CIRCUIT_BREAKER_THRESHOLD = 0.5  # open circuit at 50% error rate
CIRCUIT_BREAKER_WINDOW = 60  # seconds


@dataclass
class CircuitBreaker:
    """Tracks error rate over a rolling window. Opens when threshold exceeded."""

    _successes: list[float] = field(init=False, default_factory=list)
    _failures: list[float] = field(init=False, default_factory=list)
    _open_until: float = field(init=False, default=0.0)

    def _prune(self, window: float = CIRCUIT_BREAKER_WINDOW):
        cutoff = time.monotonic() - window
        self._successes = [t for t in self._successes if t > cutoff]
        self._failures = [t for t in self._failures if t > cutoff]

    def record_success(self):
        self._successes.append(time.monotonic())

    def record_failure(self):
        self._failures.append(time.monotonic())

    def is_open(self) -> bool:
        if self._open_until > time.monotonic():
            return True
        self._prune()
        total = len(self._successes) + len(self._failures)
        if total < 5:
            return False  # not enough data
        error_rate = len(self._failures) / total
        if error_rate >= CIRCUIT_BREAKER_THRESHOLD:
            self._open_until = time.monotonic() + 30  # open for 30s
            return True
        return False


def decorrelated_jitter(last_backoff: float) -> float:
    """Decorrelated jitter: next = random(base, 3 * last)."""
    return min(MAX_BACKOFF_S, random.uniform(BASE_BACKOFF_S, 3 * last_backoff))


class RetryError(Exception):
    """Raised when all retries are exhausted."""

    def __init__(self, message: str, last_error: Exception | None = None):
        super().__init__(message)
        self.last_error = last_error


def with_retry(
    fn: Callable[[], T],
    *,
    max_retries: int = MAX_RETRIES,
    idempotent: bool = True,
    circuit_breaker: CircuitBreaker | None = None,
    on_retry: Callable[[int, float, Exception], None] | None = None,
) -> T:
    """
    Execute fn with retry logic.

    Args:
        fn: The function to execute (must be idempotent if retrying).
        max_retries: Maximum number of retry attempts.
        idempotent: If False, will not retry (raises immediately).
        circuit_breaker: Optional circuit breaker instance.
        on_retry: Callback(attempt, backoff, error) for logging/metrics.
    """
    if not idempotent:
        # Non-idempotent operations get exactly one attempt
        return fn()

    if circuit_breaker and circuit_breaker.is_open():
        raise RetryError("Circuit breaker open — service appears down")

    last_backoff = BASE_BACKOFF_S
    last_error: Exception | None = None

    for attempt in range(1, max_retries + 1):
        try:
            result = fn()
            if circuit_breaker:
                circuit_breaker.record_success()
            return result
        except Exception as e:
            last_error = e
            if circuit_breaker:
                circuit_breaker.record_failure()

            if attempt >= max_retries:
                break

            # Check if circuit breaker tripped
            if circuit_breaker and circuit_breaker.is_open():
                break

            backoff = decorrelated_jitter(last_backoff)
            last_backoff = backoff

            if on_retry:
                on_retry(attempt, backoff, e)

            logger.warning(
                "retry attempt=%d/%d backoff=%.1fs error=%s",
                attempt, max_retries, backoff, str(e)[:200],
            )
            time.sleep(backoff)

    raise RetryError(
        f"All {max_retries} retries exhausted",
        last_error=last_error,
    )

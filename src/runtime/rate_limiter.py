"""
Token bucket rate limiter — per-endpoint, respects vendor ceilings.

Never exceeds RATE_LIMIT_CEILING_PCT of the vendor's stated limit.
Pauses ALL requests (not just the failed one) on 429.
"""
import time
import threading
from dataclasses import dataclass, field


@dataclass
class TokenBucket:
    """Thread-safe token bucket with configurable fill rate."""

    capacity: float
    fill_rate: float  # tokens per second
    _tokens: float = field(init=False)
    _last_fill: float = field(init=False)
    _lock: threading.Lock = field(init=False, default_factory=threading.Lock)
    _paused_until: float = field(init=False, default=0.0)

    def __post_init__(self):
        self._tokens = self.capacity
        self._last_fill = time.monotonic()

    def _refill(self):
        now = time.monotonic()
        elapsed = now - self._last_fill
        self._tokens = min(self.capacity, self._tokens + elapsed * self.fill_rate)
        self._last_fill = now

    def acquire(self, timeout: float = 30.0) -> bool:
        """
        Block until a token is available, or timeout.
        Returns True if acquired, False if timed out.
        """
        deadline = time.monotonic() + timeout
        while True:
            with self._lock:
                # Respect global pause (429 backoff)
                if self._paused_until > time.monotonic():
                    wait = self._paused_until - time.monotonic()
                    if time.monotonic() + wait > deadline:
                        return False
                    self._lock.release()
                    time.sleep(min(wait, 0.5))
                    self._lock.acquire()
                    continue

                self._refill()
                if self._tokens >= 1.0:
                    self._tokens -= 1.0
                    return True

            # No token available — wait briefly and retry
            if time.monotonic() > deadline:
                return False
            time.sleep(0.1)

    def pause(self, seconds: float):
        """Pause all requests for `seconds` (called on 429 response)."""
        with self._lock:
            self._paused_until = max(
                self._paused_until, time.monotonic() + seconds
            )


class RateLimiterRegistry:
    """
    Manages rate limiters per (connector, endpoint) pair.
    Ceiling enforcement: fill_rate = vendor_limit * ceiling_pct / 100.
    """

    def __init__(self, ceiling_pct: int = 70):
        self._limiters: dict[str, TokenBucket] = {}
        self._ceiling_pct = ceiling_pct

    def get(self, key: str, vendor_limit_per_second: float) -> TokenBucket:
        if key not in self._limiters:
            effective_rate = vendor_limit_per_second * self._ceiling_pct / 100
            self._limiters[key] = TokenBucket(
                capacity=max(1.0, effective_rate * 2),  # burst = 2x rate
                fill_rate=effective_rate,
            )
        return self._limiters[key]

"""Bound benchmark work while allowing cleanup to preserve its artifacts."""

import signal
import time
from contextlib import contextmanager
from types import FrameType
from typing import Iterator


class BudgetExpired(BaseException):
    """Stop the series without being caught as an ordinary point failure."""


class Budget:
    """Enforce a monotonic deadline across setup, backup, inventory and copies."""

    def __init__(self, hours: float | None) -> None:
        """Remember the optional deadline without installing a signal handler yet."""
        self.deadline = time.monotonic() + hours * 3600 if hours else None

    def remaining(self) -> float | None:
        """Return time left, refusing to begin work after expiry."""
        if self.deadline is None:
            return None
        remaining = self.deadline - time.monotonic()
        if remaining <= 0:
            raise BudgetExpired("The total benchmark time budget has expired")
        return remaining

    def limit(self, seconds: float) -> float:
        """Cap an operation by the remaining total budget."""
        remaining = self.remaining()
        return min(seconds, remaining) if remaining is not None else seconds

    @staticmethod
    def _expired(_signum: int, _frame: FrameType | None) -> None:
        """Interrupt blocking preparation as well as polling loops."""
        raise BudgetExpired("The total benchmark time budget has expired")

    @contextmanager
    def watch(self) -> Iterator[None]:
        """Apply a POSIX timer to work, leaving cleanup outside the timer."""
        remaining = self.remaining()
        if remaining is None:
            yield
            return
        previous = signal.signal(signal.SIGALRM, self._expired)
        signal.setitimer(signal.ITIMER_REAL, remaining)
        try:
            yield
        finally:
            signal.setitimer(signal.ITIMER_REAL, 0)
            signal.signal(signal.SIGALRM, previous)

"""Pluggable execution for query jobs without requiring a message broker.

Standalone TernoDBI has no Celery dependency, so jobs run through an `Executor`
interface. `SynchronousExecutor` runs jobs inline for tests, the CLI, and
single-process deployments. A thread-pool or Celery executor can be added later
without changing dispatch or the tool layer.

The executor receives a job ID and zero-argument callable. It is responsible for
running it, while the job itself handles state transitions, result storage, and
errors.
"""

from __future__ import annotations
from typing import Callable

_executor: "Executor | None" = None


class Executor:
    def submit(self, job_id: str, run: Callable[[], None]) -> None:  # pragma: no cover
        raise NotImplementedError


class SynchronousExecutor(Executor):
    """Run the job inline. `get_query_results` will find it already terminal."""

    def submit(self, job_id: str, run: Callable[[], None]) -> None:
        run()


def get_executor() -> Executor:
    global _executor
    if _executor is None:
        _executor = SynchronousExecutor()
    return _executor


def set_executor(executor: Executor) -> None:
    """Swap the executor — used by tests and by a deployment wiring in Celery."""
    global _executor
    _executor = executor


__all__ = ["Executor", "SynchronousExecutor", "get_executor", "set_executor"]

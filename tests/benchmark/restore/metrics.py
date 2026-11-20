"""Observe restore workers without recording object keys or changing data flow."""

import time
from functools import wraps
from typing import Any, Callable

from botocore.response import StreamingBody
from prometheus_client import Counter, Histogram

from ch_backup.storage.async_pipeline.stages.encryption.decrypt_stage import (
    DecryptStage,
)
from ch_backup.storage.async_pipeline.stages.filesystem.write_files_stage import (
    WriteFilesStage,
)
from ch_backup.storage.engine.s3.s3_engine import S3StorageEngine
from ch_backup.storage.engine.s3.s3_retry import RetryExponential

BUCKETS = (0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 300)


class RestoreMetrics:
    """Count stage time, successful body bytes and outer retry sleeps per process."""

    def __init__(self, registry: Any) -> None:
        """Register bounded metrics in the existing per-process registry."""
        self.wall = Histogram(
            "bench_restore_stage_seconds",
            "Stage wall time; nested stages overlap",
            ("stage", "outcome"),
            buckets=BUCKETS,
            registry=registry,
        )
        self.cpu = Counter(
            "bench_restore_stage_cpu_seconds",
            "Calling-thread CPU time",
            ("stage",),
            registry=registry,
        )
        self.bytes = Counter(
            "bench_restore_body_bytes",
            "Bytes returned by successful body reads",
            registry=registry,
        )
        self.sleeps = Histogram(
            "bench_restore_retry_sleep_seconds",
            "Actual outer retry sleep time",
            buckets=BUCKETS,
            registry=registry,
        )

    def wrap(self, original: Callable, stage: str) -> Callable:
        """Observe a synchronous call while preserving its value and exception."""

        @wraps(original)
        def measured(*args: Any, **kwargs: Any) -> Any:
            """Record one call without inspecting its input or exception message."""
            started, cpu, outcome = time.monotonic(), time.thread_time(), "error"
            try:
                value = original(*args, **kwargs)
                if stage == "body_read":
                    self.bytes.inc(len(value))
                outcome = "success"
                return value
            finally:
                self.wall.labels(stage, outcome).observe(time.monotonic() - started)
                self.cpu.labels(stage).inc(time.thread_time() - cpu)

        return measured

    def wrap_retry_init(self, original: Callable) -> Callable:
        """Observe sleeps of new retry instances without changing their policy."""

        @wraps(original)
        def initialize(instance: Any, *args: Any, **kwargs: Any) -> None:
            """Keep each instance's original sleep function and invocation semantics."""
            original(instance, *args, **kwargs)
            sleep = instance.sleep_function

            @wraps(sleep)
            def measured(*sleep_args: Any, **sleep_kwargs: Any) -> Any:
                """Measure an actual wait, including an interrupted one."""
                started = time.monotonic()
                try:
                    return sleep(*sleep_args, **sleep_kwargs)
                finally:
                    self.sleeps.observe(time.monotonic() - started)

            instance.sleep_function = measured

        return initialize

    def install(self) -> None:
        """Patch only synchronous worker operations; do not time generator creation."""
        for owner, method, stage in (
            (StreamingBody, "read", "body_read"),
            (S3StorageEngine, "download_part", "download_chunk"),
            (DecryptStage, "__call__", "decrypt"),
            (WriteFilesStage, "__call__", "tar_write"),
        ):
            setattr(owner, method, self.wrap(getattr(owner, method), stage))
        setattr(
            RetryExponential,
            "__init__",
            self.wrap_retry_init(RetryExponential.__init__),
        )

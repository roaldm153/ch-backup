"""Record bounded S3 metrics and optional Memray captures in each Python process."""

import atexit
import importlib
import json
import os
import threading
import time
from functools import wraps
from multiprocessing.util import Finalize
from pathlib import Path

import psutil
from botocore.client import BaseClient
from botocore.httpsession import URLLib3Session
from prometheus_client import CollectorRegistry, Counter, Histogram, write_to_textfile

SETTINGS = "CH_BACKUP_BENCH_INSTRUMENTATION"
OPERATIONS = {
    "CopyObject",
    "UploadPartCopy",
    "GetObject",
    "HeadObject",
    "PutObject",
    "ListObjects",
    "ListObjectsV2",
    "DeleteObject",
    "DeleteObjects",
    "CreateMultipartUpload",
    "CompleteMultipartUpload",
    "AbortMultipartUpload",
    "UploadPart",
    "HeadBucket",
}
BUCKETS = (0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 300)


class S3Metrics:
    """Keep fixed histograms without retaining requests, keys or credentials."""

    def __init__(self) -> None:
        """Create a private registry and thread-local operation context."""
        self.registry = CollectorRegistry()
        self.operation = threading.local()
        self.calls = Histogram(
            "bench_s3_call_seconds",
            "Logical S3 calls including retries",
            ("operation", "outcome"),
            buckets=BUCKETS,
            registry=self.registry,
        )
        self.attempts = Histogram(
            "bench_s3_attempt_seconds",
            "HTTP attempts, excluding response body reads",
            ("operation", "outcome"),
            buckets=BUCKETS,
            registry=self.registry,
        )
        self.retries = Counter(
            "bench_s3_retries",
            "Retries reported by completed S3 responses",
            ("operation",),
            registry=self.registry,
        )

    def wrap_call(self, original):
        """Measure logical S3 calls without changing their result or exceptions."""

        @wraps(original)
        def call(client, operation_name, api_params):
            """Observe one S3 operation and restore any enclosing operation."""
            if client.meta.service_model.service_name != "s3":
                return original(client, operation_name, api_params)
            previous = getattr(self.operation, "name", None)
            name = operation_name if operation_name in OPERATIONS else "other"
            self.operation.name = name
            started, outcome, response = time.monotonic(), "error", {}
            try:
                response = original(client, operation_name, api_params)
                outcome = "success"
                return response
            except Exception as error:
                response = getattr(error, "response", {})
                raise
            finally:
                self.calls.labels(name, outcome).observe(time.monotonic() - started)
                self.retries.labels(name).inc(
                    response.get("ResponseMetadata", {}).get("RetryAttempts", 0)
                )
                self.operation.name = previous

        return call

    def wrap_send(self, original):
        """Measure actual HTTP attempts inside a logical S3 call."""

        @wraps(original)
        def send(session, request):
            """Count retries and transport failures without inspecting request data."""
            name = getattr(self.operation, "name", None)
            if name is None:
                return original(session, request)
            started, outcome = time.monotonic(), "transport_error"
            try:
                response = original(session, request)
                category = response.status_code // 100
                outcome = f"{category}xx" if category in (1, 2, 3, 4, 5) else "other"
                return response
            finally:
                self.attempts.labels(name, outcome).observe(time.monotonic() - started)

        return send


class Instrumentation:
    """Own one process's capture, metrics writer and shutdown hooks."""

    def __init__(self, settings: dict) -> None:
        """Start capture before importing worker tasks and write a coverage manifest."""
        self.out = Path(settings["out"])
        self.pid = os.getpid()
        self.stopped = threading.Event()
        self.metrics = S3Metrics()
        self.tracker = None
        if settings.get("memory"):
            memray = importlib.import_module("memray")
            self.tracker = memray.Tracker(
                destination=memray.FileDestination(
                    str(self.out / f"memory-{self.pid}.bin"), compress_on_exit=False
                ),
                native_traces=settings.get("native", False),
                trace_python_allocators=settings.get("python_allocators", False),
                file_format=(
                    memray.FileFormat.AGGREGATED_ALLOCATIONS
                    if settings.get("memory_format") == "aggregated"
                    else memray.FileFormat.ALL_ALLOCATIONS
                ),
            )
            self.tracker.__enter__()
        BaseClient._make_api_call = self.metrics.wrap_call(
            BaseClient._make_api_call
        )  # pylint: disable=protected-access
        URLLib3Session.send = self.metrics.wrap_send(URLLib3Session.send)
        self.manifest = {
            "pid": self.pid,
            "created": psutil.Process().create_time(),
            "memory": self.tracker is not None,
            "memory_format": settings.get("memory_format", "all"),
            "started": time.monotonic(),
            "status": "running",
        }
        self.flush()
        self.writer = threading.Thread(target=self._write_loop, daemon=True)
        self.writer.start()
        self.finalizer = Finalize(None, self.close, exitpriority=20)
        atexit.register(self.close)

    def flush(self) -> None:
        """Replace cumulative metrics and coverage metadata atomically."""
        write_to_textfile(str(self.out / f"s3-{self.pid}.prom"), self.metrics.registry)
        path = self.out / f"instrumentation-{self.pid}.json"
        temporary = path.with_suffix(".tmp")
        temporary.write_text(json.dumps(self.manifest), encoding="utf-8")
        temporary.replace(path)

    def _write_loop(self) -> None:
        """Persist partial metrics without retaining per-request records."""
        while not self.stopped.wait(5):
            try:
                self.flush()
            except OSError as error:
                self.manifest["write_error"] = type(error).__name__

    def close(self) -> None:
        """Flush once and finalize a capture on normal interpreter shutdown."""
        if self.stopped.is_set():
            return
        self.stopped.set()
        self.writer.join(timeout=10)
        if self.tracker is not None:
            self.tracker.__exit__(None, None, None)
        self.manifest.update(status="completed", ended=time.monotonic())
        self.flush()


def start() -> Instrumentation | None:
    """Enable instrumentation only in explicitly marked benchmark processes."""
    settings = os.environ.get(SETTINGS)
    return Instrumentation(json.loads(settings)) if settings else None

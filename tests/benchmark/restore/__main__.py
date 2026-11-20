"""Run one comparison series per checkout, protecting shared staging files."""

import fcntl
from pathlib import Path

from tests.benchmark.restore.driver import main

Path("staging").mkdir(exist_ok=True)
with Path("staging/restore-benchmark.lock").open("a", encoding="utf-8") as lock:
    try:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError as error:
        raise SystemExit(
            "A restore benchmark is already running in this checkout"
        ) from error
    main()

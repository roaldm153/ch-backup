"""
Environment of the benchmark: containers of the integration tests and commands in them.

Commands go through the docker CLI without timeouts of docker-py and of the HTTP client of
ClickHouse. Long commands run detached inside the container, and the driver polls their exit
code, so a long copy survives a broken connection to Docker.
"""

import os
import shlex
import subprocess
import time
from pathlib import Path
from types import SimpleNamespace
from typing import Callable

from ch_backup.backup.metadata import BackupMetadata, BackupState
from tests.integration import env_control
from tests.integration.modules import s3

CLICKHOUSE = "clickhouse01"
TARGET_BUCKET = "ch-backup-copy"
PATH_ROOT = "ch_backup"


class CommandError(RuntimeError):
    """A command in the container failed."""


class Environment:
    """Containers of one version of ClickHouse, with images built once for all points."""

    def __init__(self, version: str, env_id: str, state_file: Path) -> None:
        if Path(env_control.SESSION_STATE_CONF).exists():
            raise RuntimeError(
                f"{env_control.SESSION_STATE_CONF} exists: an environment of the integration "
                "tests shares staging/ with the benchmark, remove it with make clean-test-env"
            )
        os.environ["CLICKHOUSE_VERSION"] = version
        os.environ["INTEGRATION_ENV_ID"] = env_id
        os.environ.setdefault("DEV_MODE", "false")
        self.version = version
        self.context = SimpleNamespace(state_file=str(state_file))

    def create(self) -> None:
        """Render configs and build images."""
        env_control.create(self.context)

    def start(self) -> None:
        """Start containers with empty storages."""
        env_control.start(self.context)

    def stop(self) -> None:
        """Remove containers with their data."""
        env_control.stop(self.context)

    @property
    def conf(self) -> dict:
        """Return the configuration of the environment."""
        return self.context.conf

    def container(self, name: str = CLICKHOUSE) -> str:
        """Return the name of the container."""
        return f"{name}.{self.conf['network_name']}"

    def exec(self, command: str, stdin: str | None = None, check: bool = True) -> str:
        """Run the shell command in the ClickHouse container and return its output."""
        args = ["docker", "exec", *(["-i"] if stdin is not None else [])]
        process = subprocess.run(
            [*args, self.container(), "bash", "-c", command],
            input=stdin,
            capture_output=True,
            text=True,
            check=False,
        )
        if check and process.returncode:
            raise CommandError(
                f"{command!r} exited with {process.returncode}: {process.stderr[-4000:]}"
            )
        return process.stdout

    def query(self, sql: str, settings: dict | None = None) -> str:
        """Run the query with clickhouse-client and return its output."""
        options = [f"--{key}={value}" for key, value in (settings or {}).items()]
        return self.exec(shlex.join(["clickhouse-client", *options, "--query", sql]))

    def put_file(self, path: str, text: str) -> None:
        """Write the file in the container."""
        directory = shlex.quote(os.path.dirname(path))
        self.exec(f"mkdir -p {directory} && cat > {shlex.quote(path)}", stdin=text)

    def restart_clickhouse(self, timeout: float = 600) -> None:
        """Restart ClickHouse server to apply its configs and wait until it answers."""
        self.exec("supervisorctl restart clickhouse")
        deadline = time.monotonic() + timeout
        while self.exec("clickhouse-client --query 'SELECT 1'", check=False) != "1\n":
            if time.monotonic() > deadline:
                raise CommandError("ClickHouse did not start after restart")
            time.sleep(1)

    def start_detached(
        self,
        command: list[str],
        workdir: str,
        env: dict | None = None,
        timeout: float | None = None,
    ) -> None:
        """Start the command in the background, saving its PID, output and exit code to workdir."""
        directory = shlex.quote(workdir)
        prefix = ["env", *(f"{k}={v}" for k, v in (env or {}).items())]
        if timeout is not None:
            prefix = [
                "timeout",
                "--signal=TERM",
                "--kill-after=60s",
                f"{timeout}s",
                *prefix,
            ]
        script = (
            f"mkdir -p {directory} && rm -f {directory}/rc && cd / || exit; "
            f"{shlex.join([*prefix, *command])} > {directory}/output.log 2>&1 & "
            f'pid=$!; echo "$pid" > {directory}/pid; wait "$pid"; echo $? > {directory}/rc'
        )
        subprocess.run(
            ["docker", "exec", "-d", self.container(), "bash", "-c", script],
            check=True,
        )

    def wait_detached(
        self,
        workdir: str,
        on_poll: Callable[[], None] | None = None,
        interval: float = 5,
    ) -> int:
        """Wait for the detached command and return its exit code."""
        rc_path = shlex.quote(f"{workdir}/rc")
        while True:
            code = self.exec(f"cat {rc_path} 2>/dev/null || true").strip()
            if code:
                return int(code)
            if on_poll:
                on_poll()
            time.sleep(interval)

    def signal_detached(self, workdir: str, signal: str = "TERM") -> None:
        """Send the signal to the detached command."""
        directory = shlex.quote(workdir)
        self.exec(
            f"test -f {directory}/rc || kill -{signal} $(cat {directory}/pid) "
            f"|| test -f {directory}/rc"
        )

    def chown(self, path: str) -> None:
        """Give files written in the container to the user of the host."""
        self.exec(f"chown -R {os.getuid()}:{os.getgid()} {shlex.quote(path)}")

    def save_diagnostics(self, destination: Path) -> None:
        """Save retained log files and running FREEZE queries before removing containers."""
        destination.mkdir(parents=True, exist_ok=True)
        commands = {
            "server-logs.tar": ["docker", "cp", f"{self.container()}:/var/log/.", "-"],
            "freeze.jsonl": [
                "docker",
                "exec",
                self.container(),
                "clickhouse-client",
                "--query",
                "SELECT query_id, elapsed, read_rows, read_bytes, memory_usage, ProfileEvents "
                "FROM system.processes WHERE query LIKE 'ALTER TABLE%FREEZE%' FORMAT JSONEachRow",
            ],
            "seaweedfs.log": [
                "docker",
                "logs",
                self.container("seaweedfs01"),
            ],
        }
        for name, command in commands.items():
            with (
                (destination / name).open("wb") as output,
                (destination / f"{name}.errors").open("w", encoding="utf-8") as errors,
            ):
                try:
                    result = subprocess.run(
                        command,
                        stdout=output,
                        stderr=errors,
                        timeout=120,
                        check=False,
                    )
                    if result.returncode:
                        errors.write(
                            f"\nDiagnostics command exited with {result.returncode}\n"
                        )
                except (OSError, subprocess.TimeoutExpired) as error:
                    errors.write(f"\nDiagnostics failed: {error}\n")

    def reset_copy_state(self, path_root: str, backup_name: str) -> None:
        """Return the copy in the target storage to the state "creating", so the next copy resumes it."""
        client = s3.S3Client(self.context, TARGET_BUCKET)
        path = f"{path_root}/{backup_name}/backup_struct.json"
        light_path = f"{path_root}/{backup_name}/backup_light_struct.json"
        backup = BackupMetadata.load_json(client.download_data(path))
        backup.state = BackupState.CREATING
        client.upload_data(backup.dump_json(light=False).encode(), path)
        client.upload_data(backup.dump_json(light=True).encode(), light_path)

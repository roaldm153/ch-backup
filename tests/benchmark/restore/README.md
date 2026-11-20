# Restore branch comparison

Run on a dedicated Linux VM with Docker. The runner creates its own integration
containers and deletes only that environment after retaining logs. Do not run two
series from the same checkout at once. No production endpoints or credentials are used.

Fetch the branches before starting; refs are resolved once to commits and retained
in detached worktrees. Uncommitted changes are not included. The observer stays fixed while the imported `ch_backup`
package comes from the selected commit. Branches use the same installed Python
dependencies; dependency changes require a separately prepared environment.

```sh
git fetch upstream main
git fetch origin
make test-benchmark-restore BENCH_ARGS='--refs upstream/main origin/feat/restore-speed origin/feat/another-change --cpu --repeats 3'
```

Smoke test, including CPU capture:

```sh
make test-benchmark-restore BENCH_ARGS='--refs upstream/main upstream/main --parts 3 --rows 131072 --payload-bytes 4096 --repeats 1 --hours 1 --cpu'
```

Default fixture: 16 local MergeTree inserts, 131072 rows each, 8192 deterministic
payload bytes per row (16 GiB logical payload). Each eight-byte block is hashed
separately to avoid a trivially compressible repeated string. One encrypted backup
is prepared and reused for every branch. This initial scenario measures local
restore; it does not yet compare server-side S3 disk copying or injected failures.

Every run drops only the test database inside the newly created container and uses
a unique restore state file. Row count and payload hashes are verified outside the
measured interval. The order of branches rotates between timing repeats.

`--workers`, `--chunk-mib`, `--parts`, `--rows`, `--payload-bytes` and `--version`
are fixed across branches in one series. `--hours` bounds the whole series.
Source and destination share the VM's disk and network; results are not directly
comparable to production object storage. OS caches are not forcibly cleared.
The proxy resolver is disabled for the isolated object-store endpoint.

Results under `staging/benchmark/restore-*/`:

- `revisions.json`, `scenario.json`, `system.txt`: pinned code and run configuration.
- `comparison.csv`: median/min/max restore seconds for verified unprofiled runs.
- `<version>/<parts>/point.json`: all outcomes, including failures and profiles.
- Per-run `summary.json`, `metrics-*.prom`, stage and process telemetry.
- `cpu-*.speedscope.json`: separate CPU runs, including Python subprocesses.

CPU capture uses consecutive 120-second windows throughout the diagnostic run,
with a cap covering the entire series budget. `--native` adds native stacks.
CPU-profile timings never enter `comparison.csv`. Stage wall-time counters can
overlap; do not sum them into total restore time. Body bytes count successful
reads, not all bytes sent on the wire. Profiling failure fails the series.

Shared environment/collector code comes from the existing copy memory benchmark,
commit `198e692e9454ff435f6771ef6145d2ae78f0020a`.

## Prepared VM

VM `fhm0td9nqhtsfpa850no`, workspace `~/restore-benchmark/src/` contains
`ClickHouse`, `ch-backup`, and `ch-tools`. Build logs are in
`~/restore-benchmark/logs/`; setup runs in tmux session `restore-setup`.
The ClickHouse source build uses 25.8 and 12 compiler jobs for 64 GiB RAM.
The benchmark itself pins the packaged server version through `--version`.
Do not interpret smoke timings collected during a source build as performance results.

The host collector needs root to read container processes and attach py-spy:

```sh
cd ~/restore-benchmark/src/ch-backup
export PATH="$HOME/restore-benchmark/tools/bin:$PATH"
make build
uv sync --group benchmark
git fetch upstream main
git fetch origin
sudo env PATH="$PATH" \
  GIT_CONFIG_COUNT=1 GIT_CONFIG_KEY_0=safe.directory GIT_CONFIG_VALUE_0="$PWD" \
  .venv/bin/python -m tests.benchmark.restore \
  --refs upstream/main origin/feat/restore-speed --cpu --repeats 3
```

A checkout lock prevents overlapping restore series. Each series also gets a UUID
for Docker resources, so equal result-directory names in different checkouts cannot
share a cleanup target. CPU completeness requires sampled host PIDs for every
Python process observed by the collector, and requires every instrumented worker
to be observed. Missing workers fail the diagnostic run.

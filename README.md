# Spark external UDF workers in Docker Sandboxes

This repository is not part of Apache Spark.

`sbx-udf-worker` is a Spark direct-worker launcher. It runs the real worker inside an `sbx` sandbox. Spark needs no sandbox code and no sandbox config. Spark spawns this command like any other worker and appends `--id` and `--connection`. It follows the launcher contract documented on `ProcessCallable` in Spark's `worker_spec.proto`.

```
Spark executor -> host socket -> sbx-udf-worker -> sbx exec -i -> guest socket -> worker
```

A socket bound inside the sandbox belongs to the guest kernel. The host cannot connect to it even when the path is on a shared mount. The launcher binds the host socket and starts one guest bridge for each connection.

A guest supervisor runs the worker in its own process group. The supervisor stops the group when its stdin closes. That happens on SIGTERM and also when Spark sends SIGKILL to the launcher, so the guest worker does not outlive the launcher.

## Use

Sign in once with `sbx login`. Create a sandbox that mounts the worker files:

```
sbx create --name udf shell /path/to/worker-files
```

Put this in the worker spec's `ProcessCallable.command`. Everything after `--` runs inside the sandbox:

```
sbx-udf-worker --sandbox udf -- python3 /path/to/worker-files/worker.py
```

The guest needs `python3` for the supervisor and the bridge.

Options:

- `--sandbox NAME` names the sandbox. Required.
- `--guest-dir DIR` is where guest sockets go. Default `/tmp/sbx-udf-workers`.
- `--ready-timeout SECONDS` bounds the wait for the guest socket. Default 60.
- `--grace-period SECONDS` is the time between SIGTERM and SIGKILL for the guest worker. Default 5. Keep it below Spark's graceful termination timeout.
- `-e NAME`, `--env NAME` forwards a host environment variable to the guest. Repeat it for more names. `ProcessCallable` environment variables reach the launcher, not the guest, unless you forward them.
- `--log FILE` appends launcher events and guest stderr.
- `--sbx PATH` overrides the `sbx` binary.

Exit codes:

- 0 after SIGTERM.
- The guest worker's exit code when the worker exits on its own.
- 124 when the guest socket does not appear before `--ready-timeout`.
- 1 when the sandbox or `sbx` fails before the worker is ready.

## Prove it

```
./prove.sh /path/to/spark
python3 stress/stress.py SANDBOX
```

`prove.sh` runs the Spark query from `PlanExternalUDFsSuite` with the worker in the sandbox. It passes only if the test returns rows `(0,0)` through `(5,5)` and no guest worker remains.

`stress/stress.py` checks these cases:

- 32 concurrent connections with random payloads.
- One 16 MiB payload.
- SIGTERM.
- SIGKILL of the launcher.
- A guest worker crash.
- A missing sandbox.
- A ready timeout.

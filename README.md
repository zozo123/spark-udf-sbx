# Spark external UDF workers in Docker Sandboxes

This repository is not part of Apache Spark.

`sbx-udf-worker` is a Spark direct-worker command. It runs the real worker inside an `sbx` sandbox. Spark needs no sandbox code and no sandbox config. Spark spawns this command like any other worker and appends `--id` and `--connection`.

```
Spark executor -> host socket -> sbx-udf-worker -> sbx exec -i -> guest socket -> worker
```

A guest socket on a shared mount is not connectable from the macOS host, so the launcher binds the host socket and starts a guest bridge for each connection.

## Use

Sign in once with `sbx login`, then create a sandbox that mounts the worker files:

```
sbx create --name udf shell /path/to/worker-files
```

Put this in the worker spec's `ProcessCallable.command`. Everything after `--` runs inside the sandbox:

```
sbx-udf-worker --sandbox udf -- python3 /path/to/worker-files/worker.py
```

Options:

- `--sandbox NAME` names the sandbox. Required.
- `--guest-dir DIR` is where guest sockets and pid files go. Default `/tmp/sbx-udf-workers`.
- `--ready-timeout SECONDS` bounds the wait for the guest socket. Default 60.
- `--log FILE` appends launcher, guest `uname`, and worker stderr.
- `--sbx PATH` overrides the `sbx` binary.

SIGTERM from Spark stops the guest worker and removes both sockets. The guest needs `python3` for the bridge.

## Prove it

```
./prove.sh /path/to/spark
```

The script exports the echo worker classpath, copies it into `work/`, creates the `spark-udf-sbx` sandbox if needed, applies `worker-command-override.patch` for one test run, and restores the suite on exit. It passes only if the test returns rows `(0,0)` through `(5,5)` and no guest worker remains.

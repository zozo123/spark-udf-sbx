#!/usr/bin/env bash
# Runs PlanExternalUDFsSuite's direct gRPC worker query with the worker inside
# a Docker Sandbox. Usage: ./prove.sh [SPARK_DIR] [SANDBOX_NAME]
set -euo pipefail

here="$(cd "$(dirname "$0")" && pwd)"
spark="$(cd "${1:-$here/../spark}" && pwd)"
sandbox="${2:-spark-udf-sbx}"
suite=sql/core/src/test/scala/org/apache/spark/sql/execution/externalUDF/PlanExternalUDFsSuite.scala
work="$here/work"
export JAVA_HOME="${JAVA_HOME:-$HOME/.sdkman/candidates/java/17.0.20-tem}"
export SPARK_LOCAL_IP=127.0.0.1

sbx ls >/dev/null

cd "$spark"
git diff --quiet -- "$suite" || { echo "$suite has local changes" >&2; exit 1; }
./build/sbt -batch 'export udf-worker-grpc/Test/fullClasspath' | tail -1 > "$here/worker-cp.txt"

python3 - "$here/worker-cp.txt" "$work" <<'PY'
import os, shutil, sys
cp, work = open(sys.argv[1]).read().strip().split(":"), sys.argv[2]
shutil.rmtree(work, ignore_errors=True)
os.makedirs(f"{work}/lib")
out = []
for i, p in enumerate(cp):
    dest = f"{work}/lib/{i:03d}-{os.path.basename(p.rstrip('/'))}"
    (shutil.copytree if os.path.isdir(p) else shutil.copy2)(p, dest)
    out.append(dest)
open(f"{work}/classpath", "w").write(":".join(out))
PY

sbx ls | awk 'NR > 1 { print $1 }' | grep -qx "$sandbox" || sbx create --name "$sandbox" shell "$here"

rm -f "$work/launcher.log"
{
  printf '%s\n' "$here/sbx-udf-worker" --sandbox "$sandbox" --log "$work/launcher.log" -- \
    java -cp "$(cat "$work/classpath")" org.apache.spark.udf.worker.grpc.testing.EchoGrpcWorkerMain
} > "$work/command.txt"

git apply "$here/worker-command-override.patch"
trap 'git -C "$spark" checkout -- "$suite"' EXIT
SBX_PROOF_COMMAND_FILE="$work/command.txt" ./build/sbt -batch \
  'sql/testOnly org.apache.spark.sql.execution.externalUDF.PlanExternalUDFsSuite -- -z "direct gRPC worker process"' \
  | tee "$work/test.log" | grep -E 'direct gRPC worker|Tests: succeeded|FAILED'

grep -E 'sbx-udf-worker|echo-worker|Linux' "$work/launcher.log"
test "$(sbx exec "$sandbox" sh -c 'ps -eo args | grep -c "[E]choGrpcWorkerMain"')" = 0
echo "guest workers left: 0"

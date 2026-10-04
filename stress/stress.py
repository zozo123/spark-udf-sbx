"""Stress sbx-udf-worker the way a Spark executor uses it.

Usage: python3 stress/stress.py SANDBOX
"""

import os
import random
import signal
import socket
import subprocess
import sys
import tempfile
import threading
import time

here = os.path.dirname(os.path.abspath(__file__))
launcher = os.path.join(os.path.dirname(here), "sbx-udf-worker")
sandbox = sys.argv[1]
worker = os.path.join(here, "echo_worker.py")
failures = []


def check(ok, message):
    print(("PASS " if ok else "FAIL ") + message, flush=True)
    if not ok:
        failures.append(message)


def guest_workers(worker_id):
    out = subprocess.run(
        ["sbx", "exec", sandbox, "sh", "-c",
         f'ps -eo args | grep -c "[e]cho_worker.py --id {worker_id}"'],
        capture_output=True, text=True)
    return int(out.stdout.strip() or "0")


def start(worker_id, extra=()):
    sock_dir = tempfile.mkdtemp(prefix="sbx-stress-")
    os.chmod(sock_dir, 0o700)
    host = os.path.join(sock_dir, "w.sock")
    proc = subprocess.Popen(
        [launcher, "--sandbox", sandbox, *extra, "--", "python3", "-u", worker,
         "--id", worker_id, "--connection", host],
        stderr=subprocess.PIPE, text=True)
    deadline = time.monotonic() + 60
    while not os.path.exists(host):
        if proc.poll() is not None or time.monotonic() > deadline:
            return proc, host, False
        time.sleep(0.05)
    return proc, host, True


def roundtrip(host, payload):
    c = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    c.settimeout(30)
    c.connect(host)
    sender = threading.Thread(target=c.sendall, args=(payload,))
    sender.start()
    got = bytearray()
    while len(got) < len(payload):
        chunk = c.recv(1 << 20)
        if not chunk:
            break
        got += chunk
    sender.join()
    c.close()
    return bytes(got) == payload


def wait_exit(proc, timeout):
    try:
        return proc.wait(timeout=timeout)
    except subprocess.TimeoutExpired:
        return None


def scenario_parallel():
    proc, host, ready = start("parallel")
    check(ready, "parallel: host socket appears")
    results = [None] * 32

    def one(i):
        results[i] = roundtrip(host, os.urandom(random.randint(1, 256 * 1024)))

    t0 = time.monotonic()
    threads = [threading.Thread(target=one, args=(i,)) for i in range(32)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    check(all(results), f"parallel: 32 concurrent connections echo random payloads "
                        f"in {time.monotonic() - t0:.2f}s")
    check(roundtrip(host, os.urandom(16 << 20)), "parallel: one 16 MiB payload echoes intact")
    proc.send_signal(signal.SIGTERM)
    code = wait_exit(proc, 20)
    check(code == 0, f"parallel: SIGTERM exits 0 (got {code})")
    check(not os.path.exists(host), "parallel: SIGTERM removes the host socket")
    check(guest_workers("parallel") == 0, "parallel: SIGTERM stops the guest worker")


def scenario_sigkill():
    proc, host, ready = start("sigkill")
    check(ready, "sigkill: host socket appears")
    check(roundtrip(host, b"before kill"), "sigkill: echo works before SIGKILL")
    proc.kill()
    proc.wait()
    deadline = time.monotonic() + 20
    left = guest_workers("sigkill")
    while left and time.monotonic() < deadline:
        time.sleep(0.5)
        left = guest_workers("sigkill")
    check(left == 0, f"sigkill: guest worker is gone after the launcher is SIGKILLed (left {left})")


def scenario_guest_crash():
    proc, host, ready = start("crash")
    check(ready, "crash: host socket appears")
    subprocess.run(["sbx", "exec", sandbox, "pkill", "-KILL", "-f",
                    "echo_worker.py --id crash"], capture_output=True)
    code = wait_exit(proc, 20)
    check(code not in (None, 0), f"crash: launcher exits non-zero when the guest dies (got {code})")


def scenario_bad_sandbox():
    sock_dir = tempfile.mkdtemp(prefix="sbx-stress-")
    host = os.path.join(sock_dir, "w.sock")
    bad = subprocess.Popen(
        [launcher, "--sandbox", "no-such-sandbox-xyz", "--ready-timeout", "15", "--",
         "python3", worker, "--id", "bad", "--connection", host],
        stderr=subprocess.PIPE, text=True)
    code = wait_exit(bad, 40)
    check(code not in (None, 0), f"bad sandbox: launcher exits non-zero (got {code})")
    check(not os.path.exists(host), "bad sandbox: no host socket is left")


def scenario_ready_timeout():
    sock_dir = tempfile.mkdtemp(prefix="sbx-stress-")
    host = os.path.join(sock_dir, "w.sock")
    slow = subprocess.Popen(
        [launcher, "--sandbox", sandbox, "--ready-timeout", "3", "--",
         "sh", "-c", "sleep 30", "--id", "slow", "--connection", host],
        stderr=subprocess.PIPE, text=True)
    code = wait_exit(slow, 40)
    check(code not in (None, 0), f"ready timeout: launcher exits non-zero (got {code})")
    left = subprocess.run(["sbx", "exec", sandbox, "sh", "-c", 'ps -eo args | grep -c "[s]leep 30"'],
                          capture_output=True, text=True).stdout.strip()
    check(left == "0", f"ready timeout: the guest command is stopped (left {left})")


for scenario in (scenario_parallel, scenario_sigkill, scenario_guest_crash,
                 scenario_bad_sandbox, scenario_ready_timeout):
    scenario()

print(f"{len(failures)} failure(s)")
sys.exit(1 if failures else 0)

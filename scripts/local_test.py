#!/usr/bin/env python3
"""Run a streaming OnionPIR experiment on localhost.

Starts the unmodified Zealot/shuffle database (cmd/testserver), the OnionPIR
streaming server, and C simulated clients as separate processes, using the
config's ports on 127.0.0.1. The counterpart of shuffle's
cmd/zealot_local_test.py.

  python3 scripts/local_test.py --config ../shuffle/testFiles/zealot_three_proxy_test_config.json
  python3 scripts/local_test.py --config ../shuffle/testFiles/fromPaper/row0.json --N 100000 --C 8
"""
import argparse
import json
import os
import signal
import subprocess
import sys
import tempfile
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import experiment as ex  # noqa: E402

BUILD_DIR = os.path.join(ex.REPO_ROOT, "build")


def build(shuffle_dir, jobs):
    cmake = ["cmake", "-DCMAKE_BUILD_TYPE=Benchmark", f"-DACTIVE_CONFIG={ex.ACTIVE_CONFIG}",
             f"-DSHUFFLE_DIR={shuffle_dir}"]
    if os.environ.get("HEXL_DIR"):
        cmake.append(f"-DHEXL_DIR={os.environ['HEXL_DIR']}")
    os.makedirs(BUILD_DIR, exist_ok=True)
    subprocess.run(cmake + [ex.REPO_ROOT], cwd=BUILD_DIR, check=True, stdout=subprocess.DEVNULL)
    subprocess.run(["make", f"-j{jobs}", "onion_server", "onion_client"], cwd=BUILD_DIR, check=True)
    subprocess.run(["go", "build", "-o", os.path.join(BUILD_DIR, "shuffle_db_server"), "./cmd/testserver"],
                   cwd=shuffle_dir, check=True)


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--config", default=os.path.join(ex.DEFAULT_SHUFFLE, "testFiles",
                                                     "zealot_three_proxy_test_config.json"),
                    help="shuffle-format JSON config (default: shuffle's zealot_three_proxy_test_config.json)")
    ap.add_argument("--N", type=int, help="override database records")
    ap.add_argument("--C", type=int, help="override concurrent clients")
    ap.add_argument("--B", type=int, help="override RecordBytes")
    ap.add_argument("--shuffle-dir", default=ex.DEFAULT_SHUFFLE, help="Zealot/shuffle checkout")
    ap.add_argument("--threads", type=int, default=0, help="server worker threads (0 = all cores)")
    ap.add_argument("--client-threads", type=int, default=0, help="client worker threads (0 = all cores)")
    ap.add_argument("--chunk-rows", type=int, default=64, help="first-dim rows per streamed chunk")
    ap.add_argument("--db-batch", type=int, default=4096, help="DBRecordRange.max_records hint")
    ap.add_argument("--connections", type=int, default=16, help="client TCP connections")
    ap.add_argument("--timeout", type=float, default=1800, help="seconds to wait for the clients")
    ap.add_argument("--demo-index", type=int, help="client 0 queries this index and prints its record")
    ap.add_argument("--no-build", action="store_true")
    ap.add_argument("-j", "--jobs", type=int, default=os.cpu_count())
    args = ap.parse_args()

    shuffle_dir = os.path.abspath(args.shuffle_dir)
    config = ex.load_config(ex.resolve(args.config, os.getcwd()), args.N, args.C, args.B)
    if not args.no_build:
        build(shuffle_dir, args.jobs)

    # localhost may resolve to ::1 first; the Go database binds IPv4.
    db_addr = f"127.0.0.1:{ex.database_port(config)}"
    onion_addr = f"127.0.0.1:{ex.onion_port(config)}"
    local = dict(config, ServerAddr=db_addr,
                 ClientAddr=f"127.0.0.1:{ex.port_of(config['ClientAddr'], 10003)}",
                 ProxyAddresses=[f"127.0.0.1:{ex.port_of(a, 10002 + i)}"
                                 for i, a in enumerate(config["ProxyAddresses"])])
    tmp = tempfile.NamedTemporaryFile("w", suffix=".json", delete=False)
    json.dump(local, tmp)
    tmp.close()

    print("==> OnionPIR streaming experiment (local)")
    print(f"  {ex.describe(config)}")
    print(f"  database {db_addr}  OnionPIR server {onion_addr}")
    procs, logs = [], {}
    try:
        for name in ("database", "server"):
            logs[name] = tempfile.TemporaryFile("w+")
        procs.append(subprocess.Popen(
            [os.path.join(BUILD_DIR, "shuffle_db_server"), "--config", tmp.name],
            cwd=shuffle_dir, stdout=logs["database"], stderr=subprocess.STDOUT))
        server = subprocess.Popen(
            [os.path.join(BUILD_DIR, "onion_server")] +
            ex.server_flags(config, db_addr, onion_addr, args.threads, args.chunk_rows, args.db_batch),
            stdout=logs["server"], stderr=subprocess.STDOUT)
        procs.append(server)
        started = time.time()
        try:
            output = subprocess.check_output(
                [os.path.join(BUILD_DIR, "onion_client")] +
                ex.client_flags(config, onion_addr, args.connections, args.client_threads,
                                int(args.timeout), args.demo_index),
                text=True, stderr=subprocess.STDOUT, timeout=args.timeout)
            failed = False
        except subprocess.CalledProcessError as exc:
            output, failed = exc.output or "", True
        except subprocess.TimeoutExpired as exc:
            partial = exc.output.decode() if isinstance(exc.output, bytes) else (exc.output or "")
            output, failed = partial + "\nTIMEOUT\n", True
        try:
            server.wait(30)
        except subprocess.TimeoutExpired:
            pass
        logs["server"].seek(0)
        server_log = logs["server"].read()
        logs["database"].seek(0)
        db_log = logs["database"].read()

        print(f"\n================ TEST RESULT ({time.time() - started:.1f}s) ================")
        print(output[output.find("=== Performance Metrics ==="):] if "=== Performance Metrics ===" in output
              else output, end="")
        print("\n=== Server/Database Timing Breakdown ===")
        print(ex.section(server_log, "=== OnionPIR streaming server timing ===") or server_log[-3000:], end="")
        print("--- database ---")
        print(ex.database_timing(db_log), end="")
        if failed:
            print("FAIL")
            sys.exit(1)
        print("PASS")
    finally:
        for p in procs:
            if p.poll() is None:
                p.send_signal(signal.SIGINT)
        for p in procs:
            try:
                p.wait(3)
            except subprocess.TimeoutExpired:
                p.kill()
        os.unlink(tmp.name)


if __name__ == "__main__":
    main()

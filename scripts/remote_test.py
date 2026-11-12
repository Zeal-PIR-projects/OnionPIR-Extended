#!/usr/bin/env python3
"""Run a streaming OnionPIR experiment across AWS instances through SSM.

The counterpart of shuffle's cmd/zealot_remote_test.py, for direct
comparison: same AWS config files, same experiment configs, same unmodified
database service.

Topology (from the AWS config, shuffle's format):
  serverInstance        the uncooperative database (shuffle cmd/testserver)
  proxyInstances        one OnionPIR server per proxy, like Zealot's proxies.
                        Server i listens on ProxyAddresses[i]'s port and serves
                        the i-th contiguous chunk of the C clients. The last
                        one is the database streamer: it alone reads the
                        database, and each server relays the records to the
                        one before it.
  onionServerInstance   optional: a single OnionPIR server instead (it takes
                        the last ProxyAddresses port, as before)
  clientInstance        the C simulated clients; defaults to the database
                        streamer's host when absent
Optional deployment knobs (AWS config, like zealotXORWorkers):
  onionThreads (0 = all cores), onionChunkRows (64), onionDbBatch (4096),
  onionConnections (16, per server), hexlPrefix (/opt/hexl)

  python3 scripts/remote_test.py --aws-config ../shuffle/awsConfigs/c8i16xlarge.json \\
      --config ../shuffle/testFiles/fromPaper/row1.json --C 100
"""
import argparse
import base64
import json
import os
import shlex
import sys
import threading
import time

try:
    import boto3
    from botocore.exceptions import ClientError
except ImportError:
    sys.exit("boto3 required: pip install boto3")

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import experiment as ex  # noqa: E402

REMOTE_CONFIG = "/tmp/onionpir_config.json"
PREFIX = "/tmp/onionpir"  # binaries /tmp/onionpir_{db,server,client}, logs *.log
REMOTE_BUILD = "/tmp/onionpir-build"
# SSM starts a non-login root shell; share the Go caches the Zealot driver uses.
GO_ENV = ("export PATH=/usr/local/go/bin:$PATH:$HOME/go/bin && "
          "export GOPATH=/tmp/gopath && export GOMODCACHE=/tmp/gomodcache && "
          "export HOME=/root")


def ssm_run(ssm, instance_id, commands, timeout=600, comment=""):
    """Run commands via SSM and return (status, stdout, stderr)."""
    command_id = ssm.send_command(
        InstanceIds=[instance_id], DocumentName="AWS-RunShellScript",
        Parameters={"commands": commands, "executionTimeout": [str(timeout)]},
        Comment=comment[:100],
    )["Command"]["CommandId"]
    deadline = time.time() + timeout + 30
    while time.time() < deadline:
        time.sleep(2)
        try:
            result = ssm.get_command_invocation(CommandId=command_id, InstanceId=instance_id)
        except ClientError as exc:
            if exc.response["Error"]["Code"] == "InvocationDoesNotExist":
                continue
            raise
        if result["Status"] not in ("Pending", "InProgress", "Delayed"):
            return result["Status"], result.get("StandardOutputContent", ""), result.get("StandardErrorContent", "")
    return "TimedOut", "", ""


def require(label, result, expected=None):
    status, stdout, stderr = result
    if status == "Success" and (expected is None or expected in stdout):
        return stdout
    raise RuntimeError(f"{label} failed (SSM status={status}):\n"
                       f"{(stderr.strip() + chr(10) + stdout.strip()).strip() or 'no output'}")


def run_parallel(jobs):
    """jobs: {label: callable}; returns {label: result}."""
    results = {}
    def run(label, fn):
        results[label] = fn()
    threads = [threading.Thread(target=run, args=item) for item in jobs.items()]
    for thread in threads: thread.start()
    for thread in threads: thread.join()
    return results


def private_ip(ec2, instance_id):
    response = ec2.describe_instances(InstanceIds=[instance_id])
    return response["Reservations"][0]["Instances"][0]["PrivateIpAddress"]


def as_ec2_user(command):
    return f"su -s /bin/bash ec2-user -c {shlex.quote(command)}"


def proto_command(shuffle):
    """proto_gen is git-ignored in shuffle; regenerate it as the checkout owner."""
    return as_ec2_user(
        f"export PATH=/usr/local/go/bin:$PATH:$HOME/go/bin && cd {shlex.quote(shuffle)} && "
        "python3 proto.py && test -f proto_gen/pir_grpc.pb.go && echo PROTO_OK")


def build_onion_command(repo, shuffle, hexl_prefix):
    q = shlex.quote
    return (f"set -e; {GO_ENV}; "
            f"HEXL_DIR=$(HEXL_PREFIX={q(hexl_prefix)} bash {q(repo)}/scripts/ec2_setup.sh | sed -n 's/^HEXL_DIR=//p'); "
            f"[ -n \"$HEXL_DIR\" ] || {{ echo 'HEXL setup failed'; exit 1; }}; "
            f"mkdir -p {REMOTE_BUILD} && cd {REMOTE_BUILD} && "
            f"cmake {q(repo)} -DCMAKE_BUILD_TYPE=Benchmark -DACTIVE_CONFIG={ex.ACTIVE_CONFIG} "
            f"-DSHUFFLE_DIR={q(shuffle)} -DHEXL_DIR=$HEXL_DIR -DGO_EXECUTABLE=/usr/local/go/bin/go > cmake.log && "
            f"make -j$(nproc) onion_server onion_client > make.log 2>&1 || (tail -n 40 make.log; exit 1); "
            f"cp onion_server {PREFIX}_server && cp onion_client {PREFIX}_client && "
            f"grep -q avx512f /proc/cpuinfo && echo CPU:avx512 || echo CPU:no-avx512; echo BUILD_OK")


def resolve_hosts(aws):
    """Return {label: instance_id}: database, server0..server{P-1} (in chain
    order; the last streams the database) and, if separate, client."""
    servers = [aws["onionServerInstance"]] if aws.get("onionServerInstance") \
        else list(aws.get("proxyInstances") or [])
    if not servers:
        raise ValueError("AWS config needs proxyInstances (or onionServerInstance)")
    if len(set(servers)) != len(servers):
        raise ValueError("each OnionPIR server needs its own instance")
    hosts = {"database": aws["serverInstance"]}
    hosts.update({f"server{i}": iid for i, iid in enumerate(servers)})
    client_id = aws.get("clientInstance") or servers[-1]
    if client_id not in servers:
        hosts["client"] = client_id
    return hosts


def server_ids(hosts):
    return [iid for label, iid in hosts.items() if label.startswith("server")]


def build_all(ssm, hosts, remote_repo, remote_shuffle, hexl_prefix):
    """Regenerate shuffle protobuf code, then build the database (shuffle
    testserver) and the OnionPIR server/client binaries, all in parallel."""
    q = shlex.quote
    print("==> Regenerating shuffle protobuf code...")
    results = run_parallel({label: (lambda iid=iid: ssm_run(ssm, iid, [proto_command(remote_shuffle)],
                                                           timeout=180, comment="onionpir-proto"))
                            for label, iid in hosts.items()})
    for label, result in results.items():
        require(f"proto regeneration on {label}", result, "PROTO_OK")

    print("==> Building (database: shuffle testserver; server/client: OnionPIR + HEXL)...")
    jobs = {"database": lambda: ssm_run(ssm, hosts["database"], [
        f"{GO_ENV} && cd {q(remote_shuffle)} && go build -buildvcs=false -o {PREFIX}_db ./cmd/testserver "
        f"&& echo BUILD_OK"], timeout=900, comment="onionpir-build-db")}
    for label, iid in hosts.items():
        if label != "database":
            jobs[label] = (lambda iid=iid: ssm_run(ssm, iid, [build_onion_command(
                remote_repo, remote_shuffle, hexl_prefix)], timeout=3600, comment="onionpir-build"))
    results = run_parallel(jobs)
    for label, result in results.items():
        stdout = require(f"build {label}", result, "BUILD_OK")
        if "CPU:no-avx512" in stdout:
            print(f"  WARNING: {label} host has no AVX-512; the first-dim product uses the scalar path")
    print("  build OK")


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--aws-config", required=True, help="shuffle-format AWS config (instance ids, region)")
    ap.add_argument("--config", default=os.path.join(ex.DEFAULT_SHUFFLE, "testFiles", "fromPaper", "row1.json"),
                    help="shuffle-format experiment config (N, C, RecordBytes, ports)")
    ap.add_argument("--N", type=int, help="override database records")
    ap.add_argument("--C", type=int, help="override concurrent clients")
    ap.add_argument("--B", type=int, help="override RecordBytes")
    ap.add_argument("--remote-repo", default="/home/ec2-user/OnionPIR-Extended")
    ap.add_argument("--remote-shuffle", default="/home/ec2-user/shuffle")
    ap.add_argument("--region")
    ap.add_argument("--timeout", type=int, default=3600, help="seconds to wait for the clients")
    ap.add_argument("--git-pull", action="store_true", help="git pull --ff-only both checkouts first")
    ap.add_argument("--demo-index", type=int, help="client 0 queries this index and prints its record")
    ap.add_argument("--output", help="also write the report to this file")
    args = ap.parse_args()

    with open(ex.resolve(args.aws_config, os.getcwd())) as file:
        aws = json.load(file)
    config = ex.load_config(ex.resolve(args.config, os.getcwd()), args.N, args.C, args.B)

    hosts = resolve_hosts(aws)
    db_id, onion_ids = hosts["database"], server_ids(hosts)
    client_id = hosts.get("client", onion_ids[-1])
    counts = ex.split_clients(config["C"], len(onion_ids))
    knobs = {"threads": aws.get("onionThreads", 0), "chunk_rows": aws.get("onionChunkRows", 64),
             "db_batch": aws.get("onionDbBatch", 4096), "connections": aws.get("onionConnections", 16)}
    for key, value in knobs.items():
        if not isinstance(value, int) or value < 0:
            raise ValueError(f"AWS config onion knob {key} must be a non-negative integer")
    hexl_prefix = aws.get("hexlPrefix", "/opt/hexl")

    session = boto3.Session(region_name=args.region or aws.get("region"))
    ec2, ssm = session.client("ec2"), session.client("ssm")
    db_addr = f"{private_ip(ec2, db_id)}:{ex.database_port(config)}"
    ports = ex.onion_ports(config, len(onion_ids))
    onion_addrs = [f"{private_ip(ec2, iid)}:{port}" for iid, port in zip(onion_ids, ports)]
    # The database reads only N, RecordBytes and ServerAddr, but parses the
    # whole shuffle config; keep it a valid one with the real address.
    db_config = dict(config, ServerAddr=f"0.0.0.0:{ex.database_port(config)}")

    report = []
    def out(text=""):
        print(text)
        report.append(text)

    print("==> OnionPIR streaming experiment (AWS)")
    print(f"  {ex.describe(config)}")
    print(f"  database {db_id} {db_addr}")
    for i, (iid, addr) in enumerate(zip(onion_ids, onion_addrs)):
        role = "  (database streamer)" if i == len(onion_ids) - 1 else ""
        print(f"  OnionPIR server {i} {iid} {addr}: {counts[i]} clients{role}")
    print(f"  clients {client_id}" + (" (co-located with the database streamer)"
                                      if client_id == onion_ids[-1] else ""))
    print(f"  knobs {knobs}")
    q = shlex.quote
    encoded = base64.b64encode(json.dumps(db_config, indent=2).encode()).decode()
    try:
        print("==> Preparing instances...")
        run_parallel({label: (lambda iid=iid: require("prepare", ssm_run(ssm, iid, [
            f"pkill -f {PREFIX}_ 2>/dev/null || true",
            f"printf %s {q(encoded)} | base64 -d > {REMOTE_CONFIG}"], comment="onionpir-prepare")))
            for label, iid in hosts.items()})

        if args.git_pull:
            print("==> Pulling checkouts...")
            for label, iid in hosts.items():
                repos = [args.remote_shuffle] + ([args.remote_repo] if label != "database" else [])
                for repo in repos:
                    require(f"git pull {label} {repo}", ssm_run(ssm, iid, [as_ec2_user(
                        f"cd {q(repo)} && git pull --ff-only")], comment="onionpir-pull"))

        build_all(ssm, hosts, args.remote_repo, args.remote_shuffle, hexl_prefix)

        print("==> Starting database...")
        require("start database", ssm_run(ssm, db_id, [
            f"cd {q(args.remote_shuffle)} || exit 1; "
            f"nohup {PREFIX}_db --config {REMOTE_CONFIG} > {PREFIX}_db.log 2>&1 &"], comment="onionpir-start-db"))
        time.sleep(2)

        print(f"==> Starting {len(onion_ids)} OnionPIR server(s)...")
        all_flags = ex.all_server_flags(
            config, [f"0.0.0.0:{port}" for port in ports], onion_addrs, db_addr,
            threads=knobs["threads"], chunk_rows=knobs["chunk_rows"], db_batch=knobs["db_batch"])
        for i, (iid, flags) in enumerate(zip(onion_ids, all_flags)):
            server_cmd = " ".join(q(a) for a in [f"{PREFIX}_server"] + flags)
            require(f"start server {i}", ssm_run(ssm, iid, [
                f"nohup {server_cmd} > {PREFIX}_server.log 2>&1 &"], comment="onionpir-start-server"))

        def wait_listening(i, iid):
            deadline = time.time() + 300
            while True:
                _, status, _ = ssm_run(ssm, iid, [
                    f"if grep -q 'listening on' {PREFIX}_server.log; then echo READY; "
                    f"elif ! pgrep -f '{PREFIX}_server ' >/dev/null; then echo EXITED; "
                    f"tail -n 30 {PREFIX}_server.log; fi"], timeout=60, comment="onionpir-check-server")
                if status.startswith("READY"):
                    return None
                if status.startswith("EXITED") or time.time() > deadline:
                    return f"OnionPIR server {i} ({iid}) did not start:\n{status}"
                time.sleep(3)
        errors = run_parallel({i: (lambda i=i, iid=iid: wait_listening(i, iid))
                               for i, iid in enumerate(onion_ids)})
        failures = [errors[i] for i in sorted(errors) if errors[i]]
        if failures:
            raise RuntimeError("\n".join(failures))

        print("==> Running clients...")
        client_cmd = " ".join(q(a) for a in [f"{PREFIX}_client"] + ex.client_flags(
            config, onion_addrs, knobs["connections"], 0, args.timeout, args.demo_index))
        started = time.time()
        status, stdout, _ = ssm_run(ssm, client_id, [
            f"{client_cmd} > {PREFIX}_client.log 2>&1; echo EXIT:$?"],
            timeout=args.timeout + 120, comment="onionpir-run-client")
        elapsed = time.time() - started
        time.sleep(1)  # server prints its report after the last reply
        sources = [("client", client_id, "client"), ("database", db_id, "db")] + \
            [(f"server{i}", iid, "server") for i, iid in enumerate(onion_ids)]
        logs = run_parallel({label: (lambda iid=iid, name=name: ssm_run(
            ssm, iid, [f"cat {PREFIX}_{name}.log 2>/dev/null || true"],
            comment=f"onionpir-log-{name}")[1]) for label, iid, name in sources})
        client_ok = status == "Success" and "EXIT:0" in stdout

        out("\n================ TEST RESULT ================")
        out("\n--- Experiment Parameters ---")
        out(f"  N={config['N']}  C={config['C']}  RecordBytes={config['RecordBytes']}  "
            f"(database {config['N'] * config['RecordBytes'] / 1e6:.1f} MB)")
        out(f"  ServerAddr(database)={db_addr}  OnionPIR servers={','.join(onion_addrs)} "
            f"(last streams the database)  clients per server={counts}  "
            f"clients on {'database streamer host' if client_id == onion_ids[-1] else client_id}")
        out(f"  onionThreads={knobs['threads']}  onionChunkRows={knobs['chunk_rows']}  "
            f"onionDbBatch={knobs['db_batch']}  onionConnections={knobs['connections']}")
        out(f"  client process wall time: {elapsed:.1f}s")
        client_log = logs["client"]
        at = client_log.find("=== Performance Metrics ===")
        out()
        out((client_log[at:] if at >= 0 else client_log).rstrip())
        out("\n=== Server/Database Timing Breakdown ===")
        for i in range(len(onion_ids)):
            if len(onion_ids) > 1:
                out(f"--- OnionPIR server {i} ---")
            out(ex.section(logs[f"server{i}"], "=== OnionPIR streaming server timing ===").rstrip()
                or "  server timing report not yet written")
        out("--- database ---")
        out(ex.database_timing(logs["database"]).rstrip())
        out("PASS" if client_ok else "FAIL")
        if args.output:
            with open(args.output, "w") as file:
                file.write("\n".join(report) + "\n")
            print(f"Report written to {args.output}")
        if not client_ok:
            sys.exit(1)
    finally:
        print("==> Cleaning up OnionPIR processes...")
        for iid in set(hosts.values()):
            ssm_run(ssm, iid, [f"pkill -f {PREFIX}_ 2>/dev/null || true"], comment="onionpir-cleanup")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        sys.exit(1)

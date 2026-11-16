#!/usr/bin/env python3
"""Run an OnionPIR sweep over database sizes, client counts or record sizes on
AWS; log everything.

One scripts/remote_test.py run per point, in order, with the clients split across
the AWS config's proxyInstances (one OnionPIR server each; the last streams
the database). A failed run is recorded and the sweep continues. Three sweeps:

  vary N  (default): C=1000, 1 KiB records, configs testFiles/C1000_B1024/N<N>.json,
          N = 1e4 .. 1e7 (--sizes)
  vary C  (--clients): N=1e6, 1 KiB records (testFiles/C1000_B1024/N1000000.json,
          or --config), C overridden per run. With fewer clients than proxies
          (C=1 on 3 proxies) one OnionPIR server runs, on the database streamer.
  vary B  (--record-sizes): N=1e6, C=1000 (same config), RecordBytes overridden
          per run. B must fit one plaintext (3072 B): larger records would
          span plaintexts, which the streaming server does not support.

  python3 scripts/sweep.py --aws-config ../shuffle/awsConfigs/c8i16xlarge.json
  python3 scripts/sweep.py --aws-config ... --sizes 10000 100000
  python3 scripts/sweep.py --aws-config ... --clients 1 10 100 1000 10000
  python3 scripts/sweep.py --aws-config ... --record-sizes 4 32 256 1024

Output, under outputs/onion_<sweep>-<timestamp>/ (or --out; outputs/ is git-ignored),
one directory per run (N<N>, C<C> or B<B>):
  <run>/driver.log   full remote_test.py output
  <run>/report.txt    remote_test.py's report (metrics + timing breakdown)
  <run>/client.log, server<i>.log, database.log   every host's full log
                     (PIR parameters, all server timing, database stream timing)
  <run>/config.json, hosts.json   the experiment and the instances it ran on
  <run>/metrics.json  the client metrics and every server timing line, parsed
  summary.csv, summary.md   one row per run

The client metrics are onion_client's own, over all C clients: query-to-result
latency (average and p99) and the bytes each client uploads (keys + query)
and downloads (result), averaged over clients.
"""
import argparse
import csv
import json
import os
import re
import subprocess
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import experiment as ex  # noqa: E402

DEFAULT_SIZES = [10_000, 100_000, 1_000_000, 10_000_000]
DEFAULT_CONFIG_DIR = os.path.join(ex.REPO_ROOT, "testFiles", "C1000_B1024")
PLAINTEXT_BYTES = 3072  # stream::plaintext_bytes(): 12 bits x 2048 coefficients


def read(path):
    try:
        with open(path) as file:
            return file.read()
    except FileNotFoundError:
        return ""


def to_ms(value, unit):
    return float(value) * (1000 if unit == "s" else 1)


CLIENT_METRICS = {  # metrics.json key: onion_client line
    "e2e_avg_ms": r"Query-to-result latency\s+\(avg\): ([\d.]+)(ms|s)",
    "e2e_p99_ms": r"Query-to-result latency\s+\(p99\): ([\d.]+)(ms|s)",
    "key_upload_B_per_client": r"Offline key upload avg: (\d+) B",
    "query_upload_B_per_client": r"Query upload avg: (\d+) B",
    "download_B_per_client": r"Result download avg: (\d+) B",
}


def client_stats(log):
    stats = {}
    for key, pattern in CLIENT_METRICS.items():
        match = re.search(pattern, log)
        if match:
            stats[key] = to_ms(*match.groups()) if key.endswith("_ms") else int(match.group(1))
    if "key_upload_B_per_client" in stats and "query_upload_B_per_client" in stats:
        stats["upload_B_per_client"] = stats["key_upload_B_per_client"] + stats["query_upload_B_per_client"]
    match = re.search(r"^Clients: (\d+)", log, re.M)
    stats["clients"] = int(match.group(1)) if match else None
    match = re.search(r"^(CORRECTNESS (?:PASS|FAIL).*)$", log, re.M)
    stats["correctness"] = match.group(1) if match else None
    return stats


TIMING_LINE = re.compile(r"^\s*(?P<key>[^:]+?):\s*(?P<value>-?[\d.]+)\s*(?P<unit>ms|MB/s)\s*$")


def server_timing(log):
    """Every 'label: value ms' (or MB/s) line of the server's timing report."""
    at = log.find("=== OnionPIR streaming server timing ===")
    timing = {}
    for line in (log[at:] if at >= 0 else "").splitlines()[1:]:
        match = TIMING_LINE.match(line)
        if match:
            timing[f"{match['key']} ({match['unit']})"] = float(match["value"])
    return timing


def collect(run_dir, n, status, wall_s):
    """metrics.json for one run, from the logs remote_test.py saved in run_dir."""
    servers = sorted((f for f in os.listdir(run_dir) if re.fullmatch(r"server\d+\.log", f)),
                     key=lambda f: int(f[6:-4]))
    config = json.loads(read(os.path.join(run_dir, "config.json")) or "{}")
    metrics = {"N": n, "record_bytes": config.get("RecordBytes"), "servers_used": len(servers), "exit_status": status, "run_wall_s": wall_s,
               "client": client_stats(read(os.path.join(run_dir, "client.log"))),
               "servers": {f[:-4]: server_timing(read(os.path.join(run_dir, f))) for f in servers}}
    with open(os.path.join(run_dir, "metrics.json"), "w") as file:
        json.dump(metrics, file, indent=2)
    return metrics


SUMMARY_COLUMNS = ["N", "clients", "record_bytes", "servers_used", "status", "e2e_avg_ms", "e2e_p99_ms", "upload_B_per_client",
                   "key_upload_B_per_client", "query_upload_B_per_client", "download_B_per_client",
                   "run_wall_s"]


def fmt(value, digits=1):
    if value is None:
        return "-"
    return f"{value:,.{digits}f}" if isinstance(value, float) else f"{value:,}"


def write_summary(out_dir, all_metrics):
    rows = [dict(m["client"], N=m["N"], record_bytes=m["record_bytes"], servers_used=m["servers_used"],
                 status="PASS" if m["exit_status"] == 0 else "FAIL",
                 run_wall_s=round(m["run_wall_s"], 1)) for m in all_metrics]
    with open(os.path.join(out_dir, "summary.csv"), "w", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=SUMMARY_COLUMNS, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)
    lines = ["| N | clients | B | servers | status | e2e avg (ms) | e2e p99 (ms) | upload / client (B) | "
             "download / client (B) | run wall (s) |",
             "|---|---|---|---|---|---|---|---|---|---|"]
    for r in rows:
        lines.append(f"| {r['N']:,} | {fmt(r.get('clients'))} | {fmt(r.get('record_bytes'))} | "
                     f"{r['servers_used']} | {r['status']} | "
                     f"{fmt(r.get('e2e_avg_ms'))} | "
                     f"{fmt(r.get('e2e_p99_ms'))} | {fmt(r.get('upload_B_per_client'))} | "
                     f"{fmt(r.get('download_B_per_client'))} | {fmt(r['run_wall_s'])} |")
    with open(os.path.join(out_dir, "summary.md"), "w") as file:
        file.write("\n".join(lines) + "\n\nUpload = offline keys + query. Server timings: "
                   "<run>/metrics.json and the raw logs.\n")
    print("\n".join(lines))


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--aws-config", required=True, help="shuffle-format AWS config (instance ids, region)")
    vary = ap.add_mutually_exclusive_group()
    vary.add_argument("--sizes", type=int, nargs="+", help="vary N (default: 1e4 1e5 1e6 1e7)")
    vary.add_argument("--clients", type=int, nargs="+", help="vary C instead, at the --config's N")
    vary.add_argument("--record-sizes", type=int, nargs="+",
                      help=f"vary RecordBytes instead (each <= {PLAINTEXT_BYTES}), at the --config's N and C")
    ap.add_argument("--config-dir", default=DEFAULT_CONFIG_DIR, help="holds N<N>.json for each size")
    ap.add_argument("--config", default=os.path.join(DEFAULT_CONFIG_DIR, "N1000000.json"),
                    help="experiment config for --clients / --record-sizes (default: N=1e6, C=1000, 1 KiB)")
    ap.add_argument("--out", help="output directory (default outputs/onion_<sweep>-<timestamp>)")
    ap.add_argument("--timeout", type=int, default=7200, help="seconds to wait for the clients, per run")
    ap.add_argument("--git-pull", action="store_true", help="git pull both checkouts before the first run")
    ap.add_argument("--rebuild-each", action="store_true",
                    help="build before every run (default: only before the first)")
    ap.add_argument("--region")
    args = ap.parse_args()

    aws_path = ex.resolve(args.aws_config, os.getcwd())
    with open(aws_path) as file:
        aws = json.load(file)
    proxies = len(aws.get("proxyInstances") or [])
    # runs: (label, N, config path, extra remote_test.py args, clients or None)
    if args.clients:
        config = ex.resolve(args.config, os.getcwd())
        n = ex.load_config(config)["N"]
        runs = [(f"C{c}", n, config, ["--C", str(c)], c) for c in args.clients]
        sweep = f"N{n}_B{ex.load_config(config)['RecordBytes']}_vary_C"
    elif args.record_sizes:
        too_big = [b for b in args.record_sizes if b > PLAINTEXT_BYTES]
        if too_big:
            sys.exit(f"RecordBytes {too_big} exceed one OnionPIR plaintext ({PLAINTEXT_BYTES} B); "
                     "records spanning plaintexts are not supported (src/stream/record_codec.h)")
        config = ex.resolve(args.config, os.getcwd())
        base = ex.load_config(config)
        runs = [(f"B{b}", base["N"], config, ["--B", str(b)], None) for b in args.record_sizes]
        sweep = f"N{base['N']}_C{base['C']}_vary_B"
    else:
        sizes = args.sizes or DEFAULT_SIZES
        config_dir = ex.resolve(args.config_dir, os.getcwd())
        runs = [(f"N{n}", n, os.path.join(config_dir, f"N{n}.json"), [], None) for n in sizes]
        sweep = "C1000_B1024"
    for _, _, path, _, c in runs:
        ex.load_config(path, c=c)  # fail fast on a missing or invalid config
    out_dir = args.out or os.path.join(ex.REPO_ROOT, "outputs",
                                       f"onion_{sweep}-" + time.strftime("%Y%m%d-%H%M%S"))
    os.makedirs(out_dir, exist_ok=True)
    print(f"==> OnionPIR sweep: {', '.join(label for label, *_ in runs)}; output {out_dir}")

    remote_test = os.path.join(os.path.dirname(os.path.abspath(__file__)), "remote_test.py")
    all_metrics, built = [], False
    for i, (label, n, config, extra, clients) in enumerate(runs):
        run_dir = os.path.join(out_dir, label)
        os.makedirs(run_dir, exist_ok=True)
        run_aws = aws_path
        if clients is not None and proxies and clients < proxies:
            # Every server needs a client: run one server, on the database streamer.
            run_aws = os.path.join(run_dir, "aws_single_server.json")
            with open(run_aws, "w") as file:
                json.dump(dict(aws, onionServerInstance=aws["proxyInstances"][-1]), file, indent=2)
            print(f"  {label}: fewer clients than the {proxies} proxies; one OnionPIR server "
                  f"({aws['proxyInstances'][-1]}, the database streamer)")
        cmd = [sys.executable, remote_test, "--aws-config", run_aws, "--config", config,
               "--timeout", str(args.timeout), "--output", os.path.join(run_dir, "report.txt"),
               "--log-dir", run_dir] + extra
        if args.region:
            cmd += ["--region", args.region]
        if args.git_pull and i == 0:
            cmd.append("--git-pull")
        if built and not args.rebuild_each:
            cmd.append("--no-build")
        print(f"\n==> {label} ({time.strftime('%H:%M:%S')})", flush=True)
        started = time.time()
        with open(os.path.join(run_dir, "driver.log"), "w") as log:
            proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
            for line in proc.stdout:
                sys.stdout.write(line)
                log.write(line)
            status = proc.wait()
        wall = time.time() - started
        built = built or "build OK" in read(os.path.join(run_dir, "driver.log"))
        print(f"==> {label} exit {status} after {wall:.0f}s", flush=True)
        all_metrics.append(collect(run_dir, n, status, wall))
        write_summary(out_dir, all_metrics)  # rewritten after every run, so partial sweeps are kept
    print(f"\nResults in {out_dir}")
    sys.exit(0 if all(m["exit_status"] == 0 for m in all_metrics) else 1)


if __name__ == "__main__":
    main()

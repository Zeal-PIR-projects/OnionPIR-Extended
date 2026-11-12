#!/usr/bin/env python3
"""Build the OnionPIR experiment binaries on remote AWS instances via SSM,
without running anything.

Runs exactly the build phase of scripts/remote_test.py: regenerates shuffle's
protobuf code on every host, then in parallel builds the database
(shuffle cmd/testserver -> /tmp/onionpir_db) and, on the OnionPIR server and
client hosts, HEXL (first use only) plus onion_server / onion_client
(-> /tmp/onionpir_server, /tmp/onionpir_client). Stops any leftover
/tmp/onionpir_* processes first, as remote_test.py does. Useful for checking that a
change compiles remotely, or for warming the build before a batch of runs.

Usage:
    python3 scripts/build_remote.py --aws-config ../shuffle/awsConfigs/c8i16xlarge.json

Options:
    --aws-config      shuffle-format AWS config (instance ids, region, optional hexlPrefix)
    --remote-repo     OnionPIR-Extended checkout on the instances
                      (default: /home/ec2-user/OnionPIR-Extended)
    --remote-shuffle  shuffle checkout on the instances (default: /home/ec2-user/shuffle)
    --region          AWS region — overrides 'region' key in --aws-config
"""
import argparse
import json
import os
import sys

try:
    import boto3
except ImportError:
    sys.exit("boto3 required: pip install boto3")

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import experiment as ex  # noqa: E402
from remote_test import PREFIX, build_all, require, resolve_hosts, run_parallel, ssm_run  # noqa: E402


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--aws-config", required=True, help="shuffle-format AWS config (instance ids, region)")
    ap.add_argument("--remote-repo", default="/home/ec2-user/OnionPIR-Extended")
    ap.add_argument("--remote-shuffle", default="/home/ec2-user/shuffle")
    ap.add_argument("--region", help="AWS region (overrides 'region' key in --aws-config)")
    args = ap.parse_args()

    with open(ex.resolve(args.aws_config, os.getcwd())) as f:
        aws = json.load(f)
    hosts = resolve_hosts(aws)
    ssm = boto3.Session(region_name=args.region or aws.get("region")).client("ssm")

    print("==> OnionPIR build (AWS)")
    for label, iid in hosts.items():
        print(f"  {label} {iid}")
    # A still-running binary makes the copy onto /tmp/onionpir_* fail (text file busy).
    print("==> Stopping leftover OnionPIR processes...")
    run_parallel({label: (lambda iid=iid: require(f"stop {label}", ssm_run(ssm, iid, [
        f"pkill -f {PREFIX}_ 2>/dev/null || true"], comment="onionpir-stop")))
        for label, iid in hosts.items()})
    build_all(ssm, hosts, args.remote_repo, args.remote_shuffle, aws.get("hexlPrefix", "/opt/hexl"))


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        sys.exit(1)

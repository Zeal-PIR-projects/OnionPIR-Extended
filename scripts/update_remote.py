#!/usr/bin/env python3
"""Update the OnionPIR-Extended repo on remote AWS instances via AWS SSM.

The counterpart of shuffle's cmd/update_remote.py. Pulls the latest changes in
parallel on the hosts that carry this repo (the OnionPIR server and client
hosts; the database host only runs shuffle's testserver). Optionally checks out
a specific branch first. Use shuffle's cmd/update_remote.py for the shuffle
checkouts.

Usage:
    python3 scripts/update_remote.py --aws-config ../shuffle/awsConfigs/c8i16xlarge.json
    python3 scripts/update_remote.py --aws-config awsConfigs/example.json --branch my-feature

Options:
    --aws-config   shuffle-format AWS config (serverInstance, onionServerInstance /
                   proxyInstances, clientInstance, optionally 'region')
    --branch       Branch to check out before pulling (default: stay on current branch)
    --remote-repo  Absolute path to the OnionPIR-Extended repo on the instances
                   (default: /home/ec2-user/OnionPIR-Extended)
    --region       AWS region — overrides 'region' key in --aws-config
"""
import argparse
import json
import os
import shlex
import sys

try:
    import boto3
except ImportError:
    sys.exit("boto3 required: pip install boto3")

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import experiment as ex  # noqa: E402
from remote_test import as_ec2_user, resolve_hosts, run_parallel, ssm_run  # noqa: E402

DEFAULT_REMOTE_REPO = "/home/ec2-user/OnionPIR-Extended"


def update_command(remote_repo, branch):
    # Run git as ec2-user so Git's ownership check passes (repo is owned by that user).
    q = shlex.quote
    if branch:
        git_cmds = (f"cd {q(remote_repo)} && git fetch origin && git checkout {q(branch)} && "
                    f"git pull origin {q(branch)}")
    else:
        git_cmds = f"cd {q(remote_repo)} && git pull"
    return as_ec2_user(git_cmds)


def main():
    ap = argparse.ArgumentParser(
        description="Pull latest repo changes on remote AWS instances via SSM"
    )
    ap.add_argument(
        "--aws-config", required=True,
        help="shuffle-format AWS config (instance ids, region)",
    )
    ap.add_argument(
        "--branch", default=None,
        help="Branch to check out before pulling (default: stay on current branch)",
    )
    ap.add_argument(
        "--remote-repo", default=DEFAULT_REMOTE_REPO,
        help="Absolute path to the OnionPIR-Extended repo on remote instances",
    )
    ap.add_argument(
        "--region", default=None,
        help="AWS region (overrides 'region' key in --aws-config)",
    )
    args = ap.parse_args()

    with open(ex.resolve(args.aws_config, os.getcwd())) as f:
        aws = json.load(f)
    instances = {label: iid for label, iid in resolve_hosts(aws).items() if label != "database"}

    ssm = boto3.Session(region_name=args.region or aws.get("region")).client("ssm")

    action = f"branch '{args.branch}'" if args.branch else "current branch"
    print(f"==> Updating {args.remote_repo} on {', '.join(instances)} ({action})...")

    command = update_command(args.remote_repo, args.branch)
    results = run_parallel({
        label: (lambda label=label, iid=iid: ssm_run(ssm, iid, [command], timeout=120,
                                                     comment=f"git-update-{label}"))
        for label, iid in instances.items()
    })

    all_ok = True
    for label, (status, stdout, stderr) in results.items():
        iid = instances[label]
        if status == "Success":
            summary = stdout.strip().splitlines()[-1] if stdout.strip() else "(no output)"
            print(f"  [{label} {iid[:12]}…] OK — {summary}")
        else:
            print(f"  [{label} {iid[:12]}…] FAILED (status={status})")
            for line in (stderr.strip() or stdout.strip()).splitlines():
                print(f"    {line}")
            all_ok = False

    return 0 if all_ok else 1


if __name__ == "__main__":
    sys.exit(main())

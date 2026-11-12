"""Shared helpers for the OnionPIR streaming experiment drivers.

Experiments take the same JSON configs as the Zealot/shuffle repository
(testFiles/*.json there), so one config describes both systems. OnionPIR uses:

  N            database records          (override: --N)
  C            concurrent clients        (override: --C)
  RecordBytes  bytes per record, "B"     (override: --B; 0/absent -> 16, as shuffle)
  ServerAddr   the uncooperative database (shuffle cmd/testserver)
  ProxyAddresses[-1]  its port is reused for the OnionPIR server, which plays
                      the role of Zealot's database streamer (P_DB)

Num_cols/Num_rows/W/Z/L/TwoRound are Zealot/ShufflePIR-only and ignored.
"""
import copy
import json
import os

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DEFAULT_SHUFFLE = os.path.normpath(os.path.join(REPO_ROOT, "..", "shuffle"))
# The streaming server supports the composite first-dimension config only.
ACTIVE_CONFIG = "CONFIG_N2048_K1_COMP"


def resolve(path, base=REPO_ROOT):
    return path if os.path.isabs(path) else os.path.join(base, path)


def load_config(path, n=None, c=None, b=None):
    """Load a shuffle-format config and apply N/C/B overrides.

    Returns a config that the shuffle database server accepts unchanged. When
    N changes, Num_rows is re-derived so the file stays a valid Zealot config
    (the database itself ignores the matrix shape).
    """
    with open(path) as file:
        config = json.load(file)
    config = copy.deepcopy(config)
    if b is not None:
        config["RecordBytes"] = b
    if not config.get("RecordBytes"):
        config["RecordBytes"] = 16  # shuffle's utility.ConfigFromFile default
    if c is not None:
        config["C"] = c
    if n is not None and n != config.get("N"):
        config["N"] = n
        cols = config.get("Num_cols") or 1
        rows = -(-n // cols)
        split = config.get("L") or 1
        config["Num_rows"] = -(-rows // split) * split
    for key in ("N", "C", "RecordBytes"):
        if not isinstance(config.get(key), int) or config[key] <= 0:
            raise ValueError(f"config {key} must be a positive integer")
    if config["N"] > 2**32 - 1:
        raise ValueError("N must fit the database's uint32 record indices")
    if len(config.get("ProxyAddresses", [])) < 2:
        # shuffle's database refuses configs with fewer than two proxies.
        raise ValueError("config needs at least two ProxyAddresses (shuffle requires it)")
    return config


def port_of(address, default):
    return address.rsplit(":", 1)[1] if ":" in address else str(default)


def database_port(config):
    return port_of(config["ServerAddr"], 10000)


def onion_port(config):
    return port_of(config["ProxyAddresses"][-1], 10001)


def describe(config):
    return (f"N={config['N']}  C={config['C']}  RecordBytes(B)={config['RecordBytes']}  "
            f"(DB size {config['N'] * config['RecordBytes'] / 1e6:.1f} MB)")


def server_flags(config, db_addr, listen, threads=0, chunk_rows=64, db_batch=4096):
    return ["--db", db_addr, "--N", str(config["N"]), "--B", str(config["RecordBytes"]),
            "--C", str(config["C"]), "--listen", listen, "--threads", str(threads),
            "--chunk-rows", str(chunk_rows), "--db-batch", str(db_batch)]


def client_flags(config, server_addr, connections=16, threads=0, timeout=3600, demo_index=None):
    flags = ["--server", server_addr, "--N", str(config["N"]), "--B", str(config["RecordBytes"]),
             "--C", str(config["C"]), "--connections", str(connections), "--timeout", str(timeout)]
    if threads:
        flags += ["--threads", str(threads)]
    if demo_index is not None:
        flags += ["--demo-index", str(demo_index)]
    return flags


def section(log, marker, stop_markers=()):
    """Return the text of `log` from `marker` up to the first stop marker."""
    at = log.find(marker)
    if at < 0:
        return ""
    end = len(log)
    for stop in stop_markers:
        found = log.find(stop, at + len(marker))
        if found >= 0:
            end = min(end, found)
    return log[at:end].rstrip() + "\n"


DB_TIMING_MARKERS = ("Database record-stream timing", "record construction total",
                     "StreamRecords send RPC wait")


def database_timing(log):
    """The shuffle database's own per-stream timing lines."""
    lines = [line for line in log.splitlines() if any(m in line for m in DB_TIMING_MARKERS)]
    return "\n".join(lines) + "\n" if lines else "  (database timing not written)\n"

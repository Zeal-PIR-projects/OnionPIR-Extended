"""Shared helpers for the OnionPIR streaming experiment drivers.

Experiments take the same JSON configs as the Zealot/shuffle repository
(testFiles/*.json there), so one config describes both systems. OnionPIR uses:

  N            database records          (override: --N)
  C            concurrent clients        (override: --C)
  RecordBytes  bytes per record, "B"     (override: --B; 0/absent -> 16, as shuffle)
  ServerAddr   the uncooperative database (shuffle cmd/testserver)
  ProxyAddresses  one OnionPIR server per proxy, on that proxy's port (with
                  fewer servers than proxies, the last ones are used). As in
                  Zealot, the last server is the database streamer (P_DB): it
                  alone reads the database, and each server relays the records
                  to the one before it. Clients are split into contiguous
                  chunks, one per server.

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


def onion_ports(config, servers):
    """Listen ports of `servers` OnionPIR servers: the last `servers` proxies'."""
    proxies = config["ProxyAddresses"]
    if not 1 <= servers <= len(proxies):
        raise ValueError(f"{servers} OnionPIR servers, but the config has "
                         f"{len(proxies)} ProxyAddresses")
    return [port_of(a, 10002 + i) for i, a in enumerate(proxies)][-servers:]


def split_clients(clients, servers):
    """Clients per server: contiguous chunks, the first C % P one larger.
    Must agree with split_clients in src/stream/wire.h."""
    if clients < servers:
        raise ValueError(f"C={clients} is fewer than the {servers} servers (each needs a client)")
    return [clients // servers + (1 if s < clients % servers else 0) for s in range(servers)]


def describe(config):
    return (f"N={config['N']}  C={config['C']}  RecordBytes(B)={config['RecordBytes']}  "
            f"(DB size {config['N'] * config['RecordBytes'] / 1e6:.1f} MB)")


def server_flags(config, clients, listen, db_addr=None, relay_to=None, threads=0,
                 chunk_rows=64, db_batch=4096):
    """Flags for one server with `clients` clients. The database streamer gets
    db_addr; the others receive the relay. relay_to: the previous server."""
    flags = ["--N", str(config["N"]), "--B", str(config["RecordBytes"]),
             "--C", str(clients), "--listen", listen, "--threads", str(threads),
             "--chunk-rows", str(chunk_rows), "--db-batch", str(db_batch)]
    if db_addr:
        flags += ["--db", db_addr]
    if relay_to:
        flags += ["--relay-to", relay_to]
    return flags


def all_server_flags(config, listen_addrs, reach_addrs, db_addr, **knobs):
    """Flags for every server, in order. Server i listens on listen_addrs[i]
    and is reached by the others at reach_addrs[i]; the last one streams the
    database and each server i > 0 relays to server i - 1."""
    counts = split_clients(config["C"], len(listen_addrs))
    last = len(listen_addrs) - 1
    return [server_flags(config, counts[i], listen_addrs[i],
                         db_addr=db_addr if i == last else None,
                         relay_to=reach_addrs[i - 1] if i > 0 else None, **knobs)
            for i in range(len(listen_addrs))]


def client_flags(config, server_addrs, connections=16, threads=0, timeout=3600, demo_index=None):
    """server_addrs: every server, in order (client chunks follow it)."""
    flags = ["--server", ",".join(server_addrs), "--N", str(config["N"]), "--B", str(config["RecordBytes"]),
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

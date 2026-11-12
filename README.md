# OnionPIR version 2

> **WARNING:** Note that this repository is for research purpose. Do not use this in production directly.

### Preliminaries

We ran our code on a `Intel(R) Xeon(R) Platinum 8358 CPU @ 2.60GHz` (Ice Lake, 32 cores per socket, AVX-512). AVX2 and AVX-512 are used to accelerate the first-dimension matrix multiply and NTT-related computation.

We use c++20 and `GCC 11.4.0` for compilation.

### Installation

On machines without AVX-512 (e.g. Apple Silicon), the first-dimension product uses an overflow-safe scalar path; set `HEXL_DIR` (env or `-DHEXL_DIR=`) to your HEXL install.

OnionPIRv2 only depends on [Intel HEXL](https://github.com/intel/hexl) for fast NTT. The expected install path is set in the top-level `CMakeLists.txt` (`HEXL_DIR`) — adjust it to match your environment, or pass `-DHEXL_DIR=...` on the cmake line. Pass `-DUSE_HEXL=OFF` to build without HEXL (scalar fallback).

```
python3 run.py            # build (Benchmark) + run pir test on k1_comp
python3 run.py --build-only
```

### Usage

`run.py` handles building with the correct compile flags and running the binary.

```
python run.py [options]
```

| Option | Description |
|---|---|
| `-c NAME`, `--config NAME` | Build configuration (default: `k1_comp`). See available configs below |
| `-v`, `--verbose` | Build in Debug mode (enables `DEBUG_PRINT` at compile time) |
| `--no-compress` | Run PIR without query compression/decompression |
| `-t NAME`, `--test NAME` | Test to run (default: `pir`). See available tests below |
| `-n N`, `--experiments N` | Number of experiment iterations (default: 5) |
| `-w N`, `--warmup N` | Number of warmup iterations (default: 3) |
| `-o FILE`, `--output FILE` | Write results to file (bare name goes to `outputs/`) |
| `-j N`, `--jobs N` | Parallel make jobs (default: all cores) |
| `--build-only` | Build without running |
| `-h`, `--help` | Show help message |

**Examples:**

```bash
# Default: k1_comp config, pir test, 5 trials + 3 warmup
python3 run.py

# Pick a different config
python3 run.py -c k1
python3 run.py -c k2_mp
python3 run.py -c n4096_k2_mp

# Verbose/debug mode — recompiles with DEBUG_PRINT enabled
python3 run.py -v

# Save results to outputs/results.txt
python3 run.py -o results.txt

# Just build, don't run
python3 run.py --build-only

# Run a specific test
python3 run.py -t fst_dim
```

**Available configs:** `k1`, `k1_comp` (default, composite-mod K=1), `k2_mp`, `n4096_k2_mp`. See `src/includes/database_constants.h` for per-config parameters.

**Available tests:** `pir` (default), `bfv`, `ext_prod`, `ext_prod_mux`, `fst_dim`, `fast_expand`, `decrypt_mod_q`, `mod_switch`, `db_shape`, `bv_ks`, `cpu_info`, `hexl_ntt`, `utils_arith`, `noise_sampling`, `rlwe_enc`, `barrett`, `plan_params`

You can also build and run manually with CMake:

```bash
mkdir build && cd build
cmake .. -DCMAKE_BUILD_TYPE=Benchmark -DACTIVE_CONFIG=CONFIG_N2048_K1_COMP
make && ./Onion-PIR --test pir
```



## Multi-client streaming mode (uncooperative database)

`onion_server` / `onion_client` run OnionPIR for **C concurrent clients** against an
**uncooperative database** that the single server must stream every round. The
database is the unmodified Go service from the Zealot/shuffle repository
(`../shuffle`, `cmd/testserver`), reached through its own gRPC API
(`Database.StreamRecords`), so results are directly comparable with Zealot.

**How it works**

- *Cross-language database access.* `dbbridge/` is a small Go package built as a
  cgo `c-archive` and linked into the C++ binaries. It imports shuffle's generated
  stubs (`proto_gen`) and record format (`utility`) straight from the shuffle
  checkout, so the API cannot drift. Clients verify each record with shuffle's
  own `utility.GenerateRecord`, through the same bridge.
- *Records → plaintexts.* With `B = RecordBytes`, each plaintext holds
  `floor(3072 / B)` whole records (3072 B = 12 bits × 2048 coefficients), so one
  query retrieves one record. `B ≤ 3072` is required.
- *One round.* Clients upload keys (offline), then queries. The server
  preprocesses each query as it arrives (expansion, RGSW selectors, first-dim
  query in NTT form). Once all C are in, it streams records `[0, N)` once, in
  chunks of `--chunk-rows` first-dimension rows, downloading the next chunk while
  it computes on the current one. Per chunk, it NTT-encodes the plaintexts, runs
  one batched first-dimension product against all C queries (each DB tile stays
  in cache across clients), and folds the rows into each client's selection tree.
  The server never stores the database. Per client it keeps O(log rows)
  ciphertexts plus the expanded query, about 26 MB at the default chunk size. The
  server prints the estimate and warns if C would exceed RAM.
- Only the composite config (`k1_comp`, the default) is supported by the
  streaming server.

**Prerequisites:** everything above, plus Go 1.25+ and a shuffle checkout next to
this repo (`../shuffle`, with `python3 proto.py` run once; override with
`-DSHUFFLE_DIR=...`). Without them, CMake skips these two targets.

**Local run** (database, server, and clients as separate processes on 127.0.0.1):

```bash
export HEXL_DIR=~/hexl_install/lib/cmake/hexl-1.2.6     # where HEXLConfig.cmake lives
python3 scripts/local_test.py                           # tiny smoke test
python3 scripts/local_test.py --config ../shuffle/testFiles/fromPaper/row1.json --N 100000 --C 16
```

**AWS run** (SSM, same conventions as shuffle's `cmd/zealot_remote_test.py`):

```bash
python3 scripts/remote_test.py --aws-config ../shuffle/awsConfigs/c8i16xlarge.json \
    --config ../shuffle/testFiles/fromPaper/row1.json --C 100 --output outputs/row1_c100.txt
```

Experiments are specified exactly as for Zealot: an AWS config plus a shuffle
experiment config, with `--N`, `--C`, `--B` (RecordBytes) overrides. In the AWS
config, `serverInstance` runs the database and `onionServerInstance` runs the
OnionPIR server (default: the last `proxyInstances` entry, Zealot's database
streamer). `clientInstance` runs the clients (default: the server host). See
`awsConfigs/example.json` for the optional `onion*` knobs. The driver builds HEXL
on first use (`scripts/ec2_setup.sh`, into `/opt/hexl`) and builds out of tree in
`/tmp/onionpir-build`. It expects both repos to be cloned on the server and
client hosts at `--remote-repo` / `--remote-shuffle` (defaults
`/home/ec2-user/OnionPIR-Extended`, `/home/ec2-user/shuffle`). The OnionPIR server
listens on the port of the config's last `ProxyAddresses` entry; the database
listens on `ServerAddr`'s port.

**Output.** The client prints Zealot's `=== Performance Metrics ===` block with
the same labels, so the two systems can be compared line for line.
`Query-to-result latency (avg)` (also printed as `Average end-to-end latency`)
runs from the start of a client's query generation to its decrypted, verified
record. Offline key setup (Galois keys + GSW(sk), about 2.9 MB per client) has
no Zealot counterpart and is reported separately. The server's timing
breakdown and the database's own stream timing follow.

### Tips

- Currently, most of the parameters can be adjusted in `src/includes/database_constants.h`. 
- You can use `clangd` when reading the code. The `compile_commands.json` file will be automatically generated after cmake.
- You can install the [Better Comments](https://marketplace.visualstudio.com/items?itemName=aaron-bond.better-comments) extension to highlight the TODO or remarked comments.
- The code also runs for clang, but we use GCC unroll in some places. Please change those lines if you want to test optimal throughput.

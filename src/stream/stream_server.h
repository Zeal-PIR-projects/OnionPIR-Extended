#pragma once
// Multi-client OnionPIR round over an uncooperative, streamed database.
//
// The server never holds the database. Per round it:
//   1. collects one query from each of C clients, preprocessing each as it
//      arrives (query expansion, RGSW selector conversion, first-dimension
//      query in NTT/limb-split form) on the thread pool;
//   2. once all C are ready, streams records [0, N) from the database once,
//      in chunks of `chunk_rows` first-dimension rows (chunk_rows * fst_dim_sz
//      plaintexts), overlapping the next chunk's download with compute;
//   3. per chunk: packs records into plaintexts and NTT-encodes them, runs
//      one batched first-dimension product of the chunk against all C
//      queries (each DB tile is reused from cache across clients), turns the
//      products into per-row ciphertexts, and folds them into each client's
//      selection tree;
//   4. finishes each client's tree, mod-switches, and replies.
//
// Selection tree. PirServer::evaluate_other_dim resolves row bits in the
// order (bit 0, bit h-1, bit h-2, ..., bit 1). A mux by an encrypted bit is
// exact in any order, so the streaming server instead merges adjacent
// subtrees bottom-up (bit 0, 1, ..., h-1) like a binary counter, using the
// client's selector for each bit. Only O(h) ciphertexts per client are live,
// and all-zero padding rows (PirParams pow2_other_dim) are never computed.
//
// Only the composite first-dimension config (CONFIG_N2048_K1_COMP, the
// default) is supported.

#include "aligned_allocator.h"
#include "bv_keyswitch.h"
#include "gsw.h"
#include "pir.h"
#include "record_codec.h"
#include "rlwe.h"
#include "thread_pool.h"

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <ostream>
#include <string>
#include <vector>

namespace stream {

struct StreamServerOptions {
  std::string db_addr;            // uncooperative database, host:port
  size_t num_clients = 1;         // C: queries per round
  size_t threads = 0;             // 0 = hardware concurrency
  size_t chunk_rows = 64;         // first-dim rows per streamed chunk (pow2)
  uint32_t db_batch_records = 4096;  // DBRecordRange.max_records hint
  uint32_t db_connect_timeout_sec = 300;
};

class StreamPirServer {
public:
  using Reply = std::function<void(const std::vector<uint8_t> &response)>;

  StreamPirServer(const PirParams &params, const RecordLayout &layout,
                  StreamServerOptions options);
  ~StreamPirServer();

  // Thread-safe. Keys must arrive before that client's query.
  void add_keys(uint32_t client_id, bvks::BvGaloisKeys galois_keys,
                GSWCt gsw_key);
  // Thread-safe. Preprocessing runs asynchronously on the pool; `reply` is
  // invoked from a pool thread with the serialized response.
  void add_query(uint32_t client_id, RlweCt query, Reply reply);

  // Blocks until all C queries are preprocessed, then streams the database
  // and replies to every client.
  void run_round();

  void print_report(std::ostream &os) const;
  size_t chunk_rows() const { return chunk_rows_; }
  // Approximate server memory held per client during the round.
  size_t bytes_per_client() const;

private:
  struct Node {  // a selection-tree node: zero, or a ciphertext
    bool present = false;
    bool zero = true;
    RlweCt ct;
  };
  struct Client {
    bvks::BvGaloisKeys galois_keys;
    GSWCt gsw_key;
    bool has_keys = false;
    bool has_query = false;
    Reply reply;
    // Preprocessed query. First-dim query per limb, layout
    // [level][B0 (fst_dim_sz) | B1 (fst_dim_sz)].
    std::unique_ptr<uint32_t[], AlignedDeleter<uint32_t>> q_lo, q_hi;
    std::vector<GSWCt> selectors;  // selectors[j] = RGSW(query bit j+1)
    // Per-chunk state.
    std::vector<uint64_t> inter_lo, inter_hi;  // [level][row][2]
    std::vector<Node> leaves;                   // chunk_rows subtree leaves
    std::vector<Node> stack;                    // levels 0..h
  };

  void preprocess(uint32_t client_id, RlweCt &query);
  GSWCt &selector_for_bit(Client &cl, size_t bit);
  // x <- mux(x, y; bit) where `bit` selects y. Zero-aware.
  void mux_into(Client &cl, Node &x, Node &y, size_t bit, size_t tid);
  void push_subtree(Client &cl, Node node, size_t level, size_t tid);
  void encode_chunk(const uint8_t *raw, size_t first_pt, size_t rows);
  void process_chunk(size_t rows);
  RlweCt zero_ct() const;

  PirParams params_;
  RecordLayout layout_;
  StreamServerOptions opts_;
  ThreadPool pool_;
  size_t n_;           // fst_dim_sz
  size_t h_;           // num_dims - 1
  size_t levels_;      // coefficients per polynomial (N * K)
  size_t chunk_rows_;  // power of two, <= 2^h
  size_t chunk_bits_;  // log2(chunk_rows_)
  uint64_t q_, q1_, q2_, q1_inv_mod_q2_;

  std::vector<std::unique_ptr<Client>> clients_;
  std::vector<std::unique_ptr<GSWEval>> key_gsw_;   // per worker
  std::vector<std::unique_ptr<GSWEval>> data_gsw_;  // per worker
  std::unique_ptr<uint32_t[], AlignedDeleter<uint32_t>> a_lo_, a_hi_;

  mutable std::mutex mu_;
  std::condition_variable cv_;
  size_t keys_received_ = 0, queries_received_ = 0, preprocessed_ = 0;
  std::string preprocess_error_;

  // Report. CPU totals (summed across threads) are labeled as such; the rest
  // are wall-clock times of the round's critical path.
  struct Report {
    double first_query_to_ready_ms = 0;  // wall: first query -> all preprocessed
    std::atomic<uint64_t> expand_us{0}, convert_us{0}, fst_prep_us{0};  // CPU
    double db_connect_ms = 0;
    double stream_wall_ms = 0, db_wait_ms = 0, encode_ms = 0, matmul_ms = 0,
           leaves_ms = 0, merge_ms = 0, finish_ms = 0, round_wall_ms = 0;
    size_t chunks = 0, real_rows = 0, records = 0;
    double db_mbytes = 0;
  } report_;
  std::chrono::steady_clock::time_point first_query_time_;
};

} // namespace stream

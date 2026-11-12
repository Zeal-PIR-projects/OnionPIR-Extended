#include "stream_server.h"

#include "db_bridge.h"
#include "matrix.h"
#include "server.h"
#include "utils.h"

#include <algorithm>
#include <bit>
#include <cstring>
#include <chrono>
#include <future>
#include <iomanip>
#include <sstream>
#include <stdexcept>
#include <thread>

namespace stream {

namespace {

using Clock = std::chrono::steady_clock;

double ms_since(Clock::time_point t) {
  return std::chrono::duration<double, std::milli>(Clock::now() - t).count();
}
uint64_t us_since(Clock::time_point t) {
  return static_cast<uint64_t>(
      std::chrono::duration_cast<std::chrono::microseconds>(Clock::now() - t)
          .count());
}

size_t default_threads() {
  return std::max<size_t>(1, std::thread::hardware_concurrency());
}

// Largest power of two <= x (x >= 1).
size_t floor_pow2(size_t x) { return size_t{1} << (std::bit_width(x) - 1); }

} // namespace

StreamPirServer::StreamPirServer(const PirParams &params,
                                 const RecordLayout &layout,
                                 StreamServerOptions options)
    : params_(params), layout_(layout), opts_(std::move(options)),
      pool_(opts_.threads ? opts_.threads : default_threads()) {
  const auto &crt = params_.get_composite_rns();
  if (!crt.enabled)
    throw std::runtime_error(
        "the streaming server requires the composite first-dimension config "
        "(-c k1_comp / CONFIG_N2048_K1_COMP)");
  n_ = params_.get_fst_dim_sz();
  h_ = params_.get_num_dims() - 1;
  levels_ = params_.get_coeff_val_cnt();
  if (params_.get_other_dim_sz() != (size_t{1} << h_))
    throw std::runtime_error("streaming server needs PirParams(..., pow2_other_dim=true)");
  if (layout_.num_real_pt > params_.get_num_pt())
    throw std::runtime_error("database does not fit the PIR shape");
  if (opts_.num_clients == 0) throw std::invalid_argument("C must be positive");

  chunk_rows_ = std::min(floor_pow2(std::max<size_t>(1, opts_.chunk_rows)),
                         size_t{1} << h_);
  chunk_bits_ = static_cast<size_t>(std::countr_zero(chunk_rows_));
  q_ = params_.get_rns_mods()[0];
  q1_ = crt.q1;
  q2_ = crt.q2;
  q1_inv_mod_q2_ = crt.q1_inv_mod_q2;

  clients_.resize(opts_.num_clients);
  for (auto &cl : clients_) cl = std::make_unique<Client>();
  for (size_t t = 0; t < pool_.size(); ++t) {
    key_gsw_.push_back(std::make_unique<GSWEval>(
        params_, params_.get_l_key(), params_.get_base_log2_key()));
    data_gsw_.push_back(std::make_unique<GSWEval>(
        params_, params_.get_l(), params_.get_base_log2()));
  }
  a_lo_ = make_unique_aligned<uint32_t, 64>(chunk_rows_ * n_ * levels_);
  a_hi_ = make_unique_aligned<uint32_t, 64>(chunk_rows_ * n_ * levels_);
}

StreamPirServer::~StreamPirServer() = default;

size_t StreamPirServer::bytes_per_client() const {
  const size_t ct = 2 * levels_ * sizeof(uint64_t);
  const size_t query = 2 * levels_ * 2 * n_ * sizeof(uint32_t);
  const size_t selectors = h_ * 2 * params_.get_l() * ct;
  const size_t inter = 2 * chunk_rows_ * 2 * levels_ * sizeof(uint64_t);
  const size_t tree = (chunk_rows_ + h_ + 1) * ct;
  return query + selectors + inter + tree;
}

RlweCt StreamPirServer::zero_ct() const {
  RlweCt ct;
  ct.c0.assign(levels_, 0);
  ct.c1.assign(levels_, 0);
  ct.ntt_form = false;
  return ct;
}

void StreamPirServer::add_keys(uint32_t client_id,
                               bvks::BvGaloisKeys galois_keys, GSWCt gsw_key) {
  if (client_id >= clients_.size())
    throw std::invalid_argument("client id out of range");
  std::lock_guard<std::mutex> lock(mu_);
  Client &cl = *clients_[client_id];
  if (cl.has_keys) throw std::invalid_argument("duplicate keys for client");
  cl.galois_keys = std::move(galois_keys);
  cl.gsw_key = std::move(gsw_key);
  cl.has_keys = true;
  ++keys_received_;
}

void StreamPirServer::attach_relay(std::shared_ptr<Conn> conn) {
  std::lock_guard<std::mutex> lock(mu_);
  if (!opts_.db_addr.empty())
    throw std::invalid_argument("relay offered to the database streamer");
  if (relay_in_) throw std::invalid_argument("duplicate relay connection");
  relay_in_ = std::move(conn);
  cv_.notify_all();
}

void StreamPirServer::add_query(uint32_t client_id, RlweCt query, Reply reply) {
  if (client_id >= clients_.size())
    throw std::invalid_argument("client id out of range");
  {
    std::lock_guard<std::mutex> lock(mu_);
    Client &cl = *clients_[client_id];
    if (!cl.has_keys) throw std::invalid_argument("query before keys");
    if (cl.has_query) throw std::invalid_argument("duplicate query for client");
    cl.has_query = true;
    cl.reply = std::move(reply);
    if (queries_received_++ == 0) first_query_time_ = Clock::now();
  }
  pool_.submit([this, client_id, q = std::move(query)]() mutable {
    std::string error;
    try {
      preprocess(client_id, q);
    } catch (const std::exception &e) {
      error = e.what();
    }
    std::lock_guard<std::mutex> lock(mu_);
    if (!error.empty() && preprocess_error_.empty()) preprocess_error_ = error;
    ++preprocessed_;
    cv_.notify_all();
  });
}

void StreamPirServer::preprocess(uint32_t client_id, RlweCt &query) {
  Client &cl = *clients_[client_id];
  const size_t tid = ThreadPool::worker_id();
  constexpr size_t N = DBConsts::PolyDegree;

  auto t0 = Clock::now();
  std::vector<RlweCt> expanded =
      server_ops::fast_expand_qry(params_, cl.galois_keys, query);
  report_.expand_us += us_since(t0);

  // RGSW selectors for the other dimensions, l BFV ciphertexts each.
  t0 = Clock::now();
  const size_t l = params_.get_l();
  cl.selectors.resize(h_);
  for (size_t d = 0; d < h_; ++d) {
    std::vector<RlweCt> lwe(expanded.begin() + n_ + d * l,
                            expanded.begin() + n_ + (d + 1) * l);
    key_gsw_[tid]->query_to_gsw(std::move(lwe), cl.gsw_key, cl.selectors[d]);
  }
  report_.convert_us += us_since(t0);

  // First-dim query: NTT under q = q1*q2, then split into per-limb u32 in
  // the layout level_mat_mat_32_split expects.
  t0 = Clock::now();
  cl.q_lo = make_unique_aligned<uint32_t, 64>(levels_ * 2 * n_);
  cl.q_hi = make_unique_aligned<uint32_t, 64>(levels_ * 2 * n_);
  uint32_t *lo = cl.q_lo.get(), *hi = cl.q_hi.get();
  for (size_t j = 0; j < n_; ++j) {
    RlweCt &ct = expanded[j];
    utils::ntt_fwd(ct.c0.data(), N, q_);
    utils::ntt_fwd(ct.c1.data(), N, q_);
    for (size_t lvl = 0; lvl < levels_; ++lvl) {
      const uint64_t v0 = ct.c0[lvl], v1 = ct.c1[lvl];
      const size_t base = lvl * 2 * n_;
      lo[base + j] = static_cast<uint32_t>(v0 % q1_);
      lo[base + n_ + j] = static_cast<uint32_t>(v1 % q1_);
      hi[base + j] = static_cast<uint32_t>(v0 % q2_);
      hi[base + n_ + j] = static_cast<uint32_t>(v1 % q2_);
    }
  }
  report_.fst_prep_us += us_since(t0);

  // Key material is no longer needed; release it before the round.
  bvks::BvGaloisKeys().keys.swap(cl.galois_keys.keys);
  GSWCt().swap(cl.gsw_key);
  cl.inter_lo.assign(chunk_rows_ * 2 * levels_, 0);
  cl.inter_hi.assign(chunk_rows_ * 2 * levels_, 0);
  cl.leaves.assign(chunk_rows_, Node{});
  cl.stack.assign(h_ + 1, Node{});
}

GSWCt &StreamPirServer::selector_for_bit(Client &cl, size_t bit) {
  // Client query bit j+1 (selectors[j]) is row bit 0 for j = 0 and row bit
  // h - j otherwise (see PirClient::get_query_indices with a power-of-two
  // other dimension).
  return bit == 0 ? cl.selectors[0] : cl.selectors[h_ - bit];
}

void StreamPirServer::mux_into(Client &cl, Node &x, Node &y, size_t bit,
                               size_t tid) {
  if (x.zero && y.zero) return;  // mux(0, 0) = 0
  if (x.zero) x.ct = zero_ct();
  if (y.zero) y.ct = zero_ct();
  server_ops::ext_prod_mux(params_, *data_gsw_[tid], x.ct, y.ct,
                           selector_for_bit(cl, bit), x.ct);
  x.zero = false;
  y = Node{};
}

void StreamPirServer::push_subtree(Client &cl, Node node, size_t level,
                                   size_t tid) {
  // Binary-counter merge: a pending left sibling at `level` combines with
  // `node`, resolving row bit `level`.
  while (level < h_ && cl.stack[level].present) {
    Node &left = cl.stack[level];
    mux_into(cl, left, node, level, tid);
    node = std::move(left);
    left = Node{};
    ++level;
  }
  node.present = true;
  cl.stack[level] = std::move(node);
}

void StreamPirServer::encode_chunk(const uint8_t *raw, size_t first_pt,
                                   size_t rows) {
  constexpr size_t N = DBConsts::PolyDegree;
  constexpr size_t kTile = 16;  // plaintexts per task; writes stay contiguous
  const size_t pts = rows * n_;
  const size_t pt_stride = layout_.records_per_pt * layout_.record_bytes;
  uint32_t *a_lo = a_lo_.get(), *a_hi = a_hi_.get();
  pool_.parallel_for((pts + kTile - 1) / kTile, [&](size_t t) {
    thread_local std::vector<uint64_t> tile;
    tile.resize(kTile * N);
    const size_t p0 = t * kTile;
    const size_t cnt = std::min(kTile, pts - p0);
    for (size_t p = 0; p < cnt; ++p) {
      const size_t lp = p0 + p;
      const size_t records = layout_.records_in_pt(first_pt + lp);
      uint64_t *c = tile.data() + p * N;
      if (records == 0) {
        std::fill(c, c + N, 0);  // padding plaintext (last row)
        continue;
      }
      bytes_to_coeffs(raw + lp * pt_stride, records * layout_.record_bytes, c);
      utils::ntt_fwd(c, N, q_);
    }
    // A layout: [level][row][col] = level * pts + local plaintext index.
    for (size_t lvl = 0; lvl < levels_; ++lvl) {
      uint32_t *dlo = a_lo + lvl * pts + p0;
      uint32_t *dhi = a_hi + lvl * pts + p0;
      for (size_t p = 0; p < cnt; ++p) {
        const uint64_t v = tile[p * N + lvl];
        dlo[p] = static_cast<uint32_t>(v % q1_);
        dhi[p] = static_cast<uint32_t>(v % q2_);
      }
    }
  });
}

void StreamPirServer::process_chunk(size_t rows) {
  const size_t C = clients_.size();

  // ---- Batched first-dim product: task = (limb, level block). Each task's
  // DB tile (lb levels x rows x n, ~1 MiB) stays in cache while every
  // client's query block streams past it.
  auto t0 = Clock::now();
  const size_t tile_bytes = rows * n_ * sizeof(uint32_t);
  const size_t lb = std::clamp<size_t>((size_t{1} << 20) / tile_bytes, 1, levels_);
  const size_t blocks = (levels_ + lb - 1) / lb;
  pool_.parallel_for(2 * blocks, [&](size_t t) {
    const bool hi = t >= blocks;
    const size_t l0 = (t % blocks) * lb;
    const size_t lc = std::min(lb, levels_ - l0);
    const uint32_t *A = (hi ? a_hi_.get() : a_lo_.get()) + l0 * rows * n_;
    const uint64_t q = hi ? q2_ : q1_;
    for (auto &cl : clients_) {
      const uint32_t *B = (hi ? cl->q_hi.get() : cl->q_lo.get()) + l0 * 2 * n_;
      uint64_t *out = (hi ? cl->inter_hi.data() : cl->inter_lo.data()) + l0 * rows * 2;
      level_mat_mat_32_split(A, B, out, rows, n_, lc, q);
    }
  });
  report_.matmul_ms += ms_since(t0);

  // ---- Row ciphertexts: CRT-compose (lo, hi) -> mod q, then INTT.
  t0 = Clock::now();
  const uint64_t q1 = q1_, q2 = q2_, q1_inv = q1_inv_mod_q2_;
  auto compose = [q1, q2, q1_inv](uint64_t lo, uint64_t hi) -> uint64_t {
    const uint64_t diff = (hi + q2 - lo % q2) % q2;
    return lo + q1 * ((diff * q1_inv) % q2);
  };
  pool_.parallel_for(C * chunk_rows_, [&](size_t t) {
    Client &cl = *clients_[t / chunk_rows_];
    const size_t i = t % chunk_rows_;
    Node &leaf = cl.leaves[i];
    leaf = Node{};
    leaf.present = true;
    if (i >= rows) return;  // past the last real row: zero leaf
    RlweCt ct;
    ct.c0.resize(levels_);
    ct.c1.resize(levels_);
    for (size_t lvl = 0; lvl < levels_; ++lvl) {
      const size_t b = lvl * rows * 2 + 2 * i;
      ct.c0[lvl] = compose(cl.inter_lo[b], cl.inter_hi[b]);
      ct.c1[lvl] = compose(cl.inter_lo[b + 1], cl.inter_hi[b + 1]);
    }
    utils::ntt_inv(ct.c0.data(), DBConsts::PolyDegree, q_);
    utils::ntt_inv(ct.c1.data(), DBConsts::PolyDegree, q_);
    ct.ntt_form = false;
    leaf.ct = std::move(ct);
    leaf.zero = false;
  });
  report_.leaves_ms += ms_since(t0);

  // ---- Reduce the chunk's leaves to one subtree (row bits
  // 0..chunk_bits-1), parallel over (client, pair) per level, then push it.
  t0 = Clock::now();
  for (size_t s = 0; s < chunk_bits_; ++s) {
    const size_t pairs = chunk_rows_ >> (s + 1);
    pool_.parallel_for(C * pairs, [&](size_t t) {
      Client &cl = *clients_[t / pairs];
      const size_t left = (t % pairs) << (s + 1);
      mux_into(cl, cl.leaves[left], cl.leaves[left + (size_t{1} << s)], s,
               ThreadPool::worker_id());
    });
  }
  pool_.parallel_for(C, [&](size_t c) {
    Client &cl = *clients_[c];
    push_subtree(cl, std::move(cl.leaves[0]), chunk_bits_, ThreadPool::worker_id());
  });
  report_.merge_ms += ms_since(t0);
}

void StreamPirServer::run_round() {
  const size_t C = clients_.size();
  {
    std::unique_lock<std::mutex> lock(mu_);
    cv_.wait(lock, [&] { return preprocessed_ == C; });
    if (!preprocess_error_.empty())
      throw std::runtime_error("query preprocessing failed: " + preprocess_error_);
    report_.first_query_to_ready_ms = ms_since(first_query_time_);
  }
  const auto round_start = Clock::now();

  const size_t real_rows = (layout_.num_real_pt + n_ - 1) / n_;
  const size_t chunks = (real_rows + chunk_rows_ - 1) / chunk_rows_;
  const size_t rpp = layout_.records_per_pt;
  const size_t B = layout_.record_bytes;
  report_.chunks = chunks;
  report_.real_rows = real_rows;
  report_.records = layout_.num_records;
  report_.db_mbytes = static_cast<double>(layout_.num_records) * B / 1e6;

  // Stream [0, N) once, from the database (the streamer) or from the next
  // server's relay, forwarding every batch to the previous server as soon as
  // it arrives. The next chunk downloads (Go bridge / relay + this reader
  // thread) while the pool computes on the current one.
  const auto connect_start = Clock::now();
  std::unique_ptr<DbRecordStream> db;
  std::shared_ptr<Conn> upstream;
  if (!opts_.db_addr.empty()) {
    db = std::make_unique<DbRecordStream>(
        opts_.db_addr, 0, static_cast<uint32_t>(layout_.num_records),
        opts_.db_batch_records, static_cast<uint32_t>(B),
        opts_.db_connect_timeout_sec);
  } else {
    std::unique_lock<std::mutex> lock(mu_);
    if (!cv_.wait_for(lock, std::chrono::seconds(opts_.relay_timeout_sec),
                      [&] { return relay_in_ != nullptr; }))
      throw std::runtime_error("timed out waiting for the database relay");
    upstream = relay_in_;
  }
  std::unique_ptr<Conn> downstream;
  if (!opts_.relay_to.empty()) {
    downstream = connect_retry(opts_.relay_to, opts_.db_connect_timeout_sec);
    FrameHeader h;
    std::vector<uint8_t> payload;
    if (!downstream->recv_frame(h, payload) ||
        static_cast<FrameType>(h.type) != FrameType::Params)
      throw std::runtime_error("relay target did not send parameters");
    const ShapeInfo mine{layout_.num_records, B, rpp, params_.get_num_pt(), n_,
                         params_.get_num_dims(), clients_.size()};
    if (!same_layout(decode_shape(payload), mine))
      throw std::runtime_error("relay target " + opts_.relay_to +
                               " has a different database layout");
    downstream->send_frame(FrameType::RelayOpen, 0, nullptr, 0);
  }
  report_.db_connect_ms = ms_since(connect_start);

  // Relay batches of at most 4 MiB, so the previous server starts on a chunk
  // while this one is still receiving it.
  const uint64_t relay_batch = std::max<uint64_t>(1, (uint64_t{4} << 20) / B);
  std::vector<uint8_t> relay_buf;
  size_t relay_pos = 0;
  auto read_relay = [&](uint8_t *dst, uint64_t bytes) {
    while (bytes > 0) {
      if (relay_pos == relay_buf.size()) {
        FrameHeader h;
        if (!upstream->recv_frame(h, relay_buf))
          throw std::runtime_error("database relay closed early");
        if (static_cast<FrameType>(h.type) != FrameType::RelayRecords)
          throw std::runtime_error("unexpected frame on the database relay");
        relay_pos = 0;
        continue;
      }
      const size_t n = std::min<uint64_t>(bytes, relay_buf.size() - relay_pos);
      std::memcpy(dst, relay_buf.data() + relay_pos, n);
      relay_pos += n;
      dst += n;
      bytes -= n;
    }
  };

  const size_t chunk_pts = chunk_rows_ * n_;
  std::vector<uint8_t> raw[2] = {std::vector<uint8_t>(chunk_pts * rpp * B),
                                 std::vector<uint8_t>(chunk_pts * rpp * B)};
  auto fill = [&](size_t k, uint8_t *dst) {
    const uint64_t first = static_cast<uint64_t>(k) * chunk_pts * rpp;
    const uint64_t last = std::min<uint64_t>(first + chunk_pts * rpp, layout_.num_records);
    for (uint64_t at = first; at < last;) {
      const uint64_t n = std::min(relay_batch, last - at);
      uint8_t *p = dst + (at - first) * B;
      if (db) db->read_exact(p, n);
      else read_relay(p, n * B);
      if (downstream) {
        const auto t0 = Clock::now();
        downstream->send_frame(FrameType::RelayRecords, 0, p, n * B);
        report_.relay_send_ms += ms_since(t0);
      }
      at += n;
    }
  };

  const auto stream_start = Clock::now();
  auto next = std::async(std::launch::async, fill, 0, raw[0].data());
  for (size_t k = 0; k < chunks; ++k) {
    auto t0 = Clock::now();
    next.get();
    report_.db_wait_ms += ms_since(t0);
    if (k + 1 < chunks)
      next = std::async(std::launch::async, fill, k + 1, raw[(k + 1) % 2].data());
    const size_t rows = std::min(chunk_rows_, real_rows - k * chunk_rows_);
    t0 = Clock::now();
    encode_chunk(raw[k % 2].data(), k * chunk_pts, rows);
    report_.encode_ms += ms_since(t0);
    process_chunk(rows);
  }
  report_.stream_wall_ms = ms_since(stream_start);

  // Remaining subtrees are all-zero padding rows; finish each tree, then
  // mod-switch and reply.
  const auto finish_start = Clock::now();
  const size_t subtrees = (size_t{1} << h_) / chunk_rows_;
  pool_.parallel_for(C, [&](size_t c) {
    Client &cl = *clients_[c];
    const size_t tid = ThreadPool::worker_id();
    for (size_t s = chunks; s < subtrees; ++s) {
      Node zero;
      zero.present = true;
      push_subtree(cl, std::move(zero), chunk_bits_, tid);
    }
    Node &root = cl.stack[h_];
    if (!root.present) throw std::logic_error("selection tree incomplete");
    RlweCt result = root.zero ? zero_ct() : std::move(root.ct);
    if (DBConsts::SmallQWidth < DBConsts::RnsMods[0])
      server_ops::mod_switch_inplace(params_, result, params_.get_small_q());
    std::ostringstream out;
    server_ops::save_resp_to_stream(params_, result, out);
    const std::string s = out.str();
    // Release the client's round state before replying.
    cl.q_lo.reset();
    cl.q_hi.reset();
    std::vector<GSWCt>().swap(cl.selectors);
    std::vector<uint64_t>().swap(cl.inter_lo);
    std::vector<uint64_t>().swap(cl.inter_hi);
    std::vector<Node>().swap(cl.leaves);
    std::vector<Node>().swap(cl.stack);
    cl.reply(std::vector<uint8_t>(s.begin(), s.end()));
  });
  report_.finish_ms = ms_since(finish_start);
  report_.round_wall_ms = ms_since(round_start);
}

void StreamPirServer::print_report(std::ostream &os) const {
  auto ms = [](double v) {
    std::ostringstream s;
    s << std::fixed << std::setprecision(1) << v << " ms";
    return s.str();
  };
  auto cpu = [&](uint64_t us) { return ms(static_cast<double>(us) / 1000.0); };
  os << "=== OnionPIR streaming server timing ===\n"
     << "  Clients: " << clients_.size() << "  threads: " << pool_.size()
     << "  chunk rows: " << chunk_rows_ << " (" << chunk_rows_ * n_
     << " plaintexts/chunk)\n"
     << "  Database streamed: " << report_.records << " records = "
     << std::fixed << std::setprecision(1) << report_.db_mbytes << " MB; "
     << report_.real_rows << " of " << (size_t{1} << h_)
     << " first-dim rows non-padding, " << report_.chunks << " chunks\n"
     << "  Query preprocessing wall (first query -> all ready): "
     << ms(report_.first_query_to_ready_ms) << "\n"
     << "    expansion CPU total (all threads): " << cpu(report_.expand_us) << "\n"
     << "    RGSW conversion CPU total (all threads): " << cpu(report_.convert_us) << "\n"
     << "    first-dim query prep CPU total (all threads): " << cpu(report_.fst_prep_us) << "\n"
     << "  Record source: "
     << (opts_.db_addr.empty() ? "relay from the next server"
                               : "database " + opts_.db_addr + " (database streamer)")
     << "; forwarding to: " << (opts_.relay_to.empty() ? "none" : opts_.relay_to) << "\n"
     << (opts_.db_addr.empty() ? "  Wait for the next server's relay: "
                               : "  Database connect + StreamRecords open: ")
     << ms(report_.db_connect_ms) << "\n"
     << "  Database pass wall (stream + compute): " << ms(report_.stream_wall_ms) << "\n"
     << "    blocked waiting on database stream: " << ms(report_.db_wait_ms) << "\n"
     << "    forwarding to the previous server (overlaps compute): " << ms(report_.relay_send_ms) << "\n"
     << "    record packing + NTT encode: " << ms(report_.encode_ms) << "\n"
     << "    batched first-dim product: " << ms(report_.matmul_ms) << "\n"
     << "    row ciphertexts (CRT + INTT): " << ms(report_.leaves_ms) << "\n"
     << "    selection-tree merges: " << ms(report_.merge_ms) << "\n"
     << "  Finish (padding merges + mod switch + reply): " << ms(report_.finish_ms) << "\n"
     << "  Round wall (all queries ready -> all replies sent): "
     << ms(report_.round_wall_ms) << "\n";
  if (report_.stream_wall_ms > 0)
    os << "  Effective database throughput: " << std::setprecision(1)
       << report_.db_mbytes / (report_.stream_wall_ms / 1000.0) << " MB/s\n";
}

} // namespace stream

// Simulates C concurrent OnionPIR clients against onion_server, mirroring
// Zealot's cmd/testzealotclient: every client picks a random record, all
// query concurrently, and each checks its record against the database's
// plaintext (shuffle's utility.GenerateRecord, via the Go bridge).
//
// Phases:
//   offline  each client generates and uploads its Galois keys and GSW(sk)
//            (Zealot has no equivalent; reported separately)
//   online   query generation + upload -> server round -> decrypt + verify
// "Query-to-result latency" (avg) is the end-to-end latency compared with
// Zealot's metric of the same name: from the start of a client's query
// generation until its record is decrypted and verified.
//
// With several servers (--server A,B,...), the clients are split into
// contiguous chunks, one per server (split_clients in wire.h); each client
// runs the unchanged protocol with its own server only.

#include "args.h"
#include "client.h"
#include "db_bridge.h"
#include "record_codec.h"
#include "thread_pool.h"
#include "wire.h"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <climits>
#include <condition_variable>
#include <cstdio>
#include <iostream>
#include <memory>
#include <mutex>
#include <random>
#include <sstream>
#include <thread>
#include <vector>

using namespace stream;
using Clock = std::chrono::steady_clock;

static const char *kUsage = R"(usage: onion_client --server HOST:PORT --N RECORDS --B RECORD_BYTES --C CLIENTS
                    [--connections K] [--threads T] [--seed S] [--timeout SEC]
                    [--demo-index I]
  --server       onion_server address, or a comma-separated list of servers; client
                 chunks are assigned in list order (server s gets the s-th chunk)
  --N --B --C    database records, bytes per record, total clients (all servers)
  --connections  TCP connections per server, shared by its clients (default 16)
  --threads      client-side worker threads (default: all cores)
  --seed         RNG seed for the queried indices (default: random)
  --timeout      seconds to wait for all results (default 3600)
  --demo-index   client 0 queries this index and prints its record
)";

namespace {

struct SimClient {
  std::unique_ptr<PirClient> pir;
  uint32_t index = 0;
  Clock::time_point offline_start, offline_end, query_start, query_sent, done;
  size_t key_bytes = 0, query_bytes = 0, result_bytes = 0;
  double decrypt_ms = 0;
  int noise_budget = 0;
  bool ok = false;
  std::vector<uint8_t> record;
};

double ms_between(Clock::time_point a, Clock::time_point b) {
  return std::chrono::duration<double, std::milli>(b - a).count();
}

std::string fmt_ms(double ms) {
  char buf[64];
  if (ms >= 1000) std::snprintf(buf, sizeof(buf), "%.3fs", ms / 1000);
  else std::snprintf(buf, sizeof(buf), "%.3fms", ms);
  return buf;
}

double avg(const std::vector<double> &v) {
  double s = 0;
  for (double x : v) s += x;
  return v.empty() ? 0 : s / static_cast<double>(v.size());
}

double p99(std::vector<double> v) {
  if (v.empty()) return 0;
  std::sort(v.begin(), v.end());
  return v[static_cast<size_t>(static_cast<double>(v.size() - 1) * 0.99)];
}

std::string printable(const std::vector<uint8_t> &bytes) {
  std::string s;
  for (uint8_t b : bytes) s += (b >= 32 && b < 127) ? static_cast<char>(b) : '.';
  return s;
}

std::vector<std::string> split_list(const std::string &s) {
  std::vector<std::string> out;
  size_t start = 0;
  for (;;) {
    const size_t comma = s.find(',', start);
    out.push_back(s.substr(start, comma - start));
    if (comma == std::string::npos) break;
    start = comma + 1;
  }
  return out;
}

// One server and the contiguous chunk of global clients it serves.
struct ServerLink {
  std::string addr;
  size_t offset = 0, count = 0;  // global clients [offset, offset + count)
  std::vector<std::unique_ptr<Conn>> conns;
};

} // namespace

int main(int argc, char **argv) {
  Args args(argc, argv, kUsage);
  const std::vector<std::string> server_addrs = split_list(args.str("server"));
  const uint64_t N = args.num("N");
  const uint32_t B = static_cast<uint32_t>(args.num("B"));
  const size_t C = args.num("C");
  const size_t conns_per_server = std::max<uint64_t>(1, args.num("connections", 16));
  const size_t threads = args.num("threads", std::max(1u, std::thread::hardware_concurrency()));
  const uint64_t seed = args.num("seed", std::random_device{}());
  const double timeout_sec = static_cast<double>(args.num("timeout", 3600));
  const int64_t demo_index = static_cast<int64_t>(args.num("demo-index", UINT32_MAX + 1LL));
  args.finish();
  const bool demo = demo_index <= static_cast<int64_t>(UINT32_MAX);

  PirClient::print_noise_budget = false;
  try {
    const RecordLayout layout = RecordLayout::make(N, B);
    const PirParams params(layout.num_real_pt, /*pow2_other_dim=*/true);
    const size_t width = ct_coeff_width(params);
    if (demo && static_cast<uint64_t>(demo_index) >= N)
      throw std::invalid_argument("--demo-index out of range");
    const size_t P = server_addrs.size();
    if (C < P) throw std::invalid_argument("need at least one client per server");

    // ---- split clients across servers and connect (servers may still be starting)
    const std::vector<size_t> offsets = split_clients(C, P);
    std::vector<ServerLink> servers(P);
    std::vector<uint32_t> server_of(C), local_id(C);
    for (size_t s = 0; s < P; ++s) {
      ServerLink &sv = servers[s];
      sv.addr = server_addrs[s];
      sv.offset = offsets[s];
      sv.count = offsets[s + 1] - offsets[s];
      for (size_t i = sv.offset; i < sv.offset + sv.count; ++i) {
        server_of[i] = static_cast<uint32_t>(s);
        local_id[i] = static_cast<uint32_t>(i - sv.offset);
      }
      const ShapeInfo expected{N, B, layout.records_per_pt, params.get_num_pt(),
                               params.get_fst_dim_sz(), params.get_num_dims(), sv.count};
      for (size_t k = 0; k < std::min(conns_per_server, sv.count); ++k) {
        sv.conns.push_back(connect_retry(sv.addr, 600));
        FrameHeader h;
        std::vector<uint8_t> payload;
        if (!sv.conns.back()->recv_frame(h, payload) ||
            static_cast<FrameType>(h.type) != FrameType::Params)
          throw std::runtime_error("server " + sv.addr + " did not send parameters");
        if (!(decode_shape(payload) == expected))
          throw std::runtime_error("server " + sv.addr + " PIR shape differs from the "
                                   "client's (check N, B, C, the server list order and "
                                   "the build config)");
      }
    }
    // Frames carry the client's index within its server's chunk.
    auto conn_of = [&](size_t i) -> Conn & {
      ServerLink &sv = servers[server_of[i]];
      return *sv.conns[local_id[i] % sv.conns.size()];
    };
    std::cout << "Layout: N=" << N << " RecordBytes=" << B
              << " records/plaintext=" << layout.records_per_pt
              << " plaintexts=" << params.get_num_pt()
              << " fst_dim=" << params.get_fst_dim_sz()
              << " num_dims=" << params.get_num_dims() << std::endl;

    std::mt19937_64 rng(seed);
    std::vector<SimClient> clients(C);
    for (auto &cl : clients) {
      cl.index = static_cast<uint32_t>(rng() % N);
      cl.pir = std::make_unique<PirClient>(params);
    }
    if (demo) clients[0].index = static_cast<uint32_t>(demo_index);

    ThreadPool pool(threads);
    std::mutex mu;
    std::condition_variable cv;
    size_t acks = 0, done = 0;
    std::string fatal;

    auto handle_result = [&](uint32_t cid, std::vector<uint8_t> payload) {
      SimClient &cl = clients[cid];
      const auto t0 = Clock::now();
      std::stringstream ss(std::string(payload.begin(), payload.end()));
      RlweCt ct = cl.pir->load_resp_from_stream(ss);
      RlwePt pt = cl.pir->decrypt_mod_q(ct);
      cl.noise_budget = cl.pir->last_noise_budget();
      const size_t off = layout.offset_of(cl.index);
      std::vector<uint8_t> bytes(off + B);
      coeffs_to_bytes(pt.data.data(), bytes.data(), bytes.size());
      cl.record.assign(bytes.begin() + off, bytes.end());
      std::vector<uint8_t> want(B);
      expected_record(cl.index, B, want.data());
      cl.ok = cl.record == want;
      cl.done = Clock::now();
      cl.decrypt_ms = ms_between(t0, cl.done);
      std::lock_guard<std::mutex> lock(mu);
      ++done;
      cv.notify_all();
    };

    // ---- one reader per connection
    for (auto &sv : servers) for (auto &conn : sv.conns) {
      std::thread([&, c = conn.get(), offset = sv.offset, count = sv.count] {
        try {
          FrameHeader h;
          std::vector<uint8_t> payload;
          while (c->recv_frame(h, payload)) {
            if (h.client_id >= count) throw std::runtime_error("bad client id from server");
            const uint32_t gid = static_cast<uint32_t>(offset + h.client_id);
            switch (static_cast<FrameType>(h.type)) {
            case FrameType::KeysAck: {
              clients[gid].offline_end = Clock::now();
              std::lock_guard<std::mutex> lock(mu);
              ++acks;
              cv.notify_all();
              break;
            }
            case FrameType::Result: {
              const uint32_t cid = gid;
              clients[cid].result_bytes = kFrameHeaderBytes + payload.size();
              pool.submit([&, cid, p = std::move(payload)]() mutable {
                try {
                  handle_result(cid, std::move(p));
                } catch (const std::exception &e) {
                  std::lock_guard<std::mutex> lock(mu);
                  fatal = e.what();
                  cv.notify_all();
                }
              });
              payload = {};
              break;
            }
            case FrameType::Error:
              throw std::runtime_error("server error: " +
                                       std::string(payload.begin(), payload.end()));
            default:
              throw std::runtime_error("unexpected frame from server");
            }
          }
        } catch (const std::exception &e) {
          std::lock_guard<std::mutex> lock(mu);
          if (fatal.empty()) fatal = e.what();
          cv.notify_all();
        }
      }).detach();
    }

    auto wait_for = [&](size_t &counter, const char *what) {
      std::unique_lock<std::mutex> lock(mu);
      const bool ok = cv.wait_for(lock, std::chrono::duration<double>(timeout_sec),
                                  [&] { return counter == C || !fatal.empty(); });
      if (!fatal.empty()) throw std::runtime_error(fatal);
      if (!ok) throw std::runtime_error(std::string("timed out waiting for ") + what);
    };

    // ---- offline: key generation + upload
    std::cout << "MILESTONE: offline phase started (key generation + upload)" << std::endl;
    pool.parallel_for(C, [&](size_t i) {
      SimClient &cl = clients[i];
      cl.offline_start = Clock::now();
      ByteWriter w;
      write_galois_keys(w, cl.pir->create_bv_galois_keys(), width);
      write_gsw(w, cl.pir->generate_gsw_from_key(), width);
      cl.key_bytes = conn_of(i).send_frame(FrameType::Keys, local_id[i], w.buf);
    });
    wait_for(acks, "key acknowledgements");
    std::cout << "MILESTONE: offline phase complete" << std::endl;

    // ---- online: all clients query concurrently
    std::cout << "MILESTONE: online phase started" << std::endl;
    pool.parallel_for(C, [&](size_t i) {
      SimClient &cl = clients[i];
      cl.query_start = Clock::now();
      ByteWriter w;
      write_ct(w, cl.pir->fast_generate_query(layout.pt_of(cl.index)), width);
      cl.query_bytes = conn_of(i).send_frame(FrameType::Query, local_id[i], w.buf);
      cl.query_sent = Clock::now();
    });
    std::cout << "MILESTONE: queries uploaded; waiting for results" << std::endl;
    wait_for(done, "results");
    std::cout << "MILESTONE: all results received" << std::endl;
    for (auto &sv : servers) for (auto &conn : sv.conns) conn->shutdown();

    // ---- report (mirrors Zealot's "=== Performance Metrics ===" block)
    std::vector<double> offline, upload, e2e, wait, gen, dec;
    double key_b = 0, query_b = 0, result_b = 0;
    size_t correct = 0;
    int min_budget = INT32_MAX;
    for (const auto &cl : clients) {
      offline.push_back(ms_between(cl.offline_start, cl.offline_end));
      upload.push_back(ms_between(cl.query_start, cl.query_sent));
      e2e.push_back(ms_between(cl.query_start, cl.done));
      dec.push_back(cl.decrypt_ms);
      wait.push_back(ms_between(cl.query_sent, cl.done) - cl.decrypt_ms);
      key_b += static_cast<double>(cl.key_bytes);
      query_b += static_cast<double>(cl.query_bytes);
      result_b += static_cast<double>(cl.result_bytes);
      correct += cl.ok;
      min_budget = std::min(min_budget, cl.noise_budget);
    }
    const double Cd = static_cast<double>(C);
    std::cout << "\n=== Performance Metrics ===\n"
              << "Scheme: OnionPIR (" << P << (P == 1 ? " server" : " servers")
              << ", streamed uncooperative database)\n"
              << "Clients: " << C << "\n";
    if (P > 1) {
      std::cout << "Clients per server:";
      for (const auto &sv : servers) std::cout << " " << sv.count;
      std::cout << "\n";
    }
    std::cout
              << "Layout: N=" << N << " RecordBytes=" << B
              << " records/plaintext=" << layout.records_per_pt
              << " plaintexts=" << params.get_num_pt()
              << " fst_dim=" << params.get_fst_dim_sz()
              << " num_dims=" << params.get_num_dims() << "\n"
              << "Query upload latency       (avg): " << fmt_ms(avg(upload)) << "\n"
              << "Query upload latency       (p99): " << fmt_ms(p99(upload)) << "\n"
              << "Query-to-result latency    (avg): " << fmt_ms(avg(e2e)) << "\n"
              << "Query-to-result latency    (p99): " << fmt_ms(p99(e2e)) << "\n"
              << "Average end-to-end latency      : " << fmt_ms(avg(e2e)) << "\n"
              << "Offline key setup latency  (avg): " << fmt_ms(avg(offline)) << "\n"
              << "Offline key setup latency  (p99): " << fmt_ms(p99(offline)) << "\n"
              << "\n=== Client Timing Breakdown (average) ===\n"
              << "  Query generation + upload: " << fmt_ms(avg(upload)) << "\n"
              << "  Result-pipeline wait: " << fmt_ms(avg(wait)) << "\n"
              << "  Decryption + decode + verify: " << fmt_ms(avg(dec)) << "\n"
              << "\nPer-client bandwidth (bytes sent by client):\n"
              << "  Query upload avg: " << static_cast<uint64_t>(query_b / Cd) << " B\n"
              << "  Offline key upload avg: " << static_cast<uint64_t>(key_b / Cd) << " B\n"
              << "Per-client bandwidth (result bytes received):\n"
              << "  Result download avg: " << static_cast<uint64_t>(result_b / Cd) << " B\n";
    if (demo) {
      std::cout << "\n=== Demo query (client 0) ===\n"
                << "  Index: " << clients[0].index << "\n"
                << "  Result: \"" << printable(clients[0].record) << "\"\n"
                << "  Query-to-result latency: " << fmt_ms(e2e[0]) << "\n"
                << "  Correctness: "
                << (clients[0].ok ? "matches the plaintext record" : "DOES NOT match the plaintext record")
                << "\n";
    }
    std::cout << "\n=== Correctness (indexed store) ===\n"
              << "Minimum noise budget after decryption: " << min_budget << " bits\n";
    if (correct != C) {
      std::cout << "CORRECTNESS FAIL: " << C - correct << "/" << C << " results incorrect" << std::endl;
      std::_Exit(1);
    }
    std::cout << "CORRECTNESS PASS: all " << C << " checked records matched plaintext" << std::endl;
    std::_Exit(0);  // detached reader threads
  } catch (const std::exception &e) {
    std::cerr << "error: " << e.what() << std::endl;
    std::cout.flush();
    std::_Exit(1);  // reader threads may still be running
  }
}

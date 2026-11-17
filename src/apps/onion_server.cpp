// OnionPIR server for a streamed, uncooperative database.
//
// Serves one round of C concurrent clients: collects key material and one
// query per client, streams the Zealot/shuffle database once, and returns
// every client's encrypted record. See src/stream/stream_server.h.
//
// With several servers, each serves its own chunk of clients. Exactly one
// (the database streamer) takes --db; the others receive the records from the
// next server's relay. --relay-to forwards them on to the previous server.

#include "args.h"
#include "record_codec.h"
#include "stream_server.h"
#include "wire.h"

#include <atomic>
#include <chrono>
#include <iostream>
#include <memory>
#include <thread>
#include <unistd.h>

using namespace stream;

static const char *kUsage = R"(usage: onion_server --N RECORDS --B RECORD_BYTES --C CLIENTS
                    [--db HOST:PORT] [--relay-to HOST:PORT] [--relay-timeout SEC]
                    [--listen HOST:PORT] [--threads T] [--chunk-rows M]
                    [--db-batch RECORDS] [--db-timeout SEC]
  --db          uncooperative database (shuffle cmd/testserver), the config's ServerAddr.
                Only the database streamer takes it; without it, the server waits
                for the next server to relay the records
  --relay-to    previous server's --listen address: forward the records to it
  --relay-timeout  seconds to wait for the next server's relay (default 3600)
  --N --B --C   database records, bytes per record (RecordBytes), this server's clients
  --listen      address for clients (default 0.0.0.0:10001)
  --threads     worker threads (default: all cores)
  --chunk-rows  first-dimension rows per streamed chunk, power of two (default 64)
  --db-batch    DBRecordRange.max_records transport hint (default 4096)
)";

int main(int argc, char **argv) {
  Args args(argc, argv, kUsage);
  StreamServerOptions opts;
  opts.db_addr = args.opt("db");
  opts.relay_to = args.opt("relay-to");
  const uint64_t N = args.num("N");
  const uint32_t B = static_cast<uint32_t>(args.num("B"));
  opts.num_clients = args.num("C");
  const std::string listen_addr = args.str("listen", "0.0.0.0:10001");
  opts.threads = args.num("threads", 0);
  opts.chunk_rows = args.num("chunk-rows", 64);
  opts.db_batch_records = static_cast<uint32_t>(args.num("db-batch", 4096));
  opts.db_connect_timeout_sec = static_cast<uint32_t>(args.num("db-timeout", 300));
  opts.relay_timeout_sec = static_cast<uint32_t>(args.num("relay-timeout", 3600));
  args.finish();
  if (N > UINT32_MAX) {
    std::cerr << "N must fit the database's uint32 record indices\n";
    return 2;
  }

  try {
    const RecordLayout layout = RecordLayout::make(N, B);
    const PirParams params(layout.num_real_pt, /*pow2_other_dim=*/true);
    params.print_params();
    StreamPirServer server(params, layout, opts);
    std::cout << "Layout: N=" << N << " RecordBytes=" << B
              << " records/plaintext=" << layout.records_per_pt
              << " plaintexts/record=" << layout.pts_per_record
              << " plaintexts=" << layout.num_real_pt << " (padded "
              << params.get_num_pt() << ")\n"
              << "Server state per client ~" << server.bytes_per_client() / (1 << 20)
              << " MiB; ~" << server.bytes_per_client() * opts.num_clients / (1 << 20)
              << " MiB for C=" << opts.num_clients << "\n";
    const double phys = static_cast<double>(sysconf(_SC_PHYS_PAGES)) *
                        static_cast<double>(sysconf(_SC_PAGE_SIZE));
    if (phys > 0 && static_cast<double>(server.bytes_per_client() * opts.num_clients) > 0.8 * phys)
      std::cout << "WARNING: estimated client state exceeds 80% of physical memory ("
                << static_cast<uint64_t>(phys / (1 << 20)) << " MiB); expect swapping or OOM\n";

    const ShapeInfo shape{N, B, layout.records_per_pt, params.get_num_pt(),
                          params.get_fst_dim_sz(), params.get_num_dims(),
                          opts.num_clients};
    const size_t width = ct_coeff_width(params);
    const int listen_fd = listen_on(listen_addr);
    std::cout << "OnionPIR server listening on " << listen_addr << ", waiting for "
              << opts.num_clients << " clients; records from "
              << (opts.db_addr.empty() ? "the next server's relay" : opts.db_addr)
              << (opts.relay_to.empty() ? "" : ", relaying to " + opts.relay_to)
              << std::endl;

    std::atomic<size_t> open_conns{0};
    std::thread acceptor([&] {
      for (;;) {
        std::shared_ptr<Conn> conn = accept_conn(listen_fd);
        ++open_conns;
        conn->send_frame(FrameType::Params, 0, encode_shape(shape));
        std::thread([&, conn] {
          try {
            FrameHeader h;
            std::vector<uint8_t> payload;
            bool relay = false;
            while (!relay && conn->recv_frame(h, payload)) {
              ByteReader r(payload.data(), payload.size());
              switch (static_cast<FrameType>(h.type)) {
              case FrameType::Keys: {
                auto galois = read_galois_keys(r, width);
                auto gsw = read_gsw(r, width);
                server.add_keys(h.client_id, std::move(galois), std::move(gsw));
                conn->send_frame(FrameType::KeysAck, h.client_id, nullptr, 0);
                break;
              }
              case FrameType::Query: {
                const uint32_t cid = h.client_id;
                server.add_query(cid, read_ct(r, width),
                                 [conn, cid](const std::vector<uint8_t> &resp) {
                                   conn->send_frame(FrameType::Result, cid, resp);
                                 });
                break;
              }
              case FrameType::RelayOpen:
                // The rest of this connection is the database relay, read by
                // the round itself.
                server.attach_relay(conn);
                relay = true;
                break;
              default:
                throw std::runtime_error("unexpected frame type " + std::to_string(h.type));
              }
            }
          } catch (const std::exception &e) {
            std::cerr << "connection error: " << e.what() << std::endl;
            std::exit(1);
          }
          --open_conns;
        }).detach();
      }
    });
    acceptor.detach();

    server.run_round();
    std::cout << "MILESTONE: all responses sent" << std::endl;
    server.print_report(std::cout);
    std::cout.flush();

    // Let clients drain their sockets and disconnect before exiting.
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(60);
    while (open_conns > 0 && std::chrono::steady_clock::now() < deadline)
      std::this_thread::sleep_for(std::chrono::milliseconds(50));
    std::_Exit(0);  // detached acceptor is blocked in accept()
  } catch (const std::exception &e) {
    std::cerr << "error: " << e.what() << std::endl;
    return 1;
  }
}

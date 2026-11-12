#pragma once
// Client <-> OnionPIR server transport: length-prefixed frames over TCP, and
// bit-packed serialization of the key material, queries, and responses.
//
// Many simulated clients share a few connections (like Zealot's clients
// share gRPC channels); every frame carries the logical client id.

#include "bv_keyswitch.h"
#include "gsw.h"
#include "pir.h"
#include "rlwe.h"
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

namespace stream {

enum class FrameType : uint32_t {
  Params = 1,   // server -> client on connect: layout + PIR shape
  Keys = 2,     // client -> server: Galois keys + GSW(sk)   (offline)
  KeysAck = 3,  // server -> client
  Query = 4,    // client -> server: packed query ciphertext (online)
  Result = 5,   // server -> client: mod-switched response
  Error = 6,    // either direction: UTF-8 message
};

struct FrameHeader {
  uint32_t magic;
  uint32_t type;
  uint32_t client_id;
  uint32_t reserved;
  uint64_t length;
};
constexpr uint32_t kFrameMagic = 0x4f4e494f;  // "OINO"
constexpr size_t kFrameHeaderBytes = sizeof(FrameHeader);

// One TCP connection. send_frame is thread-safe; recv_frame must be called
// from a single reader thread.
class Conn {
public:
  explicit Conn(int fd);
  ~Conn();
  Conn(const Conn &) = delete;
  Conn &operator=(const Conn &) = delete;

  // Returns total bytes written (header + payload).
  size_t send_frame(FrameType type, uint32_t client_id, const void *payload,
                    size_t length);
  size_t send_frame(FrameType type, uint32_t client_id,
                    const std::vector<uint8_t> &payload) {
    return send_frame(type, client_id, payload.data(), payload.size());
  }
  // False on clean EOF before a header; throws on errors.
  bool recv_frame(FrameHeader &header, std::vector<uint8_t> &payload);
  void shutdown();

private:
  int fd_;
  std::mutex send_mu_;
};

// "host:port" helpers. connect_retry keeps trying until timeout_sec elapses.
std::pair<std::string, uint16_t> split_host_port(const std::string &addr);
std::unique_ptr<Conn> connect_retry(const std::string &addr, double timeout_sec);
int listen_on(const std::string &addr);
std::unique_ptr<Conn> accept_conn(int listen_fd);

// ---------------------------------------------------------------- serialization
class ByteWriter {
public:
  std::vector<uint8_t> buf;
  void u32(uint32_t v);
  void u64(uint64_t v);
  // `count` values, each < 2^width, packed LSB-first.
  void packed(const uint64_t *values, size_t count, size_t width);
};

class ByteReader {
public:
  ByteReader(const uint8_t *data, size_t size) : p_(data), end_(data + size) {}
  uint32_t u32();
  uint64_t u64();
  void packed(uint64_t *values, size_t count, size_t width);
  bool done() const { return p_ == end_; }

private:
  void need(size_t n) const;
  const uint8_t *p_;
  const uint8_t *end_;
};

// Bits needed for any residue under the ciphertext moduli.
size_t ct_coeff_width(const PirParams &params);

void write_ct(ByteWriter &w, const RlweCt &ct, size_t width);
RlweCt read_ct(ByteReader &r, size_t width);
void write_gsw(ByteWriter &w, const GSWCt &gsw, size_t width);
GSWCt read_gsw(ByteReader &r, size_t width);
void write_galois_keys(ByteWriter &w, const bvks::BvGaloisKeys &keys, size_t width);
bvks::BvGaloisKeys read_galois_keys(ByteReader &r, size_t width);

// Params frame: what both sides derived from (N, B). Clients refuse to run
// against a server whose shape differs.
struct ShapeInfo {
  uint64_t num_records, record_bytes, records_per_pt, num_pt, fst_dim_sz,
      num_dims, num_clients;
  bool operator==(const ShapeInfo &) const = default;
};
std::vector<uint8_t> encode_shape(const ShapeInfo &s);
ShapeInfo decode_shape(const std::vector<uint8_t> &payload);

} // namespace stream

#include "wire.h"

#include <arpa/inet.h>
#include <bit>
#include <cerrno>
#include <chrono>
#include <cstring>
#include <netdb.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <stdexcept>
#include <sys/socket.h>
#include <thread>
#include <unistd.h>

namespace stream {

static_assert(std::endian::native == std::endian::little,
              "frames are written in native (little-endian) byte order");

namespace {

std::runtime_error sys_error(const std::string &what) {
  return std::runtime_error(what + ": " + std::strerror(errno));
}

void set_socket_options(int fd) {
  int one = 1;
  setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
  int buf = 8 << 20;
  setsockopt(fd, SOL_SOCKET, SO_SNDBUF, &buf, sizeof(buf));
  setsockopt(fd, SOL_SOCKET, SO_RCVBUF, &buf, sizeof(buf));
#ifdef SO_NOSIGPIPE
  setsockopt(fd, SOL_SOCKET, SO_NOSIGPIPE, &one, sizeof(one));
#endif
}

void write_all(int fd, const void *data, size_t len) {
  const char *p = static_cast<const char *>(data);
  while (len > 0) {
#ifdef MSG_NOSIGNAL
    const ssize_t n = ::send(fd, p, len, MSG_NOSIGNAL);
#else
    const ssize_t n = ::send(fd, p, len, 0);
#endif
    if (n < 0) {
      if (errno == EINTR) continue;
      throw sys_error("send");
    }
    p += n;
    len -= static_cast<size_t>(n);
  }
}

// Returns false on EOF before any byte was read.
bool read_all(int fd, void *data, size_t len) {
  char *p = static_cast<char *>(data);
  size_t got = 0;
  while (got < len) {
    const ssize_t n = ::recv(fd, p + got, len - got, 0);
    if (n == 0) {
      if (got == 0) return false;
      throw std::runtime_error("connection closed mid-frame");
    }
    if (n < 0) {
      if (errno == EINTR) continue;
      throw sys_error("recv");
    }
    got += static_cast<size_t>(n);
  }
  return true;
}

addrinfo *resolve(const std::string &host, uint16_t port, bool passive) {
  addrinfo hints{};
  hints.ai_family = AF_INET;
  hints.ai_socktype = SOCK_STREAM;
  if (passive) hints.ai_flags = AI_PASSIVE;
  addrinfo *res = nullptr;
  const std::string port_s = std::to_string(port);
  const int rc = getaddrinfo(host.empty() ? nullptr : host.c_str(),
                             port_s.c_str(), &hints, &res);
  if (rc != 0)
    throw std::runtime_error("getaddrinfo(" + host + "): " + gai_strerror(rc));
  return res;
}

} // namespace

Conn::Conn(int fd) : fd_(fd) { set_socket_options(fd_); }
Conn::~Conn() { ::close(fd_); }

size_t Conn::send_frame(FrameType type, uint32_t client_id, const void *payload,
                        size_t length) {
  FrameHeader h{kFrameMagic, static_cast<uint32_t>(type), client_id, 0, length};
  std::lock_guard<std::mutex> lock(send_mu_);
  write_all(fd_, &h, sizeof(h));
  if (length) write_all(fd_, payload, length);
  return sizeof(h) + length;
}

bool Conn::recv_frame(FrameHeader &header, std::vector<uint8_t> &payload) {
  if (!read_all(fd_, &header, sizeof(header))) return false;
  if (header.magic != kFrameMagic) throw std::runtime_error("bad frame magic");
  payload.resize(header.length);
  if (header.length && !read_all(fd_, payload.data(), header.length))
    throw std::runtime_error("connection closed mid-frame");
  return true;
}

void Conn::shutdown() { ::shutdown(fd_, SHUT_RDWR); }

std::pair<std::string, uint16_t> split_host_port(const std::string &addr) {
  const auto colon = addr.rfind(':');
  if (colon == std::string::npos)
    throw std::invalid_argument("expected host:port, got '" + addr + "'");
  return {addr.substr(0, colon),
          static_cast<uint16_t>(std::stoul(addr.substr(colon + 1)))};
}

std::unique_ptr<Conn> connect_retry(const std::string &addr, double timeout_sec) {
  const auto [host, port] = split_host_port(addr);
  const auto deadline = std::chrono::steady_clock::now() +
                        std::chrono::duration<double>(timeout_sec);
  for (;;) {
    addrinfo *res = resolve(host, port, false);
    const int fd = ::socket(res->ai_family, res->ai_socktype, res->ai_protocol);
    if (fd < 0) {
      freeaddrinfo(res);
      throw sys_error("socket");
    }
    const int rc = ::connect(fd, res->ai_addr, res->ai_addrlen);
    freeaddrinfo(res);
    if (rc == 0) return std::make_unique<Conn>(fd);
    const int err = errno;
    ::close(fd);
    if (std::chrono::steady_clock::now() > deadline) {
      errno = err;
      throw sys_error("connect " + addr);
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
  }
}

int listen_on(const std::string &addr) {
  const auto [host, port] = split_host_port(addr);
  addrinfo *res = resolve(host, port, true);
  const int fd = ::socket(res->ai_family, res->ai_socktype, res->ai_protocol);
  if (fd < 0) {
    freeaddrinfo(res);
    throw sys_error("socket");
  }
  int one = 1;
  setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
  if (::bind(fd, res->ai_addr, res->ai_addrlen) != 0) {
    freeaddrinfo(res);
    ::close(fd);
    throw sys_error("bind " + addr);
  }
  freeaddrinfo(res);
  if (::listen(fd, 1024) != 0) throw sys_error("listen");
  return fd;
}

std::unique_ptr<Conn> accept_conn(int listen_fd) {
  for (;;) {
    const int fd = ::accept(listen_fd, nullptr, nullptr);
    if (fd >= 0) return std::make_unique<Conn>(fd);
    if (errno == EINTR) continue;
    throw sys_error("accept");
  }
}

// ---------------------------------------------------------------- serialization
void ByteWriter::u32(uint32_t v) {
  const auto *p = reinterpret_cast<const uint8_t *>(&v);
  buf.insert(buf.end(), p, p + 4);
}
void ByteWriter::u64(uint64_t v) {
  const auto *p = reinterpret_cast<const uint8_t *>(&v);
  buf.insert(buf.end(), p, p + 8);
}
void ByteWriter::packed(const uint64_t *values, size_t count, size_t width) {
  const size_t start = buf.size();
  buf.resize(start + (count * width + 7) / 8, 0);
  uint8_t *out = buf.data() + start;
  const uint64_t mask = width == 64 ? ~uint64_t{0} : (uint64_t{1} << width) - 1;
  unsigned __int128 acc = 0;
  size_t acc_bits = 0;
  for (size_t i = 0; i < count; ++i) {
    acc |= static_cast<unsigned __int128>(values[i] & mask) << acc_bits;
    acc_bits += width;
    while (acc_bits >= 8) {
      *out++ = static_cast<uint8_t>(acc);
      acc >>= 8;
      acc_bits -= 8;
    }
  }
  if (acc_bits) *out = static_cast<uint8_t>(acc);
}

void ByteReader::need(size_t n) const {
  if (static_cast<size_t>(end_ - p_) < n)
    throw std::runtime_error("truncated message");
}
uint32_t ByteReader::u32() {
  need(4);
  uint32_t v;
  std::memcpy(&v, p_, 4);
  p_ += 4;
  return v;
}
uint64_t ByteReader::u64() {
  need(8);
  uint64_t v;
  std::memcpy(&v, p_, 8);
  p_ += 8;
  return v;
}
void ByteReader::packed(uint64_t *values, size_t count, size_t width) {
  const size_t bytes = (count * width + 7) / 8;
  need(bytes);
  const uint64_t mask = width == 64 ? ~uint64_t{0} : (uint64_t{1} << width) - 1;
  unsigned __int128 acc = 0;
  size_t acc_bits = 0;
  const uint8_t *in = p_;
  for (size_t i = 0; i < count; ++i) {
    while (acc_bits < width) {
      acc |= static_cast<unsigned __int128>(*in++) << acc_bits;
      acc_bits += 8;
    }
    values[i] = static_cast<uint64_t>(acc) & mask;
    acc >>= width;
    acc_bits -= width;
  }
  p_ += bytes;
}

size_t ct_coeff_width(const PirParams &params) {
  uint64_t max_q = 0;
  for (uint64_t q : params.get_rns_mods()) max_q = std::max(max_q, q);
  return static_cast<size_t>(std::bit_width(max_q - 1));
}

static void write_vec(ByteWriter &w, const std::vector<uint64_t> &v, size_t width) {
  w.u32(static_cast<uint32_t>(v.size()));
  w.packed(v.data(), v.size(), width);
}
static std::vector<uint64_t> read_vec(ByteReader &r, size_t width) {
  std::vector<uint64_t> v(r.u32());
  r.packed(v.data(), v.size(), width);
  return v;
}

void write_ct(ByteWriter &w, const RlweCt &ct, size_t width) {
  w.u32(ct.ntt_form ? 1 : 0);
  write_vec(w, ct.c0, width);
  write_vec(w, ct.c1, width);
}
RlweCt read_ct(ByteReader &r, size_t width) {
  RlweCt ct;
  ct.ntt_form = r.u32() != 0;
  ct.c0 = read_vec(r, width);
  ct.c1 = read_vec(r, width);
  return ct;
}

void write_gsw(ByteWriter &w, const GSWCt &gsw, size_t width) {
  w.u32(static_cast<uint32_t>(gsw.size()));
  for (const auto &row : gsw) write_vec(w, row, width);
}
GSWCt read_gsw(ByteReader &r, size_t width) {
  GSWCt gsw(r.u32());
  for (auto &row : gsw) row = read_vec(r, width);
  return gsw;
}

void write_galois_keys(ByteWriter &w, const bvks::BvGaloisKeys &keys, size_t width) {
  w.u32(static_cast<uint32_t>(keys.keys.size()));
  for (const auto &key : keys.keys) {
    w.u32(key.galois_k);
    w.u32(static_cast<uint32_t>(key.cts.size()));
    for (const auto &ct : key.cts) {
      write_vec(w, ct.a, width);
      write_vec(w, ct.b, width);
    }
  }
}
bvks::BvGaloisKeys read_galois_keys(ByteReader &r, size_t width) {
  bvks::BvGaloisKeys keys;
  keys.keys.resize(r.u32());
  for (auto &key : keys.keys) {
    key.galois_k = r.u32();
    key.cts.resize(r.u32());
    for (auto &ct : key.cts) {
      ct.a = read_vec(r, width);
      ct.b = read_vec(r, width);
    }
  }
  return keys;
}

std::vector<uint8_t> encode_shape(const ShapeInfo &s) {
  ByteWriter w;
  for (uint64_t v : {s.num_records, s.record_bytes, s.records_per_pt, s.num_pt,
                     s.fst_dim_sz, s.num_dims, s.num_clients})
    w.u64(v);
  return std::move(w.buf);
}
ShapeInfo decode_shape(const std::vector<uint8_t> &payload) {
  ByteReader r(payload.data(), payload.size());
  ShapeInfo s;
  s.num_records = r.u64();
  s.record_bytes = r.u64();
  s.records_per_pt = r.u64();
  s.num_pt = r.u64();
  s.fst_dim_sz = r.u64();
  s.num_dims = r.u64();
  s.num_clients = r.u64();
  return s;
}

} // namespace stream

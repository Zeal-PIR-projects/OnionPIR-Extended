#pragma once
// Maps the uncooperative database's flat records onto OnionPIR plaintexts.
//
// The database (shuffle/Zealot) serves N fixed-size records of B bytes. A
// plaintext polynomial carries (PlainMod - 1) bits per coefficient, i.e.
// plaintext_bytes() bytes (ptb).
//
// B <= ptb: we pack floor(ptb / B) whole records per plaintext, in index order:
//   record r  ->  plaintext r / records_per_pt, byte offset (r % rpp) * B
// Records never straddle plaintexts, so one PIR query retrieves one record.
//
// B > ptb: each record spans k = ceil(B / ptb) plaintexts ("planes"). Record r
// is PIR index r in every plane; plane j holds its bytes [j*ptb, (j+1)*ptb).
// The server evaluates the same query against all k planes and returns k
// ciphertexts (OnionPIRv2 paper, Sec. 4.3).

#include "database_constants.h"
#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <stdexcept>
#include <string>

namespace stream {

constexpr size_t plaintext_bits_per_coeff() { return DBConsts::PlainMod - 1; }
constexpr size_t plaintext_bytes() {
  return plaintext_bits_per_coeff() * DBConsts::PolyDegree / 8;
}

struct RecordLayout {
  uint64_t num_records = 0;     // N
  uint32_t record_bytes = 0;    // B
  size_t records_per_pt = 0;    // floor(plaintext_bytes / B), or 1 if B > ptb
  size_t pts_per_record = 1;    // k: planes per record, ceil(B / ptb) if B > ptb
  size_t num_real_pt = 0;       // ceil(N / records_per_pt), per plane

  static RecordLayout make(uint64_t num_records, uint32_t record_bytes) {
    if (num_records == 0 || record_bytes == 0)
      throw std::invalid_argument("N and RecordBytes must be positive");
    RecordLayout l;
    l.num_records = num_records;
    l.record_bytes = record_bytes;
    if (record_bytes <= plaintext_bytes()) {
      l.records_per_pt = plaintext_bytes() / record_bytes;
    } else {
      l.records_per_pt = 1;
      l.pts_per_record = (record_bytes + plaintext_bytes() - 1) / plaintext_bytes();
    }
    l.num_real_pt = (num_records + l.records_per_pt - 1) / l.records_per_pt;
    return l;
  }
  size_t pt_of(uint64_t record) const { return record / records_per_pt; }
  size_t offset_of(uint64_t record) const {
    return (record % records_per_pt) * record_bytes;
  }
  // Records stored in plaintext `pt` (the last real plaintext may be partial).
  size_t records_in_pt(size_t pt) const {
    const uint64_t first = static_cast<uint64_t>(pt) * records_per_pt;
    if (first >= num_records) return 0;
    const uint64_t left = num_records - first;
    return left < records_per_pt ? static_cast<size_t>(left) : records_per_pt;
  }
  // Bytes of plaintext `pt` in `plane` (0 for padding past the last record).
  size_t segment_bytes(size_t pt, size_t plane) const {
    const size_t records = records_in_pt(pt);
    if (pts_per_record == 1) return records * record_bytes;
    if (records == 0) return 0;
    const size_t start = plane * plaintext_bytes();
    return std::min(plaintext_bytes(), record_bytes - start);
  }
};

// Little-endian bitstream packing: coefficient c holds bits [c*w, (c+1)*w) of
// `bytes` (zero-padded), w = plaintext_bits_per_coeff(). Every coefficient is
// < 2^w < plain_mod, so it is a valid plaintext value.
void bytes_to_coeffs(const uint8_t *bytes, size_t num_bytes, uint64_t *coeffs);
// Inverse: recover the first num_bytes bytes from N coefficients.
void coeffs_to_bytes(const uint64_t *coeffs, uint8_t *bytes, size_t num_bytes);

} // namespace stream

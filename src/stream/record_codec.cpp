#include "record_codec.h"

namespace stream {

void bytes_to_coeffs(const uint8_t *bytes, size_t num_bytes, uint64_t *coeffs) {
  constexpr size_t w = plaintext_bits_per_coeff();
  constexpr uint64_t mask = (uint64_t{1} << w) - 1;
  uint64_t acc = 0;
  size_t acc_bits = 0, byte = 0;
  for (size_t c = 0; c < DBConsts::PolyDegree; ++c) {
    while (acc_bits < w) {
      const uint64_t next = byte < num_bytes ? bytes[byte] : 0;
      acc |= next << acc_bits;
      acc_bits += 8;
      ++byte;
    }
    coeffs[c] = acc & mask;
    acc >>= w;
    acc_bits -= w;
  }
}

void coeffs_to_bytes(const uint64_t *coeffs, uint8_t *bytes, size_t num_bytes) {
  constexpr size_t w = plaintext_bits_per_coeff();
  constexpr uint64_t mask = (uint64_t{1} << w) - 1;
  uint64_t acc = 0;
  size_t acc_bits = 0, c = 0;
  for (size_t b = 0; b < num_bytes; ++b) {
    while (acc_bits < 8) {
      acc |= (coeffs[c++] & mask) << acc_bits;
      acc_bits += w;
    }
    bytes[b] = static_cast<uint8_t>(acc);
    acc >>= 8;
    acc_bits -= 8;
  }
}

} // namespace stream

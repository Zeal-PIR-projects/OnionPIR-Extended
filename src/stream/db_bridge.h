#pragma once
// C++ side of the Go database bridge (dbbridge/, linked as a cgo c-archive).
// The bridge is a client of the unmodified Zealot/shuffle database service:
// it calls Database.StreamRecords over gRPC and returns raw record bytes.

#include <cstdint>
#include <string>

namespace stream {

class DbRecordStream {
public:
  // Requests records [start, end) from the database at `addr` (host:port).
  // max_records is the DBRecordRange batch hint (the database also caps a
  // batch at 4 MiB). Waits up to connect_timeout_sec for the database.
  DbRecordStream(const std::string &addr, uint32_t start, uint32_t end,
                 uint32_t max_records, uint32_t record_bytes,
                 uint32_t connect_timeout_sec);
  ~DbRecordStream();
  DbRecordStream(const DbRecordStream &) = delete;
  DbRecordStream &operator=(const DbRecordStream &) = delete;

  // Fill dst with exactly `count` records (blocking). Throws if the database
  // stream fails or ends early.
  void read_exact(uint8_t *dst, uint64_t count);

private:
  int64_t handle_;
  uint32_t record_bytes_;
};

// The database's plaintext record (shuffle's utility.GenerateRecord), for
// verifying retrieved records.
void expected_record(uint32_t index, uint32_t record_bytes, uint8_t *dst);

} // namespace stream

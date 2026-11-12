package main

// C ABI for the OnionPIR server and client (built with -buildmode=c-archive;
// the generated libdbbridge.h declares these). Errors are written into a
// caller-supplied buffer as a NUL-terminated string.

/*
#include <stddef.h>
#include <stdint.h>
*/
import "C"

import (
	"errors"
	"sync"
	"time"
	"unsafe"

	"github.com/Zeal-PIR-projects/shuffle/utility"
)

var (
	streamsMu  sync.Mutex
	streams    = map[int64]*recordStream{}
	nextHandle int64

	errUnknownHandle = errors.New("unknown database stream handle")
)

func writeErr(err error, buf *C.char, length C.size_t) {
	if buf == nil || length == 0 {
		return
	}
	msg := err.Error()
	dst := unsafe.Slice((*byte)(unsafe.Pointer(buf)), int(length))
	n := copy(dst[:len(dst)-1], msg)
	dst[n] = 0
}

func lookup(handle C.int64_t) *recordStream {
	streamsMu.Lock()
	defer streamsMu.Unlock()
	return streams[int64(handle)]
}

// DBStreamOpen connects to the uncooperative database at addr ("host:port")
// and requests records [start, end). Returns a positive handle, or -1.
//
//export DBStreamOpen
func DBStreamOpen(addr *C.char, start, end, maxRecords, recordBytes C.uint32_t,
	connectTimeoutSec C.uint32_t, errBuf *C.char, errLen C.size_t) C.int64_t {
	s, err := openStream(C.GoString(addr), uint32(start), uint32(end), uint32(maxRecords),
		int(recordBytes), time.Duration(connectTimeoutSec)*time.Second)
	if err != nil {
		writeErr(err, errBuf, errLen)
		return -1
	}
	streamsMu.Lock()
	defer streamsMu.Unlock()
	nextHandle++
	streams[nextHandle] = s
	return C.int64_t(nextHandle)
}

// DBStreamRead blocks until records are available and copies up to
// maxRecords of them (record_bytes each) into dst. Returns the number of
// records copied, 0 at the end of the range, or -1 on error.
//
//export DBStreamRead
func DBStreamRead(handle C.int64_t, dst *C.uint8_t, maxRecords C.uint64_t,
	errBuf *C.char, errLen C.size_t) C.int64_t {
	s := lookup(handle)
	if s == nil {
		writeErr(errUnknownHandle, errBuf, errLen)
		return -1
	}
	buf := unsafe.Slice((*byte)(unsafe.Pointer(dst)), int(maxRecords)*s.recordBytes)
	n, err := s.read(buf)
	if err != nil {
		writeErr(err, errBuf, errLen)
		return -1
	}
	return C.int64_t(n)
}

//export DBStreamClose
func DBStreamClose(handle C.int64_t) {
	streamsMu.Lock()
	s := streams[int64(handle)]
	delete(streams, int64(handle))
	streamsMu.Unlock()
	if s != nil {
		s.close()
	}
}

// DBExpectedRecord writes the database's plaintext record `index` (exactly
// what the shuffle database serves, via utility.GenerateRecord) into dst,
// which must hold recordBytes bytes. Clients use it to verify retrievals.
//
//export DBExpectedRecord
func DBExpectedRecord(index, recordBytes C.uint32_t, dst *C.uint8_t) {
	record := utility.GenerateRecord(uint32(index), uint32(recordBytes))
	copy(unsafe.Slice((*byte)(unsafe.Pointer(dst)), int(recordBytes)), record)
}

func main() {}

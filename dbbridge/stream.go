package main

// Pure-Go core of the database bridge. It is a client of the unmodified
// uncooperative database from the Zealot/shuffle repository: it calls
// Database.StreamRecords for a flat record range and turns each DBRecordBatch
// (three record bytes per uint32 limb) back into raw record bytes. The cgo
// exports in export.go are thin wrappers around this file.

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"sync"
	"time"

	"github.com/Zeal-PIR-projects/shuffle/proto_gen"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
)

// Batches decoded ahead of the C++ consumer. The database caps a batch at
// 4 MiB of logical payload, so this bounds read-ahead to roughly 128 MiB.
const readAheadBatches = 32

type recordStream struct {
	conn        *grpc.ClientConn
	cancel      context.CancelFunc
	batches     chan []byte
	recordBytes int
	total       uint64 // records requested
	delivered   uint64 // records handed to the consumer
	pending     []byte // undelivered tail of the current batch

	errMu sync.Mutex
	err   error
}

func (s *recordStream) setErr(err error) {
	s.errMu.Lock()
	defer s.errMu.Unlock()
	if s.err == nil {
		s.err = err
	}
}

func (s *recordStream) getErr() error {
	s.errMu.Lock()
	defer s.errMu.Unlock()
	return s.err
}

func recordWords(recordBytes int) int { return (recordBytes + 2) / 3 }

// unpackRecords decodes count records of recordBytes bytes from the packed
// limb layout of utility.PackRecord (byte i of a record lives in bits
// 8*(i%3) of word i/3). Equivalent to calling utility.UnpackRecord per
// record, without the per-record allocation.
func unpackRecords(words []uint32, count, recordBytes int, out []byte) {
	wpr := recordWords(recordBytes)
	full := recordBytes / 3
	for r := 0; r < count; r++ {
		ws := words[r*wpr : (r+1)*wpr]
		dst := out[r*recordBytes : (r+1)*recordBytes]
		for w := 0; w < full; w++ {
			v := ws[w]
			dst[3*w] = byte(v)
			dst[3*w+1] = byte(v >> 8)
			dst[3*w+2] = byte(v >> 16)
		}
		for i := full * 3; i < recordBytes; i++ {
			dst[i] = byte(ws[i/3] >> uint(8*(i%3)))
		}
	}
}

func waitForReady(ctx context.Context, conn *grpc.ClientConn) error {
	conn.Connect()
	for state := conn.GetState(); state != connectivity.Ready; state = conn.GetState() {
		if !conn.WaitForStateChange(ctx, state) {
			return fmt.Errorf("database %s not ready: %w", conn.Target(), ctx.Err())
		}
	}
	return nil
}

// openStream requests records [start, end) and starts decoding them in the
// background. maxRecords is the DBRecordRange transport hint.
func openStream(addr string, start, end, maxRecords uint32, recordBytes int,
	connectTimeout time.Duration) (*recordStream, error) {
	if recordBytes <= 0 {
		return nil, errors.New("record_bytes must be positive")
	}
	if end <= start {
		return nil, fmt.Errorf("empty record range [%d, %d)", start, end)
	}
	conn, err := grpc.NewClient(addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		// A 4 MiB logical batch is up to ~5 bytes per varint limb on the wire.
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(math.MaxInt32)))
	if err != nil {
		return nil, err
	}
	readyCtx, readyCancel := context.WithTimeout(context.Background(), connectTimeout)
	defer readyCancel()
	if err := waitForReady(readyCtx, conn); err != nil {
		conn.Close()
		return nil, err
	}

	ctx, cancel := context.WithCancel(context.Background())
	rs, err := proto_gen.NewDatabaseClient(conn).StreamRecords(ctx, &proto_gen.DBRecordRange{
		StartIndex: start, EndIndex: end, MaxRecords: maxRecords,
	})
	if err != nil {
		cancel()
		conn.Close()
		return nil, err
	}

	s := &recordStream{
		conn: conn, cancel: cancel, batches: make(chan []byte, readAheadBatches),
		recordBytes: recordBytes, total: uint64(end - start),
	}
	wpr := recordWords(recordBytes)
	go func() {
		defer close(s.batches)
		next := start
		for {
			batch, err := rs.Recv()
			if err == io.EOF {
				return
			}
			if err != nil {
				s.setErr(err)
				return
			}
			if batch.StartIndex != next {
				s.setErr(fmt.Errorf("database batch starts at %d, expected %d", batch.StartIndex, next))
				return
			}
			if len(batch.Values)%wpr != 0 {
				s.setErr(fmt.Errorf("database batch has %d limbs, not a multiple of %d per record", len(batch.Values), wpr))
				return
			}
			count := len(batch.Values) / wpr
			buf := make([]byte, count*recordBytes)
			unpackRecords(batch.Values, count, recordBytes, buf)
			next += uint32(count)
			select {
			case s.batches <- buf:
			case <-ctx.Done():
				return
			}
		}
	}()
	return s, nil
}

// read copies up to len(dst)/recordBytes whole records into dst, blocking
// until at least one is available. It returns 0 at the end of the range.
func (s *recordStream) read(dst []byte) (int, error) {
	if len(s.pending) == 0 {
		batch, ok := <-s.batches
		if !ok {
			if err := s.getErr(); err != nil {
				return 0, err
			}
			if s.delivered != s.total {
				return 0, fmt.Errorf("database stream ended after %d of %d records", s.delivered, s.total)
			}
			return 0, nil
		}
		s.pending = batch
	}
	count := min(len(dst)/s.recordBytes, len(s.pending)/s.recordBytes)
	if count == 0 {
		return 0, errors.New("destination buffer smaller than one record")
	}
	n := count * s.recordBytes
	copy(dst[:n], s.pending[:n])
	s.pending = s.pending[n:]
	s.delivered += uint64(count)
	return count, nil
}

func (s *recordStream) close() {
	s.cancel()
	// Drain so the receive goroutine can observe cancellation and exit.
	for range s.batches {
	}
	s.conn.Close()
}

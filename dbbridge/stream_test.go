package main

import (
	"bytes"
	"fmt"
	"math/rand"
	"net"
	"testing"
	"time"

	"github.com/Zeal-PIR-projects/shuffle/database"
	"github.com/Zeal-PIR-projects/shuffle/utility"
)

func TestUnpackMatchesShuffle(t *testing.T) {
	for _, recordBytes := range []int{1, 2, 3, 4, 5, 16, 1000, 1001} {
		const count = 7
		var words []uint32
		var want []byte
		for r := 0; r < count; r++ {
			record := make([]byte, recordBytes)
			rand.Read(record)
			want = append(want, record...)
			words = append(words, utility.PackRecord(record)...)
		}
		got := make([]byte, count*recordBytes)
		unpackRecords(words, count, recordBytes, got)
		if !bytes.Equal(got, want) {
			t.Fatalf("recordBytes=%d: unpack mismatch", recordBytes)
		}
	}
}

func freeAddr(t *testing.T) string {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return l.Addr().String()
}

// Streams from the real shuffle database server, in process.
func TestStreamFromShuffleDatabase(t *testing.T) {
	for _, recordBytes := range []uint32{4, 1000} {
		t.Run(fmt.Sprint(recordBytes), func(t *testing.T) {
			cfg := &utility.Config{N: 5000, RecordBytes: recordBytes, ServerAddr: freeAddr(t)}
			db, err := database.DatabaseFromGeneratedRecords(cfg)
			if err != nil {
				t.Fatal(err)
			}
			db.InitServer()

			const start, end = 13, 5000
			s, err := openStream(cfg.ServerAddr, start, end, 333, int(recordBytes), 10*time.Second)
			if err != nil {
				t.Fatal(err)
			}
			defer s.close()
			buf := make([]byte, 100*int(recordBytes)) // smaller than a batch
			index := uint32(start)
			for {
				n, err := s.read(buf)
				if err != nil {
					t.Fatal(err)
				}
				if n == 0 {
					break
				}
				for r := 0; r < n; r++ {
					got := buf[r*int(recordBytes) : (r+1)*int(recordBytes)]
					if !bytes.Equal(got, utility.GenerateRecord(index, recordBytes)) {
						t.Fatalf("record %d mismatch", index)
					}
					index++
				}
			}
			if index != end {
				t.Fatalf("received %d records, want %d", index-start, end-start)
			}
		})
	}
}

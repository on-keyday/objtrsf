package trsf_test

import (
	"math/rand"
	"testing"

	"github.com/on-keyday/objtrsf/trsf"
)

func TestOrdering(t *testing.T) {
	chunkedData := [][]byte{
		[]byte("This is the first chunk. "),
		[]byte("Here comes the second chunk. "),
		[]byte("Finally, this is the third chunk."),
	}

	var receivedData []byte
	for _, chunk := range chunkedData {
		receivedData = append(receivedData, chunk...)
	}

	q := trsf.NewOrderingQueue()
	q.Push(&trsf.RecvData{Offset: 0, Data: chunkedData[0], Eof: false})
	q.Push(&trsf.RecvData{Offset: uint64(len(chunkedData[0])), Data: chunkedData[1], Eof: false})
	q.Push(&trsf.RecvData{Offset: uint64(len(chunkedData[0]) + len(chunkedData[1])), Data: chunkedData[2], Eof: true})
	var result []byte
	for {
		data, eof := q.ReadDirect(uint64(len(receivedData)))
		if data == nil {
			break
		}
		result = append(result, data...)
		if eof {
			break
		}
	}

	if string(result) != string(receivedData) {
		t.Fatalf("data mismatch:\nexpected: %q\ngot:      %q", string(receivedData), string(result))
	}

	q = trsf.NewOrderingQueue()
	// out of order push
	q.Push(&trsf.RecvData{Offset: uint64(len(chunkedData[0])), Data: chunkedData[1], Eof: false})
	q.Push(&trsf.RecvData{Offset: 0, Data: chunkedData[0], Eof: false})
	q.Push(&trsf.RecvData{Offset: uint64(len(chunkedData[0]) + len(chunkedData[1])), Data: chunkedData[2], Eof: true})
	result = nil
	for {
		data, eof := q.ReadDirect(uint64(len(receivedData)))
		if data == nil {
			break
		}
		result = append(result, data...)
		if eof {
			break
		}
	}

	if string(result) != string(receivedData) {
		t.Fatalf("data mismatch in out-of-order push:\nexpected: %q\ngot:      %q", string(receivedData), string(result))
	}
}

func TestOrderingOverlap(t *testing.T) {
	q := trsf.NewOrderingQueue()
	err := q.Push(&trsf.RecvData{Offset: 0, Data: []byte("Hello, "), Eof: false})
	if err != nil {
		t.Fatalf("unexpected error on first push: %v", err)
	}
	err = q.Push(&trsf.RecvData{Offset: 5, Data: []byte("World!"), Eof: false})
	if err == nil {
		t.Fatal("expected error on overlapping push, got nil")
	}
}

func TestOrderingAfterEOF(t *testing.T) {
	q := trsf.NewOrderingQueue()
	err := q.Push(&trsf.RecvData{Offset: 0, Data: []byte("Hello, "), Eof: true})
	if err != nil {
		t.Fatalf("unexpected error on EOF push: %v", err)
	}
	err = q.Push(&trsf.RecvData{Offset: 7, Data: []byte("World!"), Eof: false})
	if err == nil {
		t.Fatal("expected error on push after EOF, got nil")
	}
}

func TestOrderingDuplicate(t *testing.T) {
	q := trsf.NewOrderingQueue()
	err := q.Push(&trsf.RecvData{Offset: 0, Data: []byte("Hello, "), Eof: false})
	if err != nil {
		t.Fatalf("unexpected error on first push: %v", err)
	}
	err = q.Push(&trsf.RecvData{Offset: 0, Data: []byte("Hello, "), Eof: false})
	if err != nil {
		t.Fatalf("unexpected error on duplicate push: %v", err)
	}
	data, eof := q.ReadDirect(7)
	if string(data) != "Hello, " {
		t.Fatalf("data mismatch on duplicate push:\nexpected: %q\ngot:      %q", "Hello, ", string(data))
	}
	if eof {
		t.Fatal("unexpected EOF on duplicate push read")
	}
}

func TestOrderingPartialRead(t *testing.T) {
	chunkedData := [][]byte{
		[]byte("Chunk1-"),
		[]byte("Chunk2-"),
		[]byte("Chunk3-"),
		[]byte("Chunk4"),
	}

	var receivedData []byte
	for _, chunk := range chunkedData {
		receivedData = append(receivedData, chunk...)
	}

	q := trsf.NewOrderingQueue()
	var offset uint64
	for _, chunk := range chunkedData {
		q.Push(&trsf.RecvData{Offset: offset, Data: chunk, Eof: false})
		offset += uint64(len(chunk))
	}
	q.Push(&trsf.RecvData{Offset: offset, Data: nil, Eof: true})

	var result []byte
	readSize := uint64(6) // read in chunks of 6 bytes
	for {
		data, eof := q.ReadDirect(readSize)
		if data == nil {
			break
		}
		result = append(result, data...)
		if eof {
			break
		}
	}

	if string(result) != string(receivedData) {
		t.Fatalf("data mismatch in partial read:\nexpected: %q\ngot:      %q", string(receivedData), string(result))
	}
}

func FuzzOrdering(f *testing.F) {
	f.Fuzz(func(t *testing.T, b []byte, i int64) {
		random := rand.New(rand.NewSource(i))

		type offsetData struct {
			offset uint64
			data   []byte
		}
		// Split b into random chunks
		var chunks []offsetData
		start := 0
		for start < len(b) {
			chunkSize := random.Intn(10) + 1 // chunk size between 1 and 10
			end := start + chunkSize
			if end > len(b) {
				end = len(b)
			}
			chunks = append(chunks, offsetData{offset: uint64(start), data: b[start:end]})
			start = end
		}

		// Shuffle chunks
		random.Shuffle(len(chunks), func(i, j int) {
			chunks[i], chunks[j] = chunks[j], chunks[i]
		})

		q := trsf.NewOrderingQueue()
		var offset uint64
		for _, chunk := range chunks {
			q.Push(&trsf.RecvData{Offset: chunk.offset, Data: chunk.data, Eof: chunk.offset+uint64(len(chunk.data)) == uint64(len(b))})
			offset += uint64(len(chunk.data))
		}

		var result []byte
		for {
			data, eof := q.ReadDirect(uint64(len(b)))
			if data == nil {
				break
			}
			result = append(result, data...)
			if eof {
				break
			}
		}

		if string(result) != string(b) {
			t.Fatalf("data mismatch in fuzz test:\nexpected: %q\ngot:      %q", string(b), string(result))
		}
	})
}

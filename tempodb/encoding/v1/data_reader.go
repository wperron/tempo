package v1

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"

	tempo_io "github.com/grafana/tempo/pkg/io"
	"github.com/grafana/tempo/tempodb/backend"
	"github.com/grafana/tempo/tempodb/encoding/common"
)

type dataReader struct {
	// dataReader common.DataReader
	r backend.ContextReader

	pool             ReaderPool
	compressedReader io.Reader

	buffer                []byte
	compressedPagesBuffer [][]byte
}

// NewDataReader creates a datareader that supports compression
func NewDataReader(r backend.ContextReader, encoding backend.Encoding) (common.DataReader, error) {
	return NewNestedDataReader(r, encoding)
}

// NewNestedDataReader is useful for nesting compression inside of a different reader
func NewNestedDataReader(r backend.ContextReader, encoding backend.Encoding) (common.DataReader, error) {
	pool, err := getReaderPool(encoding)
	if err != nil {
		return nil, err
	}

	return &dataReader{
		r:    r,
		pool: pool,
	}, nil
}

// Read returns the pages requested in the passed records.  It
// assumes that if there are multiple records they are ordered
// and contiguous
func (r *dataReader) Read(ctx context.Context, records []common.Record, buffer []byte) ([][]byte, []byte, error) {
	compressedPages, buffer, err := func(ctx context.Context, records []common.Record, buffer []byte) ([][]byte, []byte, error) {
		if len(records) == 0 {
			return nil, buffer, nil
		}

		start := records[0].Start
		length := uint32(0)
		for _, record := range records {
			length += record.Length
		}

		buffer = make([]byte, length)
		_, err := r.r.ReadAt(ctx, buffer, int64(start))
		if err != nil {
			return nil, nil, err
		}

		slicePages := make([][]byte, 0, len(records))
		cursor := uint32(0)
		previousEnd := uint64(0)
		for _, record := range records {
			end := cursor + record.Length
			if end > uint32(len(buffer)) {
				return nil, nil, fmt.Errorf("record out of bounds while reading pages: %d, %d, %d, %d", cursor, record.Length, end, len(buffer))
			}

			if previousEnd != record.Start && previousEnd != 0 {
				return nil, nil, fmt.Errorf("non-contiguous pages requested from dataReader: %d, %+v", previousEnd, record)
			}

			slicePages = append(slicePages, buffer[cursor:end])
			cursor += record.Length
			previousEnd = record.Start + uint64(record.Length)
		}

		return slicePages, buffer, nil
	}(ctx, records, buffer)
	if err != nil {
		return nil, nil, err
	}

	if cap(r.compressedPagesBuffer) < len(compressedPages) {
		// extend r.compressedPagesBuffer
		diff := len(compressedPages) - cap(r.compressedPagesBuffer)
		r.compressedPagesBuffer = append(r.compressedPagesBuffer[:cap(r.compressedPagesBuffer)], make([][]byte, diff)...)
	} else {
		r.compressedPagesBuffer = r.compressedPagesBuffer[:len(compressedPages)]
	}

	// now decompress
	for i, page := range compressedPages {
		reader, err := r.getCompressedReader(page)
		if err != nil {
			return nil, nil, err
		}

		r.compressedPagesBuffer[i], err = tempo_io.ReadAllWithBuffer(reader, len(page), r.compressedPagesBuffer[i])
		if err != nil {
			return nil, nil, err
		}
	}

	return r.compressedPagesBuffer, buffer, nil
}

func (r *dataReader) Close() {
	if r.compressedReader != nil {
		r.pool.PutReader(r.compressedReader)
	}
}

// NextPage implements common.DataReader (kind of)
func (r *dataReader) NextPage(buffer []byte) ([]byte, uint32, error) {
	page, pageLen, err := func(buffer []byte) ([]byte, uint32, error) {
		reader, err := r.r.Reader()
		if err != nil {
			return nil, 0, err
		}

		// v0 pages are just single objects. this method will return one object at a time from the encapsulated reader
		var totalLength uint32
		err = binary.Read(reader, binary.LittleEndian, &totalLength)
		if err != nil {
			return nil, 0, err
		}

		if cap(buffer) < int(totalLength) {
			buffer = make([]byte, totalLength)
		} else {
			buffer = buffer[:totalLength]
		}
		binary.LittleEndian.PutUint32(buffer, totalLength)

		_, err = reader.Read(buffer[uint32Size:])
		if err != nil {
			return nil, 0, err
		}

		return buffer, totalLength, nil
	}(buffer)
	if err != nil {
		return nil, 0, err
	}
	reader, err := r.getCompressedReader(page)
	if err != nil {
		return nil, 0, err
	}
	r.buffer, err = tempo_io.ReadAllWithBuffer(reader, len(page), r.buffer)
	if err != nil {
		return nil, 0, err
	}
	return r.buffer, pageLen, nil
}

func (r *dataReader) getCompressedReader(page []byte) (io.Reader, error) {
	var err error
	if r.compressedReader == nil {
		r.compressedReader, err = r.pool.GetReader(bytes.NewReader(page))
	} else {
		r.compressedReader, err = r.pool.ResetReader(bytes.NewReader(page), r.compressedReader)
	}
	return r.compressedReader, err
}

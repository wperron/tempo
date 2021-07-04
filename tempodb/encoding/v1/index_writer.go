package v1

import (
	"github.com/grafana/tempo/tempodb/encoding/common"
)

type indexWriter struct {
	r common.RecordReaderWriter
}

// NewIndexWriter returns an index writer that writes to the provided io.Writer.
// The index has not changed between v0 and v1.
func NewIndexWriter() common.IndexWriter {
	return &indexWriter{
		r: NewRecordReaderWriter(),
	}
}

// Write implements common.IndexWriter
func (w *indexWriter) Write(records []common.Record) ([]byte, error) {
	return w.r.MarshalRecords(records)
}

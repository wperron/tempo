package v2

import (
	"bytes"
	"io"

	"github.com/grafana/tempo/tempodb/backend"
	"github.com/grafana/tempo/tempodb/encoding/common"
)

// meteredWriter is a struct that is used to count the number of bytes
// written to a block after compression.  Unfortunately the compression io.Reader
// returns bytes before compression so this is necessary to know the actual number of
// byte written.
type meteredWriter struct {
	wrappedWriter io.Writer
	bytesWritten  int
}

func (m *meteredWriter) Write(p []byte) (n int, err error) {
	m.bytesWritten += len(p)
	return m.wrappedWriter.Write(p)
}

type dataWriter struct {
	buffer            *bytes.Buffer
	outputWriter      *meteredWriter
	compressionWriter io.WriteCloser

	pool     WriterPool
	objectRW common.ObjectReaderWriter
}

// NewDataWriter creates a paged page writer
func NewDataWriter(writer io.Writer, encoding backend.Encoding) (common.DataWriter, error) {
	pool, err := GetWriterPool(encoding)
	if err != nil {
		return nil, err
	}

	outputWriter := &meteredWriter{
		wrappedWriter: writer,
	}

	compressionWriter, err := pool.GetWriter(outputWriter)
	if err != nil {
		return nil, err
	}

	return &dataWriter{
		buffer:            &bytes.Buffer{},
		outputWriter:      outputWriter,
		pool:              pool,
		compressionWriter: compressionWriter,
		objectRW:          NewObjectReaderWriter(),
	}, nil
}

// Write implements DataWriter
func (p *dataWriter) Write(id common.ID, obj []byte) (int, error) {
	return p.objectRW.MarshalObjectToWriter(id, obj, p.buffer)
}

// CutPage implements DataWriter
func (p *dataWriter) CutPage() (int, error) {
	var err error
	p.compressionWriter, err = p.pool.ResetWriter(p.outputWriter, p.compressionWriter)
	if err != nil {
		return 0, err
	}

	buffer := p.buffer.Bytes()
	_, err = p.compressionWriter.Write(buffer)
	if err != nil {
		return 0, err
	}

	// now clear our v0 buffer so we can start the new block page
	p.compressionWriter.Close()
	p.buffer.Reset()

	// TODO(wperron) lint error that bytesWritten is never used here
	// bytesWritten := p.outputWriter.bytesWritten
	p.outputWriter.bytesWritten = 0

	// v1Buffer currently has all of the v1 bytes. let's wrap it in our page and write
	bytesWritten, err := marshalPageToWriter(p.buffer.Bytes(), p.outputWriter, constDataHeader)
	if err != nil {
		return 0, err
	}
	p.buffer.Reset()

	return bytesWritten, err
}

// Complete implements DataWriter
func (p *dataWriter) Complete() error {
	if p.compressionWriter != nil {
		p.pool.PutWriter(p.compressionWriter)
		p.compressionWriter = nil
	}

	return nil
}

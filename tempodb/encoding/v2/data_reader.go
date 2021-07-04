package v2

import (
	"context"

	"github.com/grafana/tempo/tempodb/backend"
	"github.com/grafana/tempo/tempodb/encoding/common"
	v1 "github.com/grafana/tempo/tempodb/encoding/v1"
)

type dataReader struct {
	contextReader backend.ContextReader
	dataReader    common.DataReader

	pageBuffer []byte
}

// constDataHeader is a singleton data header.  the data header is
//  stateless b/c there are no fields.  to very minorly reduce allocations all
//  data should just use this.
var constDataHeader = &dataHeader{}

// NewDataReader constructs a v2 DataReader that handles paged...reading
func NewDataReader(r backend.ContextReader, encoding backend.Encoding) (common.DataReader, error) {
	dr, err := v1.NewDataReader(r, encoding)
	if err != nil {
		return nil, err
	}

	v2DataReader := &dataReader{
		contextReader: r,
		dataReader:    dr,
	}

	// wrap the paged reader in a compressed/v1 reader and return that
	v1DataReader, err := v1.NewNestedDataReader(v2DataReader.contextReader, encoding)
	if err != nil {
		return nil, err
	}

	return v1DataReader, nil
}

// Read implements common.DataReader
func (r *dataReader) Read(ctx context.Context, records []common.Record, buffer []byte) ([][]byte, []byte, error) {
	v0Pages, buffer, err := r.dataReader.Read(ctx, records, buffer)
	if err != nil {
		return nil, nil, err
	}

	pages := make([][]byte, 0, len(v0Pages))
	for _, v0Page := range v0Pages {
		page, err := unmarshalPageFromBytes(v0Page, constDataHeader)
		if err != nil {
			return nil, nil, err
		}

		pages = append(pages, page.data)
	}

	return pages, buffer, nil
}

func (r *dataReader) Close() {
	r.dataReader.Close()
}

// NextPage implements common.DataReader
func (r *dataReader) NextPage(buffer []byte) ([]byte, uint32, error) {
	reader, err := r.contextReader.Reader()
	if err != nil {
		return nil, 0, err
	}

	page, err := unmarshalPageFromReader(reader, constDataHeader, r.pageBuffer)
	if err != nil {
		return nil, 0, err
	}
	r.pageBuffer = page.data

	return page.data, page.totalLength, nil
}

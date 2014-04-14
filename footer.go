package hio

import (
	"github.com/go-hep/rio"
)

type FileFooter struct {
	Keys []fileEntry
}

type fileEntry struct {
	Name string
	Pos  int64
	Len  int64
}

func newFileFooterFrom(stream *rio.Stream) (FileFooter, error) {
	var err error
	ftr := FileFooter{
		Keys: make([]fileEntry, 0),
	}

	rec := stream.Record("hio.FileFooter")
	rec.SetUnpack(true)
	err = rec.Connect("hio.FileFooter", &ftr)
	if err != nil {
		return ftr, err
	}

	rec, err = stream.ReadRecord()
	if err != nil {
		return ftr, err
	}

	return ftr, err
}

// EOF

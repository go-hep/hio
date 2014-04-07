package hio

import (
	"github.com/go-hep/rio"
)

type FileHeader struct {
	Version Version
	Keys    []string
}

func newFileHeaderFrom(stream *rio.Stream) (FileHeader, error) {
	var err error
	hdr := FileHeader{
		Keys: make([]string, 0),
	}

	rec := stream.Record("hio.FileHeader")
	rec.SetUnpack(true)
	err = rec.Connect("hio.FileHeader", &hdr)
	if err != nil {
		return hdr, err
	}

	rec, err = stream.ReadRecord()
	if err != nil {
		return hdr, err
	}

	return hdr, err
}

func newFileHeader(stream *rio.Stream) (FileHeader, error) {
	var err error
	hdr := FileHeader{
		Keys: make([]string, 0),
	}

	rec := stream.Record("hio.FileHeader")
	rec.SetUnpack(true)
	err = rec.Connect("hio.FileHeader", &hdr)
	if err != nil {
		return hdr, err
	}

	return hdr, err
}

// EOF

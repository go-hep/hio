package hio

import (
	"os"

	"github.com/go-hep/rio"
)

type File struct {
	f      *rio.Stream
	Header FileHeader
}

func Open(fname string) (*File, error) {
	f, err := rio.Open(fname)
	if err != nil {
		return nil, err
	}
	fh, err := newFileHeaderFrom(f)
	if err != nil {
		return nil, err
	}
	return &File{
		f:      f,
		Header: fh,
	}, err
}

func Create(fname string) (*File, error) {
	f, err := rio.Create(fname)
	if err != nil {
		return nil, err
	}
	return &File{
		f:      f,
		Header: FileHeader{make([]string, 0)},
	}, err
}

// Name returns the name of the file
func (f *File) Name() string {
	return f.f.Name()
}

// Fd returns the integer Unix file descriptor referencing the open file.
func (f *File) Fd() uintptr {
	return f.f.Fd()
}

// Close closes the File, rendering it unusable for I/O.
// It returns an error, if any
func (f *File) Close() error {
	return f.f.Close()
}

// Stat returns the FileInfo structure describing file. If there is an
// error, it will be of type *PathError.
func (f *File) Stat() (os.FileInfo, error) {
	return f.f.Stat()
}

// Sync commits the current contents of the file to stable storage.
// Typically, this means flushing the file system's in-memory copy of
// recently written data to disk.
func (f *File) Sync() error {
	return f.f.Sync()
}

// EOF

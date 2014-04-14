package hio

import (
	"fmt"
	"os"

	"github.com/go-hep/rio"
)

type File struct {
	f      *rio.Stream
	mode   string
	header FileHeader
	footer FileFooter
	dict   dict
	tosync map[string]struct{}
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

	pos := f.CurPos()

	_, err = f.Seek(fh.Pos, 0)
	if err != nil {
		return nil, err
	}

	ft, err := newFileFooterFrom(f)
	if err != nil {
		return nil, err
	}

	_, err = f.Seek(pos, 0)
	if err != nil {
		return nil, err
	}

	hfile := &File{
		f:      f,
		mode:   "r",
		header: fh,
		footer: ft,
		dict:   dict{make(map[string]Value)},
		tosync: make(map[string]struct{}),
	}

	for _, key := range hfile.footer.Keys {
		hfile.dict.db[key.Name] = nil
	}
	return hfile, err
}

func Create(fname string) (*File, error) {
	f, err := rio.Create(fname)
	if err != nil {
		return nil, err
	}

	hfile := &File{
		f:    f,
		mode: "w",
		header: FileHeader{
			Version: g_version,
		},
		footer: FileFooter{
			Keys: make([]fileEntry, 0),
		},
		dict:   dict{make(map[string]Value)},
		tosync: make(map[string]struct{}),
	}

	rec := hfile.f.Record("hio.FileHeader")
	err = rec.Connect("hio.FileHeader", &hfile.header)
	if err != nil {
		return nil, err
	}

	err = hfile.f.WriteRecord(rec)
	if err != nil {
		return nil, err
	}

	rec = hfile.f.Record("hio.FileFooter")
	err = rec.Connect("hio.FileFooter", &hfile.footer)
	if err != nil {
		return nil, err
	}

	return hfile, err
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
	var err error
	err = f.Sync()
	if err != nil {
		return err
	}

	// if file opened in write-mode, write header back
	if f.mode == "w" {

		entries := make([]fileEntry, 0, len(f.tosync))
		for k := range f.tosync {
			rec := f.f.Record(k)
			if rec == nil {
				err = fmt.Errorf("hio: could not retrieve [%s] record", k)
				return err
			}

			pos := f.f.CurPos()
			v, err := f.dict.get(k)
			if err != nil {
				return err
			}

			err = rec.Connect(k, v)
			if err != nil {
				return err
			}
			err = f.f.WriteRecord(rec)
			if err != nil {
				return err
			}
			entries = append(entries,
				fileEntry{
					Name: k,
					Pos:  pos,
					Len:  f.f.CurPos() - pos,
				},
			)
		}

		f.header.Pos = f.f.CurPos()
		_, err = f.f.Seek(0, 0)
		if err != nil {
			return err
		}

		rec := f.f.Record("hio.FileHeader")
		if rec == nil {
			err = fmt.Errorf("hio: could not retrieve hio.FileHeader record")
			return err
		}
		err = f.f.WriteRecord(rec)
		if err != nil {
			return err
		}

		if f.header.Pos != 0 {
			_, err = f.f.Seek(f.header.Pos, 0)
			if err != nil {
				return err
			}
		}

		rec = f.f.Record("hio.FileFooter")
		if rec == nil {
			err = fmt.Errorf("hio: could not retrieve hio.FileHeader record")
			return err
		}
		f.footer.Keys = append(f.footer.Keys, entries...)

		err = f.f.WriteRecord(rec)
		if err != nil {
			return err
		}

		err = f.Sync()
		if err != nil {
			return err
		}
	}

	err = f.f.Close()
	if err != nil {
		return err
	}

	return err
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

// Keys returns the list of objects contained in the file
func (f *File) Keys() []string {
	return f.dict.Keys()
}

func (f *File) Get(name string, v Value) error {
	vv, err := f.dict.get(name)
	if err != nil {
		return err
	}

	if vv == nil {
		// load from file
		rec := f.f.Record(name)
		if rec == nil {
			return fmt.Errorf("hio: no such record [%s] on file [%s]", name, f.Name())
		}
		rec.SetUnpack(true)
		err = rec.Connect(name, v)
		if err != nil {
			return err
		}
		_, err = f.f.ReadRecord()
		if err != nil {
			return err
		}

		err = f.Set(name, v)
		if err != nil {
			return err
		}
	}

	return f.dict.Get(name, v)
}

func (f *File) Has(name string) bool {
	return f.dict.Has(name)
}

func (f *File) Del(name string) error {
	//TODO(sbinet): check r/w file
	err := f.dict.Del(name)
	if err != nil {
		return err
	}
	if _, ok := f.tosync[name]; ok {
		delete(f.tosync, name)
	}
	return err
}

func (f *File) Set(name string, v Value) error {
	//TODO(sbinet): check r/w file
	err := f.dict.Set(name, v)
	if err != nil {
		return err
	}
	f.tosync[name] = struct{}{}
	return err
}

func (f *File) Version() Version {
	return f.header.Version
}

// check interfaces
var _ Dict = (*File)(nil)

// EOF

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
	begin  int64 // start of file payload
	tosync pmap
	tables pmap
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

	begin := f.CurPos()

	_, err = f.Seek(fh.Pos, 0)
	if err != nil {
		return nil, err
	}

	ft, err := newFileFooterFrom(f)
	if err != nil {
		return nil, err
	}

	_, err = f.Seek(begin, 0)
	if err != nil {
		return nil, err
	}

	hfile := &File{
		f:      f,
		mode:   "r",
		header: fh,
		footer: ft,
		dict:   newdict(),
		begin:  begin,
		tosync: newpmap(),
		tables: newpmap(),
	}

	for _, key := range hfile.footer.Keys {
		hfile.dict.Set(key.Name, nil)
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
		dict:   newdict(),
		tosync: newpmap(),
		tables: newpmap(),
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
	hfile.begin = hfile.f.CurPos()

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

		curpos := f.f.CurPos()
		entries := make([]fileEntry, 0, f.tosync.Len()+f.tables.Len())
		for _, item := range f.tables.slice {
			k := item.k
			pos := item.v
			hdr := "hio.Header/" + k
			rec := f.f.Record(hdr)
			if rec == nil {
				err = fmt.Errorf("hio: could not retrieve [%s] record", k)
				return err
			}

			v, err := f.dict.get(k)
			if err != nil {
				return err
			}

			table := v.(*Table)
			err = rec.Connect(hdr, &table.hdr)
			if err != nil && err != rio.ErrBlockConnected {
				return err
			}

			_, err = f.f.Seek(pos, 0)
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
		_, err = f.f.Seek(curpos, 0)
		if err != nil {
			return err
		}

		for _, k := range f.tosync.keys() {
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
		if table, ok := v.(*Table); ok {
			pos := f.f.CurPos()
			defer f.f.Seek(pos, 0)

			_, err = f.f.Seek(f.begin, 0)
			if err != nil {
				return err
			}

			// load from file
			hdrname := "hio.Header/" + name
			rec := f.f.Record(hdrname)
			if rec == nil {
				return fmt.Errorf("hio: no such record [%s] on file [%s]", name, f.Name())
			}
			rec.SetUnpack(true)
			err = rec.Connect(hdrname, &table.hdr)
			if err != nil {
				return err
			}
			_, err = f.f.ReadRecord()
			if err != nil {
				return err
			}

			err = f.dict.Set(name, table)
			if err != nil {
				return err
			}
		} else {
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

			err = f.dict.Set(name, v)
			if err != nil {
				return err
			}
		}
	}

	if table, ok := v.(*Table); ok {
		stream, err := rio.Open(f.f.Name())
		if err != nil {
			return err
		}
		table.setStream(stream)
		table.doclose = true
	}

	return f.dict.Get(name, v)
}

func (f *File) Has(name string) bool {
	return f.dict.Has(name)
}

func (f *File) Del(name string) error {
	if f.mode != "w" {
		return fmt.Errorf("hio: only writable files can delete keys")
	}

	err := f.dict.Del(name)
	if err != nil {
		return err
	}
	if f.tosync.has(name) {
		f.tosync.del(name)
	}
	return err
}

func (f *File) Set(name string, v Value) error {
	if f.mode != "w" {
		return fmt.Errorf("hio: only writable files can add/modify keys")
	}

	err := f.dict.Set(name, v)
	if err != nil {
		return err
	}
	pos := f.f.CurPos()
	if table, ok := v.(*Table); ok {
		f.tables.set(name, pos)
		hdrname := "hio.Header/" + name
		rec := f.f.Record(hdrname)
		err = rec.Connect(hdrname, &table.hdr)
		if err != nil {
			return err
		}
		err = f.f.WriteRecord(rec)
		if err != nil {
			return err
		}

		_ = f.f.Record(name)
	} else {
		f.tosync.set(name, pos)
	}
	return err
}

func (f *File) Version() Version {
	return f.header.Version
}

// check interfaces
var _ Dict = (*File)(nil)

// EOF

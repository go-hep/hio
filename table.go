package hio

import (
	"bytes"
	"encoding/gob"
	"fmt"

	"github.com/go-hep/rio"
)

type tableHeader struct {
	Name    string
	Version uint32
	Entries int64
}

func NewTable(f *File, name string) (*Table, error) {
	var err error

	table := &Table{
		hdr: tableHeader{
			Name:    name,
			Version: 0,
			Entries: 0,
		},
		stream: f.f,
	}

	err = f.Set(name, table)
	if err != nil {
		return nil, err
	}

	return table, err
}

type Table struct {
	hdr     tableHeader
	stream  *rio.Stream
	rec     *rio.Record
	doclose bool // whether we need to close the stream ourselves
}

func (table *Table) MarshalBinary(buf *bytes.Buffer) error {
	enc := gob.NewEncoder(buf)
	err := enc.Encode(&table.hdr)
	return err
}

func (table *Table) UnmarshalBinary(buf *bytes.Buffer) error {
	dec := gob.NewDecoder(buf)
	err := dec.Decode(&table.hdr)
	return err
}

func (table *Table) Name() string {
	return table.hdr.Name
}

func (table *Table) Version() uint32 {
	return table.hdr.Version
}

func (table *Table) setStream(w *rio.Stream) {
	table.stream = w
}

func (table *Table) Close() error {
	var err error
	if table.stream != nil {
		err = table.stream.Sync()
		if err != nil {
			return err
		}

		if table.doclose {
			err = table.stream.Close()
			if err != nil {
				return err
			}
		}
	}
	table.stream = nil
	return err
}

func (table *Table) Entries() int64 {
	return table.hdr.Entries
}

func (table *Table) Write(ptr interface{}) error {
	if table.rec == nil {
		rec := table.stream.Record(table.hdr.Name)
		if rec == nil {
			return fmt.Errorf("hio: no such table [%s]", table.hdr.Name)
		}
		table.rec = rec
	}
	rec := table.rec
	err := rec.Connect(table.hdr.Name, ptr)
	if err != nil && err != rio.ErrBlockConnected {
		return err
	}

	err = table.stream.WriteRecord(rec)
	table.hdr.Entries++

	return err
}

func (table *Table) Read(ptr interface{}) error {

	//fmt.Printf("table.read(%q) - pos=%d...\n", table.hdr.Name, table.stream.CurPos())
	if table.rec == nil {
		rec := table.stream.Record(table.hdr.Name)
		if rec == nil {
			return fmt.Errorf("hio: no such table [%s]", table.hdr.Name)
		}
		rec.SetUnpack(true)
		table.rec = rec
	}
	rec := table.rec

	err := rec.Connect(table.hdr.Name, ptr)
	if err != nil && err != rio.ErrBlockConnected {
		return err
	}

	for {
		rec, err = table.stream.ReadRecord()
		if err != nil {
			return err
		}
		if rec.Name() == table.hdr.Name {
			break
		}
	}
	return err
}

// EOF

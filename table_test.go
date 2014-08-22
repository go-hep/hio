package hio

import (
	"bufio"
	"compress/zlib"
	"encoding/gob"
	"fmt"
	"io"
	"math/rand"
	"os"
	"reflect"
	"testing"

	"github.com/go-hep/hbook"
)

type tableData struct {
	Ints    []int64
	Floats  []float64
	Strings []string
}

func TestTable(t *testing.T) {
	const fname = "testdata/write-table.hio"
	defer os.RemoveAll(fname)
	testTableCreate(t, fname)
	testTableRead(t, fname)
}

func testTableCreate(t *testing.T, fname string) {
	const nentries = 10
	const tname = "my-table"
	f, err := Create(fname)
	if err != nil {
		t.Fatalf("could not create file [%s]: %v", fname, err)
	}
	defer func() {
		err = f.Close()
		if err != nil {
			t.Fatalf("could not close file [%s]: %v", fname, err)
		}
	}()

	if f.Name() != fname {
		t.Fatalf("expected name %q. got %q", fname, f.Name())
	}

	table, err := NewTable(f, tname)
	if err != nil {
		t.Fatalf("could not create table [%s]: %v", fname, err)
	}

	if table.Name() != tname {
		t.Fatalf("expected table name [%s]. got [%s]", tname, table.Name())
	}

	for i := 0; i < nentries; i++ {
		data := tableData{
			Ints: []int64{
				int64(i) + 100,
				int64(i) + 200,
				int64(i) + 300,
			},
			Floats: []float64{
				float64(i) + 100,
				float64(i) + 200,
				float64(i) + 300,
			},
			Strings: []string{
				fmt.Sprintf("my-string-%d", i+100),
				fmt.Sprintf("my-string-%d", i+200),
				fmt.Sprintf("my-string-%d", i+300),
			},
		}
		err = table.Write(&data)
		if err != nil {
			t.Fatalf("could not write to table [name=%s, i=%d]: %v", fname, i, err)
		}
	}

	if table.Entries() != nentries {
		t.Fatalf("expected [%d] entries. got [%d]", nentries, table.Entries())
	}
}

func testTableRead(t *testing.T, fname string) {
	const nentries = 10
	const tname = "my-table"
	f, err := Open(fname)
	if err != nil {
		t.Fatalf("could not open file [%s]: %v", fname, err)
	}
	defer func() {
		err = f.Close()
		if err != nil {
			t.Fatalf("could not close file [%s]: %v", fname, err)
		}
	}()

	if f.Name() != fname {
		t.Fatalf("expected name %q. got %q", fname, f.Name())
	}

	var table Table
	err = f.Get(tname, &table)
	if err != nil {
		t.Fatalf("could not retrieve table [name=%s, file=%s]: %v", tname, fname, err)
	}

	if table.Name() != tname {
		t.Fatalf("expected table name [%s]. got [%s]", tname, table.Name())
	}

	if table.Entries() != nentries {
		t.Fatalf("expected [%d] entries. got [%d]", nentries, table.Entries())
	}

	for i := 0; i < nentries+1; i++ {
		refdata := tableData{
			Ints: []int64{
				int64(i) + 100,
				int64(i) + 200,
				int64(i) + 300,
			},
			Floats: []float64{
				float64(i) + 100,
				float64(i) + 200,
				float64(i) + 300,
			},
			Strings: []string{
				fmt.Sprintf("my-string-%d", i+100),
				fmt.Sprintf("my-string-%d", i+200),
				fmt.Sprintf("my-string-%d", i+300),
			},
		}
		var data tableData
		err = table.Read(&data)
		if i == nentries {
			if err != io.EOF {
				t.Fatalf("read too many entries")
			}
			break
		}
		if err != nil {
			t.Fatalf("could not read table [name=%s, i=%d]: %v", fname, i, err)
		}

		if !reflect.DeepEqual(data, refdata) {
			t.Fatalf("expected (n=%d):\nref=%v\new=%v", i, refdata, data)
		}
	}

}

func TestTableHist(t *testing.T) {
	const fname = "testdata/table-hist.hio"
	const nentries = 10
	const tname = "my-table"
	//os.RemoveAll(fname)

	href := func() *hbook.H1D {
		f, err := Create(fname)
		if err != nil {
			t.Fatalf("could not create file [%s]: %v", fname, err)
		}
		defer func() {
			err = f.Close()
			if err != nil {
				t.Fatalf("could not close file [%s]: %v", fname, err)
			}
		}()

		if f.Name() != fname {
			t.Fatalf("expected name %q. got %q", fname, f.Name())
		}

		table, err := NewTable(f, tname)
		if err != nil {
			t.Fatalf("could not create table [%s]: %v", fname, err)
		}

		if table.Name() != tname {
			t.Fatalf("expected table name [%s]. got [%s]", tname, table.Name())
		}

		h := hbook.NewH1D(100, 0, 100)
		h.Annotation()["title"] = "histo title"
		h.Annotation()["name"] = "histo name"

		for i := 0; i < nentries; i++ {
			h.Fill(rand.Float64()*100., 1.)
		}

		err = f.Set("histo-title", h)
		if err != nil {
			t.Fatalf("could not save histo: %v", err)
		}

		for i := 0; i < nentries; i++ {
			data := tableData{
				Ints: []int64{
					int64(i) + 100,
					int64(i) + 200,
					int64(i) + 300,
				},
				Floats: []float64{
					float64(i) + 100,
					float64(i) + 200,
					float64(i) + 300,
				},
				Strings: []string{
					fmt.Sprintf("my-string-%d", i+100),
					fmt.Sprintf("my-string-%d", i+200),
					fmt.Sprintf("my-string-%d", i+300),
				},
			}
			err = table.Write(&data)
			if err != nil {
				t.Fatalf("could not write to table [name=%s, i=%d]: %v", fname, i, err)
			}
		}

		if table.Entries() != nentries {
			t.Fatalf("expected [%d] entries. got [%d]", nentries, table.Entries())
		}
		return h
	}()

	hnew := func() *hbook.H1D {

		f, err := Open(fname)
		if err != nil {
			t.Fatalf("could not open file [%s]: %v", fname, err)
		}
		defer f.Close()

		var h hbook.H1D
		err = f.Get("histo-title", &h)
		if err != nil {
			t.Fatalf("could not retrieve histo: %v", err)
		}

		var table Table
		err = f.Get(tname, &table)
		if err != nil {
			t.Fatalf("could not retrieve table [name=%s, file=%s]: %v", tname, fname, err)
		}

		if table.Name() != tname {
			t.Fatalf("expected table name [%s]. got [%s]", tname, table.Name())
		}

		if table.Entries() != nentries {
			t.Fatalf("expected [%d] entries. got [%d]", nentries, table.Entries())
		}
		for i := 0; i < nentries+1; i++ {
			refdata := tableData{
				Ints: []int64{
					int64(i) + 100,
					int64(i) + 200,
					int64(i) + 300,
				},
				Floats: []float64{
					float64(i) + 100,
					float64(i) + 200,
					float64(i) + 300,
				},
				Strings: []string{
					fmt.Sprintf("my-string-%d", i+100),
					fmt.Sprintf("my-string-%d", i+200),
					fmt.Sprintf("my-string-%d", i+300),
				},
			}
			var data tableData
			err = table.Read(&data)
			if i == nentries {
				if err != io.EOF {
					t.Fatalf("read too many entries (err=%#v)", err)
				}
				break
			}
			if err != nil {
				t.Fatalf("could not read table [name=%s, i=%d]: %v", fname, i, err)
			}

			if !reflect.DeepEqual(data, refdata) {
				t.Fatalf("expected (n=%d):\nref=%v\new=%v", i, refdata, data)
			}
		}
		return &h
	}()

	if !reflect.DeepEqual(href, hnew) {
		t.Fatalf("ref=%v\nnew=%v\n", href, hnew)
	}

}

func Benchmark__WriteTableInt64____(b *testing.B) {
	const fname = "testdata/bench-write-table-int64.hio"
	const tname = "my-table"
	//defer os.RemoveAll(fname)

	b.StopTimer()
	f, err := Create(fname)
	if err != nil {
		b.Fatalf("could not create file [%s]: %v", fname, err)
	}
	defer f.Close()

	table, err := NewTable(f, tname)
	if err != nil {
		b.Fatalf("could not create table [%s]: %v", tname, err)
	}
	defer table.Close()

	b.StartTimer()

	for i := 0; i < b.N; i++ {
		data := int64(i)
		err = table.Write(&data)
		if err != nil {
			b.Fatalf("[i=%d] could not write data: %v", i, err)
		}
	}
}

func Benchmark__ReadTableInt64_____(b *testing.B) {
	const fname = "testdata/bench-write-table-int64.hio"
	const tname = "my-table"
	//defer os.RemoveAll(fname)

	b.StopTimer()
	f, err := Open(fname)
	if err != nil {
		b.Fatalf("could not open file [%s]: %v", fname, err)
	}
	defer f.Close()

	var table Table
	err = f.Get(tname, &table)
	if err != nil {
		b.Fatalf("could not retrieve table [%s]: %v", tname, err)
	}
	defer table.Close()

	b.StartTimer()

	for i := 0; i < b.N; i++ {
		data := int64(0)
		err = table.Read(&data)
		if err != nil && err != io.EOF {
			b.Fatalf("[i=%d] could not read data: %v (%d)", i, err, table.Entries())
		}
	}
}

func Benchmark__WriteTableFloat64__(b *testing.B) {
	const fname = "testdata/bench-write-table-float64.hio"
	const tname = "my-table"
	//defer os.RemoveAll(fname)

	b.StopTimer()
	f, err := Create(fname)
	if err != nil {
		b.Fatalf("could not create file [%s]: %v", fname, err)
	}
	defer f.Close()

	table, err := NewTable(f, tname)
	if err != nil {
		b.Fatalf("could not create table [%s]: %v", tname, err)
	}
	defer table.Close()

	b.StartTimer()

	for i := 0; i < b.N; i++ {
		data := float64(i)
		err = table.Write(&data)
		if err != nil {
			b.Fatalf("[i=%d] could not write data: %v", i, err)
		}
	}
}

func Benchmark__ReadTableFloat64___(b *testing.B) {
	const fname = "testdata/bench-write-table-float64.hio"
	const tname = "my-table"
	//defer os.RemoveAll(fname)

	b.StopTimer()
	f, err := Open(fname)
	if err != nil {
		b.Fatalf("could not open file [%s]: %v", fname, err)
	}
	defer f.Close()

	var table Table
	err = f.Get(tname, &table)
	if err != nil {
		b.Fatalf("could not retrieve table [%s]: %v", tname, err)
	}
	defer table.Close()

	b.StartTimer()

	for i := 0; i < b.N; i++ {
		data := float64(0)
		err = table.Read(&data)
		if err != nil && err != io.EOF {
			b.Fatalf("[i=%d] could not read data: %v (%d)", i, err, table.Entries())
		}
	}
}

func Benchmark__WriteTableBlob4k___(b *testing.B) {
	const fname = "testdata/bench-write-table-blob4k.hio"
	const tname = "my-table"
	//defer os.RemoveAll(fname)

	b.StopTimer()
	f, err := Create(fname)
	if err != nil {
		b.Fatalf("could not create file [%s]: %v", fname, err)
	}
	defer f.Close()

	table, err := NewTable(f, tname)
	if err != nil {
		b.Fatalf("could not create table [%s]: %v", tname, err)
	}
	defer table.Close()

	b.StartTimer()

	data := [4 * 1024]byte{}
	for i := 0; i < b.N; i++ {
		err = table.Write(&data)
		if err != nil {
			b.Fatalf("[i=%d] could not write data: %v", i, err)
		}
	}
}

func Benchmark__ReadTableBlob4k____(b *testing.B) {
	const fname = "testdata/bench-write-table-blob4k.hio"
	const tname = "my-table"
	//defer os.RemoveAll(fname)

	b.StopTimer()
	f, err := Open(fname)
	if err != nil {
		b.Fatalf("could not open file [%s]: %v", fname, err)
	}
	defer f.Close()

	var table Table
	err = f.Get(tname, &table)
	if err != nil {
		b.Fatalf("could not retrieve table [%s]: %v", tname, err)
	}
	defer table.Close()

	b.StartTimer()

	data := [4 * 1024]byte{}
	for i := 0; i < b.N; i++ {
		err = table.Read(&data)
		if err != nil && err != io.EOF {
			b.Fatalf("[i=%d] could not read data: %v (%d)", i, err, table.Entries())
		}
	}
}

func Benchmark__WriteTableStruct___(b *testing.B) {
	const fname = "testdata/bench-write-table-struct.hio"
	const tname = "my-table"
	//defer os.RemoveAll(fname)

	b.StopTimer()
	f, err := Create(fname)
	if err != nil {
		b.Fatalf("could not create file [%s]: %v", fname, err)
	}
	defer f.Close()

	table, err := NewTable(f, tname)
	if err != nil {
		b.Fatalf("could not create table [%s]: %v", tname, err)
	}
	defer table.Close()

	b.StartTimer()

	data := struct {
		Int    int64
		Float  float64
		String string
	}{
		String: "some data",
	}
	for i := 0; i < b.N; i++ {
		data.Int = int64(i)
		data.Float = float64(i)
		err = table.Write(&data)
		if err != nil {
			b.Fatalf("[i=%d] could not write data: %v", i, err)
		}
	}
}

func Benchmark__ReadTableStruct____(b *testing.B) {
	const fname = "testdata/bench-write-table-struct.hio"
	const tname = "my-table"
	//defer os.RemoveAll(fname)

	b.StopTimer()
	f, err := Open(fname)
	if err != nil {
		b.Fatalf("could not open file [%s]: %v", fname, err)
	}
	defer f.Close()

	var table Table
	err = f.Get(tname, &table)
	if err != nil {
		b.Fatalf("could not retrieve table [%s]: %v", tname, err)
	}
	defer table.Close()

	b.StartTimer()

	data := struct {
		Int    int64
		Float  float64
		String string
	}{}
	for i := 0; i < b.N; i++ {
		err = table.Read(&data)
		if err != nil && err != io.EOF {
			b.Fatalf("[i=%d] could not read data: %v (%d)", i, err, table.Entries())
		}
	}
}

func Benchmark__WriteGob___________(b *testing.B) {
	b.StopTimer()
	var f io.WriteCloser
	f, err := os.Create("testdata/simple.gob")
	if err != nil {
		b.Fatalf("could not create file: %v", err)
	}
	defer f.Close()

	enc := gob.NewEncoder(f)

	b.StartTimer()
	for i := 0; i < b.N; i++ {
		err = enc.Encode(&i)
		if err != nil {
			b.Fatalf("[i=%d]: %v", i, err)
		}
	}
}

func Benchmark__WriteGobBuffered___(b *testing.B) {
	b.StopTimer()
	f, err := os.Create("testdata/simple.buf.gob")
	if err != nil {
		b.Fatalf("could not create file: %v", err)
	}
	defer f.Close()

	enc := gob.NewEncoder(bufio.NewWriter(f))

	b.StartTimer()
	for i := 0; i < b.N; i++ {
		err = enc.Encode(&i)
		if err != nil {
			b.Fatalf("[i=%d]: %v", i, err)
		}
	}
}

func Benchmark__WriteComprGob______(b *testing.B) {
	b.StopTimer()
	f, err := os.Create("testdata/simple.gob.gz")
	if err != nil {
		b.Fatalf("could not create file: %v", err)
	}
	defer f.Close()

	gzip := zlib.NewWriter(f)
	defer gzip.Close()

	enc := gob.NewEncoder(gzip)

	b.StartTimer()
	for i := 0; i < b.N; i++ {
		err = enc.Encode(&i)
		if err != nil {
			b.Fatalf("[i=%d]: %v", i, err)
		}
	}
}

func Benchmark__WriteComprBufGob___(b *testing.B) {
	b.StopTimer()
	f, err := os.Create("testdata/simple.buf.gob.gz")
	if err != nil {
		b.Fatalf("could not create file: %v", err)
	}
	defer f.Close()

	gzip := zlib.NewWriter(bufio.NewWriter(f))
	defer gzip.Close()

	enc := gob.NewEncoder(gzip)

	b.StartTimer()
	for i := 0; i < b.N; i++ {
		err = enc.Encode(&i)
		if err != nil {
			b.Fatalf("[i=%d]: %v", i, err)
		}
	}
}

// EOF

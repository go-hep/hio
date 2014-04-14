package hio

import (
	"fmt"
	"io"
	"reflect"
	"testing"
)

type tableData struct {
	Ints    []int64
	Floats  []float64
	Strings []string
}

func TestTable(t *testing.T) {
	const fname = "testdata/write-table.hio"
	//defer os.RemoveAll(fname)
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

// EOF

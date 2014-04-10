package hio

import (
	"os"
	"reflect"
	"sort"
	"testing"
)

func TestFileOpen(t *testing.T) {
	const fname = "testdata/read-data.hio"
	testFileOpen(t, fname)
}

func TestFileCreate(t *testing.T) {
	const fname = "testdata/write-data-1.hio"
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

}

func TestFileCreateAndFill(t *testing.T) {
	const fname = "testdata/write-data-2.hio"
	testFileCreateAndFill(t, fname)
	testFileOpen(t, fname)
}

func TestFileInspect(t *testing.T) {
	const fname = "testdata/file-inspect.hio"
	defer os.RemoveAll(fname)
	testFileCreateAndFill(t, fname)
	testFileOpen(t, fname)

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

	// goes through the whole file
	_, _ = f.f.ReadRecord()

	// rewind
	_, err = f.f.Seek(0, 0)
	if err != nil {
		t.Fatalf("could not rewind file [%s]: %v", fname, err)
	}

	recnames := make([]string, 0)
	for _, rec := range f.f.Records() {
		recnames = append(recnames, rec.Name())
	}
	sort.Strings(recnames)

	keys := []string{"hio.FileHeader"}
	keys = append(keys, g_keys...)
	sort.Strings(keys)

	if !reflect.DeepEqual(recnames, keys) {
		t.Fatalf("expected file [%s] content: %v. got=%v", fname, keys, recnames)
	}
}

func testFileOpen(t *testing.T, fname string) {
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

	fkeys := f.Keys()

	if !reflect.DeepEqual(fkeys, g_keys) {
		t.Fatalf("expected keys=%v. got %v.", g_keys, fkeys)
	}

	for _, table := range g_table {
		v := reflect.New(reflect.ValueOf(table.value).Type())
		v.Elem().Set(reflect.ValueOf(table.value))

		w := reflect.New(reflect.ValueOf(table.value).Type())
		err = f.Get(table.name, w.Interface())
		if err != nil {
			t.Fatalf("could not get data [%s] from file: %v", table.name, err)
		}
		vv := v.Elem().Interface()
		ww := w.Elem().Interface()
		if !reflect.DeepEqual(vv, ww) {
			t.Fatalf("expected [%s] data to be %v (%T). got=%v (%T)",
				table.name,
				vv, vv,
				ww, ww,
			)
		}
	}
}

func testFileCreateAndFill(t *testing.T, fname string) {

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

	for _, table := range g_table {
		v := reflect.New(reflect.ValueOf(table.value).Type())
		v.Elem().Set(reflect.ValueOf(table.value))
		err = f.Set(table.name, v.Interface())
		if err != nil {
			t.Fatalf("could not put data [%s] into file: %v", table.name, err)
		}

		w := reflect.New(reflect.ValueOf(table.value).Type())
		err = f.Get(table.name, w.Interface())
		if err != nil {
			t.Fatalf("could not get data [%s] from file: %v", table.name, err)
		}
		vv := v.Elem().Interface()
		ww := w.Elem().Interface()
		if !reflect.DeepEqual(vv, ww) {
			t.Fatalf("expected [%s] data to be %v (%T). got=%v (%T)",
				table.name,
				vv, vv,
				ww, ww,
			)
		}

	}

	fkeys := f.Keys()

	if !reflect.DeepEqual(fkeys, g_keys) {
		t.Fatalf("expected keys=%v. got %v.", g_keys, fkeys)
	}

}

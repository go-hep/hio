package hio_test

import (
	"reflect"
	"sort"
	"testing"

	"github.com/go-hep/hio"
)

func TestFileOpen(t *testing.T) {
	const fname = "testdata/read-data.hio"
	testFileOpen(t, fname)
}

func TestFileCreate(t *testing.T) {
	const fname = "testdata/write-data-1.hio"
	f, err := hio.Create(fname)
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

func testFileOpen(t *testing.T, fname string) {
	f, err := hio.Open(fname)
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
	keys := []string{"int64", "float64"}
	sort.Strings(keys)

	if !reflect.DeepEqual(fkeys, keys) {
		t.Fatalf("expected keys=%v. got %v.", keys, fkeys)
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
}

func testFileCreateAndFill(t *testing.T, fname string) {

	f, err := hio.Create(fname)
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
	keys := []string{"int64", "float64"}
	sort.Strings(keys)

	if !reflect.DeepEqual(fkeys, keys) {
		t.Fatalf("expected keys=%v. got %v.", keys, fkeys)
	}

}

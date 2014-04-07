package hio

import (
	"fmt"
	"reflect"
	"sort"
)

type Dict interface {
	Get(name string, v Value) error
	Has(name string) bool
	Del(name string) error
	Put(name string, v Value) error

	Keys() []string
	//Values() []Value
}

type Value interface {
	//Name() string
	//Value() reflect.Value
}

type dict struct {
	db map[string]Value
}

func (d dict) get(name string) (Value, error) {
	v, ok := d.db[name]
	if !ok {
		return nil, fmt.Errorf("hio: no such key [%s]", name)
	}
	return v, nil
}

func (d dict) Get(name string, v Value) error {
	vv, ok := d.db[name]
	if !ok || vv == nil {
		return fmt.Errorf("hio: no such key [%s]", name)
	}

	rv := reflect.ValueOf(vv)
	rr := reflect.Indirect(rv)
	ptr := reflect.ValueOf(v)

	// fmt.Printf("v:  %v (%T)\n", ptr.Elem().Interface(), ptr.Elem().Interface())
	// fmt.Printf("vv: %v (%T)\n", vv, vv)

	// fmt.Printf("rr: %v (%T)\n", rr.Interface(), rr.Interface())
	// fmt.Printf("rv: %v (%T)\n", rv.Elem().Interface(), rv.Elem().Interface())

	//rv := reflect.Indirect(ptr)
	//rv.Set(reflect.ValueOf(vv))
	//pval := reflect.Indirect(ptr)
	pval := ptr.Elem()
	pval.Set(rr)

	// fmt.Printf("pv: %v (%T)\n", pval.Interface(), pval.Interface())
	// fmt.Printf("v:  %v (%T)\n", ptr.Elem().Interface(), ptr.Elem().Interface())

	return nil
}

func (d dict) Has(name string) bool {
	_, ok := d.db[name]
	return ok
}

func (d *dict) Del(name string) error {
	var err error
	if _, ok := d.db[name]; ok {
		delete(d.db, name)
	} else {
		return fmt.Errorf("hio: no such key [%s]", name)
	}
	return err
}

func (d *dict) Put(name string, v Value) error {
	var err error
	d.db[name] = v
	return err
}

func (d dict) Keys() []string {
	keys := make([]string, 0, len(d.db))
	for k := range d.db {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// EOF

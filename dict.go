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
	Set(name string, v Value) error

	Keys() []string
	//Values() []Value
}

type Value interface {
	//Name() string
	//Value() reflect.Value
}

type ditem struct {
	k string
	v Value
}

type dict struct {
	slice []ditem
}

func newdict() dict {
	return dict{
		slice: make([]ditem, 0),
	}
}

func (d dict) getidx(name string) int {
	for idx, item := range d.slice {
		if item.k == name {
			return idx
		}
	}
	return -1
}

func (d dict) get(name string) (Value, error) {
	i := d.getidx(name)
	if i < 0 {
		return nil, fmt.Errorf("hio: no such key [%s]", name)
	}

	return d.slice[i].v, nil
}

func (d dict) Get(name string, v Value) error {
	idx := d.getidx(name)
	if idx < 0 {
		return fmt.Errorf("hio: no such key [%s]", name)
	}
	vv := d.slice[idx].v

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
	idx := d.getidx(name)
	return idx >= 0
}

func (d *dict) Del(name string) error {
	var err error
	i := d.getidx(name)
	if i < 0 {
		return fmt.Errorf("hio: no such key [%s]", name)
	}

	d.slice = append(d.slice[:i], d.slice[i+1:]...)

	return err
}

func (d *dict) Set(name string, v Value) error {
	var err error
	i := d.getidx(name)
	if i >= 0 {
		d.slice[i].v = v
	} else {
		d.slice = append(d.slice, ditem{k: name, v: v})
	}
	return err
}

func (d dict) Keys() []string {
	keys := make([]string, 0, len(d.slice))
	for _, item := range d.slice {
		keys = append(keys, item.k)
	}
	sort.Strings(keys)
	return keys
}

// EOF

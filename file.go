package hio

import (
	"fmt"
	"os"
	"reflect"
)

type File interface {
	Dict

	Driver() Driver
	Name() string
	Close() error
	Sync() error
}

type Dict interface {
	Get(name string) (Value, error)
	Has(name string) bool
	Del(name string) error
	Put(name string, v Value) error

	Keys() []string
	Values() []Value
}

type Value interface {
	Name() string
	Type() reflect.Type
}

func Open(driver, fname string) (File, error) {
	drv, ok := drivers[driver]
	if !ok {
		return nil, fmt.Errorf("hio.Open: no driver [%s] registered", driver)
	}

	return drv.Open(fname, os.O_RDONLY, 0644)
}

func Create(driver, fname string) (File, error) {
	drv, ok := drivers[driver]
	if !ok {
		return nil, fmt.Errorf("hio.Open: no driver [%s] registered", driver)
	}

	return drv.Open(fname, os.O_WRONLY|os.O_TRUNC|os.O_CREATE, 0644)
}

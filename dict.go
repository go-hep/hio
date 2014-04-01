package hio

import (
	"reflect"
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
	Name() string
	Value() reflect.Value
}

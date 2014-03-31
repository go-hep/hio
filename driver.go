package hio

import (
	"os"
)

var drivers = make(map[string]Driver)

// Register makes a file i/o driver available by the provided name.
// If Register is called twice with the same name or if driver is nil,
// it panics.
func Register(name string, driver Driver) {
	if driver == nil {
		panic("hio: Register driver is nil")
	}
	_, dup := drivers[name]
	if dup {
		panic("hio: Register called twice for driver " + name)
	}
	drivers[name] = driver
}

type Driver interface {
	Version() int
	Name() string
	Open(name string, flag int, perm os.FileMode) (File, error)
}

// EOF

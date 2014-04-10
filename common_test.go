package hio

import (
	"sort"
)

type MyStruct struct {
	Float  float64
	Int    int64
	String string

	Floats  []float64
	Ints    []int64
	Strings []string
}

var g_table = []struct {
	name  string
	value interface{}
}{
	{
		name:  "int64",
		value: int64(42),
	},
	{
		name:  "float64",
		value: float64(66.6),
	},
	{
		name: "my-struct",
		value: MyStruct{
			Float:   66.6,
			Int:     42,
			String:  "mystruct",
			Floats:  []float64{11.1, 22.2, 33.3},
			Ints:    []int64{1, 2, 3},
			Strings: []string{"str-01", "str-02", "str-03"},
		},
	},
}

var g_keys []string

func init() {
	g_keys = make([]string, 0, len(g_table))
	for _, table := range g_table {
		g_keys = append(g_keys, table.name)
	}
	sort.Strings(g_keys)
}

// EOF

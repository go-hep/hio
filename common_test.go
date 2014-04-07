package hio_test

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
}

// EOF

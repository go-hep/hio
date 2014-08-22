package hio

type omapitem struct {
	k string
	v int64
}

// omap is an ordered map, keeping the order of insertion
type omap struct {
	i map[string]int // indices
	d []omapitem     // data
}

func newomap() omap {
	return omap{
		i: make(map[string]int),
		d: make([]omapitem, 0),
	}
}

func (m *omap) Len() int {
	return len(m.d)
}

func (m *omap) get(k string) int64 {
	i := m.i[k]
	return m.d[i].v
}

func (m *omap) add(k string, v int64) {
	n := m.Len()
	m.i[k] = n
	m.d = append(m.d, omapitem{k: k, v: v})
}

func (m *omap) has(k string) bool {
	_, ok := m.i[k]
	return ok
}

func (m *omap) del(k string) {
	i := m.i[k]
	delete(m.i, k)

	copy(m.d[i:], m.d[i+1:])
	m.d[len(m.d)-1] = omapitem{}
	m.d = m.d[:len(m.d)-1]

	// adjust indices
	for k, v := range m.i {
		if v > i {
			m.i[k] = v - 1
		}
	}
}

func (m *omap) keys() []string {
	keys := make([]string, 0, m.Len())
	for _, v := range m.d {
		keys = append(keys, v.k)
	}

	return keys
}

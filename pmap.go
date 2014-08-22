package hio

type pitem struct {
	k string
	v int64
}

// pmap is a map of pos/offsets, keeping the order of insertion
type pmap struct {
	slice []pitem
}

func newpmap() pmap {
	return pmap{
		slice: make([]pitem, 0),
	}
}

func (p *pmap) Len() int {
	return len(p.slice)
}

func (p *pmap) getidx(k string) int {
	for i, item := range p.slice {
		if item.k == k {
			return i
		}
	}
	return -1
}

func (p *pmap) get(k string) int64 {
	i := p.getidx(k)
	if i < 0 {
		panic("hio: no such key [" + k + "]")
	}
	return p.slice[i].v
}

func (p *pmap) set(k string, v int64) {
	i := p.getidx(k)
	if i < 0 {
		p.slice = append(p.slice,
			pitem{
				k: k,
				v: v,
			},
		)
	} else {
		p.slice[i].v = v
	}
}

func (p *pmap) has(k string) bool {
	return p.getidx(k) >= 0
}

func (p *pmap) del(k string) {
	i := p.getidx(k)
	if i < 0 {
		panic("hio: no such key [" + k + "]")
	}

	p.slice = append(p.slice[:i], p.slice[i+1:]...)
}

func (p *pmap) keys() []string {
	keys := make([]string, 0, p.Len())
	for _, v := range p.slice {
		keys = append(keys, v.k)
	}

	return keys
}

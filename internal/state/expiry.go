package state

type expiry struct {
	at uint64
	tr int
}

func (m *Machine) earlier(i, j int) bool {
	a, b := m.expiries[i], m.expiries[j]
	if a.at != b.at {
		return a.at < b.at
	}
	return m.transfer(a.tr).ID.Cmp(m.transfer(b.tr).ID) < 0
}

func (m *Machine) swap(i, j int) {
	q := m.expiries
	q[i], q[j] = q[j], q[i]
	m.transfer(q[i].tr).heap = i
	m.transfer(q[j].tr).heap = j
}

func (m *Machine) up(i int) {
	for i > 0 {
		parent := (i - 1) / 2
		if !m.earlier(i, parent) {
			return
		}
		m.swap(i, parent)
		i = parent
	}
}

func (m *Machine) down(i int) {
	for {
		next := i
		if l := 2*i + 1; l < len(m.expiries) && m.earlier(l, next) {
			next = l
		}
		if r := 2*i + 2; r < len(m.expiries) && m.earlier(r, next) {
			next = r
		}
		if next == i {
			return
		}
		m.swap(i, next)
		i = next
	}
}

func (m *Machine) queue(tr int) {
	t := m.transfer(tr)
	t.heap = len(m.expiries)
	m.expiries = append(m.expiries, expiry{at: t.expiresAt(), tr: tr})
	m.up(t.heap)
}

func (m *Machine) unqueue(pos int) {
	last := len(m.expiries) - 1
	m.transfer(m.expiries[pos].tr).heap = -1
	if pos != last {
		m.expiries[pos] = m.expiries[last]
		m.transfer(m.expiries[pos].tr).heap = pos
	}
	m.expiries = m.expiries[:last]
	if pos < last {
		m.down(pos)
		m.up(pos)
	}
}

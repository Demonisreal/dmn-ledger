package state

type expiry struct {
	at uint64
	tr int
}

func (m *Machine) NextExpiry() (uint64, bool) {
	if len(m.expiries) == 0 {
		return 0, false
	}
	return m.expiries[0].at, true
}

func (m *Machine) expiresBefore(i, j int) bool {
	a, b := m.expiries[i], m.expiries[j]
	if a.at != b.at {
		return a.at < b.at
	}
	return m.transfer(a.tr).ID.Cmp(m.transfer(b.tr).ID) < 0
}

func (m *Machine) swapExpiries(i, j int) {
	q := m.expiries
	q[i], q[j] = q[j], q[i]
	m.transfer(q[i].tr).heap = i
	m.transfer(q[j].tr).heap = j
}

func (m *Machine) siftUp(i int) {
	for i > 0 {
		parent := (i - 1) / 2
		if !m.expiresBefore(i, parent) {
			return
		}
		m.swapExpiries(i, parent)
		i = parent
	}
}

func (m *Machine) siftDown(i int) {
	for {
		next := i
		if l := 2*i + 1; l < len(m.expiries) && m.expiresBefore(l, next) {
			next = l
		}
		if r := 2*i + 2; r < len(m.expiries) && m.expiresBefore(r, next) {
			next = r
		}
		if next == i {
			return
		}
		m.swapExpiries(i, next)
		i = next
	}
}

func (m *Machine) scheduleExpiry(tr int) {
	t := m.transfer(tr)
	t.heap = len(m.expiries)
	m.expiries = append(m.expiries, expiry{at: t.expiresAt(), tr: tr})
	m.siftUp(t.heap)
}

func (m *Machine) cancelExpiry(pos int) {
	last := len(m.expiries) - 1
	m.transfer(m.expiries[pos].tr).heap = -1
	moved := m.expiries[last]
	m.expiries = m.expiries[:last]
	if pos == last {
		return
	}
	m.expiries[pos] = moved
	m.transfer(moved.tr).heap = pos
	m.siftDown(pos)
	m.siftUp(pos)
}

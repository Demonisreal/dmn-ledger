package sim

type evKind uint8

const (
	evTick evKind = iota + 1
	evDeliver
	evWake
	evRestart
	evFault
	evHeal
	evQuiet
	evNudge
	evKill
)

type event struct {
	at    uint64
	seq   uint64
	kind  evKind
	id    uint64
	epoch uint64
	msg   Msg
	win   int
}

type queue []event

func (q queue) less(i, j int) bool {
	if q[i].at != q[j].at {
		return q[i].at < q[j].at
	}
	return q[i].seq < q[j].seq
}

func (q *queue) push(ev event) {
	*q = append(*q, ev)
	h := *q
	for j := len(h) - 1; j > 0; {
		i := (j - 1) / 2
		if !h.less(j, i) {
			break
		}
		h[i], h[j] = h[j], h[i]
		j = i
	}
}

func (q *queue) pop() event {
	h := *q
	n := len(h) - 1
	ev := h[0]
	h[0] = h[n]
	h[n] = event{}
	h = h[:n]
	*q = h
	for i := 0; ; {
		j := 2*i + 1
		if j >= n {
			break
		}
		if r := j + 1; r < n && h.less(r, j) {
			j = r
		}
		if !h.less(j, i) {
			break
		}
		h[i], h[j] = h[j], h[i]
		i = j
	}
	return ev
}

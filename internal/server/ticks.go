package server

import "time"

type ticks struct {
	every time.Duration
	given int64
	most  int64
}

func (t *ticks) take(elapsed time.Duration) int64 {
	owed := int64(elapsed/t.every) - t.given
	if owed > t.most {
		t.given += owed - t.most
		owed = t.most
	}
	t.given += owed
	return owed
}

func (t *ticks) wake(elapsed time.Duration) time.Duration {
	return time.Duration(t.given+1)*t.every - elapsed
}

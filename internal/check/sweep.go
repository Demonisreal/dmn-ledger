package check

import (
	"fmt"
	"runtime/debug"
	"sync"
)

type Panic struct {
	Seed  uint64
	Value any
	Stack []byte
}

func (p *Panic) Error() string {
	return fmt.Sprintf("seed %d panicked: %v\n%s", p.Seed, p.Value, p.Stack)
}

func Sweep(from, count uint64, workers int, try func(seed uint64) error) (uint64, error) {
	var (
		mu    sync.Mutex
		wg    sync.WaitGroup
		taken uint64
		first uint64
		found error
	)
	for range workers {
		wg.Go(func() {
			for {
				mu.Lock()
				seed := from + taken
				if taken == count || found != nil && seed > first {
					mu.Unlock()
					return
				}
				taken++
				mu.Unlock()
				if err := guard(seed, try); err != nil {
					mu.Lock()
					if found == nil || seed < first {
						first, found = seed, err
					}
					mu.Unlock()
				}
			}
		})
	}
	wg.Wait()
	return first, found
}

func guard(seed uint64, try func(uint64) error) (err error) {
	defer func() {
		if v := recover(); v != nil {
			err = &Panic{Seed: seed, Value: v, Stack: debug.Stack()}
		}
	}()
	return try(seed)
}

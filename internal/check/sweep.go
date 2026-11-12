package check

import "sync"

func Sweep(from, count uint64, workers int, try func(seed uint64) error) (uint64, error) {
	var (
		mu    sync.Mutex
		wg    sync.WaitGroup
		next  = from
		first uint64
		found error
	)
	end := from + count
	for range workers {
		wg.Go(func() {
			for {
				mu.Lock()
				seed := next
				if seed >= end || found != nil && seed > first {
					mu.Unlock()
					return
				}
				next++
				mu.Unlock()
				if err := try(seed); err != nil {
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

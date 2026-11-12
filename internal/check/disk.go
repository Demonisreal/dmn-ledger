//go:build !simbug

package check

import "github.com/Demonisreal/dmn-ledger/internal/disk"

func lyingDisk() func(uint64, disk.FS) disk.FS {
	return nil
}

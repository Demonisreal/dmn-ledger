package control

import (
	"errors"
	"fmt"
	"io/fs"
	"net"
	"strconv"
	"strings"
)

var ErrAbort = errors.New("control: the plan must not run")

const MinAvailable = 4 << 30

func CheckStart(fsys fs.FS, m Machine, pgPort int) error {
	var why []string
	if m.Power != Mains {
		why = append(why, "not on mains power")
	}
	if kb, err := field(fsys, "proc/meminfo", "MemAvailable:"); err != nil {
		why = append(why, fmt.Sprintf("meminfo: %v", err))
	} else if kb*1024 < MinAvailable {
		why = append(why, fmt.Sprintf("%d MiB available, want %d", kb/1024, MinAvailable>>20))
	}
	comms, err := fs.Glob(fsys, "proc/*/comm")
	if err != nil {
		return err
	}
	for _, name := range comms {
		if b, err := fs.ReadFile(fsys, name); err == nil && strings.TrimSpace(string(b)) == "ledgerd" {
			why = append(why, "ledgerd already runs as "+strings.TrimSuffix(strings.TrimPrefix(name, "proc/"), "/comm"))
		}
	}
	ln, err := net.Listen("tcp", net.JoinHostPort("127.0.0.1", strconv.Itoa(pgPort)))
	if err != nil {
		why = append(why, fmt.Sprintf("port %d taken", pgPort))
	} else {
		_ = ln.Close()
	}
	if len(why) > 0 {
		return fmt.Errorf("%w: %s", ErrAbort, strings.Join(why, "; "))
	}
	return nil
}

func TooManyBad(bad, runs int) bool {
	return 4*bad > runs
}

// Package history records the calls of real clients in windows and hands each closed window to
// check.Replayer in a goroutine of its own, so a cluster of processes is judged by the same oracle
// and linearizability check as the simulator.
package history

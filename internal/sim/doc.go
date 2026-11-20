// Package sim runs a whole cluster inside one goroutine on virtual time, driven by a single seed.
// The network, the disks and the fault plan all draw from that seed, so every run, and every
// failure it finds, can be replayed exactly.
package sim

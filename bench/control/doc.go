// Package control measures the machine next to a run: flushes of the disk, CPU time, frequency,
// throttling and memory, once a second. It sorts each second by the speed of the disk, decides which
// seconds count, and says when two runs of the same measurement agree.
package control

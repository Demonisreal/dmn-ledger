// Package ledgerrun is the load target for a real cluster: it builds ledgerd, runs one or three
// nodes as child processes on loopback, books through the public SDK, reads the stats line of
// every node on SIGUSR1 and checks the books after a run.
package ledgerrun

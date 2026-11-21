// Package local runs three ledgerd processes on loopback with a faultnet link for each direction
// between two nodes and, if asked, one from the clients to each node. The chaos test and the demo
// start their clusters with it, and it makes the data directory of each node before the node first
// starts. It needs Linux.
package local

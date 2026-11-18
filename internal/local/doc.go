// Package local runs three ledgerd processes on loopback with a faultnet link for each direction
// between two nodes and, if asked, one from the clients to each node. The chaos test and the demo
// start their clusters with it. It needs Linux.
package local

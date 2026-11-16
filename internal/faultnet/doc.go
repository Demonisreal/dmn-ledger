// Package faultnet is a TCP proxy for one direction of a link, from a node to a peer or from a
// client to a node. It reads the frame header and the kind in a peer hello, nothing else, so it can
// delay, drop, swallow, stall or corrupt whole frames without tearing the stream.
package faultnet

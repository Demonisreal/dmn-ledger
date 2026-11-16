// Package server drives one replica.Node over real sockets and a real clock. A single loop owns
// the node and sends only what Node.Process returned, so nothing leaves the process before it is
// on disk; readers and writers around it only carry frames.
package server

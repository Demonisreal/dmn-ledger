// Package server drives one replica.Node over real sockets and a real clock. A single loop owns
// the node and sends only what Node.Process returned, so nothing leaves the process before it is
// on disk; readers and writers around it only carry frames. With a key, the loop signs the head
// under each receipt and head that Process returned, after Process and only there.
package server

// Package sim runs a whole cluster inside one goroutine on virtual time, driven by a single seed.
// The network, the disks and the fault plan all draw from that seed, so every run, and every
// failure it finds, can be replayed exactly.
//
// A Process is one node or client. Tick and Receive only change memory; Run does the disk work
// and returns the messages to send. The world sets From, deep-copies every Body through Clone
// when the message leaves, and holds a node's messages back until the fsync time its Run cost
// has passed. Inputs that arrive meanwhile are fed in together before the next Run. An error
// from a node's Run stops that node, which later restarts through Config.Node on the same disk,
// after a process crash or a power loss. A process must take time from Clock or Now, randomness
// from Rand and Nonces, and must not keep state outside itself, or the run stops being
// reproducible.
//
// Nodes have the addresses 1..Nodes, clients follow them. Body.String must not print pointers
// or map contents: every event, and every copy that is sent, becomes one line of the trace,
// and TraceHash is the SHA-256 over all of them.
package sim

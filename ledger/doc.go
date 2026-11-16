// Package ledger is the Go client of a dmn-ledger cluster. A Client registers one session, sends one
// request at a time and retries it with the same number on the next node until one answers, so every
// write applies exactly once; writes that arrive while a request is out share the next one.
//
// A write that returns an error matching ErrUncertain was sent, and its outcome is unknown: send the
// same transfers again with the same IDs, and those that already applied come back as Exists. Any
// other error means the write was not applied. After ErrEvicted the client is finished and never
// registers again on its own; dial a new one and decide what to send again.
package ledger

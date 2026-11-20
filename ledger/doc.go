// Package ledger is the Go client of a dmn-ledger cluster. A Client registers one session, sends one
// request at a time and retries it with the same number on the next node until one answers, so every
// write applies exactly once; writes that arrive while a request is out share the next one.
//
// A write that returns an error matching ErrUncertain was sent, and its outcome is unknown: send the
// same transfers again with the same IDs, and those that already applied come back as Exists. Any
// other error means the write was not applied. After ErrEvicted the client is finished and never
// registers again on its own; dial a new one and decide what to send again.
//
// Prove, ProveBatch and Head fetch receipts as bytes, stamped by the answering leader if it has a key,
// and VerifyProof checks one offline: a valid receipt shows that the holder of a trusted key declared
// the head of this cluster's chain at one link number, and SHA-256 puts the record under that head;
// it shows neither a majority, nor that the head is the newest, nor a balance or the state of a
// transfer today, nor that anything is absent. ProveBatch takes Result.Batch, the time of the batch a
// write ran in, which is earlier than Result.Timestamp for a write bundled behind others. Links asks
// for at most 1024 links per request and, when a request fails, returns the links it already has
// together with the error. ConnectProofs folds the links between two verified proofs: ErrConflict
// means two heads with the same number and different hashes, which proves two histories; ErrBadLinks
// means only that these links do not connect the heads, which proves nothing about them.
package ledger

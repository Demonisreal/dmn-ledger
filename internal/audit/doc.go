// Package audit holds the hash formats of the audit trail: a Merkle tree per batch after RFC 6962,
// a chain of links with one head per batch, and receipts that lead from a record to a head. It only
// hashes; keys and signatures live in internal/notary.
package audit

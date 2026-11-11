// Package state is the deterministic ledger state machine replicated by the cluster.
// It applies batches of accounts and transfers under the rules in docs/fachregeln.md.
// A Machine comes from New or ReadSnapshot and is not safe for concurrent use, not even for Digest.
package state

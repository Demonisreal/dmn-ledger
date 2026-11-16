// Package notary stamps heads of the audit trail: an Ed25519 signature of one node over cluster ID,
// link number and head hash, checked against a list of trusted keys. It is the only package that
// handles a signing key; the deterministic core never imports it.
package notary

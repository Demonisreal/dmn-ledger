// Package pg is the load target for Postgres: one container on the same disk, a schema with
// balances and constraints, SQL functions that lock accounts in id order, and a client that books the
// same stream as the cluster under read committed or serializable, one transaction per transfer or
// batched per lane.
package pg

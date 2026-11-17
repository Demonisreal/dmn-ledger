// Package pg is the load target for Postgres: one container on the same disk, a schema with
// balances and constraints, the four variants as SQL functions, and a client that books the same
// stream as the cluster, one transaction per transfer or batched per lane.
package pg

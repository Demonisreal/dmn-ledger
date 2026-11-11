// Package wal stores the replicated log in segment files, together with the hard state
// (term and vote) and snapshots. Nothing counts as written before Sync returns, and after
// any I/O error a WAL or HardStateFile refuses further work until it is opened again.
package wal

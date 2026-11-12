// Package wal stores the replicated log in segment files, together with the metadata
// (term, vote, repair flag and seal) and snapshots. Nothing counts as written before Sync
// returns, and after any I/O error a WAL or MetaFile refuses further work until it is
// opened again. The seal is the highest index the node may have acknowledged: a broken
// frame up to the seal is corruption, a broken frame behind it is a torn write.
package wal

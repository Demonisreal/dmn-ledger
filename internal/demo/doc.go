// Package demo is the controller of the public demo: it runs a cluster of three ledgerd, puts a
// small load on it, judges every answer with check.Replayer, lets visitors trigger one of four
// faults that it lifts itself, and serves what happens as state, lines and receipts.
package demo

// Ledgerd runs one node of a dmn-ledger cluster, on a data directory that ledgerctl init made. It
// takes its configuration only from LEDGER_ variables in the environment, refuses any it does not
// know, and stops cleanly on SIGTERM or SIGINT.
package main

// Ledgerctl talks to a dmn-ledger cluster through the ledger package: it shows the status of every
// node, creates accounts and transfers, and looks them up. It also makes key files, fetches stamped
// receipts and heads, and checks them offline: a receipt against a list of node keys, two heads over
// the links between them, and a copy of a data directory recounted up to a head saved earlier.
package main

# dmn-ledger

A replicated double-entry ledger database in Go. Work in progress.

The goal: no cent is ever lost or created, even when a node crashes mid-transfer, the network
splits or a disk returns corrupted data, and every transfer can be proven afterwards.

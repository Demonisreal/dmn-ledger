// Package check runs one seed of the simulated cluster under the safety and liveness checkers
// and reports the first violation with seed, virtual time, node and reason. The profile beyond
// runs without the checks at the end. Sweep runs many seeds and reports a panic like a violation.
// The checkers are listed in docs/ARCHITECTURE.md under "Checkers".
package check

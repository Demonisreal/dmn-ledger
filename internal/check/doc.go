// Package check runs one seed of the simulated cluster under the safety and liveness checkers
// and reports the first violation with seed, virtual time, node and reason. In the profile beyond
// only the log replay is left of the checks at the end. Sweep runs many seeds and reports a panic
// like a violation.
// The checkers are listed in docs/ARCHITECTURE.md under "Checkers".
package check

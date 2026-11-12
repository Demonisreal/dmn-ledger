// Package check runs one seed of the simulated cluster under every safety and liveness checker
// and reports the first violation with seed, virtual time, node and reason. The checkers are
// listed in docs/ARCHITECTURE.md under "Simulator".
package check

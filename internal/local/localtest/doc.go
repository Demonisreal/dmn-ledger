// Package localtest holds what the tests with ledgerd processes share: building the binaries once
// in TestMain, finding the ledgerd children of a process and waiting for a condition. Only tests
// import it.
package localtest

// Package chaos runs three ledgerd processes under kill -9, SIGSTOP and a faulty network and judges
// what the clients saw with the same replayer and linearizability check as the simulator. The tests
// need Linux and the build tag chaos; with the tag simbug they also hunt planted bugs.
package chaos

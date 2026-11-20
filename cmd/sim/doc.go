// Sim runs seeds of the deterministic simulator through the checkers, one seed with its trace or a
// range of seeds on all cores, and prints the command that replays the first failure. Built with
// -tags simbug, it plants one of the bugs from -bug to show that the checkers find it.
package main

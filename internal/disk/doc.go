// Package disk is the narrow file system interface the log is written against.
// OS uses the real file system; Sim keeps files in memory and reproduces torn writes,
// lost unsynced data, failed fsyncs and I/O errors, all driven by one seeded generator.
// A test can also flip a bit or garble a sector in data at rest.
package disk

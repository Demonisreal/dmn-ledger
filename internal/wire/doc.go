// Package wire frames and encodes what crosses a socket: client requests, replies and status,
// the peer hello, every raft message and the snapshot stream. A decoder checks each length before
// it allocates and accepts only what the encoders produce, so a decoded frame encodes back to the
// same bytes. The layouts are pinned in testdata/frames.golden; changing one means a new Version.
package wire

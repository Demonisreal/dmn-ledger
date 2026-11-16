package notary

import (
	"crypto/ed25519"
	"encoding/binary"
	"errors"
	"slices"

	"github.com/Demonisreal/dmn-ledger/internal/audit"
)

const StampSize = 8 + ed25519.PublicKeySize + ed25519.SignatureSize

const domain = "dmn-ledger head v1\n"

var (
	ErrUnsigned  = errors.New("notary: receipt carries no stamp")
	ErrCluster   = errors.New("notary: stamp is for another cluster")
	ErrUntrusted = errors.New("notary: key is not trusted")
	ErrSignature = errors.New("notary: bad signature")
)

func message(cluster uint64, h audit.Head) []byte {
	msg := make([]byte, 0, len(domain)+8+8+audit.HashSize)
	msg = append(msg, domain...)
	msg = binary.BigEndian.AppendUint64(msg, cluster)
	msg = binary.BigEndian.AppendUint64(msg, h.Seq)
	return append(msg, h.Hash[:]...)
}

func Sign(key ed25519.PrivateKey, cluster uint64, h audit.Head) []byte {
	stamp := make([]byte, 0, StampSize)
	stamp = binary.BigEndian.AppendUint64(stamp, cluster)
	stamp = append(stamp, key.Public().(ed25519.PublicKey)...)
	return append(stamp, ed25519.Sign(key, message(cluster, h))...)
}

func Signer(stamp []byte, h audit.Head) (cluster uint64, key ed25519.PublicKey, ok bool) {
	if len(stamp) != StampSize {
		return 0, nil, false
	}
	cluster = binary.BigEndian.Uint64(stamp)
	key = ed25519.PublicKey(stamp[8 : 8+ed25519.PublicKeySize : 8+ed25519.PublicKeySize])
	return cluster, key, ed25519.Verify(key, message(cluster, h), stamp[8+ed25519.PublicKeySize:])
}

func Verify(proof []byte, cluster uint64, trusted []ed25519.PublicKey) (audit.Receipt, audit.Head, ed25519.PublicKey, error) {
	r, h, stamp, err := audit.Decode(proof)
	if err != nil {
		return audit.Receipt{}, audit.Head{}, nil, err
	}
	if stamp == nil {
		return audit.Receipt{}, audit.Head{}, nil, ErrUnsigned
	}
	signedFor, key, ok := Signer(stamp, h)
	switch {
	case signedFor != cluster:
		return audit.Receipt{}, audit.Head{}, nil, ErrCluster
	case !slices.ContainsFunc(trusted, func(k ed25519.PublicKey) bool { return k.Equal(key) }):
		return audit.Receipt{}, audit.Head{}, nil, ErrUntrusted
	case !ok:
		return audit.Receipt{}, audit.Head{}, nil, ErrSignature
	}
	return r, h, key, nil
}

package gcpkms

import (
	"bytes"
	"crypto/aes"
	"encoding/binary"
	"time"
)

// SetClock replaces the backend clock.
func (b *InMemoryBackend) SetClock(now func() time.Time) {
	b.mu.Lock("SetClock")
	defer b.mu.Unlock()

	b.now = now
}

// KWPWrap exposes RFC 5649 key wrapping for building import fixtures.
func KWPWrap(kek, key []byte) ([]byte, error) { return kwpWrap(kek, key) }

// kwpWrap implements RFC 5649 AES key wrap with padding (test fixtures).
func kwpWrap(kek, key []byte) ([]byte, error) {
	block, err := aes.NewCipher(kek)
	if err != nil {
		return nil, errInvalid("The wrapping key is invalid.")
	}

	padLen := (kwpHalf - len(key)%kwpHalf) % kwpHalf
	p := append(bytes.Clone(key), make([]byte, padLen)...)

	var a [kwpHalf]byte

	copy(a[:], kwpPrefix())
	binary.BigEndian.PutUint32(a[4:], uint32(len(key)))

	if len(p) == kwpHalf {
		buf := append(a[:], p...)
		block.Encrypt(buf, buf)

		return buf, nil
	}

	n := len(p) / kwpHalf
	r := bytes.Clone(p)
	buf := make([]byte, 2*kwpHalf)

	for j := range kwpRounds {
		for i := 1; i <= n; i++ {
			copy(buf, a[:])
			copy(buf[kwpHalf:], r[(i-1)*kwpHalf:i*kwpHalf])
			block.Encrypt(buf, buf)
			t := uint64(n*j + i)
			binary.BigEndian.PutUint64(a[:], binary.BigEndian.Uint64(buf[:kwpHalf])^t)
			copy(r[(i-1)*kwpHalf:], buf[kwpHalf:])
		}
	}

	return append(a[:], r...), nil
}

// KWPUnwrap exposes RFC 5649 key unwrapping.
func KWPUnwrap(kek, wrapped []byte) ([]byte, error) { return kwpUnwrap(kek, wrapped) }

package ocifilestorage

import (
	"encoding/binary"
	"errors"
	"fmt"
	"strings"
)

// Keytab encryption types by kerberos enctype number, as named by the File Storage API.
//
//nolint:gochecknoglobals // immutable lookup table
var keytabEnctypes = map[uint16]string{
	17: "AES128_CTS_HMAC_SHA1_96",
	18: "AES256_CTS_HMAC_SHA1_96",
	19: "AES128_CTS_HMAC_SHA256_128",
	20: "AES256_CTS_HMAC_SHA384_192",
}

const (
	keytabMagic     = 0x0502
	keytabMinLen    = 2
	signBit         = 1 << 31
	wordSize        = 1 << 32
	uint16Len       = 2
	keytabEntryMin  = 4
	keytabNameTSLen = 8
)

var errKeytab = errors.New("invalid keytab")

type keytabEntry struct {
	Principal string `json:"principal"`
	Enctype   string `json:"encryptionType"`
	Kvno      int64  `json:"keyVersionNumber"`
}

type keytabReader struct {
	data []byte
	off  int
}

func (r *keytabReader) take(n int) ([]byte, error) {
	if n < 0 || r.off+n > len(r.data) {
		return nil, fmt.Errorf("%w: truncated", errKeytab)
	}

	out := r.data[r.off : r.off+n]
	r.off += n

	return out, nil
}

func (r *keytabReader) u16() (uint16, error) {
	b, err := r.take(uint16Len)
	if err != nil {
		return 0, err
	}

	return binary.BigEndian.Uint16(b), nil
}

func (r *keytabReader) str() (string, error) {
	n, err := r.u16()
	if err != nil {
		return "", err
	}

	b, err := r.take(int(n))

	return string(b), err
}

// parseKeytab reads an MIT keytab (file format version 2) and returns its entries.
func parseKeytab(data []byte) ([]keytabEntry, error) {
	if len(data) < keytabMinLen || binary.BigEndian.Uint16(data) != keytabMagic {
		return nil, fmt.Errorf("%w: missing version 2 header", errKeytab)
	}

	r := &keytabReader{data: data, off: keytabMinLen}

	var out []keytabEntry

	for r.off < len(data) {
		szb, err := r.take(keytabEntryMin)
		if err != nil {
			return nil, err
		}

		size := int64(binary.BigEndian.Uint32(szb))
		if size >= signBit { // a negative size marks a hole left by a deleted entry
			if _, err = r.take(int(wordSize - size)); err != nil {
				return nil, err
			}

			continue
		}

		body, err := r.take(int(size))
		if err != nil {
			return nil, err
		}

		ent, err := parseKeytabEntry(body)
		if err != nil {
			return nil, err
		}

		out = append(out, ent)
	}

	if len(out) == 0 {
		return nil, fmt.Errorf("%w: no entries", errKeytab)
	}

	return out, nil
}

func parseKeytabEntry(body []byte) (keytabEntry, error) {
	r := &keytabReader{data: body}

	n, err := r.u16()
	if err != nil {
		return keytabEntry{}, err
	}

	realm, err := r.str()
	if err != nil {
		return keytabEntry{}, err
	}

	comps := make([]string, 0, n)

	for range n {
		c, cerr := r.str()
		if cerr != nil {
			return keytabEntry{}, cerr
		}

		comps = append(comps, c)
	}

	if _, err = r.take(keytabNameTSLen); err != nil { // name type and timestamp
		return keytabEntry{}, err
	}

	rest, err := r.take(1)
	if err != nil {
		return keytabEntry{}, err
	}

	kvno := int64(rest[0])

	et, err := r.u16()
	if err != nil {
		return keytabEntry{}, err
	}

	name, ok := keytabEnctypes[et]
	if !ok {
		return keytabEntry{}, fmt.Errorf("%w: unsupported encryption type %d", errKeytab, et)
	}

	return keytabEntry{Principal: strings.Join(comps, "/") + "@" + realm, Enctype: name, Kvno: kvno}, nil
}

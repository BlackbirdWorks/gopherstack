package ec2

import (
	"crypto/ed25519"
	"crypto/hmac"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"math/big"
	"slices"
	"strings"

	"golang.org/x/crypto/ssh"
)

const (
	keyFormatPEM = "pem"
	keyFormatPPK = "ppk"
	ppkLineWidth = 64
)

// ppkWriter accumulates SSH wire-format (RFC 4251) strings and mpints.
type ppkWriter struct{ buf []byte }

func (w *ppkWriter) str(b []byte) {
	w.buf = binary.BigEndian.AppendUint32(w.buf, uint32(len(b))) //nolint:gosec // PPK blobs are far below 4 GiB
	w.buf = append(w.buf, b...)
}

func (w *ppkWriter) mpint(n *big.Int) {
	b := n.Bytes()
	if len(b) > 0 && b[0]&0x80 != 0 {
		b = append([]byte{0}, b...)
	}

	w.str(b)
}

// pemToPPK re-encodes an unencrypted PEM private key as a PuTTY PPK v3 file
// (Encryption: none, so the MAC key is empty).
func pemToPPK(pemKey, comment string) (string, error) {
	raw, err := ssh.ParseRawPrivateKey([]byte(pemKey))
	if err != nil {
		return "", fmt.Errorf("parse private key: %w", err)
	}

	var algo string

	var pub, priv ppkWriter

	switch k := raw.(type) {
	case *rsa.PrivateKey:
		algo = ssh.KeyAlgoRSA
		pub.str([]byte(algo))
		pub.mpint(big.NewInt(int64(k.E)))
		pub.mpint(k.N)
		priv.mpint(k.D)
		priv.mpint(k.Primes[0])
		priv.mpint(k.Primes[1])
		priv.mpint(k.Precomputed.Qinv)
	case *ed25519.PrivateKey:
		algo = ssh.KeyAlgoED25519
		pub.str([]byte(algo))
		pub.str((*k)[ed25519.SeedSize:])
		seed := slices.Clone(k.Seed())
		slices.Reverse(seed)
		priv.mpint(new(big.Int).SetBytes(seed))
	default:
		return "", fmt.Errorf("%w: unsupported key type %T", ErrInvalidParameter, raw)
	}

	return renderPPK(algo, comment, pub.buf, priv.buf), nil
}

func renderPPK(algo, comment string, pub, priv []byte) string {
	var mac ppkWriter

	mac.str([]byte(algo))
	mac.str([]byte("none"))
	mac.str([]byte(comment))
	mac.str(pub)
	mac.str(priv)

	h := hmac.New(sha256.New, nil)
	_, _ = h.Write(mac.buf)

	var sb strings.Builder

	sb.WriteString("PuTTY-User-Key-File-3: " + algo + "\nEncryption: none\nComment: " + comment + "\n")
	writePPKLines(&sb, "Public-Lines", pub)
	writePPKLines(&sb, "Private-Lines", priv)
	sb.WriteString("Private-MAC: " + hex.EncodeToString(h.Sum(nil)) + "\n")

	return sb.String()
}

func writePPKLines(sb *strings.Builder, header string, blob []byte) {
	enc := base64.StdEncoding.EncodeToString(blob)
	lines := (len(enc) + ppkLineWidth - 1) / ppkLineWidth

	fmt.Fprintf(sb, "%s: %d\n", header, lines)

	for i := 0; i < len(enc); i += ppkLineWidth {
		sb.WriteString(enc[i:min(i+ppkLineWidth, len(enc))] + "\n")
	}
}

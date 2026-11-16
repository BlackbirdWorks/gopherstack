package httputils

import (
	"bytes"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"math/big"
	"net/http"
	"sort"
	"strings"
)

// SigV4aAlgorithm is the asymmetric signing algorithm of multi-Region SigV4a requests.
const SigV4aAlgorithm = "AWS4-ECDSA-P256-SHA256"

const (
	codeIncompleteSignature = "IncompleteSignatureException"
	sigV4aNMinus            = 2
	bitsPerByte             = 8
	sigV4aScopeParts        = 4
	sigV4aKeyBits           = 256
	sigV4aMaxCounter        = 0xff
	sigV4aKeyByteSize       = sigV4aKeyBits / 8
)

// VerifyV4a checks the ECDSA P-256 signature of a SigV4a Authorization header. The key pair is derived from
// the access key id in the credential and the validator secret, as the SDK signer does (FIPS 186-4 B.4.2,
// aws-sdk-go-v2/internal/v4a deriveKeyFromAccessKeyPair).
func (v *SigV4Validator) VerifyV4a(r *http.Request) *SigV4Error {
	parsed, perr := parseSigV4aHeader(HeaderValue(r.Header, "Authorization"))
	if perr != nil {
		return perr
	}

	payloadHash := HeaderValue(r.Header, "X-Amz-Content-Sha256")
	if payloadHash == "" {
		payloadHash = hashRequestBody(r)
	}

	amzDate := HeaderValue(r.Header, "X-Amz-Date")
	if amzDate == "" {
		return &SigV4Error{
			Code:    codeIncompleteSignature,
			Message: "Authorization header requires existence of either a 'X-Amz-Date' or a 'Date' header.",
			Status:  http.StatusBadRequest,
		}
	}

	scope := strings.Join([]string{parsed.date, parsed.service, sigV4TerminalScope}, "/")
	canonical := buildCanonicalRequest(r, parsed.signedHeaders, payloadHash)
	hashed := sha256.Sum256([]byte(strings.Join([]string{
		SigV4aAlgorithm, amzDate, scope, hashHex([]byte(canonical)),
	}, "\n")))

	sig, derr := hex.DecodeString(parsed.signature)
	key := deriveSigV4aKey(parsed.accessKey, v.secretKey)

	if derr != nil || key == nil || !verifyASN1(&key.PublicKey, hashed[:], sig) {
		return &SigV4Error{
			Code: "InvalidSignatureException",
			Message: "The request signature we calculated does not match the signature you " +
				"provided. Check your AWS Secret Access Key and signing method.",
			Status: http.StatusForbidden,
		}
	}

	return nil
}

type sigV4aHeader struct {
	accessKey     string
	date          string
	service       string
	signature     string
	signedHeaders []string
}

func parseSigV4aHeader(auth string) (sigV4aHeader, *SigV4Error) {
	malformed := &SigV4Error{
		Code:    codeIncompleteSignature,
		Message: "Authorization header requires 'Credential', 'Signature' and 'SignedHeaders' parameters.",
		Status:  http.StatusBadRequest,
	}

	var p sigV4aHeader

	var credential string

	for field := range strings.SplitSeq(strings.TrimSpace(strings.TrimPrefix(auth, SigV4aAlgorithm)), ",") {
		key, val, found := strings.Cut(strings.TrimSpace(field), "=")
		if !found {
			continue
		}

		switch strings.TrimSpace(key) {
		case "Credential":
			credential = strings.TrimSpace(val)
		case "SignedHeaders":
			for h := range strings.SplitSeq(strings.TrimSpace(val), ";") {
				if h != "" {
					p.signedHeaders = append(p.signedHeaders, strings.ToLower(h))
				}
			}
		case "Signature":
			p.signature = strings.TrimSpace(val)
		}
	}

	parts := strings.Split(credential, "/")
	if len(parts) != sigV4aScopeParts || parts[0] == "" || parts[sigV4aScopeParts-1] != sigV4TerminalScope ||
		p.signature == "" || len(p.signedHeaders) == 0 {
		return p, malformed
	}

	p.accessKey, p.date, p.service = parts[0], parts[1], parts[2]
	sort.Strings(p.signedHeaders)

	return p, nil
}

func hashHex(b []byte) string {
	sum := sha256.Sum256(b)

	return hex.EncodeToString(sum[:])
}

func verifyASN1(pub *ecdsa.PublicKey, hash, sig []byte) bool {
	return ecdsa.VerifyASN1(pub, hash, sig)
}

// deriveSigV4aKey returns the P-256 key pair for an access key id and secret, or nil when no candidate fits.
func deriveSigV4aKey(accessKey, secret string) *ecdsa.PrivateKey {
	curve := elliptic.P256()
	nMinusTwo := new(big.Int).Sub(curve.Params().N, big.NewInt(sigV4aNMinus))
	input := append([]byte("AWS4A"), secret...)

	for counter := byte(1); ; counter++ {
		ctx := append([]byte(accessKey), counter)
		raw := kdfCounterMode(input, []byte(SigV4aAlgorithm), ctx, sigV4aKeyBits)

		c := new(big.Int).SetBytes(raw)
		if c.Cmp(nMinusTwo) >= 0 {
			if counter == sigV4aMaxCounter {
				return nil
			}

			continue
		}

		d := c.Add(c, big.NewInt(1))

		priv, err := ecdsa.ParseRawPrivateKey(curve, d.FillBytes(make([]byte, sigV4aKeyByteSize)))
		if err != nil {
			return nil
		}

		return priv
	}
}

// kdfCounterMode is the NIST SP 800-108 counter-mode KDF with HMAC-SHA256 and a 4-byte counter; bitLen must
// be a multiple of the hash size so one block per counter value is used.
func kdfCounterMode(key, label, ctx []byte, bitLen uint32) []byte {
	var fixed bytes.Buffer

	fixed.Write(label)
	fixed.WriteByte(0)
	fixed.Write(ctx)

	var lenBytes [4]byte
	binary.BigEndian.PutUint32(lenBytes[:], bitLen)
	fixed.Write(lenBytes[:])

	var out []byte

	blocks := bitLen / (sha256.Size * bitsPerByte)
	for i := uint32(1); i <= blocks; i++ {
		h := hmac.New(sha256.New, key)

		var ctr [4]byte
		binary.BigEndian.PutUint32(ctr[:], i)
		h.Write(ctr[:])
		h.Write(fixed.Bytes())
		out = h.Sum(out)
	}

	return out
}

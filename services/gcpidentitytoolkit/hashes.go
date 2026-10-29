package gcpidentitytoolkit

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/hmac"
	"crypto/md5" //nolint:gosec // wire-compatible import of legacy MD5 password hashes
	"crypto/pbkdf2"
	"crypto/rand"
	"crypto/sha1" //nolint:gosec // wire-compatible import of legacy SHA1 password hashes
	"crypto/sha256"
	"crypto/sha512"
	"crypto/subtle"
	"encoding/base64"
	"hash"

	"golang.org/x/crypto/bcrypt"
	"golang.org/x/crypto/scrypt"
)

const (
	algScrypt         = "SCRYPT"
	algStdScrypt      = "STANDARD_SCRYPT"
	algBcrypt         = "BCRYPT"
	algPBKDFSHA1      = "PBKDF_SHA1"
	algPBKDF2SHA256   = "PBKDF2_SHA256"
	orderSaltFirst    = "SALT_AND_PASSWORD"
	nativeRounds      = 8
	nativeMemoryCost  = 14
	scryptKeyLen      = 32
	sha1Len           = 20
	nativeSignerBytes = 64
	saltBytes         = 16
	algMD5            = "MD5"
	algSHA1           = "SHA1"
	algSHA256         = "SHA256"
	algSHA512         = "SHA512"
)

// hashSpec describes how one password hash was produced; the zero Algorithm means the project's native scrypt.
type hashSpec struct {
	Algorithm         string `json:"algorithm"`
	PasswordHashOrder string `json:"passwordHashOrder,omitempty"`
	SignerKey         []byte `json:"signerKey,omitempty"`
	SaltSeparator     []byte `json:"saltSeparator,omitempty"`
	Rounds            int64  `json:"rounds,omitempty"`
	MemoryCost        int64  `json:"memoryCost,omitempty"`
	CPUMemCost        int64  `json:"cpuMemCost,omitempty"`
	BlockSize         int64  `json:"blockSize,omitempty"`
	Parallelization   int64  `json:"parallelization,omitempty"`
	DKLen             int64  `json:"dkLen,omitempty"`
}

func newNativeSpec() hashSpec {
	key := make([]byte, nativeSignerBytes)
	_, _ = rand.Read(key)

	return hashSpec{
		Algorithm: algScrypt, SignerKey: key, SaltSeparator: []byte{0x1d},
		Rounds: nativeRounds, MemoryCost: nativeMemoryCost,
	}
}

// decodeB64 accepts standard and URL alphabets, padded or not.
func decodeB64(s string) ([]byte, error) {
	encodings := []*base64.Encoding{
		base64.RawURLEncoding, base64.URLEncoding, base64.RawStdEncoding, base64.StdEncoding,
	}

	for _, enc := range encodings {
		if b, err := enc.DecodeString(s); err == nil {
			return b, nil
		}
	}

	return nil, errBadBase64
}

func newSalt() []byte {
	salt := make([]byte, saltBytes)
	_, _ = rand.Read(salt)

	return salt
}

func hasher(alg string) func() hash.Hash {
	switch alg {
	case algHMACSHA256, algSHA256, algPBKDF2SHA256:
		return sha256.New
	case algHMACSHA512, algSHA512:
		return sha512.New
	case algHMACSHA1, algSHA1, algPBKDFSHA1:
		return sha1.New
	case algHMACMD5, algMD5:
		return md5.New
	}

	return nil
}

func (h hashSpec) message(password string, salt []byte) []byte {
	pw := []byte(password)
	if h.PasswordHashOrder == orderSaltFirst {
		out := append(append([]byte{}, salt...), h.SaltSeparator...)

		return append(out, pw...)
	}

	out := append(append([]byte{}, pw...), h.SaltSeparator...)

	return append(out, salt...)
}

// compute returns the raw hash of password.
func (h hashSpec) compute(password string, salt []byte) ([]byte, error) {
	switch h.Algorithm {
	case algScrypt:
		return h.firebaseScrypt(password, salt)
	case algStdScrypt:
		return scrypt.Key(
			[]byte(password),
			salt,
			int(h.CPUMemCost),
			int(h.BlockSize),
			int(h.Parallelization),
			int(h.DKLen),
		)
	case algPBKDFSHA1:
		return pbkdf2.Key(sha1.New, password, salt, int(h.Rounds), sha1Len)
	case algPBKDF2SHA256:
		return pbkdf2.Key(sha256.New, password, salt, int(h.Rounds), sha256.Size)
	case algHMACSHA256, algHMACSHA512, algHMACSHA1, algHMACMD5:
		mac := hmac.New(hasher(h.Algorithm), h.SignerKey)
		mac.Write(h.message(password, salt))

		return mac.Sum(nil), nil
	case algMD5, algSHA1, algSHA256, algSHA512:
		return h.iterated(password, salt), nil
	}

	return nil, errBadAlgorithm
}

func (h hashSpec) iterated(password string, salt []byte) []byte {
	hf := hasher(h.Algorithm)
	cur := h.message(password, salt)

	for range max(h.Rounds, 1) {
		d := hf()
		d.Write(cur)
		cur = d.Sum(nil)
	}

	return cur
}

// firebaseScrypt is Firebase's modified scrypt: scrypt over salt||separator, then AES-256-CTR of the signer key.
func (h hashSpec) firebaseScrypt(password string, salt []byte) ([]byte, error) {
	s := append(append([]byte{}, salt...), h.SaltSeparator...)

	dk, err := scrypt.Key([]byte(password), s, 1<<h.MemoryCost, int(h.Rounds), 1, scryptKeyLen)
	if err != nil {
		return nil, err //nolint:wrapcheck // parameter error surfaced as invalid hash config
	}

	block, err := aes.NewCipher(dk)
	if err != nil {
		return nil, err //nolint:wrapcheck // unreachable: dk is 32 bytes
	}

	out := make([]byte, len(h.SignerKey))
	//nolint:gosec // Firebase's password scheme is defined with an all-zero CTR IV
	cipher.NewCTR(block, make([]byte, aes.BlockSize)).XORKeyStream(out, h.SignerKey)

	return out, nil
}

// verify reports whether password matches the stored hash and salt.
func (h hashSpec) verify(password string, salt, stored []byte) bool {
	if h.Algorithm == algBcrypt {
		return bcrypt.CompareHashAndPassword(stored, []byte(password)) == nil
	}

	got, err := h.compute(password, salt)
	if err != nil {
		return false
	}

	return subtle.ConstantTimeCompare(got, stored) == 1
}

// validate rejects specs the backend cannot compute.
func (h hashSpec) validate() error {
	check, ok := specChecks[h.Algorithm]
	if !ok || (check != nil && !check(h)) {
		return errBadAlgorithm
	}

	return nil
}

//nolint:gochecknoglobals // immutable lookup table
var specChecks = map[string]func(hashSpec) bool{
	algBcrypt: nil,
	algScrypt: func(h hashSpec) bool {
		return len(h.SignerKey) > 0 && h.Rounds >= 1 && h.MemoryCost >= 1 && h.MemoryCost <= 20
	},
	algStdScrypt: func(h hashSpec) bool {
		return h.CPUMemCost >= 2 && h.BlockSize >= 1 && h.Parallelization >= 1 && h.DKLen >= 1
	},
	algPBKDFSHA1:    func(h hashSpec) bool { return h.Rounds >= 1 },
	algPBKDF2SHA256: func(h hashSpec) bool { return h.Rounds >= 1 },
	algHMACSHA256:   func(h hashSpec) bool { return len(h.SignerKey) > 0 },
	algHMACSHA512:   func(h hashSpec) bool { return len(h.SignerKey) > 0 },
	algHMACSHA1:     func(h hashSpec) bool { return len(h.SignerKey) > 0 },
	algHMACMD5:      func(h hashSpec) bool { return len(h.SignerKey) > 0 },
	algMD5:          func(h hashSpec) bool { return h.Rounds >= 1 },
	algSHA1:         func(h hashSpec) bool { return h.Rounds >= 1 },
	algSHA256:       func(h hashSpec) bool { return h.Rounds >= 1 },
	algSHA512:       func(h hashSpec) bool { return h.Rounds >= 1 },
}

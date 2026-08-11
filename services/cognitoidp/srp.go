package cognitoidp

// srp.go implements the server side of Cognito's USER_SRP_AUTH/ADMIN_USER_SRP_AUTH
// handshake: AWS's variant of SRP-6a (RFC 5054 group parameters, HKDF-derived session
// key, HMAC-SHA256 password-claim signature). The exact algorithm, constants, and byte
// layout below were verified against the reference client implementation
// (amazon-cognito-identity-js AuthenticationHelper.js/CognitoUser.js -- the source AWS
// itself publishes for the client half of this protocol), so a real aws-sdk-based
// client performing SRP can interoperate with this emulator.

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"math/big"
	"strings"
)

// srpNHex is the 3072-bit SRP group prime AWS Cognito uses (RFC 5054's 3072-bit
// group). g=2 is the matching generator.
const srpNHex = "FFFFFFFFFFFFFFFFC90FDAA22168C234C4C6628B80DC1CD1" +
	"29024E088A67CC74020BBEA63B139B22514A08798E3404DD" +
	"EF9519B3CD3A431B302B0A6DF25F14374FE1356D6D51C245" +
	"E485B576625E7EC6F44C42E9A637ED6B0BFF5CB6F406B7ED" +
	"EE386BFB5A899FA5AE9F24117C4B1FE649286651ECE45B3D" +
	"C2007CB8A163BF0598DA48361C55D39A69163FA8FD24CF5F" +
	"83655D23DCA3AD961C62F356208552BB9ED529077096966D" +
	"670C354E4ABC9804F1746C08CA18217C32905E462E36CE3B" +
	"E39E772C180E86039B2783A2EC07A28FB5C55DF06F4C52C9" +
	"DE2BCBF6955817183995497CEA956AE515D2261898FA0510" +
	"15728E5A8AAAC42DAD33170D04507A33A85521ABDF1CBA64" +
	"ECFB850458DBEF0A8AEA71575D060C7DB3970F85A6E1E4C7" +
	"ABF5AE8CDB0933D71E8C94E04A25619DCEE3D2261AD2EE6B" +
	"F12FFA06D98A0864D87602733EC86A64521F2B18177B200C" +
	"BBE117577A615D6C770988C0BAD946E208E24FA074E5AB31" +
	"43DB5BFCE0FD108E4B82D120A93AD2CAFFFFFFFFFFFFFFFF"

// srpInfoBits is the fixed HKDF info-string Cognito's protocol uses to derive the
// password-claim session key from the SRP shared secret S.
const srpInfoBits = "Caldera Derived Key"

const (
	srpSaltLen        = 16 // bytes
	srpSecretBlockLen = 32 // bytes
	hexBase           = 16
	srpGVal           = 2
	// srpHKDFCounter is HKDF's one-byte expand-round counter (RFC 5869 T(1) = HMAC(PRK,
	// info | 0x01)); Cognito's protocol only ever does a single expand round.
	srpHKDFCounter = 0x01
)

func srpN() *big.Int {
	n, _ := new(big.Int).SetString(srpNHex, hexBase)

	return n
}

func srpG() *big.Int { return big.NewInt(srpGVal) }

// srpK is the SRP-6a multiplier: k = H(PAD(N) | PAD(g)).
func srpK() *big.Int {
	digest := sha256.Sum256(append(srpPadHex(srpN()), srpPadHex(srpG())...))

	return new(big.Int).SetBytes(digest[:])
}

// srpPadHex renders n as hex, left-padded to an even number of digits, with an extra
// leading zero byte if the first digit would otherwise be interpreted as a sign bit by
// implementations that pad to N's fixed byte width. This mirrors AuthenticationHelper.js's
// padHex exactly (needed for a byte-for-byte-identical hash on both ends of the protocol).
func srpPadHex(n *big.Int) []byte {
	h := n.Text(hexBase)
	if len(h)%2 == 1 {
		h = "0" + h
	} else if strings.IndexByte("89abcdefABCDEF", h[0]) >= 0 {
		h = "00" + h
	}

	b, _ := hex.DecodeString(h)

	return b
}

// srpPoolName returns the portion of a user pool ID used in SRP hashing/signing (the
// pool ID minus its "<region>_" prefix), matching the client's
// `this.pool.getUserPoolId().split('_')[1]`.
func srpPoolName(userPoolID string) string {
	if _, suffix, ok := strings.Cut(userPoolID, "_"); ok {
		return suffix
	}

	return userPoolID
}

// srpComputeX derives the SRP private exponent x = H(PAD(salt) | H(poolName | username
// | ":" | password)) from a plaintext password. This is only ever computable at the
// moment a password is known in plaintext (SignUp, AdminSetUserPassword, ...) -- exactly
// like real Cognito, which must derive and store an SRP verifier at password-set time
// since it never stores the plaintext password itself.
func srpComputeX(poolName, username, password string, salt *big.Int) *big.Int {
	// This is SRP-6a's private-exponent derivation (RFC 5054 x = H(salt, H(username,
	// ":", password))), the same step amazon-cognito-identity-js performs client-side
	// with SHA-256 -- not password storage, so a slow KDF here would break wire
	// compatibility with real AWS SDK clients. The result (x) is never stored or
	// compared directly; it only feeds g^x mod N below to produce the verifier v.
	//
	// CodeQL flags this as go/weak-sensitive-data-hashing; alert #254 is judged a
	// false positive for the reasons above. Inline `codeql[...]` comments do NOT
	// suppress Code Scanning alerts (that is legacy LGTM syntax), so do not add one
	// here expecting it to work -- dismiss via the API or UI instead.
	inner := sha256.Sum256([]byte(poolName + username + ":" + password))
	combined := append(srpPadHex(salt), inner[:]...)
	digest := sha256.Sum256(combined)

	return new(big.Int).SetBytes(digest[:])
}

// computeSRPVerifier generates a fresh random salt and computes the matching SRP
// verifier v = g^x mod N for a plaintext password, returning both as padded hex
// strings ready to store on a User and echo back in a PASSWORD_VERIFIER challenge's
// SALT parameter.
func computeSRPVerifier(userPoolID, username, password string) (string, string, error) {
	saltBytes := make([]byte, srpSaltLen)
	if _, readErr := rand.Read(saltBytes); readErr != nil {
		return "", "", fmt.Errorf("generating SRP salt: %w", readErr)
	}

	salt := new(big.Int).SetBytes(saltBytes)
	poolName := srpPoolName(userPoolID)
	x := srpComputeX(poolName, username, password, salt)
	v := new(big.Int).Exp(srpG(), x, srpN())

	return hex.EncodeToString(srpPadHex(salt)), hex.EncodeToString(srpPadHex(v)), nil
}

// srpCalculateU derives the scrambling parameter u = H(PAD(A) | PAD(B)).
func srpCalculateU(a, bPub *big.Int) *big.Int {
	combined := append(srpPadHex(a), srpPadHex(bPub)...)
	digest := sha256.Sum256(combined)

	return new(big.Int).SetBytes(digest[:])
}

// srpServerB computes the server's SRP-6a public ephemeral value B = (k*v + g^b) mod N.
func srpServerB(v, b *big.Int) *big.Int {
	n := srpN()
	gb := new(big.Int).Exp(srpG(), b, n)
	kv := new(big.Int).Mul(srpK(), v)
	sum := new(big.Int).Add(kv, gb)

	return sum.Mod(sum, n)
}

// srpServerS computes the shared secret from the server's side: S = (A * v^u) ^ b mod N.
func srpServerS(a, v, u, b *big.Int) *big.Int {
	n := srpN()
	vu := new(big.Int).Exp(v, u, n)
	avu := new(big.Int).Mul(a, vu)
	avu.Mod(avu, n)

	return new(big.Int).Exp(avu, b, n)
}

// srpHKDF derives the 16-byte password-claim session key from S and u, matching
// AuthenticationHelper.js's computehkdf(ikm=PAD(S), salt=PAD(u)): a standard two-step
// HKDF-SHA256 (extract then one expand round with the fixed "Caldera Derived Key" info
// string), truncated to 16 bytes.
func srpHKDF(s, u *big.Int) []byte {
	ikm := srpPadHex(s)
	saltArg := srpPadHex(u)

	extract := hmac.New(sha256.New, saltArg)
	extract.Write(ikm)
	prk := extract.Sum(nil)

	info := append([]byte(srpInfoBits), srpHKDFCounter)
	expand := hmac.New(sha256.New, prk)
	expand.Write(info)
	full := expand.Sum(nil)

	return full[:16]
}

// srpComputeSignature computes the PASSWORD_CLAIM_SIGNATURE HMAC: HMAC-SHA256(hkdfKey,
// poolName | username | secretBlock | timestamp), matching CognitoUser.js's
// authenticateUserDefaultAuth signature computation exactly (byte order and content).
func srpComputeSignature(hkdfKey []byte, poolName, username string, secretBlock []byte, timestamp string) []byte {
	mac := hmac.New(sha256.New, hkdfKey)
	mac.Write([]byte(poolName))
	mac.Write([]byte(username))
	mac.Write(secretBlock)
	mac.Write([]byte(timestamp))

	return mac.Sum(nil)
}

// hashAndSRP hashes password with bcrypt (for USER_PASSWORD_AUTH/ADMIN_*_AUTH) and
// computes its matching SRP salt/verifier (for USER_SRP_AUTH) in one call, so every
// password-setting call site keeps both credential forms in sync.
func hashAndSRP(userPoolID, username, password string) (string, string, string, error) {
	h, err := bcryptHashPassword(password)
	if err != nil {
		return "", "", "", err
	}

	saltHex, verifierHex, err := computeSRPVerifier(userPoolID, username, password)
	if err != nil {
		return "", "", "", err
	}

	return h, saltHex, verifierHex, nil
}

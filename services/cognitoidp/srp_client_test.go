package cognitoidp_test

// srp_client_test.go implements the client half of Cognito's USER_SRP_AUTH/
// ADMIN_USER_SRP_AUTH handshake, independently of the server implementation in
// services/cognitoidp/srp.go, so integration tests can drive the real protocol exactly
// as a real aws-sdk-based client would: SRP_A on InitiateAuth, then
// PASSWORD_CLAIM_SECRET_BLOCK/SIGNATURE/TIMESTAMP on RespondToAuthChallenge. The
// algorithm mirrors amazon-cognito-identity-js's AuthenticationHelper.js/CognitoUser.js/
// DateHelper.js (the reference client implementation AWS itself publishes).

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"math/big"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

const testSRPNHex = "FFFFFFFFFFFFFFFFC90FDAA22168C234C4C6628B80DC1CD1" +
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

const testSRPInfoBits = "Caldera Derived Key"

const testHexBase = 16

func testSRPN() *big.Int {
	n, _ := new(big.Int).SetString(testSRPNHex, testHexBase)

	return n
}

func testSRPG() *big.Int { return big.NewInt(2) }

func testSRPPadHex(n *big.Int) []byte {
	h := n.Text(testHexBase)
	if len(h)%2 == 1 {
		h = "0" + h
	} else if strings.IndexByte("89abcdefABCDEF", h[0]) >= 0 {
		h = "00" + h
	}

	b, _ := hex.DecodeString(h)

	return b
}

func testSRPK() *big.Int {
	d := sha256.Sum256(append(testSRPPadHex(testSRPN()), testSRPPadHex(testSRPG())...))

	return new(big.Int).SetBytes(d[:])
}

func testSRPPoolName(userPoolID string) string {
	if _, suffix, ok := strings.Cut(userPoolID, "_"); ok {
		return suffix
	}

	return userPoolID
}

func testSRPCalculateU(a, bPub *big.Int) *big.Int {
	d := sha256.Sum256(append(testSRPPadHex(a), testSRPPadHex(bPub)...))

	return new(big.Int).SetBytes(d[:])
}

func testSRPHKDF(s, u *big.Int) []byte {
	extract := hmac.New(sha256.New, testSRPPadHex(u))
	extract.Write(testSRPPadHex(s))
	prk := extract.Sum(nil)

	info := append([]byte(testSRPInfoBits), 0x01)
	expand := hmac.New(sha256.New, prk)
	expand.Write(info)
	full := expand.Sum(nil)

	return full[:16]
}

// testDateNow formats the current UTC time exactly as DateHelper.js's getNowString:
// "ddd MMM D HH:mm:ss UTC YYYY" (day-of-month unpadded, time fields zero-padded).
func testDateNow() string {
	now := time.Now().UTC()
	weekdays := [...]string{"Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"}
	months := [...]string{"Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"}

	return fmt.Sprintf("%s %s %d %02d:%02d:%02d UTC %d",
		weekdays[now.Weekday()], months[now.Month()-1], now.Day(),
		now.Hour(), now.Minute(), now.Second(), now.Year())
}

// srpTestClient is one SRP-6a client session: a persistent small-a/large-A keypair
// used across both InitiateAuth and RespondToAuthChallenge for one login attempt.
type srpTestClient struct {
	a *big.Int
	A *big.Int
}

func newSRPTestClient(t *testing.T) *srpTestClient {
	t.Helper()

	n := testSRPN()

	var a *big.Int

	for {
		buf := make([]byte, 128)
		_, err := rand.Read(buf)
		require.NoError(t, err)

		a = new(big.Int).SetBytes(buf)
		a.Mod(a, n)

		if a.Sign() != 0 {
			break
		}
	}

	return &srpTestClient{a: a, A: new(big.Int).Exp(testSRPG(), a, n)}
}

// srpA returns the client's public ephemeral value as a hex string, for AuthParameters["SRP_A"].
func (c *srpTestClient) srpA() string { return c.A.Text(testHexBase) }

// challengeResponses computes PASSWORD_CLAIM_SECRET_BLOCK/SIGNATURE/TIMESTAMP/USERNAME
// from the server's PASSWORD_VERIFIER ChallengeParameters and the real password,
// exactly as CognitoUser.js's authenticateUserDefaultAuth does.
func (c *srpTestClient) challengeResponses(
	t *testing.T, userPoolID, password string, params map[string]string,
) map[string]string {
	t.Helper()

	salt, ok := new(big.Int).SetString(params["SALT"], testHexBase)
	require.True(t, ok, "SALT must be valid hex")

	bPub, ok := new(big.Int).SetString(params["SRP_B"], testHexBase)
	require.True(t, ok, "SRP_B must be valid hex")

	userIDForSRP := params["USER_ID_FOR_SRP"]
	require.NotEmpty(t, userIDForSRP)

	u := testSRPCalculateU(c.A, bPub)
	require.NotZero(t, u.Sign(), "U must not be zero")

	poolName := testSRPPoolName(userPoolID)
	inner := sha256.Sum256([]byte(poolName + userIDForSRP + ":" + password))
	xDigest := sha256.Sum256(append(testSRPPadHex(salt), inner[:]...))
	x := new(big.Int).SetBytes(xDigest[:])

	n := testSRPN()
	gx := new(big.Int).Exp(testSRPG(), x, n)
	kgx := new(big.Int).Mul(testSRPK(), gx)
	base := new(big.Int).Sub(bPub, kgx)
	base.Mod(base, n)
	exp := new(big.Int).Add(c.a, new(big.Int).Mul(u, x))
	s := new(big.Int).Exp(base, exp, n)

	hkdfKey := testSRPHKDF(s, u)

	secretBlockB64 := params["SECRET_BLOCK"]
	secretBlock, err := base64.StdEncoding.DecodeString(secretBlockB64)
	require.NoError(t, err)

	timestamp := testDateNow()

	mac := hmac.New(sha256.New, hkdfKey)
	mac.Write([]byte(poolName))
	mac.Write([]byte(userIDForSRP))
	mac.Write(secretBlock)
	mac.Write([]byte(timestamp))
	sig := base64.StdEncoding.EncodeToString(mac.Sum(nil))

	return map[string]string{
		"USERNAME":                    userIDForSRP,
		"PASSWORD_CLAIM_SECRET_BLOCK": secretBlockB64,
		"TIMESTAMP":                   timestamp,
		"PASSWORD_CLAIM_SIGNATURE":    sig,
	}
}

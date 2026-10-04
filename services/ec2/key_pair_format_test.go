package ec2_test

import (
	"crypto/ed25519"
	"crypto/hmac"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"math/big"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	ec2sdk "github.com/aws/aws-sdk-go-v2/service/ec2"
	"github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/crypto/ssh"
)

type ppkFile struct {
	headers map[string]string
	public  []byte
	private []byte
}

func parsePPK(t *testing.T, text string) ppkFile {
	t.Helper()

	lines := strings.Split(strings.TrimSpace(text), "\n")
	f := ppkFile{headers: map[string]string{}}

	for i := 0; i < len(lines); i++ {
		name, val, _ := strings.Cut(lines[i], ": ")
		f.headers[name] = val

		if name != "Public-Lines" && name != "Private-Lines" {
			continue
		}

		n, err := strconv.Atoi(val)
		require.NoError(t, err)

		blob, err := base64.StdEncoding.DecodeString(strings.Join(lines[i+1:i+1+n], ""))
		require.NoError(t, err)

		if name == "Public-Lines" {
			f.public = blob
		} else {
			f.private = blob
		}

		i += n
	}

	return f
}

func readSSHString(t *testing.T, b []byte) ([]byte, []byte) {
	t.Helper()
	require.GreaterOrEqual(t, len(b), 4)

	n := int(binary.BigEndian.Uint32(b))
	require.GreaterOrEqual(t, len(b), 4+n)

	return b[4 : 4+n], b[4+n:]
}

func (f ppkFile) macMatches(t *testing.T) bool {
	t.Helper()

	var data []byte

	for _, part := range [][]byte{
		[]byte(strings.TrimPrefix(f.headers["PuTTY-User-Key-File-3"], "")),
		[]byte(f.headers["Encryption"]),
		[]byte(f.headers["Comment"]),
		f.public,
		f.private,
	} {
		data = binary.BigEndian.AppendUint32(data, uint32(len(part)))
		data = append(data, part...)
	}

	h := hmac.New(sha256.New, nil)
	h.Write(data)

	return hex.EncodeToString(h.Sum(nil)) == f.headers["Private-MAC"]
}

func TestRealClient_CreateKeyPairKeyFormat(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		keyType types.KeyType
		format  types.KeyFormat
		algo    string
		wantPPK bool
	}{
		{"rsa_pem_default", types.KeyTypeRsa, "", "", false},
		{"rsa_ppk", types.KeyTypeRsa, types.KeyFormatPpk, "ssh-rsa", true},
		{"ed25519_ppk", types.KeyTypeEd25519, types.KeyFormatPpk, "ssh-ed25519", true},
		{"ed25519_pem", types.KeyTypeEd25519, types.KeyFormatPem, "", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, client := newMiscClient(t)

			out, err := client.CreateKeyPair(t.Context(), &ec2sdk.CreateKeyPairInput{
				KeyName:   aws.String("k-" + tt.name),
				KeyType:   tt.keyType,
				KeyFormat: tt.format,
			})
			require.NoError(t, err)

			material := aws.ToString(out.KeyMaterial)
			if !tt.wantPPK {
				assert.Contains(t, material, "-----BEGIN")

				return
			}

			require.True(t, strings.HasPrefix(material, "PuTTY-User-Key-File-3: "+tt.algo+"\n"))

			f := parsePPK(t, material)
			assert.Equal(t, "none", f.headers["Encryption"])
			assert.Equal(t, "k-"+tt.name, f.headers["Comment"])
			assert.True(t, f.macMatches(t))

			desc, err := client.DescribeKeyPairs(t.Context(), &ec2sdk.DescribeKeyPairsInput{
				KeyNames:         []string{"k-" + tt.name},
				IncludePublicKey: aws.Bool(true),
			})
			require.NoError(t, err)

			authorized, _, _, _, err := ssh.ParseAuthorizedKey([]byte(aws.ToString(desc.KeyPairs[0].PublicKey)))
			require.NoError(t, err)
			assert.Equal(t, authorized.Marshal(), f.public)

			checkPPKPrivate(t, tt.algo, f)
		})
	}
}

func checkPPKPrivate(t *testing.T, algo string, f ppkFile) {
	t.Helper()

	_, rest := readSSHString(t, f.public)

	if algo == "ssh-rsa" {
		eBytes, afterE := readSSHString(t, rest)
		nBytes, _ := readSSHString(t, afterE)
		dBytes, privRest := readSSHString(t, f.private)
		pBytes, privRest := readSSHString(t, privRest)
		qBytes, privRest := readSSHString(t, privRest)
		iqmp, _ := readSSHString(t, privRest)

		key := &rsa.PrivateKey{
			N: new(big.Int).SetBytes(nBytes), E: int(new(big.Int).SetBytes(eBytes).Int64()),
			D:      new(big.Int).SetBytes(dBytes),
			Primes: []*big.Int{new(big.Int).SetBytes(pBytes), new(big.Int).SetBytes(qBytes)},
		}
		require.NoError(t, key.Validate())
		key.Precompute()
		assert.Equal(t, key.Precomputed.Qinv, new(big.Int).SetBytes(iqmp))

		return
	}

	pub, _ := readSSHString(t, rest)
	seedMpint, _ := readSSHString(t, f.private)
	seed := new(big.Int).SetBytes(seedMpint).FillBytes(make([]byte, ed25519.SeedSize))
	slices.Reverse(seed)

	derived := ed25519.NewKeyFromSeed(seed).Public()
	assert.Equal(t, pub, []byte(derived.(ed25519.PublicKey)))
}

func TestRealClient_CreateKeyPairRejectsUnknownKeyFormat(t *testing.T) {
	t.Parallel()

	_, client := newMiscClient(t)

	_, err := client.CreateKeyPair(t.Context(), &ec2sdk.CreateKeyPairInput{
		KeyName:   aws.String("bad"),
		KeyFormat: types.KeyFormat("p12"),
	})
	require.ErrorContains(t, err, "InvalidParameterValue")
}

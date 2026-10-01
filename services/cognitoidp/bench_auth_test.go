package cognitoidp_test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cognitoidp"
)

func benchPools(b *testing.B, n int) (*cognitoidp.InMemoryBackend, string, string, *cognitoidp.TokenResult) {
	b.Helper()

	be := newTestBackend()

	var (
		cid, pid string
		tokens   *cognitoidp.TokenResult
	)

	for i := range n {
		pool, err := be.CreateUserPool(fmt.Sprintf("pool-%d", i))
		require.NoError(b, err)

		client, err := be.CreateUserPoolClient(pool.ID, "c")
		require.NoError(b, err)

		_, err = be.SignUp(client.ClientID, "bob", "Pass1234!", map[string]string{"email": "bob@x.com"})
		require.NoError(b, err)
		require.NoError(b, be.AdminConfirmSignUp(pool.ID, "bob"))

		cid, pid = client.ClientID, pool.ID
		res, err := be.InitiateAuth(cid, "USER_PASSWORD_AUTH", "bob", "Pass1234!")
		require.NoError(b, err)

		tokens = res.Tokens
	}

	return be, cid, pid, tokens
}

func BenchmarkCognitoInitiateAuth(b *testing.B) {
	be, cid, _, _ := benchPools(b, 1)

	b.ReportAllocs()
	b.ResetTimer()

	for range b.N {
		if _, err := be.InitiateAuth(cid, "USER_PASSWORD_AUTH", "bob", "Pass1234!"); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkCognitoGetUser(b *testing.B) {
	for _, n := range []int{1, 20} {
		b.Run(fmt.Sprintf("pools%d", n), func(b *testing.B) {
			be, _, _, tokens := benchPools(b, n)

			b.ReportAllocs()
			b.ResetTimer()

			for range b.N {
				if _, err := be.GetUser(tokens.AccessToken); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkCognitoJWKS(b *testing.B) {
	be, _, pid, _ := benchPools(b, 1)

	b.ReportAllocs()
	b.ResetTimer()

	for range b.N {
		if _, err := be.GetUserPoolJWKS(pid); err != nil {
			b.Fatal(err)
		}
	}
}

package cognitoidp_test

import (
	"fmt"
	"sync/atomic"
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

func BenchmarkCognitoInitiateAuthParallel(b *testing.B) {
	be := newTestBackend()

	pool, err := be.CreateUserPool("bench")
	require.NoError(b, err)

	client, err := be.CreateUserPoolClient(pool.ID, "c")
	require.NoError(b, err)

	const users = 64

	for i := range users {
		name := fmt.Sprintf("user-%d", i)

		_, err = be.SignUp(client.ClientID, name, "Pass1234!", map[string]string{"email": name + "@x.com"})
		require.NoError(b, err)
		require.NoError(b, be.AdminConfirmSignUp(pool.ID, name))
	}

	var next atomic.Int64

	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		name := fmt.Sprintf("user-%d", next.Add(1)%users)

		for pb.Next() {
			if _, authErr := be.InitiateAuth(client.ClientID, "USER_PASSWORD_AUTH", name, "Pass1234!"); authErr != nil {
				b.Error(authErr)

				return
			}
		}
	})
}

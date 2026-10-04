package secretsmanager_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/secretsmanager"
)

func benchSMBackend(b *testing.B, n int) *secretsmanager.InMemoryBackend {
	b.Helper()

	be := secretsmanager.NewInMemoryBackend()
	b.Cleanup(be.StopRotationScheduler)

	for i := range n {
		_, err := be.CreateSecret(context.Background(), &secretsmanager.CreateSecretInput{
			Name:         fmt.Sprintf("app/svc%d/secret%d", i%20, i),
			SecretString: "s3cret-value",
			Tags:         []secretsmanager.Tag{{Key: "env", Value: "prod"}, {Key: "team", Value: "a"}},
		})
		require.NoError(b, err)
	}

	return be
}

func BenchmarkGetSecretValue(b *testing.B) {
	be := benchSMBackend(b, 1000)
	ctx := context.Background()
	in := &secretsmanager.GetSecretValueInput{SecretID: "app/svc0/secret0"}

	b.ReportAllocs()
	b.ResetTimer()

	for range b.N {
		if _, err := be.GetSecretValue(ctx, in); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDescribeSecret(b *testing.B) {
	be := benchSMBackend(b, 1000)
	ctx := context.Background()
	in := &secretsmanager.DescribeSecretInput{SecretID: "app/svc0/secret0"}

	b.ReportAllocs()
	b.ResetTimer()

	for range b.N {
		if _, err := be.DescribeSecret(ctx, in); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkListSecretsFiltered(b *testing.B) {
	be := benchSMBackend(b, 2000)
	ctx := context.Background()
	pageSize := int64(20)
	in := &secretsmanager.ListSecretsInput{
		MaxResults: &pageSize,
		Filters:    []secretsmanager.SecretFilter{{Key: "name", Values: []string{"app/svc3/"}}},
	}

	b.ReportAllocs()
	b.ResetTimer()

	for range b.N {
		if _, err := be.ListSecrets(ctx, in); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkListSecretsUnfiltered(b *testing.B) {
	be := benchSMBackend(b, 2000)
	ctx := context.Background()
	pageSize := int64(20)
	in := &secretsmanager.ListSecretsInput{MaxResults: &pageSize}

	b.ReportAllocs()
	b.ResetTimer()

	for range b.N {
		if _, err := be.ListSecrets(ctx, in); err != nil {
			b.Fatal(err)
		}
	}
}

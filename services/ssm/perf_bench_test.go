package ssm_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ssm"
)

func benchSSMBackend(b *testing.B, n int) *ssm.InMemoryBackend {
	b.Helper()

	be := ssm.NewInMemoryBackend()
	for i := range n {
		typ := "String"
		if i%4 == 0 {
			typ = "SecureString"
		}

		_, err := be.PutParameter(context.Background(), &ssm.PutParameterInput{
			Name:  fmt.Sprintf("/app/svc%d/key%d", i%20, i),
			Type:  typ,
			Value: "value-0123456789",
		})
		require.NoError(b, err)
	}

	return be
}

func BenchmarkGetParameter(b *testing.B) {
	be := benchSSMBackend(b, 2000)
	ctx := context.Background()
	in := &ssm.GetParameterInput{Name: "/app/svc0/key0", WithDecryption: true}

	b.ReportAllocs()
	b.ResetTimer()

	for range b.N {
		if _, err := be.GetParameter(ctx, in); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkGetParameters(b *testing.B) {
	be := benchSSMBackend(b, 2000)
	ctx := context.Background()
	in := &ssm.GetParametersInput{
		Names:          []string{"/app/svc0/key0", "/app/svc1/key1", "/app/svc2/key2", "/app/svc4/key4"},
		WithDecryption: true,
	}

	b.ReportAllocs()
	b.ResetTimer()

	for range b.N {
		if _, err := be.GetParameters(ctx, in); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkGetParametersByPath(b *testing.B) {
	be := benchSSMBackend(b, 5000)
	ctx := context.Background()
	pageSize := int64(10)
	in := &ssm.GetParametersByPathInput{Path: "/app/svc3", Recursive: true, WithDecryption: true, MaxResults: &pageSize}

	b.ReportAllocs()
	b.ResetTimer()

	for range b.N {
		if _, err := be.GetParametersByPath(ctx, in); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkPutParameterHistory(b *testing.B) {
	be := ssm.NewInMemoryBackend()
	ctx := context.Background()
	in := &ssm.PutParameterInput{Name: "/app/hist", Type: "String", Value: "v", Overwrite: true}

	b.ReportAllocs()
	b.ResetTimer()

	for range b.N {
		if _, err := be.PutParameter(ctx, in); err != nil {
			b.Fatal(err)
		}
	}
}

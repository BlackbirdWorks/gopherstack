package lambda_test

import (
	"context"
	"testing"
	"time"

	"github.com/blackbirdworks/gopherstack/services/lambda"
)

// closeBackend registers a t.Cleanup that gracefully shuts down the backend,
// stopping its function-URL servers, runtime API servers and background
// goroutines so the goleak TestMain does not report leaks. It returns the
// backend for convenient inline use.
func closeBackend(t *testing.T, b *lambda.InMemoryBackend) *lambda.InMemoryBackend {
	t.Helper()

	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		b.Close(ctx)
	})

	return b
}

package lambda_test

import (
	"context"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/lambda"
)

// slowCWLBackend is a CWLogsBackend that sleeps on PutLogLines to simulate slow I/O.
type slowCWLBackend struct {
	delay time.Duration
}

func (s *slowCWLBackend) EnsureLogGroupAndStream(_, _ string) error { return nil }

func (s *slowCWLBackend) PutLogLines(_, _ string, _ []string) error {
	time.Sleep(s.delay)

	return nil
}

// TestLambda_PushInvocationLog_NonBlocking verifies that calling PushInvocationLog with a
// slow CWLogsBackend returns quickly when run asynchronously (via the goroutine that wraps
// the call at invocation time). The exported function itself is synchronous, so we verify
// that the exported helper completes in finite time and doesn't deadlock or panic.
func TestLambda_PushInvocationLog_NonBlocking(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		cwlDelay     time.Duration
		wantMaxDelay time.Duration
	}{
		{
			name:         "slow_cwl_returns_eventually",
			cwlDelay:     50 * time.Millisecond,
			wantMaxDelay: 500 * time.Millisecond,
		},
		{
			name:         "no_cwl_backend_returns_immediately",
			cwlDelay:     0,
			wantMaxDelay: 200 * time.Millisecond,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			synctest.Test(t, func(t *testing.T) {
				bk := lambda.NewInMemoryBackend(nil, nil, lambda.DefaultSettings(), "000000000000", "us-east-1")
				closeBackend(t, bk)

				if tt.cwlDelay > 0 {
					bk.SetCWLogsBackend(&slowCWLBackend{delay: tt.cwlDelay})
				}

				require.NoError(t, bk.CreateFunction(&lambda.FunctionConfiguration{FunctionName: "log-test-fn"}))

				start := time.Now()

				// Run pushInvocationLog in a goroutine, just as the production code does.
				done := make(chan struct{})
				go func() {
					lambda.PushInvocationLog(context.Background(), bk, "log-test-fn", []byte(`{}`), []byte(`"ok"`))
					close(done)
				}()

				select {
				case <-done:
					elapsed := time.Since(start)
					assert.Less(t, elapsed, tt.wantMaxDelay, "pushInvocationLog took too long: %v", elapsed)
				case <-time.After(tt.wantMaxDelay):
					t.Fatalf("pushInvocationLog did not complete within %v", tt.wantMaxDelay)
				}
			})
		})
	}
}

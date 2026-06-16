package acm_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/acm"
)

// TestDeleteCertificate_StopsAutoValidateTimer verifies that deleting a
// certificate with a pending auto-validate timer stops and removes the timer,
// preventing goroutine leaks when the janitor is disabled.
func TestDeleteCertificate_StopsAutoValidateTimer(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		count int
	}{
		{name: "single cert", count: 1},
		{name: "multiple certs", count: 5},
		{name: "many certs", count: 10},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := acm.NewInMemoryBackend("123456789012", "us-east-1")
			require.Equal(t, 0, b.TimerCountForTest(), "baseline: no timers at start")

			arns := make([]string, 0, tc.count)
			for i := range tc.count {
				cert, err := b.RequestCertificate(
					"example.com",
					"AMAZON_ISSUED",
					"DNS",
					"", // no idempotency token
					"",
					"",
					"",
					[]string{"www.example.com"},
				)
				require.NoError(t, err, "RequestCertificate[%d] must succeed", i)
				arns = append(arns, cert.ARN)
			}

			// Each PENDING_VALIDATION cert registers one auto-validate timer.
			require.Equal(t, tc.count, b.TimerCountForTest(),
				"timer should be registered for each pending cert")

			for _, certARN := range arns {
				require.NoError(t, b.DeleteCertificate(certARN),
					"DeleteCertificate must succeed")
			}

			require.Equal(t, 0, b.TimerCountForTest(),
				"all timers must be stopped and removed after delete")
		})
	}
}

// TestDeleteCertificate_TimersDoNotAccumulateAcrossCreateDelete verifies that
// repeated create-then-delete cycles do not cause the timer map to grow, even
// without the background janitor running.
func TestDeleteCertificate_TimersDoNotAccumulateAcrossCreateDelete(t *testing.T) {
	t.Parallel()

	b := acm.NewInMemoryBackend("123456789012", "us-east-1")

	const cycles = 20

	for i := range cycles {
		cert, err := b.RequestCertificate(
			"cycle.example.com",
			"AMAZON_ISSUED",
			"DNS",
			"",
			"",
			"",
			"",
			nil,
		)
		require.NoError(t, err, "cycle %d: RequestCertificate failed", i)
		require.NoError(t, b.DeleteCertificate(cert.ARN), "cycle %d: DeleteCertificate failed", i)
	}

	require.Equal(t, 0, b.TimerCountForTest(),
		"timer map must be empty after all create-delete cycles complete")
}

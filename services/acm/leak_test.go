package acm_test

import (
	"testing"
	"time"

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
			b.SetAutoValidateDelayForTest(10 * time.Second)
			require.Equal(t, 0, b.TimerCountForTest(), "baseline: no timers at start")

			arns := make([]string, 0, tc.count)
			for i := range tc.count {
				cert, err := b.RequestCertificate(
					t.Context(),
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
				require.NoError(t, b.DeleteCertificate(t.Context(), certARN),
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
	b.SetAutoValidateDelayForTest(10 * time.Second)

	const cycles = 20

	for i := range cycles {
		cert, err := b.RequestCertificate(
			t.Context(),
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
		require.NoError(t, b.DeleteCertificate(t.Context(), cert.ARN), "cycle %d: DeleteCertificate failed", i)
	}

	require.Equal(t, 0, b.TimerCountForTest(),
		"timer map must be empty after all create-delete cycles complete")
}

// TestDeleteCertificate_StopsRenewalTimer verifies that deleting a certificate
// that has a pending renewal auto-validate timer stops and removes that timer,
// preventing goroutine leaks when the janitor is disabled.
func TestDeleteCertificate_StopsRenewalTimer(t *testing.T) {
	t.Parallel()

	b := acm.NewInMemoryBackend("123456789012", "us-east-1")

	// Request a DNS-validated cert; it starts PENDING_VALIDATION with an auto-validate timer.
	cert, err := b.RequestCertificate(
		t.Context(),
		"renew-leak.example.com",
		"AMAZON_ISSUED",
		"DNS",
		"",
		"",
		"",
		"",
		nil,
	)
	require.NoError(t, err)

	// Wait for auto-validate to fire and transition the cert to ISSUED (clears the timer).
	require.Eventually(t, func() bool {
		c, descErr := b.DescribeCertificate(t.Context(), cert.ARN)

		return descErr == nil && c.Status == "ISSUED"
	}, time.Second, 10*time.Millisecond, "cert must reach ISSUED before renewal")

	require.Equal(t, 0, b.TimerCountForTest(), "no timers expected after initial auto-validate")

	// Renew the cert — this schedules a new autoValidateRenewal timer.
	require.NoError(t, b.RenewCertificate(t.Context(), cert.ARN))
	require.Equal(t, 1, b.TimerCountForTest(), "renewal must register one timer")

	// Delete the cert — the renewal timer must be stopped and removed.
	require.NoError(t, b.DeleteCertificate(t.Context(), cert.ARN))
	require.Equal(t, 0, b.TimerCountForTest(),
		"renewal timer must be stopped and removed after DeleteCertificate")
}

// TestDeleteCertificate_StopsResendValidationEmailTimer verifies that deleting a
// certificate after ResendValidationEmail stops and removes the rescheduled timer,
// preventing goroutine leaks when the janitor is disabled.
func TestDeleteCertificate_StopsResendValidationEmailTimer(t *testing.T) {
	t.Parallel()

	b := acm.NewInMemoryBackend("123456789012", "us-east-1")
	b.SetAutoValidateDelayForTest(10 * time.Second)

	// Request with EMAIL validation so the cert starts PENDING_VALIDATION.
	cert, err := b.RequestCertificate(
		t.Context(),
		"resend-leak.example.com",
		"AMAZON_ISSUED",
		"EMAIL",
		"",
		"",
		"",
		"",
		nil,
	)
	require.NoError(t, err)
	require.Equal(t, 1, b.TimerCountForTest(), "one timer after RequestCertificate")

	// Resend resets the timer; count should remain 1 (old stopped, new registered).
	require.NoError(t, b.ResendValidationEmail(t.Context(), cert.ARN, cert.DomainName, cert.DomainName))
	require.Equal(t, 1, b.TimerCountForTest(), "timer count must stay 1 after ResendValidationEmail")

	// Delete the cert — the rescheduled timer must be stopped and removed.
	require.NoError(t, b.DeleteCertificate(t.Context(), cert.ARN))
	require.Equal(t, 0, b.TimerCountForTest(),
		"rescheduled timer must be stopped and removed after DeleteCertificate")
}

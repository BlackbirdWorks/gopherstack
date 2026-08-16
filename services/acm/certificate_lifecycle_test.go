package acm_test

import (
	"context"
	"testing"
	"time"

	sdktypes "github.com/aws/aws-sdk-go-v2/service/acm/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/acm"
)

// TestACMBackend_AutoValidation verifies the DNS validation auto-transition.
func TestACMBackend_AutoValidation(t *testing.T) {
	t.Parallel()

	b := acm.NewInMemoryBackend("000000000000", "us-east-1")
	cert, err := b.RequestCertificate(context.Background(), "auto.example.com", "", "DNS", "", "", "", "", nil)
	require.NoError(t, err)
	assert.Equal(t, "PENDING_VALIDATION", cert.Status)

	// Wait for auto-validation (should happen within 500ms)
	require.Eventually(t, func() bool {
		c, descErr := b.DescribeCertificate(context.Background(), cert.ARN)
		if descErr != nil {
			return false
		}

		if c.Status != "ISSUED" {
			return false
		}

		for _, dvo := range c.DomainValidationOptions {
			if dvo.ValidationStatus != "SUCCESS" {
				return false
			}
		}

		return true
	}, 2*time.Second, 50*time.Millisecond)
}

// TestACMBackend_StatusLifecycle verifies the full certificate status lifecycle transitions.
func TestACMBackend_StatusLifecycle(t *testing.T) {
	t.Parallel()

	certPEM, keyPEM := generateTestCert(t)

	tests := []struct {
		setup      func(t *testing.T, b *acm.InMemoryBackend) string
		transition func(t *testing.T, b *acm.InMemoryBackend, certARN string) error
		wantErr    error
		name       string
		wantStatus string
	}{
		{
			name: "issued_to_expired",
			setup: func(t *testing.T, b *acm.InMemoryBackend) string {
				t.Helper()
				cert, err := b.RequestCertificate(
					context.Background(),
					"expire-me.example.com",
					"",
					"",
					"",
					"",
					"",
					"",
					nil,
				)
				require.NoError(t, err)

				return cert.ARN
			},
			transition: func(_ *testing.T, b *acm.InMemoryBackend, certARN string) error {
				return b.ExpireCertificate(context.Background(), certARN)
			},
			wantStatus: "EXPIRED",
		},
		{
			name: "issued_to_inactive",
			setup: func(t *testing.T, b *acm.InMemoryBackend) string {
				t.Helper()
				cert, err := b.ImportCertificate(context.Background(), certPEM, keyPEM, "", "")
				require.NoError(t, err)

				return cert.ARN
			},
			transition: func(_ *testing.T, b *acm.InMemoryBackend, certARN string) error {
				return b.InactivateCertificate(context.Background(), certARN)
			},
			wantStatus: "INACTIVE",
		},
		{
			name: "pending_to_validation_timed_out",
			setup: func(t *testing.T, b *acm.InMemoryBackend) string {
				t.Helper()
				cert, err := b.RequestCertificate(
					context.Background(),
					"timeout.example.com",
					"",
					"DNS",
					"",
					"",
					"",
					"",
					nil,
				)
				require.NoError(t, err)
				require.Equal(t, "PENDING_VALIDATION", cert.Status)

				return cert.ARN
			},
			transition: func(_ *testing.T, b *acm.InMemoryBackend, certARN string) error {
				return b.TimeoutPendingValidation(context.Background(), certARN)
			},
			wantStatus: "VALIDATION_TIMED_OUT",
		},
		{
			name: "pending_to_failed",
			setup: func(t *testing.T, b *acm.InMemoryBackend) string {
				t.Helper()
				cert, err := b.RequestCertificate(
					context.Background(),
					"fail-me.example.com",
					"",
					"EMAIL",
					"",
					"",
					"",
					"",
					nil,
				)
				require.NoError(t, err)
				require.Equal(t, "PENDING_VALIDATION", cert.Status)

				return cert.ARN
			},
			transition: func(_ *testing.T, b *acm.InMemoryBackend, certARN string) error {
				return b.FailCertificate(context.Background(), certARN, "NO_AVAILABLE_CONTACTS")
			},
			wantStatus: "FAILED",
		},
		{
			name: "expire_not_found",
			setup: func(_ *testing.T, _ *acm.InMemoryBackend) string {
				return "arn:aws:acm:us-east-1:000000000000:certificate/nonexistent"
			},
			transition: func(_ *testing.T, b *acm.InMemoryBackend, certARN string) error {
				return b.ExpireCertificate(context.Background(), certARN)
			},
			wantErr: acm.ErrCertNotFound,
		},
		{
			name: "expire_wrong_status",
			setup: func(t *testing.T, b *acm.InMemoryBackend) string {
				t.Helper()
				cert, err := b.RequestCertificate(
					context.Background(),
					"already-revoked.example.com",
					"",
					"",
					"",
					"",
					"",
					"",
					nil,
				)
				require.NoError(t, err)
				require.NoError(t, b.RevokeCertificate(context.Background(), cert.ARN, "UNSPECIFIED"))

				return cert.ARN
			},
			transition: func(_ *testing.T, b *acm.InMemoryBackend, certARN string) error {
				return b.ExpireCertificate(context.Background(), certARN)
			},
			wantErr: acm.ErrInvalidParameter,
		},
		{
			name: "timeout_already_issued",
			setup: func(t *testing.T, b *acm.InMemoryBackend) string {
				t.Helper()
				cert, err := b.RequestCertificate(
					context.Background(),
					"already-issued.example.com",
					"",
					"",
					"",
					"",
					"",
					"",
					nil,
				)
				require.NoError(t, err)
				require.Equal(t, "ISSUED", cert.Status)

				return cert.ARN
			},
			transition: func(_ *testing.T, b *acm.InMemoryBackend, certARN string) error {
				return b.TimeoutPendingValidation(context.Background(), certARN)
			},
			wantErr: acm.ErrInvalidParameter,
		},
		{
			name: "fail_already_issued",
			setup: func(t *testing.T, b *acm.InMemoryBackend) string {
				t.Helper()
				cert, err := b.RequestCertificate(
					context.Background(),
					"already-issued2.example.com",
					"",
					"",
					"",
					"",
					"",
					"",
					nil,
				)
				require.NoError(t, err)

				return cert.ARN
			},
			transition: func(_ *testing.T, b *acm.InMemoryBackend, certARN string) error {
				return b.FailCertificate(context.Background(), certARN, "DOMAIN_NOT_ALLOWED")
			},
			wantErr: acm.ErrInvalidParameter,
		},
		{
			name: "inactivate_not_found",
			setup: func(_ *testing.T, _ *acm.InMemoryBackend) string {
				return "arn:aws:acm:us-east-1:000000000000:certificate/ghost"
			},
			transition: func(_ *testing.T, b *acm.InMemoryBackend, certARN string) error {
				return b.InactivateCertificate(context.Background(), certARN)
			},
			wantErr: acm.ErrCertNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := acm.NewInMemoryBackend("000000000000", "us-east-1")
			certARN := tt.setup(t, b)
			err := tt.transition(t, b, certARN)

			if tt.wantErr != nil {
				require.Error(t, err)
				assert.ErrorIs(t, err, tt.wantErr)

				return
			}

			require.NoError(t, err)

			cert, descErr := b.DescribeCertificate(context.Background(), certARN)
			require.NoError(t, descErr)
			assert.Equal(t, tt.wantStatus, cert.Status)
		})
	}
}

// TestACMBackend_FailCertificate_FailureReason verifies FailureReason is persisted.
func TestACMBackend_FailCertificate_FailureReason(t *testing.T) {
	t.Parallel()

	tests := []struct {
		reason string
		name   string
	}{
		{name: "no_available_contacts", reason: "NO_AVAILABLE_CONTACTS"},
		{name: "domain_not_allowed", reason: "DOMAIN_NOT_ALLOWED"},
		{name: "invalid_public_domain", reason: "INVALID_PUBLIC_DOMAIN"},
		{name: "caa_error", reason: "CAA_ERROR"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := acm.NewInMemoryBackend("000000000000", "us-east-1")
			cert, err := b.RequestCertificate(
				context.Background(),
				"fail.example.com",
				"",
				"EMAIL",
				"",
				"",
				"",
				"",
				nil,
			)
			require.NoError(t, err)

			require.NoError(t, b.FailCertificate(context.Background(), cert.ARN, tt.reason))

			described, descErr := b.DescribeCertificate(context.Background(), cert.ARN)
			require.NoError(t, descErr)
			assert.Equal(t, "FAILED", described.Status)
			assert.Equal(t, tt.reason, described.FailureReason)
		})
	}
}

// Anti-drift: every value the pinned SDK's types.RevocationReason enum knows
// about must be accepted, including the deprecated-but-still-live SUPERCEDED
// alias AWS keeps alongside SUPERSEDED (types/enums.go). Catches a
// hand-maintained allowlist falling behind again.
func TestACMBackend_RevokeCertificate_EverySDKRevocationReasonAccepted(t *testing.T) {
	t.Parallel()

	certPEM, keyPEM := generateTestCert(t)

	for _, reason := range sdktypes.RevocationReason("").Values() {
		t.Run(string(reason), func(t *testing.T) {
			t.Parallel()

			b := acm.NewInMemoryBackend("000000000000", "us-east-1")
			cert, err := b.ImportCertificate(context.Background(), certPEM, keyPEM, "", "")
			require.NoError(t, err)

			err = b.RevokeCertificate(context.Background(), cert.ARN, string(reason))
			require.NoError(t, err, "expected SDK RevocationReason %s to be accepted", reason)
		})
	}
}

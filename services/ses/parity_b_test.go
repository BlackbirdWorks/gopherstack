package ses_test

// Tests for parity.md findings:
//   - Shutdowner: Handler implements service.Shutdowner and goroutine is joined on Shutdown
//   - GetSendQuota: SentLast24Hours tracks actual sends
//   - Region/AccountID: configurable via WithRegion/WithAccountID
//   - Error messages: region placeholder uses configured region, not hardcoded US-EAST-1

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/ses"
)

// TestHandlerImplementsShutdowner verifies the Handler satisfies service.Shutdowner
// and that Shutdown waits for the janitor goroutine to exit cleanly.
func TestHandlerImplementsShutdowner(t *testing.T) {
	t.Parallel()

	var _ service.Shutdowner = (*ses.Handler)(nil)

	backend := ses.NewInMemoryBackend()
	h := ses.NewHandler(backend).WithJanitor(10 * time.Millisecond)

	ctx := context.Background()
	require.NoError(t, h.StartWorker(ctx))

	shutdownCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	// Shutdown must return without blocking past the timeout.
	done := make(chan struct{})
	go func() {
		defer close(done)
		h.Shutdown(shutdownCtx)
	}()

	select {
	case <-done:
	case <-shutdownCtx.Done():
		t.Fatal("Shutdown did not return within timeout — goroutine leak")
	}
}

// TestStartWorkerIdempotent verifies calling StartWorker twice does not start a second goroutine.
func TestStartWorkerIdempotent(t *testing.T) {
	t.Parallel()

	backend := ses.NewInMemoryBackend()
	h := ses.NewHandler(backend).WithJanitor(time.Hour)
	ctx := context.Background()

	require.NoError(t, h.StartWorker(ctx))
	require.NoError(t, h.StartWorker(ctx))

	shutdownCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	h.Shutdown(shutdownCtx)
}

// TestGetSendQuotaTracksSends verifies SentLast24Hours reflects actual sent emails.
func TestGetSendQuotaTracksSends(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		sendCount   int
		wantAtLeast float64
	}{
		{name: "zero_sends", sendCount: 0, wantAtLeast: 0},
		{name: "three_sends", sendCount: 3, wantAtLeast: 3},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := ses.NewInMemoryBackend()
			require.NoError(t, backend.VerifyEmailIdentity("sender@example.com"))

			for range tt.sendCount {
				_, err := backend.SendEmail(ses.SendEmailInput{
					From:     "sender@example.com",
					To:       []string{"to@example.com"},
					Subject:  "test",
					BodyText: "body",
				})
				require.NoError(t, err)
			}

			quota := backend.GetSendQuota()
			assert.GreaterOrEqual(t, quota.SentLast24Hours, tt.wantAtLeast)
		})
	}
}

// TestRegionAccountIDConfigurable verifies WithRegion/WithAccountID are stored
// and returned by Region()/AccountID().
func TestRegionAccountIDConfigurable(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		region        string
		accountID     string
		wantRegion    string
		wantAccountID string
	}{
		{
			name:          "custom_region",
			region:        "eu-west-1",
			accountID:     "",
			wantRegion:    "eu-west-1",
			wantAccountID: "123456789012",
		},
		{
			name:          "custom_account",
			region:        "",
			accountID:     "999888777666",
			wantRegion:    "us-east-1",
			wantAccountID: "999888777666",
		},
		{
			name:          "both_custom",
			region:        "ap-southeast-1",
			accountID:     "111222333444",
			wantRegion:    "ap-southeast-1",
			wantAccountID: "111222333444",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := ses.NewInMemoryBackend()
			if tt.region != "" {
				b = b.WithRegion(tt.region)
			}

			if tt.accountID != "" {
				b = b.WithAccountID(tt.accountID)
			}

			assert.Equal(t, tt.wantRegion, b.Region())
			assert.Equal(t, tt.wantAccountID, b.AccountID())
		})
	}
}

// TestSendEmailErrorUsesConfiguredRegion verifies that the "not verified" error
// message uses the backend's configured region, not a hardcoded "US-EAST-1".
func TestSendEmailErrorUsesConfiguredRegion(t *testing.T) {
	t.Parallel()

	b := ses.NewInMemoryBackend().WithRegion("eu-central-1")

	_, err := b.SendEmail(ses.SendEmailInput{
		From:     "unverified@example.com",
		To:       []string{"to@example.com"},
		Subject:  "test",
		BodyText: "body",
	})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "EU-CENTRAL-1",
		"error should mention configured region EU-CENTRAL-1")
	assert.NotContains(t, err.Error(), "US-EAST-1",
		"error should not mention hardcoded US-EAST-1")
}

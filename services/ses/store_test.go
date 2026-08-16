package ses_test

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ses"
)

// TestSESNewOps_BackendReset verifies that Reset() clears all new maps.
func TestBackendReset(t *testing.T) {
	t.Parallel()

	b := ses.NewInMemoryBackend()

	require.NoError(t, b.CreateReceiptRuleSet("rs1"))
	require.NoError(t, b.CreateReceiptFilter(ses.ReceiptFilter{Name: "f1", Policy: "Allow", CIDR: "0.0.0.0/0"}))
	require.NoError(t, b.CreateConfigurationSet("cs1"))
	require.NoError(t, b.CreateConfigurationSetEventDestination(
		"cs1", ses.EventDestination{Name: "dest1", MatchingEventTypes: []string{"send"}},
	))
	require.NoError(t, b.CreateConfigurationSetTrackingOptions("cs1", "track.example.com"))
	b.AddCustomVerifTemplateInternal(ses.CustomVerificationEmailTemplate{TemplateName: "tmpl1"})

	assert.Equal(t, 1, b.ReceiptRuleSetCount())
	assert.Equal(t, 1, b.ReceiptFilterCount())
	assert.Equal(t, 1, b.EventDestinationCount())
	assert.Equal(t, 1, b.TrackingOptionsCount())
	assert.Equal(t, 1, b.CustomVerifTemplateCount())

	b.Reset()

	assert.Equal(t, 0, b.ReceiptRuleSetCount())
	assert.Equal(t, 0, b.ReceiptFilterCount())
	assert.Equal(t, 0, b.EventDestinationCount())
	assert.Equal(t, 0, b.TrackingOptionsCount())
	assert.Equal(t, 0, b.CustomVerifTemplateCount())
}

// TestBackend_Region_AccountID tests Region and AccountID methods.
func TestBackend_Region_AccountID(t *testing.T) {
	t.Parallel()

	b := ses.NewInMemoryBackend()
	assert.Equal(t, "us-east-1", b.Region())
	assert.Equal(t, "123456789012", b.AccountID())
}

func TestSESBackend_Reset(t *testing.T) {
	t.Parallel()

	b := ses.NewInMemoryBackend()
	require.NoError(t, b.VerifyEmailIdentity("r@test.com"))

	_, err := b.SendEmail(ses.SendEmailInput{
		From: "r@test.com", To: []string{"t@test.com"}, Subject: "s", BodyText: "b",
	})
	require.NoError(t, err)

	require.NoError(t, b.CreateTemplate(ses.EmailTemplate{TemplateName: "t1"}))
	require.NoError(t, b.CreateConfigurationSet("cs1"))

	// Override TTL before Reset.
	b.SetEmailTTL(time.Millisecond)

	b.Reset()

	assert.Equal(t, 0, b.IdentityCount())
	assert.Equal(t, 0, b.EmailCount())
	assert.Equal(t, 0, b.EmailsByIDCount())
	assert.Equal(t, 0, b.TemplateCount())
	assert.Equal(t, 0, b.ConfigSetCount())
	// TTL must be restored to the default.
	assert.Equal(t, ses.DefaultEmailTTL, b.GetEmailTTL())
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

func TestSESBackend_SnapshotIsolation(t *testing.T) {
	t.Parallel()

	b := ses.NewInMemoryBackend()
	require.NoError(t, b.VerifyEmailIdentity("snap@test.com"))

	_, err := b.SendEmail(ses.SendEmailInput{
		From: "snap@test.com", To: []string{"to@test.com"}, Subject: "Test", BodyText: "body",
	})
	require.NoError(t, err)

	snap := b.Snapshot(t.Context())
	require.NotNil(t, snap)

	// Mutate original after snapshot.
	require.NoError(t, b.VerifyEmailIdentity("after@test.com"))

	_, err = b.SendEmail(ses.SendEmailInput{
		From: "snap@test.com", To: []string{"to@test.com"}, Subject: "Test2", BodyText: "body2",
	})
	require.NoError(t, err)

	// Restore into a fresh backend.
	fresh := ses.NewInMemoryBackend()
	require.NoError(t, fresh.Restore(t.Context(), snap))

	// Fresh backend should have the original state, not the mutated state.
	assert.Equal(t, 1, fresh.IdentityCount())
	assert.Equal(t, 1, fresh.EmailCount())
}

func TestSESBackend_ConcurrentAccess(t *testing.T) {
	t.Parallel()

	b := ses.NewInMemoryBackend()

	var wg sync.WaitGroup

	// Concurrent verify.
	for i := range 50 {
		wg.Go(func() {
			_ = b.VerifyEmailIdentity(fmt.Sprintf("user%d@test.com", i))
		})
	}

	wg.Wait()

	assert.Equal(t, 50, b.IdentityCount())

	// Concurrent send + list.
	for i := range 50 {
		wg.Go(func() {
			_, _ = b.SendEmail(ses.SendEmailInput{
				From:     fmt.Sprintf("user%d@test.com", i),
				To:       []string{"to@test.com"},
				Subject:  fmt.Sprintf("Subject %d", i),
				BodyText: "body",
			})
		})

		wg.Go(func() {
			_ = b.ListEmails()
		})
	}

	wg.Wait()

	assert.Equal(t, 50, b.EmailCount())
}

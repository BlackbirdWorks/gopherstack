package securityhub_test

import (
	"testing"

	"github.com/blackbirdworks/gopherstack/services/securityhub"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBackend_AccountIDAndRegion(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		accountID string
		region    string
	}{
		{name: "standard", accountID: "123456789012", region: "us-east-1"},
		{name: "other region", accountID: "000000000000", region: "eu-west-1"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			b := securityhub.NewInMemoryBackend(tc.accountID, tc.region)
			assert.Equal(t, tc.accountID, b.AccountID())
			assert.Equal(t, tc.region, b.Region())
		})
	}
}

func TestBackend_Reset(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "reset clears hub and findings"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			b := securityhub.NewInMemoryBackend("000000000000", "us-east-1")
			require.NoError(t, b.EnableHub(false, nil))
			_, _, _ = b.ImportFindings([]map[string]any{
				securityhub.ValidFinding(map[string]any{"Id": "f1", "ProductArn": "arn:aws:securityhub:::product/x/y"}),
			})
			assert.True(t, securityhub.IsHubEnabled(b))
			assert.Equal(t, 1, securityhub.FindingCount(b))

			b.Reset()

			assert.False(t, securityhub.IsHubEnabled(b))
			assert.Equal(t, 0, securityhub.FindingCount(b))
		})
	}
}

func TestBackend_SnapshotRestore(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "snapshot and restore preserves state"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			b := securityhub.NewInMemoryBackend("000000000000", "us-east-1")
			require.NoError(t, b.EnableHub(false, nil))
			_, _, _ = b.ImportFindings([]map[string]any{
				securityhub.ValidFinding(
					map[string]any{"Id": "snap-1", "ProductArn": "arn:aws:securityhub:::product/x/y"},
				),
			})
			snap := b.Snapshot(t.Context())
			assert.NotEmpty(t, snap)

			b2 := securityhub.NewInMemoryBackend("000000000000", "us-east-1")
			require.NoError(t, b2.Restore(t.Context(), snap))
			assert.True(t, securityhub.IsHubEnabled(b2))
			assert.Equal(t, 1, securityhub.FindingCount(b2))
		})
	}
}

func TestBackend_Restore_InvalidJSON(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		data []byte
	}{
		{name: "bad json", data: []byte(`{bad`)},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			b := securityhub.NewInMemoryBackend("000000000000", "us-east-1")
			err := b.Restore(t.Context(), tc.data)
			assert.Error(t, err)
		})
	}
}

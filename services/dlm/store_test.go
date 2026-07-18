package dlm_test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/dlm"
)

// ---------------------------------------------------------------------------
// Backend: AccountID, Region, Reset, Snapshot, Restore
// ---------------------------------------------------------------------------

func TestBackend_AccountIDAndRegion(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		accountID  string
		region     string
		wantAcct   string
		wantRegion string
	}{
		{
			name:       "returns configured accountID and region",
			accountID:  "123456789012",
			region:     "us-west-2",
			wantAcct:   "123456789012",
			wantRegion: "us-west-2",
		},
		{
			name:       "empty values passthrough",
			accountID:  "",
			region:     "",
			wantAcct:   "",
			wantRegion: "",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := dlm.NewInMemoryBackend(tc.accountID, tc.region)
			assert.Equal(t, tc.wantAcct, b.AccountID())
			assert.Equal(t, tc.wantRegion, b.Region())
		})
	}
}

func TestBackend_Reset(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		createCount int
	}{
		{name: "reset clears all policies", createCount: 3},
		{name: "reset on empty backend is safe", createCount: 0},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := dlm.NewInMemoryBackend("000000000000", "us-east-1")
			for i := range tc.createCount {
				_, err := b.CreateLifecyclePolicy(
					fmt.Sprintf("desc-%d", i),
					"arn:aws:iam::000000000000:role/r",
					"ENABLED",
					nil,
					nil,
				)
				require.NoError(t, err)
			}

			b.Reset()

			policies, err := b.GetLifecyclePolicies(dlm.PolicyFilter{})
			require.NoError(t, err)
			assert.Empty(t, policies)
		})
	}
}

func TestBackend_SnapshotAndRestore(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		badJSON     bool
		wantRestErr bool
	}{
		{
			name:        "snapshot roundtrip preserves policies",
			badJSON:     false,
			wantRestErr: false,
		},
		{
			name:        "restore with invalid JSON returns error",
			badJSON:     true,
			wantRestErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := dlm.NewInMemoryBackend("000000000000", "us-east-1")
			p, err := b.CreateLifecyclePolicy(
				"snap-policy",
				"arn:aws:iam::000000000000:role/r",
				"ENABLED",
				map[string]string{"k": "v"},
				nil,
			)
			require.NoError(t, err)

			snap := b.Snapshot(t.Context())
			require.NotEmpty(t, snap)

			if tc.badJSON {
				err = b.Restore(t.Context(), []byte("not json"))
				require.Error(t, err)

				return
			}

			// Restore into fresh backend.
			b2 := dlm.NewInMemoryBackend("000000000000", "us-east-1")
			require.NoError(t, b2.Restore(t.Context(), snap))

			got, err := b2.GetLifecyclePolicy(p.PolicyID)
			require.NoError(t, err)
			assert.Equal(t, p.PolicyID, got.PolicyID)
			assert.Equal(t, "snap-policy", got.Description)
		})
	}
}

func TestBackend_SnapshotNullFields(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		payload []byte
	}{
		{
			name:    "restore null policies field initialises empty map",
			payload: []byte(`{"policies":null,"tags":null}`),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := dlm.NewInMemoryBackend("000000000000", "us-east-1")
			require.NoError(t, b.Restore(t.Context(), tc.payload))

			policies, err := b.GetLifecyclePolicies(dlm.PolicyFilter{})
			require.NoError(t, err)
			assert.Empty(t, policies)
		})
	}
}

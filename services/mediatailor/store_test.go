package mediatailor_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/mediatailor"
)

func TestAccountIDAndRegion(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		accountID string
		region    string
	}{
		{name: "returns configured account and region", accountID: "123456789012", region: "us-west-2"},
		{name: "returns defaults", accountID: "000000000000", region: "us-east-1"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := mediatailor.NewInMemoryBackend(tt.accountID, tt.region)
			assert.Equal(t, tt.accountID, b.AccountID())
			assert.Equal(t, tt.region, b.Region())
		})
	}
}

func TestReset(t *testing.T) {
	t.Parallel()

	b := mediatailor.NewInMemoryBackend("000000000000", "us-east-1")
	_, err := b.PutPlaybackConfiguration("test", "https://ads.com", "https://video.com", nil, nil)
	require.NoError(t, err)
	b.Reset()
	assert.Equal(t, 0, mediatailor.PlaybackConfigurationCount(b))
}

func TestSnapshotAndRestore(t *testing.T) {
	t.Parallel()

	b := mediatailor.NewInMemoryBackend("111111111111", "eu-west-1")
	_, err := b.PutPlaybackConfiguration("snap-cfg", "https://ads.com", "https://video.com", nil, nil)
	require.NoError(t, err)

	snap := b.Snapshot(t.Context())
	require.NotEmpty(t, snap)

	b2 := mediatailor.NewInMemoryBackend("000000000000", "us-east-1")
	err = b2.Restore(t.Context(), snap)
	require.NoError(t, err)

	assert.Equal(t, "111111111111", b2.AccountID())
	assert.Equal(t, "eu-west-1", b2.Region())
	assert.Equal(t, 1, mediatailor.PlaybackConfigurationCount(b2))

	cfg, err := b2.GetPlaybackConfiguration("snap-cfg")
	require.NoError(t, err)
	assert.Equal(t, "snap-cfg", cfg.Name)
}

func TestRestoreInvalidJSON(t *testing.T) {
	t.Parallel()

	b := mediatailor.NewInMemoryBackend("000000000000", "us-east-1")
	err := b.Restore(t.Context(), []byte("not json"))
	require.Error(t, err)
}

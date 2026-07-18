package transfer_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/transfer"
)

// TestWebAppCountExport verifies WebAppCount export.
func TestWebAppCountExport(t *testing.T) {
	t.Parallel()

	b := transfer.NewInMemoryBackend(t.Context(), "000000000000", "us-east-1")
	assert.Equal(t, 0, transfer.WebAppCount(b))

	_, err := b.CreateWebApp(nil)
	require.NoError(t, err)

	assert.Equal(t, 1, transfer.WebAppCount(b))
}

// TestAddWebAppInternal verifies the AddWebAppInternal seed helper.
func TestAddWebAppInternal(t *testing.T) {
	t.Parallel()

	b := transfer.NewInMemoryBackend(t.Context(), "000000000000", "us-east-1")
	b.AddWebAppInternal("webapp-test123")

	assert.Equal(t, 1, transfer.WebAppCount(b))
}

func TestCreateWebApp(t *testing.T) {
	t.Parallel()

	b := newTestBackend(t)
	w, err := b.CreateWebApp(map[string]string{"env": "prod"})
	require.NoError(t, err)
	assert.NotEmpty(t, w.WebAppID)
}

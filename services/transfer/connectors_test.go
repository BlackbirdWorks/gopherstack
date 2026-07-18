package transfer_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/services/transfer"
)

// TestConnectorCountExport verifies ConnectorCount export.
func TestConnectorCountExport(t *testing.T) {
	t.Parallel()

	b := transfer.NewInMemoryBackend(t.Context(), "000000000000", "us-east-1")
	assert.Equal(t, 0, transfer.ConnectorCount(b))

	_, err := b.CreateConnector("https://example.com", "", nil, nil, nil)
	require.NoError(t, err)

	assert.Equal(t, 1, transfer.ConnectorCount(b))
}

// TestConnectorURLValidation verifies CreateConnector rejects empty URL.
func TestConnectorURLValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		url     string
		name    string
		wantErr bool
	}{
		{name: "valid url", url: "https://example.com", wantErr: false},
		{name: "empty url", url: "", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := transfer.NewInMemoryBackend(t.Context(), "000000000000", "us-east-1")
			_, err := b.CreateConnector(tt.url, "", nil, nil, nil)

			if tt.wantErr {
				require.Error(t, err)
				assert.ErrorIs(t, err, awserr.ErrInvalidParameter)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// TestAddConnectorInternal verifies the AddConnectorInternal seed helper.
func TestAddConnectorInternal(t *testing.T) {
	t.Parallel()

	b := transfer.NewInMemoryBackend(t.Context(), "000000000000", "us-east-1")
	b.AddConnectorInternal("c-test123", "https://example.com")

	assert.Equal(t, 1, transfer.ConnectorCount(b))

	require.NoError(t, b.DeleteConnector("c-test123"))
	assert.Equal(t, 0, transfer.ConnectorCount(b))
}

func TestCreateConnector(t *testing.T) {
	t.Parallel()

	b := newTestBackend(t)
	c, err := b.CreateConnector("https://example.com", "arn:role", nil, nil, map[string]string{"env": "test"})
	require.NoError(t, err)
	assert.NotEmpty(t, c.ConnectorID)
	assert.Equal(t, "https://example.com", c.URL)
	assert.Equal(t, "arn:role", c.AccessRole)
}

func TestDeleteConnector(t *testing.T) {
	t.Parallel()

	b := newTestBackend(t)
	c, err := b.CreateConnector("https://example.com", "", nil, nil, nil)
	require.NoError(t, err)

	require.NoError(t, b.DeleteConnector(c.ConnectorID))

	// Double delete should fail
	err = b.DeleteConnector(c.ConnectorID)
	require.Error(t, err)
	assert.ErrorIs(t, err, awserr.ErrNotFound)
}

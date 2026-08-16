package memorydb_test

import (
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/memorydb"
)

// TestProvider_Init tests that the Provider initialises successfully.
func TestProvider_Init(t *testing.T) {
	t.Parallel()

	p := &memorydb.Provider{}

	assert.Equal(t, "MemoryDB", p.Name())

	svc, err := p.Init(&service.AppContext{Logger: slog.Default()})
	require.NoError(t, err)
	assert.NotNil(t, svc)
	assert.Equal(t, "MemoryDB", svc.Name())
}

// TestRefinement1_ErrNilAppContext verifies that Init rejects a nil context.
func TestProviderInit_NilAppContext(t *testing.T) {
	t.Parallel()

	p := &memorydb.Provider{}
	_, err := p.Init(nil)

	require.Error(t, err)
	require.ErrorIs(t, err, memorydb.ErrNilAppContext)
}

// TestHandler_Provider_Init_WithNilContext tests that Init returns error for nil context.
func TestHandler_Provider_Init_WithNilContext(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		wantErr bool
	}{
		{name: "nil context returns ErrNilAppContext", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			p := &memorydb.Provider{}
			_, err := p.Init(nil)

			if tt.wantErr {
				assert.ErrorIs(t, err, memorydb.ErrNilAppContext)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

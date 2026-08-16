package glacier_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/glacier"
)

// TestProviderInit_NilCtx verifies that Init with nil context returns ErrNilAppContext.
func TestProviderInit_NilCtx(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr error
		name    string
	}{
		{name: "nil_ctx_returns_error", wantErr: glacier.ErrNilAppContext},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			p := glacier.Provider{}
			_, err := p.Init(nil)

			require.Error(t, err)
			assert.ErrorIs(t, err, tt.wantErr)
		})
	}
}

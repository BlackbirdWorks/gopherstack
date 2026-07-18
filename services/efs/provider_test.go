package efs_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/efs"
)

// TestProviderInit_NilCtx verifies ErrNilAppContext is returned for nil context.
func TestProviderInit_NilCtx(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr error
		name    string
	}{
		{
			name:    "nil_ctx_returns_sentinel",
			wantErr: efs.ErrNilAppContext,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			p := &efs.Provider{}
			_, err := p.Init(nil)

			require.ErrorIs(t, err, tt.wantErr)
		})
	}
}

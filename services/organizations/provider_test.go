package organizations_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/organizations"
)

// TestProvider_NilAppContextError verifies ErrNilAppContext is returned.
func TestProvider_NilAppContextError(t *testing.T) {
	t.Parallel()

	tests := []struct {
		ctx     any
		name    string
		wantErr bool
	}{
		{name: "nil_ctx_returns_error", ctx: nil, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			p := &organizations.Provider{}
			_, err := p.Init(nil)

			if tt.wantErr {
				require.ErrorIs(t, err, organizations.ErrNilAppContext)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

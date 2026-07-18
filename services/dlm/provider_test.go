package dlm_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/dlm"
)

// ---------------------------------------------------------------------------
// Provider
// ---------------------------------------------------------------------------

func TestProvider_Name(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		want string
	}{
		{name: "returns DLM", want: "DLM"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			p := &dlm.Provider{}
			assert.Equal(t, tc.want, p.Name())
		})
	}
}

func TestProvider_Init(t *testing.T) {
	t.Parallel()

	tests := []struct {
		ctx     *service.AppContext
		name    string
		wantErr bool
	}{
		{
			name:    "nil AppContext returns error",
			ctx:     nil,
			wantErr: true,
		},
		{
			name:    "valid AppContext returns handler",
			ctx:     &service.AppContext{},
			wantErr: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			p := &dlm.Provider{}
			got, err := p.Init(tc.ctx)
			if tc.wantErr {
				require.Error(t, err)
				assert.Nil(t, got)
			} else {
				require.NoError(t, err)
				assert.NotNil(t, got)
			}
		})
	}
}

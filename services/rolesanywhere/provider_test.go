package rolesanywhere_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/rolesanywhere"
)

// ---- Provider ----

func TestProvider_Name(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		wantName string
	}{
		{"provider name is RolesAnywhere", "RolesAnywhere"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			p := &rolesanywhere.Provider{}
			assert.Equal(t, tt.wantName, p.Name())
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
			name:    "nil context returns error",
			ctx:     nil,
			wantErr: true,
		},
		{
			name:    "empty context returns handler",
			ctx:     &service.AppContext{},
			wantErr: false,
		},
		{
			name:    "context with config provider returns handler",
			ctx:     &service.AppContext{Config: &fakeConfigProvider{}},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			p := &rolesanywhere.Provider{}
			got, err := p.Init(tt.ctx)
			if tt.wantErr {
				require.Error(t, err)
				assert.Nil(t, got)
			} else {
				require.NoError(t, err)
				assert.NotNil(t, got)
			}
		})
	}
}

// fakeConfigProvider implements config.Provider for tests.
type fakeConfigProvider struct{}

func (f *fakeConfigProvider) GetGlobalConfig() *config.GlobalConfig {
	return config.NewGlobalConfig("999999999999", "us-west-2", 0, 0, false, 0)
}

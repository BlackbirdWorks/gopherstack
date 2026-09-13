package azurearm_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/azurearm"
)

type fakeConfig struct {
	settings  azurearm.Settings
	vhostPort int
}

func (c fakeConfig) GetAzureARMSettings() azurearm.Settings { return c.settings }

func (c fakeConfig) GetAzureStorageVHostPort() int { return c.vhostPort }

func TestProvider_Init(t *testing.T) {
	t.Parallel()

	tests := []struct {
		appCtx   *service.AppContext
		name     string
		wantPort int
		wantErr  bool
	}{
		{
			name:    "nil app context errors",
			appCtx:  nil,
			wantErr: true,
		},
		{
			name:     "no ConfigProvider uses defaults",
			appCtx:   &service.AppContext{Config: struct{}{}},
			wantPort: azurearm.DefaultPort,
		},
		{
			name: "ConfigProvider settings are used",
			appCtx: &service.AppContext{Config: fakeConfig{
				settings: azurearm.Settings{Port: 19999, Environment: "custom"},
			}},
			wantPort: 19999,
		},
		{
			name: "matching vhost ports succeed",
			appCtx: &service.AppContext{Config: fakeConfig{
				settings:  azurearm.Settings{Port: 19999, StorageVHostPort: 10010},
				vhostPort: 10010,
			}},
			wantPort: 19999,
		},
		{
			name: "mismatched vhost ports error",
			appCtx: &service.AppContext{Config: fakeConfig{
				settings:  azurearm.Settings{Port: 19999, StorageVHostPort: 10010},
				vhostPort: 10011,
			}},
			wantErr: true,
		},
		{
			name: "AdvertiseStorageVHost override skips the mismatch check",
			appCtx: &service.AppContext{Config: fakeConfig{
				settings: azurearm.Settings{
					Port: 19999, StorageVHostPort: 10010, AdvertiseStorageVHost: "storage.example.com:9999",
				},
				vhostPort: 10011,
			}},
			wantPort: 19999,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			p := &azurearm.Provider{}

			reg, err := p.Init(tt.appCtx)
			if tt.wantErr {
				require.Error(t, err)

				if tt.appCtx == nil {
					require.ErrorIs(t, err, azurearm.ErrNilAppContext)
				} else {
					require.ErrorIs(t, err, azurearm.ErrStorageVHostPortMismatch)
				}

				return
			}

			require.NoError(t, err)

			h, ok := reg.(*azurearm.Handler)
			require.True(t, ok)
			assert.Equal(t, tt.wantPort, h.Port)
		})
	}
}

func TestProvider_Name(t *testing.T) {
	t.Parallel()

	p := &azurearm.Provider{}
	assert.Equal(t, "AzureARM", p.Name())
}

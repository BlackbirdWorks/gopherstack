package azurefile_test

import (
	"net"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/azurefile"
)

type fakeConfig struct{ port int }

func (f fakeConfig) GetAzureFileSettings() azurefile.Settings {
	return azurefile.Settings{Port: f.port}
}

func TestProviderInit(t *testing.T) {
	t.Parallel()

	tests := []struct {
		ctx      *service.AppContext
		name     string
		wantPort int
		wantErr  bool
	}{
		{name: "nil context", wantErr: true},
		{name: "default port", ctx: &service.AppContext{}, wantPort: azurefile.DefaultPort},
		{name: "configured port", ctx: &service.AppContext{Config: fakeConfig{port: 12345}}, wantPort: 12345},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			p := &azurefile.Provider{}
			assert.Equal(t, "AzureFile", p.Name())

			reg, err := p.Init(tt.ctx)
			if tt.wantErr {
				require.ErrorIs(t, err, azurefile.ErrNilAppContext)

				return
			}

			require.NoError(t, err)

			h, ok := reg.(*azurefile.Handler)
			require.True(t, ok)
			assert.Equal(t, tt.wantPort, h.Port)
			assert.Equal(t, "AzureFile", h.Name())
			assert.Contains(t, h.GetSupportedOperations(), "CreateShare")
			assert.Equal(t, 0, h.MatchPriority())
		})
	}
}

func TestStartWorkerBindsAndShutsDown(t *testing.T) {
	t.Parallel()

	l, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)

	port := l.Addr().(*net.TCPAddr).Port
	require.NoError(t, l.Close())

	h := azurefile.NewHandler(azurefile.NewInMemoryBackend())
	h.Port = port
	require.NoError(t, h.StartWorker(t.Context()))

	second := azurefile.NewHandler(azurefile.NewInMemoryBackend())
	second.Port = port
	require.Error(t, second.StartWorker(t.Context()), "a taken port fails fast")

	conn, err := (&net.Dialer{}).DialContext(t.Context(), "tcp", net.JoinHostPort("127.0.0.1", itoa(port)))
	require.NoError(t, err)
	require.NoError(t, conn.Close())

	h.Shutdown(t.Context())
	h.Shutdown(t.Context())
}

func TestRouteMatcherNeverMatches(t *testing.T) {
	t.Parallel()

	h := azurefile.NewHandler(azurefile.NewInMemoryBackend())
	assert.False(t, h.RouteMatcher()(nil))
}

func TestProviderPassesVerifyAuth(t *testing.T) {
	t.Parallel()

	reg, err := (&azurefile.Provider{}).Init(&service.AppContext{Config: verifyConfig{}})
	require.NoError(t, err)

	h, ok := reg.(*azurefile.Handler)
	require.True(t, ok)
	assert.True(t, h.VerifyAuth)
}

type verifyConfig struct{}

func (verifyConfig) GetAzureFileSettings() azurefile.Settings {
	return azurefile.Settings{Port: azurefile.DefaultPort, VerifyAuth: true}
}

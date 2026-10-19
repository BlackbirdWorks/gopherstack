package extensions

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLoadConfig(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr error
		check   func(*testing.T, Config)
		name    string
		file    string
		inline  []string
	}{
		{
			name: "yaml with defaults",
			file: "fail_open: false\nhook_timeout: 2s\n" +
				"extensions:\n  - name: a\n    command: [./a, --x]\n    restart: always\n" +
				"    health_interval: 3s\n  - name: b\n    image: ghcr.io/x/b:1\n",
			check: func(t *testing.T, c Config) {
				t.Helper()
				assert.False(t, c.FailOpenDefault())
				assert.Equal(t, 2*time.Second, c.HookTimeout)
				assert.Equal(t, RestartAlways, c.Extensions[0].Restart)
				assert.Equal(t, 3*time.Second, c.Extensions[0].HealthInterval)
				assert.Equal(t, RestartOnFailure, c.Extensions[1].Restart)
				assert.Equal(t, defaultMaxRestarts, c.Extensions[1].MaxRestarts)
			},
		},
		{
			name: "json file",
			file: `{"extensions":[{"name":"j","command":["./j"]}]}`,
			check: func(t *testing.T, c Config) {
				t.Helper()
				assert.True(t, c.FailOpenDefault())
				assert.Equal(t, []string{"./j"}, c.Extensions[0].Command)
			},
		},
		{
			name:   "inline command and image",
			inline: []string{"c=./bin/c --flag", "i=image:alpine:3"},
			check: func(t *testing.T, c Config) {
				t.Helper()
				assert.Equal(t, []string{"./bin/c", "--flag"}, c.Extensions[0].Command)
				assert.Equal(t, "alpine:3", c.Extensions[1].Image)
			},
		},
		{name: "duplicate", inline: []string{"a=x", "A=y"}, wantErr: ErrInvalidSpec},
		{name: "bad inline", inline: []string{"nonsense"}, wantErr: ErrInvalidSpec},
		{name: "bad name", inline: []string{"a/b=x"}, wantErr: ErrInvalidSpec},
		{
			name:    "both command and image",
			file:    "extensions:\n  - {name: a, command: [x], image: y}\n",
			wantErr: ErrInvalidSpec,
		},
		{
			name:    "bad restart",
			file:    "extensions:\n  - {name: a, command: [x], restart: sometimes}\n",
			wantErr: ErrInvalidSpec,
		},
		{name: "bad yaml", file: "extensions: [", wantErr: ErrInvalidConfig},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			s := Settings{Inline: tt.inline}

			if tt.file != "" {
				s.Config = filepath.Join(t.TempDir(), "ext.yaml")
				require.NoError(t, os.WriteFile(s.Config, []byte(tt.file), 0o600))
			}

			cfg, err := LoadConfig(s)
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)

				return
			}

			require.NoError(t, err)
			tt.check(t, cfg)
		})
	}

	t.Run("missing file", func(t *testing.T) {
		t.Parallel()

		_, err := LoadConfig(Settings{Config: filepath.Join(t.TempDir(), "nope.yaml")})
		require.ErrorIs(t, err, ErrInvalidConfig)
	})
}

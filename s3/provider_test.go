package s3_test

import (
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/s3"
)

func TestProvider_Name(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		wantName string
	}{
		{
			name:     "returns_s3",
			wantName: "S3",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			p := &s3.Provider{}
			assert.Equal(t, tt.wantName, p.Name())
		})
	}
}

func TestProvider_Init(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		config   any
		wantName string
	}{
		{
			name:     "no_config",
			config:   nil,
			wantName: "S3",
		},
		{
			name:     "with_config",
			config:   &mockS3Config{},
			wantName: "S3",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			p := &s3.Provider{}
			appCtx := &service.AppContext{
				Logger: slog.Default(),
				Config: tt.config,
			}

			svc, err := p.Init(appCtx)
			require.NoError(t, err)
			assert.NotNil(t, svc)
			assert.Equal(t, tt.wantName, svc.Name())
		})
	}
}

// mockS3Config implements s3.ConfigProvider for testing.
type mockS3Config struct{}

func (m *mockS3Config) GetS3Settings() s3.Settings {
	return s3.Settings{}
}

func (m *mockS3Config) GetS3Endpoint() string {
	return "localhost:8080"
}

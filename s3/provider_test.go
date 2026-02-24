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

	p := &s3.Provider{}
	assert.Equal(t, "S3", p.Name())
}

func TestProvider_Init_NoConfig(t *testing.T) {
	t.Parallel()

	p := &s3.Provider{}
	appCtx := &service.AppContext{
		Logger: slog.Default(),
	}

	svc, err := p.Init(appCtx)
	require.NoError(t, err)
	assert.NotNil(t, svc)
	assert.Equal(t, "S3", svc.Name())
}

func TestProvider_Init_WithConfig(t *testing.T) {
	t.Parallel()

	p := &s3.Provider{}
	appCtx := &service.AppContext{
		Logger: slog.Default(),
		Config: &mockS3Config{},
	}

	svc, err := p.Init(appCtx)
	require.NoError(t, err)
	assert.NotNil(t, svc)
}

// mockS3Config implements s3.ConfigProvider for testing.
type mockS3Config struct{}

func (m *mockS3Config) GetS3Settings() s3.Settings {
	return s3.Settings{}
}

func (m *mockS3Config) GetS3Endpoint() string {
	return "localhost:8080"
}

package dynamodb_test

import (
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/dynamodb"
)

func TestProvider_Name(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		wantName string
	}{
		{
			name:     "returns_DynamoDB",
			wantName: "DynamoDB",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			p := &dynamodb.Provider{}
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
			wantName: "DynamoDB",
		},
		{
			name:     "with_config",
			config:   &mockDDBConfig{},
			wantName: "DynamoDB",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			p := &dynamodb.Provider{}
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

// mockDDBConfig implements dynamodb.ConfigProvider for testing.
type mockDDBConfig struct{}

func (m *mockDDBConfig) GetDynamoDBSettings() dynamodb.Settings {
	return dynamodb.Settings{}
}

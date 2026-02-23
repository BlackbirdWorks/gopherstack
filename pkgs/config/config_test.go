package config_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/blackbirdworks/gopherstack/pkgs/config"
)

func TestGlobalConfig(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		accountID string
		region    string
	}{
		{
			name:      "default values",
			accountID: "000000000000",
			region:    "us-east-1",
		},
		{
			name:      "custom account ID",
			accountID: "123456789012",
			region:    "eu-west-1",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			cfg := config.GlobalConfig{
				AccountID: tc.accountID,
				Region:    tc.region,
			}

			assert.Equal(t, tc.accountID, cfg.AccountID)
			assert.Equal(t, tc.region, cfg.Region)
		})
	}
}

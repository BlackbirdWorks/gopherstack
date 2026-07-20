package config_test

import (
	"testing"
	"time"

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

			cfg := config.NewGlobalConfig(tc.accountID, tc.region, 0, 0, false, 0)

			assert.Equal(t, tc.accountID, cfg.GetAccountID())
			assert.Equal(t, tc.region, cfg.GetRegion())
		})
	}
}

func TestGlobalConfig_ZeroValue(t *testing.T) {
	t.Parallel()

	tests := []struct {
		cfg  *config.GlobalConfig
		name string
	}{
		{
			name: "zero value struct",
			cfg:  &config.GlobalConfig{},
		},
		{
			name: "nil struct pointer",
			cfg:  nil,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			assert.Empty(t, tc.cfg.GetAccountID())
			assert.Empty(t, tc.cfg.GetRegion())
			assert.Equal(t, 0, tc.cfg.GetLatencyMs())
			assert.False(t, tc.cfg.IsIAMEnforced())
			assert.Equal(t, time.Duration(0), tc.cfg.GetJanitorTimeout())
			assert.Equal(t, time.Duration(0), tc.cfg.GetAutoPurgeTTL())
			assert.True(t, tc.cfg.GetStartTime().IsZero())
		})
	}
}

func TestGlobalConfig_Update(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		accountID     string
		region        string
		wantAccountID string
		wantRegion    string
		latencyMs     int
		enforceIAM    bool
	}{
		{
			name:          "updates all fields",
			accountID:     "111111111111",
			region:        "eu-central-1",
			latencyMs:     100,
			enforceIAM:    true,
			wantAccountID: "111111111111",
			wantRegion:    "eu-central-1",
		},
		{
			name:          "update on zero value",
			accountID:     "222222222222",
			region:        "ap-southeast-1",
			latencyMs:     0,
			enforceIAM:    false,
			wantAccountID: "222222222222",
			wantRegion:    "ap-southeast-1",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			cfg := &config.GlobalConfig{}
			cfg.Update(tc.accountID, tc.region, tc.latencyMs, time.Minute, tc.enforceIAM, time.Hour)

			assert.Equal(t, tc.wantAccountID, cfg.GetAccountID())
			assert.Equal(t, tc.wantRegion, cfg.GetRegion())
			assert.Equal(t, tc.latencyMs, cfg.GetLatencyMs())
			assert.Equal(t, tc.enforceIAM, cfg.IsIAMEnforced())
			assert.Equal(t, time.Minute, cfg.GetJanitorTimeout())
			assert.Equal(t, time.Hour, cfg.GetAutoPurgeTTL())
		})
	}
}

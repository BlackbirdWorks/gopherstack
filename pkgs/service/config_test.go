package service_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

type providerStub struct{ cfg *config.GlobalConfig }

func (p providerStub) GetGlobalConfig() *config.GlobalConfig { return p.cfg }

func TestAccountRegion(t *testing.T) {
	t.Parallel()

	cfg := config.NewGlobalConfig("123456789012", "eu-west-1", 0, 0, false, 0)

	tests := []struct {
		name        string
		ctx         *service.AppContext
		wantAccount string
		wantRegion  string
	}{
		{
			"FromProvider",
			&service.AppContext{Config: providerStub{cfg: cfg}},
			"123456789012",
			"eu-west-1",
		},
		{
			"WithoutProviderReturnsEmpty",
			&service.AppContext{Config: "not a provider"},
			"",
			"",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			account, region := service.AccountRegion(tt.ctx)
			assert.Equal(t, tt.wantAccount, account)
			assert.Equal(t, tt.wantRegion, region)
		})
	}
}

func TestAccountRegionOrDefault(t *testing.T) {
	t.Parallel()

	cfg := config.NewGlobalConfig("123456789012", "eu-west-1", 0, 0, false, 0)

	tests := []struct {
		name        string
		ctx         *service.AppContext
		wantAccount string
		wantRegion  string
	}{
		{
			"FromProvider",
			&service.AppContext{Config: providerStub{cfg: cfg}},
			"123456789012",
			"eu-west-1",
		},
		{
			"WithoutProviderReturnsDefaults",
			&service.AppContext{Config: nil},
			config.DefaultAccountID,
			config.DefaultRegion,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			account, region := service.AccountRegionOrDefault(tt.ctx)
			assert.Equal(t, tt.wantAccount, account)
			assert.Equal(t, tt.wantRegion, region)
		})
	}
}

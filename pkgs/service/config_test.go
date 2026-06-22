package service_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

func TestAccountRegionFromProvider(t *testing.T) {
	t.Parallel()

	cfg := config.NewGlobalConfig("123456789012", "eu-west-1", 0, 0, false, 0)
	ctx := &service.AppContext{Config: providerStub{cfg: cfg}}

	account, region := service.AccountRegion(ctx)
	assert.Equal(t, "123456789012", account)
	assert.Equal(t, "eu-west-1", region)
}

func TestAccountRegionWithoutProviderReturnsEmpty(t *testing.T) {
	t.Parallel()

	ctx := &service.AppContext{Config: "not a provider"}

	account, region := service.AccountRegion(ctx)
	assert.Empty(t, account)
	assert.Empty(t, region)
}

func TestAccountRegionOrDefaultFromProvider(t *testing.T) {
	t.Parallel()

	cfg := config.NewGlobalConfig("123456789012", "eu-west-1", 0, 0, false, 0)
	ctx := &service.AppContext{Config: providerStub{cfg: cfg}}

	account, region := service.AccountRegionOrDefault(ctx)
	assert.Equal(t, "123456789012", account)
	assert.Equal(t, "eu-west-1", region)
}

func TestAccountRegionOrDefaultWithoutProviderReturnsDefaults(t *testing.T) {
	t.Parallel()

	ctx := &service.AppContext{Config: nil}

	account, region := service.AccountRegionOrDefault(ctx)
	assert.Equal(t, config.DefaultAccountID, account)
	assert.Equal(t, config.DefaultRegion, region)
}

type providerStub struct{ cfg *config.GlobalConfig }

func (p providerStub) GetGlobalConfig() *config.GlobalConfig { return p.cfg }

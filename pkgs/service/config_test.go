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

	type args struct {
		ctx *service.AppContext
	}
	type wants struct {
		account string
		region  string
	}

	tests := []struct {
		name  string
		args  args
		wants wants
	}{
		{
			name: "from provider",
			args: args{
				ctx: &service.AppContext{
					Config: providerStub{
						cfg: config.NewGlobalConfig("123456789012", "eu-west-1", 0, 0, false, 0),
					},
				},
			},
			wants: wants{
				account: "123456789012",
				region:  "eu-west-1",
			},
		},
		{
			name: "without provider returns empty",
			args: args{
				ctx: &service.AppContext{Config: "not a provider"},
			},
			wants: wants{
				account: "",
				region:  "",
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			account, region := service.AccountRegion(tc.args.ctx)
			assert.Equal(t, tc.wants.account, account)
			assert.Equal(t, tc.wants.region, region)
		})
	}
}

func TestAccountRegionOrDefault(t *testing.T) {
	t.Parallel()

	type args struct {
		ctx *service.AppContext
	}
	type wants struct {
		account string
		region  string
	}

	tests := []struct {
		name  string
		args  args
		wants wants
	}{
		{
			name: "from provider",
			args: args{
				ctx: &service.AppContext{
					Config: providerStub{
						cfg: config.NewGlobalConfig("123456789012", "eu-west-1", 0, 0, false, 0),
					},
				},
			},
			wants: wants{
				account: "123456789012",
				region:  "eu-west-1",
			},
		},
		{
			name: "without provider returns defaults",
			args: args{
				ctx: &service.AppContext{Config: nil},
			},
			wants: wants{
				account: config.DefaultAccountID,
				region:  config.DefaultRegion,
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			account, region := service.AccountRegionOrDefault(tc.args.ctx)
			assert.Equal(t, tc.wants.account, account)
			assert.Equal(t, tc.wants.region, region)
		})
	}
}

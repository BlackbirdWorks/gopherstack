package route53resolver_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	ec2backend "github.com/blackbirdworks/gopherstack/services/ec2"
	"github.com/blackbirdworks/gopherstack/services/route53resolver"
)

type ec2Config struct{ h service.Registerable }

func (c ec2Config) GetEC2Handler() service.Registerable { return c.h }

func TestCreateResolverEndpointDerivesHostVPC(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		subnet    string
		useSubnet bool
		wired     bool
		wantVPC   bool
	}{
		{name: "known_subnet", useSubnet: true, wired: true, wantVPC: true},
		{name: "unknown_subnet", subnet: "subnet-nope", wired: true},
		{name: "ec2_not_wired", useSubnet: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ec2 := ec2backend.NewInMemoryBackend("000000000000", "us-east-1")
			vpc, err := ec2.CreateVpc("10.0.0.0/16", "")
			require.NoError(t, err)

			sub, err := ec2.CreateSubnet(vpc.ID, "10.0.1.0/24", "us-east-1a")
			require.NoError(t, err)

			subnet := tt.subnet
			if tt.useSubnet {
				subnet = sub.ID
			}

			b := route53resolver.NewInMemoryBackend("000000000000", "us-east-1")
			if tt.wired {
				b.SetAppConfig(ec2Config{h: ec2backend.NewHandler(ec2)})
			} else {
				b.SetAppConfig(struct{}{})
			}

			ep, err := b.CreateResolverEndpoint(
				context.Background(), "ep", "INBOUND", "",
				[]route53resolver.IPAddress{{SubnetID: subnet, IP: "10.0.1.5"}, {SubnetID: subnet, IP: "10.0.1.6"}},
				nil, "", nil, "", "", "", false, false, false, false,
			)
			require.NoError(t, err)

			if tt.wantVPC {
				assert.Equal(t, vpc.ID, ep.HostVPCID)
			} else {
				assert.Empty(t, ep.HostVPCID)
			}
		})
	}
}

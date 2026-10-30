package app

import (
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/chaos"
	"github.com/blackbirdworks/gopherstack/pkgs/portalloc"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	elbv2backend "github.com/blackbirdworks/gopherstack/services/elbv2"
)

func TestELBv2DefaultSubnetZoneFollowsLoadBalancerRegion(t *testing.T) {
	t.Parallel()

	cli := &CLI{AccountID: "000000000000", Region: "us-east-1"}
	portAlloc, err := portalloc.New(19700, 19800)
	require.NoError(t, err)

	cli.faultStore = chaos.NewFaultStore()

	services, err := initializeServicesT(t, &service.AppContext{
		Logger: slog.Default(), Config: cli, JanitorCtx: t.Context(), PortAlloc: portAlloc,
	})
	require.NoError(t, err)

	h, ok := serviceByName(services)["ELBv2"].(*elbv2backend.Handler)
	require.True(t, ok)

	tests := []struct {
		region string
		zone   string
	}{
		{region: "us-east-1", zone: "us-east-1a"},
		{region: "us-west-2", zone: "us-west-2a"},
		{region: "eu-west-1", zone: "eu-west-1a"},
		{region: "ap-southeast-2", zone: "ap-southeast-2a"},
	}

	for _, tt := range tests {
		t.Run(tt.region, func(t *testing.T) {
			t.Parallel()

			lb, createErr := h.BackendFor(tt.region).CreateLoadBalancer(elbv2backend.CreateLoadBalancerInput{
				Name: "web", Type: "application", Subnets: []string{"subnet-default"},
			})
			require.NoError(t, createErr)
			require.Len(t, lb.AvailabilityZones, 1)
			assert.Equal(t, tt.zone, lb.AvailabilityZones[0].ZoneName)
		})
	}
}

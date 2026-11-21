package gcpcompute_test

import (
	"net/http"
	"testing"

	compute "cloud.google.com/go/compute/apiv1"
	"cloud.google.com/go/compute/apiv1/computepb"
	"github.com/stretchr/testify/require"
)

func TestInterconnectAttachmentReferencesStayInRegion(t *testing.T) {
	t.Parallel()

	foreign := "regions/" + otherRegion + "/interconnectAttachments/x"

	tests := []struct {
		insert func(t *testing.T, e *env) error
		name   string
	}{
		{func(t *testing.T, e *env) error {
			t.Helper()

			op, err := client(t, e, compute.NewVpnGatewaysRESTClient).Insert(t.Context(),
				&computepb.InsertVpnGatewayRequest{
					Project: project, Region: region, VpnGatewayResource: &computepb.VpnGateway{
						Name: s("gw"), Network: s("global/networks/vpc"),
						VpnInterfaces: []*computepb.VpnGatewayVpnGatewayInterface{{InterconnectAttachment: s(foreign)}},
					},
				})

			return e.finish(t, op, err)
		}, "vpn gateway interface"},
		{func(t *testing.T, e *env) error {
			t.Helper()

			op, err := e.routers(t).Insert(t.Context(), &computepb.InsertRouterRequest{
				Project: project, Region: region, RouterResource: &computepb.Router{
					Name: s("rtr"), Network: s("global/networks/vpc"),
					Interfaces: []*computepb.RouterInterface{
						{Name: s("if0"), LinkedInterconnectAttachment: s(foreign)},
					},
				},
			})

			return e.finish(t, op, err)
		}, "router interface"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			e.network(t, "vpc", false)

			assertAPIError(t, tt.insert(t, e), http.StatusBadRequest, "invalid")
		})
	}
}

func TestInterconnectMacsecKeyNamesUnique(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		keys   []string
		status int
	}{
		{"distinct", []string{"a", "b"}, 0},
		{"duplicate", []string{"a", "a"}, http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			ic := dedicated("ic")
			ic.Macsec = &computepb.InterconnectMacsec{}

			for _, k := range tt.keys {
				ic.Macsec.PreSharedKeys = append(
					ic.Macsec.PreSharedKeys, &computepb.InterconnectMacsecPreSharedKey{Name: s(k)},
				)
			}

			err := e.interconnect(t, ic)
			if tt.status != 0 {
				assertAPIError(t, err, tt.status, "invalid")

				return
			}

			require.NoError(t, err)
		})
	}
}

func TestInstantSnapshotGroupHoldsItsPolicy(t *testing.T) {
	t.Parallel()

	tests := []struct {
		policy *computepb.ResourcePolicy
		name   string
		status int
	}{
		{&computepb.ResourcePolicy{
			Name: s("cg"), DiskConsistencyGroupPolicy: &computepb.ResourcePolicyDiskConsistencyGroupPolicy{},
		}, "consistency group", 0},
		{named(daily("00:00"), "cg"), "snapshot schedule", http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			require.NoError(t, e.resPolicy(t, tt.policy))
			e.disk(t, "d1")
			require.NoError(t, e.changeDisk(t, "d1", true, rpRef("cg")))

			err := e.isgInsert(t, rpRef("cg"))
			if tt.status != 0 {
				assertAPIError(t, err, tt.status, "invalid")

				return
			}

			require.NoError(t, err)
			require.NoError(t, e.changeDisk(t, "d1", false, rpRef("cg")))

			_, derr := e.resPolicies(t).Delete(t.Context(), &computepb.DeleteResourcePolicyRequest{
				Project: project, Region: region, ResourcePolicy: "cg",
			})
			assertAPIError(t, derr, http.StatusBadRequest, "resourceInUseByAnotherResource")
		})
	}
}

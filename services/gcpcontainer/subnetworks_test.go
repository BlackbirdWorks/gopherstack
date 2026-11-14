//nolint:staticcheck // the API still serves fields the proto marks deprecated
package gcpcontainer_test

import (
	"errors"
	"testing"

	"cloud.google.com/go/container/apiv1/containerpb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/iterator"
	"google.golang.org/grpc/codes"
)

func TestListUsableSubnetworks(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		parent  string
		filter  string
		wantMsg string
		used    bool
		want    codes.Code
		wantLen int
	}{
		{name: "all", parent: "projects/" + project, wantLen: 1},
		{name: "in use", parent: "projects/" + project, used: true, wantLen: 1},
		{name: "filter match", parent: "projects/" + project, filter: "networkProjectId=" + project, wantLen: 1},
		{name: "filter other project", parent: "projects/" + project, filter: "networkProjectId=other", wantLen: 0},
		{name: "other project", parent: "projects/other", wantLen: 0},
		{
			name:    "bad filter",
			parent:  "projects/" + project,
			filter:  "name=x",
			want:    codes.InvalidArgument,
			wantMsg: "filter",
		},
		{name: "bad parent", parent: "nope", want: codes.InvalidArgument, wantMsg: "parent"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := computeEnv(t)

			if tt.used {
				e.create(t, zone, &containerpb.Cluster{
					Name: "s", InitialNodeCount: 1, Network: "vpc", Subnetwork: "sub",
					IpAllocationPolicy: &containerpb.IPAllocationPolicy{
						UseIpAliases: true, ClusterSecondaryRangeName: "pods", ServicesSecondaryRangeName: "svcs",
					},
				})
			}

			all, err := collectSubnets(t, e, tt.parent, tt.filter)
			if tt.want != codes.OK {
				assertCode(t, tt.want, err, tt.wantMsg)

				return
			}

			require.NoError(t, err)

			require.Len(t, all, tt.wantLen)

			if tt.wantLen == 0 {
				return
			}

			sub := all[0]
			assert.Equal(t, "projects/"+project+"/regions/"+region+"/subnetworks/sub", sub.GetSubnetwork())
			assert.Equal(t, "projects/"+project+"/global/networks/vpc", sub.GetNetwork())
			assert.Equal(t, "10.10.0.0/20", sub.GetIpCidrRange())
			require.Len(t, sub.GetSecondaryIpRanges(), 2)

			pods, svcs := sub.GetSecondaryIpRanges()[0], sub.GetSecondaryIpRanges()[1]
			assert.Equal(t, "pods", pods.GetRangeName())
			assert.Equal(t, "172.20.0.0/16", pods.GetIpCidrRange())

			if tt.used {
				assert.Equal(t, containerpb.UsableSubnetworkSecondaryRange_IN_USE_MANAGED_POD, pods.GetStatus())
				assert.Equal(t, containerpb.UsableSubnetworkSecondaryRange_IN_USE_SERVICE, svcs.GetStatus())

				return
			}

			assert.Equal(t, containerpb.UsableSubnetworkSecondaryRange_UNUSED, pods.GetStatus())
		})
	}
}

func collectSubnets(t *testing.T, e *env, parent, filter string) ([]*containerpb.UsableSubnetwork, error) {
	t.Helper()

	var out []*containerpb.UsableSubnetwork

	it := e.gc.ListUsableSubnetworks(
		t.Context(),
		&containerpb.ListUsableSubnetworksRequest{Parent: parent, Filter: filter},
	)

	for {
		s, err := it.Next()
		if errors.Is(err, iterator.Done) {
			return out, nil
		}

		if err != nil {
			return nil, err
		}

		out = append(out, s)
	}
}

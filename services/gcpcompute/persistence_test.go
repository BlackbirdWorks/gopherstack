package gcpcompute_test

import (
	"testing"

	"cloud.google.com/go/compute/apiv1/computepb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/blackbirdworks/gopherstack/services/gcpcompute"
)

func TestSnapshotRestoreRoundTrip(t *testing.T) {
	t.Parallel()

	src := newEnv(t)
	src.baseNet(t)

	spec := vmSpec("vm1")
	spec.Labels = map[string]string{"env": "x"}
	vm := src.vm(t, spec)

	op, err := src.addresses(t).
		Insert(
			t.Context(),
			&computepb.InsertAddressRequest{
				Project:         project,
				Region:          region,
				AddressResource: &computepb.Address{Name: s("keep")},
			},
		)
	src.done(t, op, err)

	op, err = src.firewalls(t).
		Insert(t.Context(), &computepb.InsertFirewallRequest{Project: project, FirewallResource: &computepb.Firewall{
			Name: s(
				"fw",
			),
			Network:      s("global/networks/vpc"),
			Allowed:      allow("tcp", "22"),
			SourceRanges: []string{"10.0.0.0/8"},
			Priority:     proto.Int32(0),
		}})
	src.done(t, op, err)

	op, err = src.routers(t).
		Insert(t.Context(), &computepb.InsertRouterRequest{Project: project, Region: region,
			RouterResource: &computepb.Router{
				Name:    s("rtr"),
				Network: s("global/networks/vpc"),
				Nats: []*computepb.RouterNat{
					{
						Name:                          s("n"),
						NatIpAllocateOption:           s("AUTO_ONLY"),
						SourceSubnetworkIpRangesToNat: s("ALL_SUBNETWORKS_ALL_IP_RANGES"),
					},
				},
			}})
	src.done(t, op, err)

	op, err = src.disks(t).CreateSnapshot(t.Context(), &computepb.CreateSnapshotDiskRequest{
		Project: project, Zone: zone, Disk: "vm1", SnapshotResource: &computepb.Snapshot{Name: s("snap")},
	})
	src.done(t, op, err)

	op, err = src.images(t).
		Insert(
			t.Context(),
			&computepb.InsertImageRequest{
				Project:       project,
				ImageResource: &computepb.Image{Name: s("img"), SourceDisk: s("zones/" + zone + "/disks/vm1")},
			},
		)
	src.done(t, op, err)

	data := src.backend.Snapshot(t.Context())
	require.NotEmpty(t, data)

	dst := newEnv(t)
	require.NoError(t, dst.backend.Restore(t.Context(), data))

	got := dst.getVM(t, "vm1")
	assert.Equal(t, vm.GetId(), got.GetId())
	assert.Equal(t, vm.GetNetworkInterfaces()[0].GetNetworkIP(), got.GetNetworkInterfaces()[0].GetNetworkIP())
	assert.Equal(
		t,
		vm.GetNetworkInterfaces()[0].GetAccessConfigs()[0].GetNatIP(),
		got.GetNetworkInterfaces()[0].GetAccessConfigs()[0].GetNatIP(),
	)
	assert.Equal(t, vm.GetMetadata().GetFingerprint(), got.GetMetadata().GetFingerprint())
	assert.Equal(t, map[string]string{"env": "x"}, got.GetLabels())

	fw, err := dst.firewalls(t).Get(t.Context(), &computepb.GetFirewallRequest{Project: project, Firewall: "fw"})
	require.NoError(t, err)
	assert.Equal(t, int32(0), fw.GetPriority())

	st, err := dst.routers(t).
		GetRouterStatus(
			t.Context(),
			&computepb.GetRouterStatusRouterRequest{Project: project, Region: region, Router: "rtr"},
		)
	require.NoError(t, err)
	assert.Len(t, st.GetResult().GetNatStatus()[0].GetAutoAllocatedNatIps(), 1)

	for _, name := range []string{"snap"} {
		_, err = dst.snapshots(t).Get(t.Context(), &computepb.GetSnapshotRequest{Project: project, Snapshot: name})
		require.NoError(t, err)
	}

	_, err = dst.images(t).Get(t.Context(), &computepb.GetImageRequest{Project: project, Image: "img"})
	require.NoError(t, err)

	_, err = dst.addresses(t).
		Get(t.Context(), &computepb.GetAddressRequest{Project: project, Region: region, Address: "keep"})
	require.NoError(t, err)

	n, err := dst.networks(t).Get(t.Context(), &computepb.GetNetworkRequest{Project: project, Network: "vpc"})
	require.NoError(t, err)
	assert.Len(t, n.GetSubnetworks(), 2)

	opsIt := dst.globalOps(t).List(t.Context(), &computepb.ListGlobalOperationsRequest{Project: project})
	_, err = opsIt.Next()
	require.NoError(t, err, "operations survive a restore")

	newIP := dst.vm(t, vmSpec("vm2")).GetNetworkInterfaces()[0].GetNetworkIP()
	assert.Equal(t, "10.10.0.3", newIP, "allocation resumes after the restored instance")

	dst.backend.Reset()

	_, err = dst.instances(t).
		Get(t.Context(), &computepb.GetInstanceRequest{Project: project, Zone: zone, Instance: "vm1"})
	assertAPIError(t, err, 404, "notFound")
}

func TestRestoreRejectsBadSnapshots(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		payload string
		wantErr bool
	}{
		{"null instance", `{"instances":{"p/z/x":null},"version":1}`, true},
		{"null network", `{"networks":{"p/x":null},"version":1}`, true},
		{"null security policy", `{"securityPolicies":{"p/global/x":null},"version":1}`, true},
		{"null resource policy", `{"resourcePolicies":{"p/r/x":null},"version":1}`, true},
		{"instance without body", `{"instances":{"p/z/x":{"phase":"start"}},"version":1}`, true},
		{"router without body", `{"routers":{"p/r/x":{}},"version":1}`, true},
		{"operation without body", `{"ops":{"k":{}},"version":1}`, true},
		{"old version is discarded", `{"version":0}`, false},
		{"empty ok", `{"version":1}`, false},
		{"garbage", `{`, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := gcpcompute.NewInMemoryBackend().Restore(t.Context(), []byte(tt.payload))
			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
		})
	}
}

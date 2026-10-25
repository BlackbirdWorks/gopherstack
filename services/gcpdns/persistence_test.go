package gcpdns_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	dnsapi "google.golang.org/api/dns/v1"

	"github.com/blackbirdworks/gopherstack/services/gcpdns"
)

func TestSnapshotRestore(t *testing.T) {
	t.Parallel()

	src := newEnv(t)
	z, err := src.svc.ManagedZones.Create(project, &dnsapi.ManagedZone{
		Name: "sec", DnsName: "sec.com.", Description: "d",
		DnssecConfig: &dnsapi.ManagedZoneDnsSecConfig{State: "on"},
	}).Context(t.Context()).Do()
	require.NoError(t, err)
	src.addRRSet(t, "sec", rrset("www.sec.com.", "A", 60, "1.2.3.4"))

	_, err = src.svc.ManagedZones.Patch(project, "sec", &dnsapi.ManagedZone{Description: "patched"}).
		Context(t.Context()).
		Do()
	require.NoError(t, err)
	_, err = src.svc.Policies.Create(project, &dnsapi.Policy{Name: "pol", EnableLogging: true}).
		Context(t.Context()).
		Do()
	require.NoError(t, err)
	_, err = src.svc.ResponsePolicies.Create(project, &dnsapi.ResponsePolicy{ResponsePolicyName: "rp"}).
		Context(t.Context()).
		Do()
	require.NoError(t, err)
	_, err = src.svc.ResponsePolicyRules.Create(project, "rp", &dnsapi.ResponsePolicyRule{
		RuleName: "r", DnsName: "x.com.", Behavior: "bypassResponsePolicy",
	}).Context(t.Context()).Do()
	require.NoError(t, err)

	data := src.backend.Snapshot(t.Context())
	require.NotEmpty(t, data)

	dst := gcpdns.NewInMemoryBackend()
	require.NoError(t, dst.Restore(t.Context(), data))

	got, err := dst.GetZone(project, "sec")
	require.NoError(t, err)
	assert.Equal(t, z.Id, got.Id)
	assert.Equal(t, "patched", got.Description)
	assert.Equal(t, z.NameServers, got.NameServers)

	rr, err := dst.GetRRSet(project, "sec", "www.sec.com.", "A")
	require.NoError(t, err)
	assert.Equal(t, []string{"1.2.3.4"}, rr.Rrdatas)

	keys, err := dst.ListKeys(project, "sec", "", 0, nil)
	require.NoError(t, err)
	assert.Len(t, keys.Data, 2)

	changes, err := dst.ListChanges(project, "sec", "", "", "", 0)
	require.NoError(t, err)
	assert.Len(t, changes.Data, 2)

	ops, err := dst.ListOperations(project, "sec", "", "", 0)
	require.NoError(t, err)
	assert.Len(t, ops.Data, 1)

	_, err = dst.GetPolicy(project, "pol")
	require.NoError(t, err)

	_, err = dst.GetRule(project, "rp", "r")
	require.NoError(t, err)

	dst.Reset()
	_, err = dst.GetZone(project, "sec")
	require.Error(t, err)
}

func TestRestoreRejectsCorruptSnapshot(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		data string
	}{
		{"garbage", "not json"},
		{"null zone", `{"version":1,"zones":{"p/z":null}}`},
		{"null response policy", `{"version":1,"responsePolicies":{"p/z":null}}`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			require.Error(t, gcpdns.NewInMemoryBackend().Restore(t.Context(), []byte(tt.data)))
		})
	}

	b := gcpdns.NewInMemoryBackend()
	require.NoError(t, b.Restore(t.Context(), []byte(`{"version":99}`)), "unknown versions start empty")
}

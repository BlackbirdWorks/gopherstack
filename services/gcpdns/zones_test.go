package gcpdns_test

import (
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	dnsapi "google.golang.org/api/dns/v1"
)

func TestCreateZoneDefaults(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	z := e.zone(t, "pub", "Example.COM.")

	assert.Equal(t, "dns#managedZone", z.Kind)
	assert.Equal(t, "example.com.", z.DnsName)
	assert.Equal(t, "public", z.Visibility)
	assert.NotZero(t, z.Id)
	assert.NotEmpty(t, z.CreationTime)
	require.Len(t, z.NameServers, 4)
	assert.True(t, strings.HasPrefix(z.NameServers[0], "ns-cloud-"))
	assert.True(t, strings.HasSuffix(z.NameServers[0], ".googledomains.com."))
	require.NotNil(t, z.DnssecConfig)
	assert.Equal(t, "off", z.DnssecConfig.State)

	rrs, err := e.svc.ResourceRecordSets.List(project, "pub").Context(t.Context()).Do()
	require.NoError(t, err)
	require.Len(t, rrs.Rrsets, 2)
	assert.Equal(t, "NS", rrs.Rrsets[0].Type)
	assert.Equal(t, z.NameServers, rrs.Rrsets[0].Rrdatas)
	assert.Equal(t, int64(21600), rrs.Rrsets[0].Ttl)
	assert.Equal(t, "SOA", rrs.Rrsets[1].Type)
	assert.Equal(t, z.NameServers[0]+" cloud-dns-hostmaster.google.com. 1 21600 3600 259200 300", rrs.Rrsets[1].Rrdatas[0])

	ch, err := e.svc.Changes.Get(project, "pub", "0").Context(t.Context()).Do()
	require.NoError(t, err)
	assert.Equal(t, "done", ch.Status)
	assert.Len(t, ch.Additions, 2)
}

func TestCreateZoneErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		zone   *dnsapi.ManagedZone
		name   string
		reason string
		status int
	}{
		{&dnsapi.ManagedZone{DnsName: "a.com.", Description: "d"}, "no name", "required", 400},
		{&dnsapi.ManagedZone{Name: "Bad_Name", DnsName: "a.com.", Description: "d"}, "bad name", "invalid", 400},
		{&dnsapi.ManagedZone{Name: "z", DnsName: "a.com", Description: "d"}, "no trailing dot", "invalid", 400},
		{&dnsapi.ManagedZone{Name: "z", Description: "d"}, "no dns name", "required", 400},
		{&dnsapi.ManagedZone{Name: "z", DnsName: "a.com."}, "no description", "required", 400},
		{&dnsapi.ManagedZone{Name: "z", DnsName: "a.com.", Description: "d", Visibility: "x"}, "bad visibility", "invalid", 400},
		{&dnsapi.ManagedZone{
			Name: "z", DnsName: "a.com.", Description: "d",
			PrivateVisibilityConfig: &dnsapi.ManagedZonePrivateVisibilityConfig{},
		}, "private config on public", "invalid", 400},
		{&dnsapi.ManagedZone{
			Name: "z", DnsName: "a.com.", Description: "d", Visibility: "private",
			PrivateVisibilityConfig: &dnsapi.ManagedZonePrivateVisibilityConfig{
				Networks: []*dnsapi.ManagedZonePrivateVisibilityConfigNetwork{{}},
			},
		}, "network without url", "required", 400},
		{&dnsapi.ManagedZone{
			Name: "z", DnsName: "a.com.", Description: "d", Visibility: "private",
			ForwardingConfig: &dnsapi.ManagedZoneForwardingConfig{},
		}, "forwarding without targets", "required", 400},
		{&dnsapi.ManagedZone{
			Name: "z", DnsName: "a.com.", Description: "d", Visibility: "private",
			DnssecConfig: &dnsapi.ManagedZoneDnsSecConfig{State: "on"},
		}, "dnssec on private", "invalid", 400},
		{&dnsapi.ManagedZone{
			Name: "z", DnsName: "a.com.", Description: "d",
			DnssecConfig: &dnsapi.ManagedZoneDnsSecConfig{State: "on", DefaultKeySpecs: []*dnsapi.DnsKeySpec{
				{Algorithm: "rsasha256", KeyLength: 999, KeyType: "keySigning"},
			}},
		}, "bad key length", "invalid", 400},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			_, err := e.svc.ManagedZones.Create(project, tt.zone).Context(t.Context()).Do()
			assertAPIError(t, err, tt.status, tt.reason)
		})
	}
}

func TestZoneCRUD(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	e.zone(t, "dup", "dup.com.")

	_, err := e.svc.ManagedZones.Create(project, &dnsapi.ManagedZone{
		Name: "dup", DnsName: "other.com.", Description: "d",
	}).Context(t.Context()).Do()
	assertAPIError(t, err, http.StatusConflict, "alreadyExists")

	_, err = e.svc.ManagedZones.Get(project, "missing").Context(t.Context()).Do()
	assertAPIError(t, err, http.StatusNotFound, "notFound")
	assert.Contains(t, err.Error(), "'parameters.managedZone' resource named 'missing' does not exist")

	require.NoError(t, e.svc.ManagedZones.Delete(project, "dup").Context(t.Context()).Do())
	assertAPIError(t, e.svc.ManagedZones.Delete(project, "dup").Context(t.Context()).Do(), 404, "notFound")
}

func TestDeleteNonEmptyZone(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	e.zone(t, "full", "full.com.")
	e.addRRSet(t, "full", rrset("www.full.com.", "A", 60, "1.2.3.4"))

	assertAPIError(t, e.svc.ManagedZones.Delete(project, "full").Context(t.Context()).Do(), 400, "containerNotEmpty")

	_, err := e.svc.ResourceRecordSets.Delete(project, "full", "www.full.com.", "A").Context(t.Context()).Do()
	require.NoError(t, err)
	require.NoError(t, e.svc.ManagedZones.Delete(project, "full").Context(t.Context()).Do())
}

func TestListZones(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	for i := range 5 {
		e.zone(t, fmt.Sprintf("z%d", i), fmt.Sprintf("z%d.com.", i))
	}

	var names []string

	err := e.svc.ManagedZones.List(project).MaxResults(2).Pages(t.Context(), func(r *dnsapi.ManagedZonesListResponse) error {
		assert.LessOrEqual(t, len(r.ManagedZones), 2)

		for _, z := range r.ManagedZones {
			names = append(names, z.Name)
		}

		return nil
	})
	require.NoError(t, err)
	assert.Equal(t, []string{"z0", "z1", "z2", "z3", "z4"}, names)

	filtered, err := e.svc.ManagedZones.List(project).DnsName("z3.com.").Context(t.Context()).Do()
	require.NoError(t, err)
	require.Len(t, filtered.ManagedZones, 1)
	assert.Equal(t, "z3", filtered.ManagedZones[0].Name)

	other, err := e.svc.ManagedZones.List("other-project").Context(t.Context()).Do()
	require.NoError(t, err)
	assert.Empty(t, other.ManagedZones)

	_, err = e.svc.ManagedZones.List(project).PageToken("%%%").Context(t.Context()).Do()
	assertAPIError(t, err, 400, "invalid")
}

func TestPrivateZone(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	net1 := "https://www.googleapis.com/compute/v1/projects/p/global/networks/n1"

	z, err := e.svc.ManagedZones.Create(project, &dnsapi.ManagedZone{
		Name: "priv", DnsName: "corp.internal.", Description: "d", Visibility: "private",
		PrivateVisibilityConfig: &dnsapi.ManagedZonePrivateVisibilityConfig{
			Networks: []*dnsapi.ManagedZonePrivateVisibilityConfigNetwork{{NetworkUrl: net1}},
		},
		Labels: map[string]string{"env": "dev"},
	}).Context(t.Context()).Do()
	require.NoError(t, err)

	assert.Equal(t, "private", z.Visibility)
	assert.Empty(t, z.NameServers)
	assert.Nil(t, z.DnssecConfig)
	require.Len(t, z.PrivateVisibilityConfig.Networks, 1)
	assert.Equal(t, "dns#managedZonePrivateVisibilityConfigNetwork", z.PrivateVisibilityConfig.Networks[0].Kind)
	assert.Equal(t, net1, z.PrivateVisibilityConfig.Networks[0].NetworkUrl)
	assert.Equal(t, map[string]string{"env": "dev"}, z.Labels)

	rrs, err := e.svc.ResourceRecordSets.List(project, "priv").Context(t.Context()).Do()
	require.NoError(t, err)
	require.Len(t, rrs.Rrsets, 2)
	assert.Equal(t, []string{"ns-gcp-private.googledomains.com."}, rrs.Rrsets[0].Rrdatas)
}

func TestForwardingAndPeering(t *testing.T) {
	t.Parallel()

	e := newEnv(t)

	fz, err := e.svc.ManagedZones.Create(project, &dnsapi.ManagedZone{
		Name: "fwd", DnsName: "fwd.com.", Description: "d", Visibility: "private",
		ForwardingConfig: &dnsapi.ManagedZoneForwardingConfig{TargetNameServers: []*dnsapi.ManagedZoneForwardingConfigNameServerTarget{
			{Ipv4Address: "10.0.0.1", ForwardingPath: "private"},
		}},
	}).Context(t.Context()).Do()
	require.NoError(t, err)
	assert.Equal(t, "10.0.0.1", fz.ForwardingConfig.TargetNameServers[0].Ipv4Address)
	assert.Equal(t, "dns#managedZoneForwardingConfigNameServerTarget", fz.ForwardingConfig.TargetNameServers[0].Kind)

	pz, err := e.svc.ManagedZones.Create(project, &dnsapi.ManagedZone{
		Name: "peer", DnsName: "peer.com.", Description: "d", Visibility: "private",
		PeeringConfig: &dnsapi.ManagedZonePeeringConfig{TargetNetwork: &dnsapi.ManagedZonePeeringConfigTargetNetwork{NetworkUrl: "n"}},
	}).Context(t.Context()).Do()
	require.NoError(t, err)
	assert.Equal(t, "n", pz.PeeringConfig.TargetNetwork.NetworkUrl)

	_, err = e.svc.ManagedZones.Create(project, &dnsapi.ManagedZone{
		Name: "both", DnsName: "both.com.", Description: "d", Visibility: "private",
		PeeringConfig:    &dnsapi.ManagedZonePeeringConfig{TargetNetwork: &dnsapi.ManagedZonePeeringConfigTargetNetwork{NetworkUrl: "n"}},
		ForwardingConfig: &dnsapi.ManagedZoneForwardingConfig{TargetNameServers: []*dnsapi.ManagedZoneForwardingConfigNameServerTarget{{Ipv4Address: "1.1.1.1"}}},
	}).Context(t.Context()).Do()
	assertAPIError(t, err, 400, "invalid")

	_, err = e.svc.ManagedZones.Create(project, &dnsapi.ManagedZone{
		Name: "pubfwd", DnsName: "pubfwd.com.", Description: "d",
		ForwardingConfig: &dnsapi.ManagedZoneForwardingConfig{TargetNameServers: []*dnsapi.ManagedZoneForwardingConfigNameServerTarget{{Ipv4Address: "1.1.1.1"}}},
	}).Context(t.Context()).Do()
	assertAPIError(t, err, 400, "invalid")
}

func TestPatchAndUpdateZone(t *testing.T) {
	t.Parallel()

	tests := []struct {
		call func(e *env, t *testing.T) (*dnsapi.Operation, error)
		name string
		desc string
	}{
		{func(e *env, t *testing.T) (*dnsapi.Operation, error) {
			return e.svc.ManagedZones.Patch(project, "z", &dnsapi.ManagedZone{Description: "patched"}).Context(t.Context()).Do()
		}, "patch", "patched"},
		{func(e *env, t *testing.T) (*dnsapi.Operation, error) {
			return e.svc.ManagedZones.Update(project, "z", &dnsapi.ManagedZone{
				Description: "updated", Labels: map[string]string{"a": "b"},
			}).Context(t.Context()).Do()
		}, "update", "updated"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			created := e.zone(t, "z", "z.com.")

			op, err := tt.call(e, t)
			require.NoError(t, err)
			assert.Equal(t, "dns#operation", op.Kind)
			assert.Equal(t, "done", op.Status)
			assert.Equal(t, "update", op.Type)
			assert.Equal(t, "test zone", op.ZoneContext.OldValue.Description)
			assert.Equal(t, tt.desc, op.ZoneContext.NewValue.Description)

			got, err := e.svc.ManagedZones.Get(project, "z").Context(t.Context()).Do()
			require.NoError(t, err)
			assert.Equal(t, tt.desc, got.Description)
			assert.Equal(t, created.Id, got.Id)
			assert.Equal(t, created.NameServers, got.NameServers)

			ops, err := e.svc.ManagedZoneOperations.List(project, "z").Context(t.Context()).Do()
			require.NoError(t, err)
			require.Len(t, ops.Operations, 1)

			one, err := e.svc.ManagedZoneOperations.Get(project, "z", op.Id).Context(t.Context()).Do()
			require.NoError(t, err)
			assert.Equal(t, op.Id, one.Id)
		})
	}
}

func TestUpdateZoneImmutable(t *testing.T) {
	t.Parallel()

	tests := []struct {
		patch *dnsapi.ManagedZone
		name  string
	}{
		{&dnsapi.ManagedZone{DnsName: "other.com."}, "dns name"},
		{&dnsapi.ManagedZone{Visibility: "private"}, "visibility"},
		{&dnsapi.ManagedZone{Name: "renamed"}, "name"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			e.zone(t, "z", "z.com.")

			_, err := e.svc.ManagedZones.Patch(project, "z", tt.patch).Context(t.Context()).Do()
			assertAPIError(t, err, 400, "invalid")
		})
	}

	e := newEnv(t)
	_, err := e.svc.ManagedZones.Patch(project, "nope", &dnsapi.ManagedZone{Description: "x"}).Context(t.Context()).Do()
	assertAPIError(t, err, 404, "notFound")
	_, err = e.svc.ManagedZoneOperations.Get(project, "nope", "1").Context(t.Context()).Do()
	assertAPIError(t, err, 404, "notFound")
}

func TestProjectQuota(t *testing.T) {
	t.Parallel()

	e := newEnv(t)

	p, err := e.svc.Projects.Get("quota-project").Context(t.Context()).Do()
	require.NoError(t, err)
	assert.Equal(t, "dns#project", p.Kind)
	assert.Equal(t, "quota-project", p.Id)
	assert.NotZero(t, p.Number)
	require.NotNil(t, p.Quota)
	assert.Equal(t, int64(10000), p.Quota.ManagedZones)
	assert.Equal(t, int64(1000), p.Quota.RrsetAdditionsPerChange)
	assert.Equal(t, int64(100000), p.Quota.TotalRrdataSizePerChange)

	again, err := e.svc.Projects.Get("quota-project").Context(t.Context()).Do()
	require.NoError(t, err)
	assert.Equal(t, p.Number, again.Number)
}

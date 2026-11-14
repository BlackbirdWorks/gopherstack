package gcpdns_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	beta2 "google.golang.org/api/dns/v1beta2"
	v2api "google.golang.org/api/dns/v2"
	"google.golang.org/api/option"
)

func geoPolicy() *beta2.RRSetRoutingPolicyGeoPolicy {
	return &beta2.RRSetRoutingPolicyGeoPolicy{Items: []*beta2.RRSetRoutingPolicyGeoPolicyGeoPolicyItem{
		{Location: "us-east1", Rrdatas: []string{"1.1.1.1"}},
	}}
}

func TestBeta2Surface(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	ctx := t.Context()

	svc, err := beta2.NewService(ctx, option.WithEndpoint(e.url), option.WithoutAuthentication())
	require.NoError(t, err)

	_, err = svc.ManagedZones.Create(project, &beta2.ManagedZone{Name: "b", DnsName: "b.com.", Description: "d"}).
		Context(ctx).Do()
	require.NoError(t, err)

	tests := []struct {
		policy *beta2.RRSetRoutingPolicy
		name   string
		both   bool
	}{
		{&beta2.RRSetRoutingPolicy{Geo: geoPolicy()}, "geo", false},
		{&beta2.RRSetRoutingPolicy{GeoPolicy: geoPolicy()}, "deprecated geoPolicy", false},
		{&beta2.RRSetRoutingPolicy{WrrPolicy: &beta2.RRSetRoutingPolicyWrrPolicy{
			Items: []*beta2.RRSetRoutingPolicyWrrPolicyWrrPolicyItem{{Weight: 1, Rrdatas: []string{"1.1.1.1"}}},
		}}, "deprecated wrrPolicy", false},
		{&beta2.RRSetRoutingPolicy{Geo: geoPolicy(), GeoPolicy: geoPolicy()}, "both", true},
	}

	for i, tt := range tests {
		got, cerr := svc.ResourceRecordSets.Create(project, "b", &beta2.ResourceRecordSet{
			Name: "r" + string(rune('a'+i)) + ".b.com.", Type: "A", Ttl: 60, RoutingPolicy: tt.policy,
		}).Context(ctx).Do()
		if tt.both {
			assertAPIError(t, cerr, 400, "invalid")

			continue
		}

		require.NoError(t, cerr, tt.name)
		require.NotNil(t, got.RoutingPolicy, tt.name)
		assert.True(t, got.RoutingPolicy.Geo != nil || got.RoutingPolicy.Wrr != nil, tt.name)
	}

	list, err := svc.ManagedZones.List(project).Context(ctx).Do()
	require.NoError(t, err)
	assert.Len(t, list.ManagedZones, 1)
}

func TestV2Surface(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	ctx := t.Context()

	svc, err := v2api.NewService(ctx, option.WithEndpoint(e.url), option.WithoutAuthentication())
	require.NoError(t, err)

	zone, err := svc.ManagedZones.Create(project, "global", &v2api.ManagedZone{
		Name: "z", DnsName: "z.com.", Description: "d",
	}).Context(ctx).Do()
	require.NoError(t, err)
	assert.Equal(t, "z", zone.Name)

	rr, err := svc.ResourceRecordSets.Create(project, "global", "z", &v2api.ResourceRecordSet{
		Name: "www.z.com.", Type: "A", Ttl: 60, Rrdatas: []string{"1.2.3.4"},
	}).Context(ctx).Do()
	require.NoError(t, err)
	assert.Equal(t, "www.z.com.", rr.Name)

	v1rr, err := e.svc.ResourceRecordSets.Get(project, "z", "www.z.com.", "A").Context(ctx).Do()
	require.NoError(t, err)
	assert.Equal(t, []string{"1.2.3.4"}, v1rr.Rrdatas)

	list, err := svc.ManagedZones.List(project, "global").Context(ctx).Do()
	require.NoError(t, err)
	assert.Len(t, list.ManagedZones, 1)

	_, err = svc.ManagedZones.List(project, "us-east1").Context(ctx).Do()
	require.Error(t, err)

	_, err = svc.Projects.Get(project, "global").Context(ctx).Do()
	require.NoError(t, err)
}

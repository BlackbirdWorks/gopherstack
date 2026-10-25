package gcpdns_test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	dnsapi "google.golang.org/api/dns/v1"
)

func TestRRSetCRUD(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	e.zone(t, "c", "c.com.")

	created, err := e.svc.ResourceRecordSets.Create(project, "c", rrset("WWW.c.com", "a", 120, "1.2.3.4")).
		Context(t.Context()).Do()
	require.NoError(t, err)
	assert.Equal(t, "www.c.com.", created.Name)
	assert.Equal(t, "A", created.Type)
	assert.Equal(t, "dns#resourceRecordSet", created.Kind)

	_, err = e.svc.ResourceRecordSets.Create(project, "c", rrset("www.c.com.", "A", 120, "1.2.3.4")).
		Context(t.Context()).Do()
	assertAPIError(t, err, 409, "alreadyExists")

	patched, err := e.svc.ResourceRecordSets.Patch(project, "c", "www.c.com.", "A", &dnsapi.ResourceRecordSet{
		Ttl: 30, Rrdatas: []string{"5.5.5.5"},
	}).Context(t.Context()).Do()
	require.NoError(t, err)
	assert.Equal(t, int64(30), patched.Ttl)
	assert.Equal(t, []string{"5.5.5.5"}, patched.Rrdatas)

	only, err := e.svc.ResourceRecordSets.Patch(project, "c", "www.c.com.", "A", &dnsapi.ResourceRecordSet{Ttl: 99}).
		Context(t.Context()).Do()
	require.NoError(t, err)
	assert.Equal(t, []string{"5.5.5.5"}, only.Rrdatas, "unpatched fields survive")

	_, err = e.svc.ResourceRecordSets.Patch(project, "c", "www.c.com.", "A", &dnsapi.ResourceRecordSet{
		Rrdatas: []string{"bad"},
	}).Context(t.Context()).Do()
	assertAPIError(t, err, 400, "invalid")

	_, err = e.svc.ResourceRecordSets.Patch(project, "c", "no.c.com.", "A", &dnsapi.ResourceRecordSet{Ttl: 1}).
		Context(t.Context()).Do()
	assertAPIError(t, err, 404, "notFound")

	_, err = e.svc.ResourceRecordSets.Delete(project, "c", "www.c.com.", "A").Context(t.Context()).Do()
	require.NoError(t, err)

	_, err = e.svc.ResourceRecordSets.Get(project, "c", "www.c.com.", "A").Context(t.Context()).Do()
	assertAPIError(t, err, 404, "notFound")

	_, err = e.svc.ResourceRecordSets.Delete(project, "c", "www.c.com.", "A").Context(t.Context()).Do()
	assertAPIError(t, err, 404, "notFound")

	_, err = e.svc.ResourceRecordSets.Get(project, "nozone", "www.c.com.", "A").Context(t.Context()).Do()
	assertAPIError(t, err, 404, "notFound")

	changes, err := e.svc.Changes.List(project, "c").Context(t.Context()).Do()
	require.NoError(t, err)
	assert.Len(t, changes.Changes, 5, "initial change plus create, two patches and delete")
}

func TestListRRSets(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	e.zone(t, "c", "c.com.")
	e.addRRSet(t, "c", rrset("b.c.com.", "A", 60, "1.1.1.1"))
	e.addRRSet(t, "c", rrset("b.c.com.", "TXT", 60, `"x"`))
	e.addRRSet(t, "c", rrset("a.c.com.", "A", 60, "1.1.1.2"))
	e.addRRSet(t, "c", rrset("x.a.c.com.", "A", 60, "1.1.1.3"))

	tests := []struct {
		name string
		typ  string
		want []string
	}{
		{"", "", []string{"c.com.:NS", "c.com.:SOA", "a.c.com.:A", "x.a.c.com.:A", "b.c.com.:A", "b.c.com.:TXT"}},
		{"b.c.com.", "", []string{"b.c.com.:A", "b.c.com.:TXT"}},
		{"b.c.com.", "TXT", []string{"b.c.com.:TXT"}},
		{"B.C.COM", "a", []string{"b.c.com.:A"}},
		{"none.c.com.", "", nil},
	}

	for _, tt := range tests {
		t.Run(fmt.Sprintf("%s/%s", tt.name, tt.typ), func(t *testing.T) {
			t.Parallel()

			call := e.svc.ResourceRecordSets.List(project, "c").MaxResults(2)
			if tt.name != "" {
				call = call.Name(tt.name)
			}

			if tt.typ != "" {
				call = call.Type(tt.typ)
			}

			var got []string

			require.NoError(t, call.Pages(t.Context(), func(r *dnsapi.ResourceRecordSetsListResponse) error {
				assert.LessOrEqual(t, len(r.Rrsets), 2)

				for _, rr := range r.Rrsets {
					got = append(got, rr.Name+":"+rr.Type)
				}

				return nil
			}))
			assert.Equal(t, tt.want, got)
		})
	}

	_, err := e.svc.ResourceRecordSets.List(project, "c").Type("A").Context(t.Context()).Do()
	assertAPIError(t, err, 400, "invalid")

	_, err = e.svc.ResourceRecordSets.List(project, "c").Filter("x").Context(t.Context()).Do()
	assertAPIError(t, err, 400, "invalid")
}

func TestRoutingPolicyRRSet(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	e.zone(t, "c", "c.com.")

	wrr := &dnsapi.ResourceRecordSet{
		Name: "lb.c.com.", Type: "A", Ttl: 60,
		RoutingPolicy: &dnsapi.RRSetRoutingPolicy{Wrr: &dnsapi.RRSetRoutingPolicyWrrPolicy{
			Items: []*dnsapi.RRSetRoutingPolicyWrrPolicyWrrPolicyItem{
				{Weight: 1, Rrdatas: []string{"1.1.1.1"}}, {Weight: 3, Rrdatas: []string{"2.2.2.2"}},
			},
		}},
	}
	got, err := e.svc.ResourceRecordSets.Create(project, "c", wrr).Context(t.Context()).Do()
	require.NoError(t, err)
	require.NotNil(t, got.RoutingPolicy.Wrr)
	assert.Len(t, got.RoutingPolicy.Wrr.Items, 2)
	assert.Empty(t, got.Rrdatas)

	bad := &dnsapi.ResourceRecordSet{
		Name: "bad.c.com.", Type: "A", Ttl: 60, Rrdatas: []string{"1.1.1.1"},
		RoutingPolicy: wrr.RoutingPolicy,
	}
	_, err = e.svc.ResourceRecordSets.Create(project, "c", bad).Context(t.Context()).Do()
	assertAPIError(t, err, 400, "invalid")

	badItem := &dnsapi.ResourceRecordSet{
		Name: "bad2.c.com.", Type: "A", Ttl: 60,
		RoutingPolicy: &dnsapi.RRSetRoutingPolicy{Wrr: &dnsapi.RRSetRoutingPolicyWrrPolicy{
			Items: []*dnsapi.RRSetRoutingPolicyWrrPolicyWrrPolicyItem{{Weight: 1, Rrdatas: []string{"nope"}}},
		}},
	}
	_, err = e.svc.ResourceRecordSets.Create(project, "c", badItem).Context(t.Context()).Do()
	assertAPIError(t, err, 400, "invalid")
}

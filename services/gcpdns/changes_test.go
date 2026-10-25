package gcpdns_test

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	dnsapi "google.golang.org/api/dns/v1"
)

func soaSerial(t *testing.T, e *env, zone string) string {
	t.Helper()

	rr, err := e.svc.ResourceRecordSets.Get(project, zone, "c.com.", "SOA").Context(t.Context()).Do()
	if err != nil {
		rr, err = e.svc.ResourceRecordSets.Get(project, zone, zoneDNS(zone), "SOA").Context(t.Context()).Do()
	}

	require.NoError(t, err)

	fields := strings.Fields(rr.Rrdatas[0])

	return fields[2]
}

func zoneDNS(zone string) string { return zone + ".com." }

func TestChangeLifecycle(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	e.zone(t, "c", "c.com.")
	assert.Equal(t, "1", soaSerial(t, e, "c"))

	ch, err := e.svc.Changes.Create(project, "c", &dnsapi.Change{
		Additions: []*dnsapi.ResourceRecordSet{
			rrset("www.c.com.", "A", 300, "1.2.3.4", "5.6.7.8"),
			rrset("mail.c.com.", "MX", 300, "10 mx1.c.com.", "20 mx2.c.com."),
		},
	}).Context(t.Context()).Do()
	require.NoError(t, err)

	assert.Equal(t, "dns#change", ch.Kind)
	assert.Equal(t, "1", ch.Id)
	assert.Equal(t, "pending", ch.Status)
	assert.NotEmpty(t, ch.StartTime)
	assert.Len(t, ch.Additions, 3, "two additions plus the SOA bump")
	assert.Len(t, ch.Deletions, 1, "the superseded SOA")
	assert.Equal(t, "2", soaSerial(t, e, "c"))

	got, err := e.svc.Changes.Get(project, "c", ch.Id).Context(t.Context()).Do()
	require.NoError(t, err)
	assert.Equal(t, "done", got.Status)

	ch2, err := e.svc.Changes.Create(project, "c", &dnsapi.Change{
		Deletions: []*dnsapi.ResourceRecordSet{rrset("www.c.com.", "A", 300, "5.6.7.8", "1.2.3.4")},
		Additions: []*dnsapi.ResourceRecordSet{rrset("www.c.com.", "A", 60, "9.9.9.9")},
	}).Context(t.Context()).Do()
	require.NoError(t, err)
	assert.Equal(t, "2", ch2.Id)

	rr, err := e.svc.ResourceRecordSets.Get(project, "c", "www.c.com.", "A").Context(t.Context()).Do()
	require.NoError(t, err)
	assert.Equal(t, []string{"9.9.9.9"}, rr.Rrdatas)
	assert.Equal(t, int64(60), rr.Ttl)
	assert.Equal(t, "3", soaSerial(t, e, "c"))
}

func TestChangeListing(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	e.zone(t, "c", "c.com.")

	for i := range 4 {
		e.addRRSet(t, "c", rrset(fmt.Sprintf("h%d.c.com.", i), "A", 60, "1.1.1.1"))
	}

	tests := []struct {
		order string
		first string
		last  string
	}{
		{"", "0", "4"},
		{"ascending", "0", "4"},
		{"descending", "4", "0"},
	}

	for _, tt := range tests {
		t.Run("order "+tt.order, func(t *testing.T) {
			t.Parallel()

			var ids []string

			call := e.svc.Changes.List(project, "c").MaxResults(2)
			if tt.order != "" {
				call = call.SortOrder(tt.order)
			}

			require.NoError(t, call.Pages(t.Context(), func(r *dnsapi.ChangesListResponse) error {
				for _, c := range r.Changes {
					assert.Equal(t, "done", c.Status)
					ids = append(ids, c.Id)
				}

				return nil
			}))
			require.Len(t, ids, 5)
			assert.Equal(t, tt.first, ids[0])
			assert.Equal(t, tt.last, ids[4])
		})
	}

	_, err := e.svc.Changes.Get(project, "c", "99").Context(t.Context()).Do()
	assertAPIError(t, err, 404, "notFound")

	_, err = e.svc.Changes.List(project, "c").SortBy("bogus").Context(t.Context()).Do()
	assertAPIError(t, err, 400, "invalid")
}

func TestChangeValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		change *dnsapi.Change
		name   string
		reason string
		status int
	}{
		{&dnsapi.Change{}, "empty", "invalid", 400},
		{
			&dnsapi.Change{Additions: []*dnsapi.ResourceRecordSet{rrset("x.c.com.", "A", 60, "1.2.3.4"), rrset("y.c.com.", "A", 60, "nope")}},
			"bad ipv4 aborts the whole change", "invalid", 400,
		},
		{
			&dnsapi.Change{Additions: []*dnsapi.ResourceRecordSet{rrset("x.other.com.", "A", 60, "1.2.3.4")}},
			"outside zone", "invalid", 400,
		},
		{
			&dnsapi.Change{Additions: []*dnsapi.ResourceRecordSet{rrset("pre.c.com.", "A", 60, "1.2.3.4")}},
			"already exists", "alreadyExists", 409,
		},
		{
			&dnsapi.Change{Deletions: []*dnsapi.ResourceRecordSet{rrset("pre.c.com.", "A", 61, "1.2.3.4")}},
			"deletion ttl mismatch", "conditionNotMet", 412,
		},
		{
			&dnsapi.Change{Deletions: []*dnsapi.ResourceRecordSet{rrset("pre.c.com.", "A", 60, "9.9.9.9")}},
			"deletion data mismatch", "conditionNotMet", 412,
		},
		{
			&dnsapi.Change{Deletions: []*dnsapi.ResourceRecordSet{rrset("gone.c.com.", "A", 60, "1.2.3.4")}},
			"deletion missing", "notFound", 404,
		},
		{
			&dnsapi.Change{Additions: []*dnsapi.ResourceRecordSet{rrset("pre.c.com.", "CNAME", 60, "t.c.com.")}},
			"cname beside other data", "invalid", 400,
		},
		{
			&dnsapi.Change{Additions: []*dnsapi.ResourceRecordSet{rrset("cn.c.com.", "CNAME", 60, "t.c.com."), rrset("cn.c.com.", "TXT", 60, `"x"`)}},
			"cname and txt in one change", "invalid", 400,
		},
		{
			&dnsapi.Change{Additions: []*dnsapi.ResourceRecordSet{rrset("cn.c.com.", "CNAME", 60, "a.c.com.", "b.c.com.")}},
			"two cname targets", "invalid", 400,
		},
		{
			&dnsapi.Change{Additions: []*dnsapi.ResourceRecordSet{rrset("c.com.", "CNAME", 60, "a.c.com.")}},
			"apex cname", "invalid", 400,
		},
		{
			&dnsapi.Change{Deletions: []*dnsapi.ResourceRecordSet{{Name: "c.com.", Type: "NS", Ttl: 21600, Rrdatas: []string{"x."}}}},
			"apex ns deletion mismatch", "conditionNotMet", 412,
		},
		{
			&dnsapi.Change{Additions: []*dnsapi.ResourceRecordSet{rrset("t.c.com.", "TXT", 60, "unquoted")}},
			"unquoted txt", "invalid", 400,
		},
		{
			&dnsapi.Change{Additions: []*dnsapi.ResourceRecordSet{rrset("m.c.com.", "MX", 60, "10 relative")}},
			"relative mx target", "invalid", 400,
		},
		{
			&dnsapi.Change{Additions: []*dnsapi.ResourceRecordSet{rrset("m.c.com.", "MX", 60, "ten mx.c.com.")}},
			"bad mx preference", "invalid", 400,
		},
		{
			&dnsapi.Change{Additions: []*dnsapi.ResourceRecordSet{rrset("a.c.com.", "AAAA", 60, "1.2.3.4")}},
			"ipv4 in aaaa", "invalid", 400,
		},
		{
			&dnsapi.Change{Additions: []*dnsapi.ResourceRecordSet{rrset("a.c.com.", "BOGUS", 60, "x")}},
			"unknown type", "invalid", 400,
		},
		{
			&dnsapi.Change{Additions: []*dnsapi.ResourceRecordSet{rrset("a.c.com.", "A", -1, "1.2.3.4")}},
			"negative ttl", "invalid", 400,
		},
		{
			&dnsapi.Change{Additions: []*dnsapi.ResourceRecordSet{{Name: "a.c.com.", Type: "A", Ttl: 60}}},
			"no data", "required", 400,
		},
		{
			&dnsapi.Change{Additions: []*dnsapi.ResourceRecordSet{rrset("s.c.com.", "SOA", 60, "ns. h. 1 1 1 1 1")}},
			"soa off apex", "invalid", 400,
		},
		{
			&dnsapi.Change{Additions: []*dnsapi.ResourceRecordSet{rrset("s.c.com.", "SRV", 60, "1 2 3")}},
			"short srv", "invalid", 400,
		},
		{
			&dnsapi.Change{Additions: []*dnsapi.ResourceRecordSet{rrset("s.c.com.", "CAA", 60, "0 issue")}},
			"short caa", "invalid", 400,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			e.zone(t, "c", "c.com.")
			e.addRRSet(t, "c", rrset("pre.c.com.", "A", 60, "1.2.3.4"))

			before, err := e.svc.ResourceRecordSets.List(project, "c").Context(t.Context()).Do()
			require.NoError(t, err)

			_, err = e.svc.Changes.Create(project, "c", tt.change).Context(t.Context()).Do()
			assertAPIError(t, err, tt.status, tt.reason)

			after, err := e.svc.ResourceRecordSets.List(project, "c").Context(t.Context()).Do()
			require.NoError(t, err)
			assert.Equal(t, before.Rrsets, after.Rrsets, "a failed change must not touch the zone")
		})
	}
}

func TestChangeValidRData(t *testing.T) {
	t.Parallel()

	tests := []struct {
		rr   *dnsapi.ResourceRecordSet
		name string
	}{
		{rrset("a.c.com.", "A", 60, "192.0.2.1"), "a"},
		{rrset("a.c.com.", "AAAA", 60, "2001:db8::1"), "aaaa"},
		{rrset("a.c.com.", "CNAME", 60, "b.c.com."), "cname"},
		{rrset("a.c.com.", "MX", 60, "10 mx.c.com."), "mx"},
		{rrset("a.c.com.", "TXT", 60, `"v=spf1 -all"`, `"second"`), "txt"},
		{rrset("a.c.com.", "NS", 60, "ns1.c.com."), "ns"},
		{rrset("a.c.com.", "SRV", 60, "1 2 443 srv.c.com."), "srv"},
		{rrset("a.c.com.", "CAA", 60, `0 issue "letsencrypt.org"`), "caa"},
		{rrset("4.3.2.1.in-addr.arpa.c.com.", "PTR", 60, "host.c.com."), "ptr"},
		{rrset("*.c.com.", "A", 60, "192.0.2.9"), "wildcard"},
		{rrset("a.c.com.", "SPF", 60, `"v=spf1 -all"`), "spf"},
		{&dnsapi.ResourceRecordSet{Name: "a.c.com.", Type: "A", Rrdatas: []string{"192.0.2.1"}}, "default ttl"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			e.zone(t, "c", "c.com.")

			ch, err := e.svc.Changes.Create(project, "c", &dnsapi.Change{
				Additions: []*dnsapi.ResourceRecordSet{tt.rr},
			}).Context(t.Context()).Do()
			require.NoError(t, err)
			assert.Equal(t, "dns#resourceRecordSet", ch.Additions[0].Kind)

			got, err := e.svc.ResourceRecordSets.Get(project, "c", tt.rr.Name, tt.rr.Type).Context(t.Context()).Do()
			require.NoError(t, err)
			assert.Equal(t, tt.rr.Rrdatas, got.Rrdatas)

			if tt.rr.Ttl == 0 {
				assert.Equal(t, int64(300), got.Ttl)
			}
		})
	}
}

func TestChangeSOA(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		newData string
		wantErr bool
	}{
		{"higher serial", "ns-cloud-x1.googledomains.com. h.example. 50 21600 3600 259200 300", false},
		{"same serial", "ns-cloud-x1.googledomains.com. h.example. 1 21600 3600 259200 300", true},
		{"lower serial", "ns-cloud-x1.googledomains.com. h.example. 0 21600 3600 259200 300", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			e.zone(t, "c", "c.com.")

			old, err := e.svc.ResourceRecordSets.Get(project, "c", "c.com.", "SOA").Context(t.Context()).Do()
			require.NoError(t, err)

			_, err = e.svc.Changes.Create(project, "c", &dnsapi.Change{
				Deletions: []*dnsapi.ResourceRecordSet{old},
				Additions: []*dnsapi.ResourceRecordSet{rrset("c.com.", "SOA", 21600, tt.newData)},
			}).Context(t.Context()).Do()

			if tt.wantErr {
				assertAPIError(t, err, 400, "invalid")

				return
			}

			require.NoError(t, err)

			now, err := e.svc.ResourceRecordSets.Get(project, "c", "c.com.", "SOA").Context(t.Context()).Do()
			require.NoError(t, err)
			assert.Equal(t, tt.newData, now.Rrdatas[0])
		})
	}
}

func TestApexRecordsProtected(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	e.zone(t, "c", "c.com.")

	for _, typ := range []string{"NS", "SOA"} {
		rr, err := e.svc.ResourceRecordSets.Get(project, "c", "c.com.", typ).Context(t.Context()).Do()
		require.NoError(t, err)

		_, err = e.svc.Changes.Create(project, "c", &dnsapi.Change{Deletions: []*dnsapi.ResourceRecordSet{rr}}).
			Context(t.Context()).Do()
		assertAPIError(t, err, 400, "invalid")

		_, err = e.svc.ResourceRecordSets.Delete(project, "c", "c.com.", typ).Context(t.Context()).Do()
		assertAPIError(t, err, 400, "invalid")
	}
}

func TestChangeQuota(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	e.zone(t, "c", "c.com.")

	adds := make([]*dnsapi.ResourceRecordSet, 1001)
	for i := range adds {
		adds[i] = rrset(fmt.Sprintf("h%d.c.com.", i), "A", 60, "1.1.1.1")
	}

	_, err := e.svc.Changes.Create(project, "c", &dnsapi.Change{Additions: adds}).Context(t.Context()).Do()
	assertAPIError(t, err, 403, "quotaExceeded")

	_, err = e.svc.Changes.Create(project, "c", &dnsapi.Change{Additions: adds[:1000]}).Context(t.Context()).Do()
	require.NoError(t, err)
}

package ocidns_test

import (
	"net/http"
	"strconv"
	"testing"
	"time"

	"github.com/oracle/oci-go-sdk/v65/common"
	ocidnssdk "github.com/oracle/oci-go-sdk/v65/dns"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const zoneName = "rec.example"

func newZoneEnv(t *testing.T) *env {
	t.Helper()

	e := newEnv(t, envOpts{})
	e.createZone(t, zoneName, nil)

	return e
}

func TestPatchZoneRecords(t *testing.T) {
	t.Parallel()

	req, prohibit := ocidnssdk.RecordOperationOperationRequire, ocidnssdk.RecordOperationOperationProhibit

	tests := []struct {
		name       string
		wantCode   string
		setup      []ocidnssdk.RecordOperation
		ops        []ocidnssdk.RecordOperation
		want       []string
		wantStatus int
		wantSerial int64
	}{
		{
			name: "add A", ops: []ocidnssdk.RecordOperation{add("www.rec.example", "A", "192.0.2.1", 300)},
			want: []string{"www.rec.example A 300 192.0.2.1"}, wantSerial: 2,
		},
		{
			name: "add normalizes rdata and case",
			ops:  []ocidnssdk.RecordOperation{add("WWW.Rec.Example.", "cname", "Target.Rec.Example", 60)},
			want: []string{"www.rec.example CNAME 60 target.rec.example."}, wantSerial: 2,
		},
		{
			name: "add two rdata in one set", ops: []ocidnssdk.RecordOperation{
				add("rec.example", "TXT", `"one"`, 120), add("rec.example", "TXT", `"two"`, 120),
			},
			want: []string{`rec.example TXT 120 "one"`, `rec.example TXT 120 "two"`}, wantSerial: 2,
		},
		{
			name:       "add existing is a no-op",
			setup:      []ocidnssdk.RecordOperation{add("a.rec.example", "A", "192.0.2.1", 300)},
			ops:        []ocidnssdk.RecordOperation{add("a.rec.example", "A", "192.0.2.1", 300)},
			want:       []string{"a.rec.example A 300 192.0.2.1"},
			wantSerial: 2,
		},
		{
			name:       "add with new ttl retimes the set",
			setup:      []ocidnssdk.RecordOperation{add("a.rec.example", "A", "192.0.2.1", 300)},
			ops:        []ocidnssdk.RecordOperation{add("a.rec.example", "A", "192.0.2.2", 30)},
			want:       []string{"a.rec.example A 30 192.0.2.1", "a.rec.example A 30 192.0.2.2"},
			wantSerial: 3,
		},
		{
			name: "remove one of two", setup: []ocidnssdk.RecordOperation{
				add("a.rec.example", "A", "192.0.2.1", 300), add("a.rec.example", "A", "192.0.2.2", 300),
			},
			ops:  []ocidnssdk.RecordOperation{remove("a.rec.example", "A", "192.0.2.1")},
			want: []string{"a.rec.example A 300 192.0.2.2"}, wantSerial: 3,
		},
		{
			name: "remove whole set", setup: []ocidnssdk.RecordOperation{
				add("a.rec.example", "A", "192.0.2.1", 300), add("a.rec.example", "A", "192.0.2.2", 300),
			},
			ops: []ocidnssdk.RecordOperation{remove("a.rec.example", "A", "")}, wantSerial: 3,
		},
		{
			name: "remove all at domain", setup: []ocidnssdk.RecordOperation{
				add("a.rec.example", "A", "192.0.2.1", 300), add("a.rec.example", "TXT", `"t"`, 300),
			},
			ops: []ocidnssdk.RecordOperation{remove("a.rec.example", "", "")}, wantSerial: 3,
		},
		{
			name:       "remove absent is a no-op",
			ops:        []ocidnssdk.RecordOperation{remove("a.rec.example", "A", "192.0.2.9")},
			wantSerial: 1,
		},
		{
			name:  "require holds",
			setup: []ocidnssdk.RecordOperation{add("a.rec.example", "A", "192.0.2.1", 300)},
			ops: []ocidnssdk.RecordOperation{
				op(req, "a.rec.example", "A", "192.0.2.1", 0), add("b.rec.example", "A", "192.0.2.5", 300),
			},
			want: []string{"a.rec.example A 300 192.0.2.1", "b.rec.example A 300 192.0.2.5"}, wantSerial: 3,
		},
		{
			name: "require fails", ops: []ocidnssdk.RecordOperation{
				op(req, "a.rec.example", "A", "192.0.2.1", 0), add("b.rec.example", "A", "192.0.2.5", 300),
			},
			wantStatus: http.StatusPreconditionFailed, wantCode: "PreconditionFailed", wantSerial: 1,
		},
		{
			name:  "prohibit fails",
			setup: []ocidnssdk.RecordOperation{add("a.rec.example", "A", "192.0.2.1", 300)},
			ops:   []ocidnssdk.RecordOperation{op(prohibit, "a.rec.example", "A", "", 0)},
			want:  []string{"a.rec.example A 300 192.0.2.1"}, wantStatus: http.StatusPreconditionFailed,
			wantCode: "PreconditionFailed", wantSerial: 2,
		},
		{
			name: "prohibit holds", ops: []ocidnssdk.RecordOperation{
				op(prohibit, "a.rec.example", "A", "", 0), add("a.rec.example", "A", "192.0.2.1", 300),
			},
			want: []string{"a.rec.example A 300 192.0.2.1"}, wantSerial: 2,
		},
		{
			name: "failed op rolls the whole request back", ops: []ocidnssdk.RecordOperation{
				add("ok.rec.example", "A", "192.0.2.1", 300), add("x.other.example", "A", "192.0.2.1", 300),
			},
			wantStatus: http.StatusBadRequest, wantCode: "InvalidParameter", wantSerial: 1,
		},
		{
			name: "out of zone", ops: []ocidnssdk.RecordOperation{add("www.other.example", "A", "192.0.2.1", 300)},
			wantStatus: http.StatusBadRequest, wantCode: "InvalidParameter", wantSerial: 1,
		},
		{
			name:       "suffix lookalike is out of zone",
			ops:        []ocidnssdk.RecordOperation{add("xrec.example", "A", "192.0.2.1", 300)},
			wantStatus: http.StatusBadRequest,
			wantCode:   "InvalidParameter",
			wantSerial: 1,
		},
		{
			name: "bad ipv4", ops: []ocidnssdk.RecordOperation{add("a.rec.example", "A", "999.0.0.1", 300)},
			wantStatus: http.StatusBadRequest, wantCode: "InvalidParameter", wantSerial: 1,
		},
		{
			name: "aaaa in a", ops: []ocidnssdk.RecordOperation{add("a.rec.example", "A", "2001:db8::1", 300)},
			wantStatus: http.StatusBadRequest, wantCode: "InvalidParameter", wantSerial: 1,
		},
		{
			name:       "mx without preference",
			ops:        []ocidnssdk.RecordOperation{add("rec.example", "MX", "mail.rec.example.", 300)},
			wantStatus: http.StatusBadRequest,
			wantCode:   "InvalidParameter",
			wantSerial: 1,
		},
		{
			name: "srv ok",
			ops: []ocidnssdk.RecordOperation{
				add("_sip._tcp.rec.example", "SRV", "10 5 5060 sip.rec.example.", 300),
			},
			want:       []string{"_sip._tcp.rec.example SRV 300 10 5 5060 sip.rec.example."},
			wantSerial: 2,
		},
		{
			name:       "caa ok",
			ops:        []ocidnssdk.RecordOperation{add("rec.example", "CAA", `0 issue "letsencrypt.org"`, 300)},
			want:       []string{`rec.example CAA 300 0 issue "letsencrypt.org"`},
			wantSerial: 2,
		},
		{
			name: "wildcard ok", ops: []ocidnssdk.RecordOperation{add("*.rec.example", "A", "192.0.2.3", 300)},
			want: []string{"*.rec.example A 300 192.0.2.3"}, wantSerial: 2,
		},
		{
			name:       "ttl above range",
			ops:        []ocidnssdk.RecordOperation{add("a.rec.example", "A", "192.0.2.1", 2147483648)},
			wantStatus: http.StatusBadRequest,
			wantCode:   "InvalidParameter",
			wantSerial: 1,
		},
		{
			name: "negative ttl", ops: []ocidnssdk.RecordOperation{add("a.rec.example", "A", "192.0.2.1", -5)},
			wantStatus: http.StatusBadRequest, wantCode: "InvalidParameter", wantSerial: 1,
		},
		{
			name: "missing ttl", ops: []ocidnssdk.RecordOperation{add("a.rec.example", "A", "192.0.2.1", 0)},
			wantStatus: http.StatusBadRequest, wantCode: "MissingParameter", wantSerial: 1,
		},
		{
			name: "unsupported type", ops: []ocidnssdk.RecordOperation{add("a.rec.example", "RRSIG", "x", 300)},
			wantStatus: http.StatusBadRequest, wantCode: "InvalidParameter", wantSerial: 1,
		},
		{
			name: "missing operation", ops: []ocidnssdk.RecordOperation{{Domain: new("a.rec.example")}},
			wantStatus: http.StatusBadRequest, wantCode: "MissingParameter", wantSerial: 1,
		},
		{
			name:       "cname beside existing a",
			setup:      []ocidnssdk.RecordOperation{add("a.rec.example", "A", "192.0.2.1", 300)},
			ops:        []ocidnssdk.RecordOperation{add("a.rec.example", "CNAME", "x.rec.example.", 300)},
			want:       []string{"a.rec.example A 300 192.0.2.1"},
			wantStatus: http.StatusBadRequest,
			wantCode:   "InvalidParameter",
			wantSerial: 2,
		},
		{
			name:  "a beside existing cname",
			setup: []ocidnssdk.RecordOperation{add("a.rec.example", "CNAME", "x.rec.example.", 300)},
			ops:   []ocidnssdk.RecordOperation{add("a.rec.example", "A", "192.0.2.1", 300)},
			want:  []string{"a.rec.example CNAME 300 x.rec.example."}, wantStatus: http.StatusBadRequest,
			wantCode: "InvalidParameter", wantSerial: 2,
		},
		{
			name: "cname and a in one request", ops: []ocidnssdk.RecordOperation{
				add("a.rec.example", "A", "192.0.2.1", 300), add("a.rec.example", "CNAME", "x.rec.example.", 300),
			},
			wantStatus: http.StatusBadRequest, wantCode: "InvalidParameter", wantSerial: 1,
		},
		{
			name:  "cname replaces a when a is removed first",
			setup: []ocidnssdk.RecordOperation{add("a.rec.example", "A", "192.0.2.1", 300)},
			ops: []ocidnssdk.RecordOperation{
				remove("a.rec.example", "A", ""), add("a.rec.example", "CNAME", "x.rec.example.", 300),
			},
			want:       []string{"a.rec.example CNAME 300 x.rec.example."},
			wantSerial: 3,
		},
		{
			name: "two cname targets", ops: []ocidnssdk.RecordOperation{
				add(
					"a.rec.example",
					"CNAME",
					"x.rec.example.",
					300,
				),
				add("a.rec.example", "CNAME", "y.rec.example.", 300),
			},
			wantStatus: http.StatusBadRequest, wantCode: "InvalidParameter", wantSerial: 1,
		},
		{
			name: "cname at apex", ops: []ocidnssdk.RecordOperation{add("rec.example", "CNAME", "x.rec.example.", 300)},
			wantStatus: http.StatusBadRequest, wantCode: "InvalidParameter", wantSerial: 1,
		},
		{
			name: "alias beside a", ops: []ocidnssdk.RecordOperation{
				add("rec.example", "ALIAS", "x.rec.example.", 300), add("rec.example", "A", "192.0.2.1", 300),
			},
			wantStatus: http.StatusBadRequest, wantCode: "InvalidParameter", wantSerial: 1,
		},
		{
			name:       "alias at apex ok",
			ops:        []ocidnssdk.RecordOperation{add("rec.example", "ALIAS", "x.rec.example", 300)},
			want:       []string{"rec.example ALIAS 300 x.rec.example."},
			wantSerial: 2,
		},
		{
			name: "add soa", ops: []ocidnssdk.RecordOperation{add("rec.example", "SOA", "ns. h. 1 2 3 4 5", 300)},
			wantStatus: http.StatusBadRequest, wantCode: "InvalidParameter", wantSerial: 1,
		},
		{
			name:       "add foreign apex ns",
			ops:        []ocidnssdk.RecordOperation{add("rec.example", "NS", "ns.evil.example.", 300)},
			wantStatus: http.StatusBadRequest,
			wantCode:   "InvalidParameter",
			wantSerial: 1,
		},
		{
			name: "remove apex ns", ops: []ocidnssdk.RecordOperation{remove("rec.example", "NS", "")},
			wantStatus: http.StatusBadRequest, wantCode: "InvalidParameter", wantSerial: 1,
		},
		{
			name:       "delegation ns below apex",
			ops:        []ocidnssdk.RecordOperation{add("sub.rec.example", "NS", "ns.sub.rec.example.", 300)},
			want:       []string{"sub.rec.example NS 300 ns.sub.rec.example."},
			wantSerial: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newZoneEnv(t)
			if len(tt.setup) > 0 {
				e.mustPatch(t, zoneName, tt.setup...)
			}

			resp, err := e.patch(t, zoneName, tt.ops...)
			if tt.wantCode != "" {
				se := svcErr(t, err)
				assert.Equal(t, tt.wantStatus, se.GetHTTPStatusCode())
				assert.Equal(t, tt.wantCode, se.GetCode())
			} else {
				require.NoError(t, err)
				assert.NotEmpty(t, *resp.ETag)
				assert.ElementsMatch(t, tt.want, userRecords(resp.Items))
			}

			assert.ElementsMatch(t, tt.want, userRecords(e.records(t, zoneName)))
			assert.Equal(t, tt.wantSerial, *e.getZone(t, zoneName).Serial)
		})
	}
}

func TestRecordMetadata(t *testing.T) {
	t.Parallel()

	e := newZoneEnv(t)
	e.mustPatch(t, zoneName, add("a.rec.example", "A", "192.0.2.1", 300))
	e.mustPatch(t, zoneName, add("b.rec.example", "A", "192.0.2.2", 300))

	byDomain := map[string]ocidnssdk.Record{}
	for _, r := range e.records(t, zoneName) {
		byDomain[*r.Domain+" "+*r.Rtype] = r
	}

	a, b := byDomain["a.rec.example A"], byDomain["b.rec.example A"]
	assert.Equal(t, "2", *a.RrsetVersion)
	assert.Equal(t, "3", *b.RrsetVersion)
	assert.NotEqual(t, *a.RecordHash, *b.RecordHash)
	assert.False(t, *a.IsProtected)
	assert.Equal(t, "3", *byDomain["rec.example SOA"].RrsetVersion)

	e.mustPatch(t, zoneName, op(ocidnssdk.RecordOperationOperationRequire, "", "", "", 0))

	resp, err := e.patch(t, zoneName, ocidnssdk.RecordOperation{
		Operation: ocidnssdk.RecordOperationOperationRequire, RecordHash: a.RecordHash, RrsetVersion: new("2"),
	})
	require.NoError(t, err)
	assert.NotEmpty(t, resp.Items)

	_, err = e.patch(t, zoneName, ocidnssdk.RecordOperation{
		Operation: ocidnssdk.RecordOperationOperationRequire, RecordHash: a.RecordHash, RrsetVersion: new("99"),
	})
	assert.Equal(t, http.StatusPreconditionFailed, svcErr(t, err).GetHTTPStatusCode())

	_, err = e.patch(t, zoneName, ocidnssdk.RecordOperation{
		Operation: ocidnssdk.RecordOperationOperationRemove, RecordHash: a.RecordHash,
	})
	require.NoError(t, err)
	assert.Equal(t, []string{"b.rec.example A 300 192.0.2.2"}, userRecords(e.records(t, zoneName)))
}

func TestRecordConditionals(t *testing.T) {
	t.Parallel()

	e := newZoneEnv(t)
	stale := *e.getZone(t, zoneName).ETag

	n := 0
	patchWith := func(ifMatch, unmod string) error {
		n++

		r := ocidnssdk.PatchZoneRecordsRequest{
			ZoneNameOrId: new(zoneName),
			Items: []ocidnssdk.RecordOperation{
				add("c"+strconv.Itoa(n)+".rec.example", "A", "192.0.2.4", 60),
			},
		}
		if ifMatch != "" {
			r.IfMatch = new(ifMatch)
		}

		if unmod != "" {
			r.IfUnmodifiedSince = new(unmod)
		}

		_, err := e.client.PatchZoneRecords(t.Context(), r)

		return err
	}

	e.clock.Advance(10 * time.Second)
	require.NoError(t, patchWith("", ""))

	se := svcErr(t, patchWith(stale, ""))
	assert.Equal(t, http.StatusPreconditionFailed, se.GetHTTPStatusCode())
	assert.Equal(t, "NoEtagMatch", se.GetCode())

	fresh := *e.getZone(t, zoneName).ETag
	e.clock.Advance(10 * time.Second)
	require.NoError(t, patchWith(fresh, ""))
	assert.Equal(t, http.StatusPreconditionFailed, svcErr(t, patchWith(fresh, "")).GetHTTPStatusCode(), "etag moved on")

	old := e.clock.Now().Add(-time.Hour).Format(http.TimeFormat)
	assert.Equal(t, http.StatusPreconditionFailed, svcErr(t, patchWith("", old)).GetHTTPStatusCode())
	require.NoError(t, patchWith("", e.clock.Now().Add(time.Hour).Format(http.TimeFormat)))

	// Only the three requests whose preconditions held changed the zone.
	assert.Len(t, userRecords(e.records(t, zoneName)), 3)
}

func TestUpdateRecordsReplace(t *testing.T) {
	t.Parallel()

	e := newZoneEnv(t)
	e.mustPatch(t, zoneName, add("old.rec.example", "A", "192.0.2.1", 300), add("keep.rec.example", "TXT", `"k"`, 300))

	rd := func(domain, rtype, rdata string, ttl int) ocidnssdk.RecordDetails {
		return ocidnssdk.RecordDetails{Domain: new(domain), Rtype: new(rtype), Rdata: new(rdata), Ttl: new(ttl)}
	}

	// Replacing the zone keeps the protected records even when the body repeats or omits them.
	all := e.records(t, zoneName)
	body := []ocidnssdk.RecordDetails{rd("new.rec.example", "A", "192.0.2.7", 60)}

	for _, r := range all {
		if *r.IsProtected {
			body = append(
				body,
				ocidnssdk.RecordDetails{
					Domain:      r.Domain,
					Rtype:       r.Rtype,
					Rdata:       r.Rdata,
					Ttl:         r.Ttl,
					IsProtected: r.IsProtected,
				},
			)
		}
	}

	resp, err := e.client.UpdateZoneRecords(t.Context(), ocidnssdk.UpdateZoneRecordsRequest{
		ZoneNameOrId: new(zoneName), Items: body,
	})
	require.NoError(t, err)
	assert.Equal(t, []string{"new.rec.example A 60 192.0.2.7"}, userRecords(resp.Items))
	assert.Len(t, resp.Items, 6)
	assert.Equal(t, int64(3), *e.getZone(t, zoneName).Serial)

	// An identical replacement is not a change.
	_, err = e.client.UpdateZoneRecords(t.Context(), ocidnssdk.UpdateZoneRecordsRequest{
		ZoneNameOrId: new(zoneName), Items: body,
	})
	require.NoError(t, err)
	assert.Equal(t, int64(3), *e.getZone(t, zoneName).Serial)

	tests := []struct {
		name  string
		code  string
		items []ocidnssdk.RecordDetails
	}{
		{
			name:  "foreign apex ns",
			items: []ocidnssdk.RecordDetails{rd(zoneName, "NS", "ns.evil.example.", 300)},
			code:  "InvalidParameter",
		},
		{
			name:  "out of zone",
			items: []ocidnssdk.RecordDetails{rd("a.elsewhere.test", "A", "192.0.2.1", 300)},
			code:  "InvalidParameter",
		},
		{
			name: "mixed ttl in a set",
			items: []ocidnssdk.RecordDetails{
				rd("a.rec.example", "A", "192.0.2.1", 300),
				rd("a.rec.example", "A", "192.0.2.2", 60),
			},
			code: "InvalidParameter",
		},
		{
			name: "cname exclusivity",
			items: []ocidnssdk.RecordDetails{
				rd("a.rec.example", "A", "192.0.2.1", 300),
				rd("a.rec.example", "CNAME", "x.rec.example.", 300),
			},
			code: "InvalidParameter",
		},
		{
			name:  "missing rdata",
			items: []ocidnssdk.RecordDetails{{Domain: new("a.rec.example"), Rtype: new("A"), Ttl: new(1)}},
			code:  "MissingParameter",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, uerr := e.client.UpdateZoneRecords(t.Context(), ocidnssdk.UpdateZoneRecordsRequest{
				ZoneNameOrId: new(zoneName), Items: tt.items,
			})
			assert.Equal(t, tt.code, svcErr(t, uerr).GetCode())
			assert.Equal(t, []string{"new.rec.example A 60 192.0.2.7"}, userRecords(e.records(t, zoneName)))
		})
	}
}

func TestDomainAndRRSetOperations(t *testing.T) {
	t.Parallel()

	e := newZoneEnv(t)
	e.mustPatch(t, zoneName,
		add("a.rec.example", "A", "192.0.2.1", 300), add("a.rec.example", "A", "192.0.2.2", 300),
		add("a.rec.example", "TXT", `"hello"`, 120), add("b.rec.example", "A", "192.0.2.3", 300),
	)

	domain, err := e.client.GetDomainRecords(t.Context(), ocidnssdk.GetDomainRecordsRequest{
		ZoneNameOrId: new(zoneName), Domain: new("A.rec.example."),
	})
	require.NoError(t, err)
	assert.Len(t, domain.Items, 3)

	rrset, err := e.client.GetRRSet(t.Context(), ocidnssdk.GetRRSetRequest{
		ZoneNameOrId: new(zoneName), Domain: new("a.rec.example"), Rtype: new("a"),
	})
	require.NoError(t, err)
	assert.ElementsMatch(
		t,
		[]string{"a.rec.example A 300 192.0.2.1", "a.rec.example A 300 192.0.2.2"},
		userRecords(rrset.Items),
	)
	assert.NotEmpty(t, *rrset.ETag)

	_, err = e.client.GetRRSet(t.Context(), ocidnssdk.GetRRSetRequest{
		ZoneNameOrId: new(zoneName), Domain: new("a.rec.example"), Rtype: new("MX"),
	})
	assert.Equal(t, http.StatusNotFound, svcErr(t, err).GetHTTPStatusCode())

	_, err = e.client.GetDomainRecords(t.Context(), ocidnssdk.GetDomainRecordsRequest{
		ZoneNameOrId: new(zoneName), Domain: new("none.rec.example"),
	})
	assert.Equal(t, http.StatusNotFound, svcErr(t, err).GetHTTPStatusCode())

	// PatchRRSet fills domain and rtype from the path.
	patched, err := e.client.PatchRRSet(t.Context(), ocidnssdk.PatchRRSetRequest{
		ZoneNameOrId: new(zoneName), Domain: new("a.rec.example"), Rtype: new("A"),
		Items: []ocidnssdk.RecordOperation{
			{Operation: ocidnssdk.RecordOperationOperationAdd, Rdata: new("192.0.2.9"), Ttl: new(60)},
			{Operation: ocidnssdk.RecordOperationOperationRemove, Rdata: new("192.0.2.1")},
		},
	})
	require.NoError(t, err)
	assert.ElementsMatch(
		t,
		[]string{"a.rec.example A 60 192.0.2.2", "a.rec.example A 60 192.0.2.9"},
		userRecords(patched.Items),
	)

	_, err = e.client.PatchRRSet(t.Context(), ocidnssdk.PatchRRSetRequest{
		ZoneNameOrId: new(zoneName), Domain: new("a.rec.example"), Rtype: new("A"),
		Items: []ocidnssdk.RecordOperation{add("a.rec.example", "TXT", `"x"`, 60)},
	})
	assert.Equal(t, "InvalidParameter", svcErr(t, err).GetCode(), "rtype differs from the path")

	patchedDomain, err := e.client.PatchDomainRecords(t.Context(), ocidnssdk.PatchDomainRecordsRequest{
		ZoneNameOrId: new(zoneName), Domain: new("a.rec.example"),
		Items: []ocidnssdk.RecordOperation{
			{
				Operation: ocidnssdk.RecordOperationOperationAdd,
				Rtype:     new("MX"),
				Rdata:     new("5 m.rec.example."),
				Ttl:       new(90),
			},
		},
	})
	require.NoError(t, err)
	assert.Len(t, patchedDomain.Items, 4)

	_, err = e.client.PatchDomainRecords(t.Context(), ocidnssdk.PatchDomainRecordsRequest{
		ZoneNameOrId: new(zoneName), Domain: new("a.rec.example"),
		Items: []ocidnssdk.RecordOperation{add("b.rec.example", "A", "192.0.2.1", 60)},
	})
	assert.Equal(t, "InvalidParameter", svcErr(t, err).GetCode(), "domain differs from the path")

	// UpdateRRSet replaces one set; an empty body removes it.
	replaced, err := e.client.UpdateRRSet(t.Context(), ocidnssdk.UpdateRRSetRequest{
		ZoneNameOrId: new(zoneName), Domain: new("a.rec.example"), Rtype: new("A"),
		Items: []ocidnssdk.RecordDetails{
			{Rdata: new("198.51.100.1"), Ttl: new(45)},
		},
	})
	require.NoError(t, err)
	assert.Equal(t, []string{"a.rec.example A 45 198.51.100.1"}, userRecords(replaced.Items))

	// UpdateDomainRecords replaces everything at the domain.
	domainUpd, err := e.client.UpdateDomainRecords(t.Context(), ocidnssdk.UpdateDomainRecordsRequest{
		ZoneNameOrId: new(zoneName), Domain: new("a.rec.example"),
		Items: []ocidnssdk.RecordDetails{
			{Rtype: new("TXT"), Rdata: new(`"only"`), Ttl: new(30)},
		},
	})
	require.NoError(t, err)
	assert.Equal(t, []string{`a.rec.example TXT 30 "only"`}, userRecords(domainUpd.Items))

	_, err = e.client.DeleteRRSet(t.Context(), ocidnssdk.DeleteRRSetRequest{
		ZoneNameOrId: new(zoneName), Domain: new("a.rec.example"), Rtype: new("TXT"),
	})
	require.NoError(t, err)

	_, err = e.client.DeleteRRSet(t.Context(), ocidnssdk.DeleteRRSetRequest{
		ZoneNameOrId: new(zoneName), Domain: new("a.rec.example"), Rtype: new("TXT"),
	})
	assert.Equal(t, http.StatusNotFound, svcErr(t, err).GetHTTPStatusCode())

	_, err = e.client.DeleteDomainRecords(t.Context(), ocidnssdk.DeleteDomainRecordsRequest{
		ZoneNameOrId: new(zoneName), Domain: new("b.rec.example"),
	})
	require.NoError(t, err)
	assert.Empty(t, userRecords(e.records(t, zoneName)))

	for _, rtype := range []string{"SOA", "NS"} {
		_, err = e.client.DeleteRRSet(t.Context(), ocidnssdk.DeleteRRSetRequest{
			ZoneNameOrId: new(zoneName), Domain: new(zoneName), Rtype: new(rtype),
		})
		assert.Equal(t, "InvalidParameter", svcErr(t, err).GetCode(), rtype)
	}

	_, err = e.client.DeleteDomainRecords(t.Context(), ocidnssdk.DeleteDomainRecordsRequest{
		ZoneNameOrId: new(zoneName), Domain: new(zoneName),
	})
	assert.Equal(
		t,
		http.StatusNotFound,
		svcErr(t, err).GetHTTPStatusCode(),
		"only protected records remain at the apex",
	)
}

func TestGetZoneRecordsListing(t *testing.T) {
	t.Parallel()

	e := newZoneEnv(t)
	e.mustPatch(t, zoneName,
		add("c.rec.example", "A", "192.0.2.3", 300), add("a.rec.example", "A", "192.0.2.1", 100),
		add("b.rec.example", "TXT", `"b"`, 200), add("a2.rec.example", "A", "192.0.2.2", 400),
	)

	list := func(mutate func(*ocidnssdk.GetZoneRecordsRequest)) ocidnssdk.GetZoneRecordsResponse {
		r := ocidnssdk.GetZoneRecordsRequest{ZoneNameOrId: new(zoneName)}
		mutate(&r)

		resp, err := e.client.GetZoneRecords(t.Context(), r)
		require.NoError(t, err)

		return resp
	}

	domains := func(resp ocidnssdk.GetZoneRecordsResponse) []string {
		var out []string
		for _, r := range resp.Items {
			if !*r.IsProtected {
				out = append(out, *r.Domain)
			}
		}

		return out
	}

	assert.Equal(
		t,
		[]string{"a.rec.example", "a2.rec.example", "b.rec.example", "c.rec.example"},
		domains(list(func(*ocidnssdk.GetZoneRecordsRequest) {})),
	)
	assert.Equal(
		t,
		[]string{"a2.rec.example"},
		domains(list(func(r *ocidnssdk.GetZoneRecordsRequest) { r.DomainContains = new("A2") })),
	)
	assert.Equal(
		t,
		[]string{"b.rec.example"},
		domains(list(func(r *ocidnssdk.GetZoneRecordsRequest) { r.Rtype = new("txt") })),
	)
	assert.Equal(
		t,
		[]string{"c.rec.example"},
		domains(list(func(r *ocidnssdk.GetZoneRecordsRequest) { r.Domain = new("C.rec.example") })),
	)
	asc := list(func(r *ocidnssdk.GetZoneRecordsRequest) {
		r.SortBy, r.SortOrder = ocidnssdk.GetZoneRecordsSortByTtl, ocidnssdk.GetZoneRecordsSortOrderAsc
	})
	assert.Equal(t, 100, *asc.Items[0].Ttl, "lowest ttl first")

	desc := list(func(r *ocidnssdk.GetZoneRecordsRequest) {
		r.SortBy, r.SortOrder = ocidnssdk.GetZoneRecordsSortByTtl, ocidnssdk.GetZoneRecordsSortOrderDesc
	})
	assert.Equal(t, 86400, *desc.Items[0].Ttl, "the apex NS set has the highest ttl")

	first := list(func(r *ocidnssdk.GetZoneRecordsRequest) { r.Limit = common.Int64(3) })
	require.Len(t, first.Items, 3)
	assert.Equal(t, 9, *first.OpcTotalItems)
	require.NotNil(t, first.OpcNextPage)

	var seen int

	for page := first; ; {
		seen += len(page.Items)
		if page.OpcNextPage == nil {
			break
		}

		next := *page.OpcNextPage
		page = list(func(r *ocidnssdk.GetZoneRecordsRequest) { r.Limit = common.Int64(3); r.Page = &next })
	}

	assert.Equal(t, 9, seen)

	_, err := e.client.GetZoneRecords(
		t.Context(),
		ocidnssdk.GetZoneRecordsRequest{ZoneNameOrId: new(zoneName), ZoneVersion: new("1")},
	)
	assert.Equal(t, http.StatusNotFound, svcErr(t, err).GetHTTPStatusCode(), "old versions are not retained")

	current := list(func(r *ocidnssdk.GetZoneRecordsRequest) { r.ZoneVersion = new("2") })
	assert.Len(t, current.Items, 9)

	_, err = e.client.GetZoneRecords(
		t.Context(),
		ocidnssdk.GetZoneRecordsRequest{ZoneNameOrId: new(zoneName), Domain: new("bad name")},
	)
	assert.Equal(t, "InvalidParameter", svcErr(t, err).GetCode())

	status, body, _ := rawGet(t, e.base+"/20180115/zones/"+zoneName+"/records?sortBy=bogus", nil)
	assert.Equal(t, http.StatusBadRequest, status)
	assert.Contains(t, body, "InvalidParameter")
}

func TestZoneRecordsConditionalGet(t *testing.T) {
	t.Parallel()

	e := newZoneEnv(t)
	etag := *e.getZone(t, zoneName).ETag
	url := e.base + "/20180115/zones/" + zoneName + "/records"

	status, _, hdr := rawGet(t, url, map[string]string{"If-None-Match": etag})
	assert.Equal(t, http.StatusNotModified, status)
	assert.Equal(t, etag, hdr.Get("ETag"))

	e.mustPatch(t, zoneName, add("n.rec.example", "A", "192.0.2.1", 60))

	status, _, _ = rawGet(t, url, map[string]string{"If-None-Match": etag})
	assert.Equal(t, http.StatusOK, status, "records changed, so the etag moved")
}

func TestPatchDomainRdataNeedsRtype(t *testing.T) {
	t.Parallel()

	req := ocidnssdk.RecordOperationOperationRequire

	tests := []struct {
		name string
		op   ocidnssdk.RecordOperation
	}{
		{name: "remove", op: ocidnssdk.RecordOperation{
			Operation: ocidnssdk.RecordOperationOperationRemove, Rdata: new("192.0.2.1"),
		}},
		{name: "require", op: ocidnssdk.RecordOperation{Operation: req, Rdata: new("192.0.2.1")}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newZoneEnv(t)
			e.mustPatch(
				t,
				zoneName,
				add("a.rec.example", "A", "192.0.2.1", 300),
				add("a.rec.example", "TXT", `"t"`, 60),
			)

			_, err := e.client.PatchDomainRecords(t.Context(), ocidnssdk.PatchDomainRecordsRequest{
				ZoneNameOrId: new(zoneName), Domain: new("a.rec.example"), Items: []ocidnssdk.RecordOperation{tt.op},
			})
			assert.Equal(t, http.StatusBadRequest, svcErr(t, err).GetHTTPStatusCode())

			assert.ElementsMatch(
				t,
				[]string{
					"a.rec.example A 300 192.0.2.1",
					`a.rec.example TXT 60 "t"`,
				},
				userRecords(e.records(t, zoneName)),
			)
		})
	}
}

package ocidns_test

import (
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/oracle/oci-go-sdk/v65/common"
	ocidnssdk "github.com/oracle/oci-go-sdk/v65/dns"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestZoneCreateShape(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{register: true})
	resp, err := e.client.CreateZone(
		t.Context(),
		ocidnssdk.CreateZoneRequest{CreateZoneDetails: ocidnssdk.CreateZoneDetails{
			Name: new("Example.COM."), CompartmentId: new(testCompartment),
			FreeformTags: map[string]string{"k": "v"},
		}},
	)
	require.NoError(t, err)

	z := resp.Zone
	assert.Equal(t, "example.com", *z.Name)
	assert.Equal(t, ocidnssdk.ZoneZoneTypePrimary, z.ZoneType)
	assert.Equal(t, ocidnssdk.ScopeGlobal, z.Scope)
	assert.Equal(t, ocidnssdk.ZoneResolutionModeStatic, z.ResolutionMode)
	assert.Equal(t, ocidnssdk.ZoneDnssecStateDisabled, z.DnssecState)
	assert.Equal(t, ocidnssdk.ZoneLifecycleStateActive, z.LifecycleState)
	assert.Equal(t, int64(1), *z.Serial)
	assert.Equal(t, "1", *z.Version)
	assert.False(t, *z.IsProtected)
	assert.Contains(t, *z.Id, "ocid1.dns-zone.oc1..")
	assert.Equal(t, map[string]string{"k": "v"}, z.FreeformTags)
	assert.Len(t, z.Nameservers, 4)
	assert.Empty(t, z.ExternalMasters)
	assert.NotEmpty(t, *resp.ETag)
	assert.NotEmpty(t, *resp.Location)
	assert.NotEmpty(t, *resp.OpcRequestId)
	assert.Contains(t, *z.Self, "/zones/"+*z.Id)

	recs := e.records(t, "example.com")
	require.Len(t, recs, 5)

	for _, r := range recs {
		assert.True(t, *r.IsProtected)
		assert.NotEmpty(t, *r.RecordHash)
	}

	var soa string

	for _, r := range recs {
		if *r.Rtype == "SOA" {
			soa = *r.Rdata
		}
	}

	assert.Contains(t, soa, "hostmaster.example.com. 1 ")
}

func TestZoneCreateValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		mutate   func(*ocidnssdk.CreateZoneDetails)
		name     string
		wantCode string
		status   int
	}{
		{name: "bad name", mutate: func(d *ocidnssdk.CreateZoneDetails) { d.Name = new("bad name!") },
			status: http.StatusBadRequest, wantCode: "InvalidParameter"},
		{name: "empty label", mutate: func(d *ocidnssdk.CreateZoneDetails) { d.Name = new("a..b") },
			status: http.StatusBadRequest, wantCode: "InvalidParameter"},
		{name: "long label", mutate: func(d *ocidnssdk.CreateZoneDetails) {
			d.Name = new(strings.Repeat("a", 64) + ".com")
		}, status: http.StatusBadRequest, wantCode: "InvalidParameter"},
		{name: "unknown compartment", mutate: func(d *ocidnssdk.CreateZoneDetails) {
			d.CompartmentId = new("ocid1.compartment.oc1..nope")
		}, status: http.StatusNotFound, wantCode: "NotAuthorizedOrNotFound"},
		{name: "secondary without masters", mutate: func(d *ocidnssdk.CreateZoneDetails) {
			d.ZoneType = ocidnssdk.CreateZoneDetailsZoneTypeSecondary
		}, status: http.StatusBadRequest, wantCode: "MissingParameter"},
		{name: "secondary with downstreams", mutate: func(d *ocidnssdk.CreateZoneDetails) {
			d.ZoneType = ocidnssdk.CreateZoneDetailsZoneTypeSecondary
			d.ExternalMasters = []ocidnssdk.ExternalMaster{{Address: new("192.0.2.1")}}
			d.ExternalDownstreams = []ocidnssdk.ExternalDownstream{{Address: new("192.0.2.2")}}
		}, status: http.StatusBadRequest, wantCode: "InvalidParameter"},
		{name: "primary with masters", mutate: func(d *ocidnssdk.CreateZoneDetails) {
			d.ExternalMasters = []ocidnssdk.ExternalMaster{{Address: new("192.0.2.1")}}
		}, status: http.StatusBadRequest, wantCode: "InvalidParameter"},
		{name: "bad master address", mutate: func(d *ocidnssdk.CreateZoneDetails) {
			d.ZoneType = ocidnssdk.CreateZoneDetailsZoneTypeSecondary
			d.ExternalMasters = []ocidnssdk.ExternalMaster{{Address: new("not-an-ip")}}
		}, status: http.StatusBadRequest, wantCode: "InvalidParameter"},
		{name: "unknown tsig key", mutate: func(d *ocidnssdk.CreateZoneDetails) {
			d.ZoneType = ocidnssdk.CreateZoneDetailsZoneTypeSecondary
			d.ExternalMasters = []ocidnssdk.ExternalMaster{
				{Address: new("192.0.2.1"), TsigKeyId: new("ocid1.tsigkey.oc1..x")},
			}
		}, status: http.StatusNotFound, wantCode: "NotAuthorizedOrNotFound"},
		{
			name:     "private without view",
			mutate:   func(d *ocidnssdk.CreateZoneDetails) { d.Scope = ocidnssdk.ScopePrivate },
			status:   http.StatusBadRequest,
			wantCode: "MissingParameter",
		},
		{name: "private unknown view", mutate: func(d *ocidnssdk.CreateZoneDetails) {
			d.Scope = ocidnssdk.ScopePrivate
			d.ViewId = new("ocid1.dnsview.oc1..nope")
		}, status: http.StatusNotFound, wantCode: "NotAuthorizedOrNotFound"},
		{name: "global with view", mutate: func(d *ocidnssdk.CreateZoneDetails) { d.ViewId = new("x") },
			status: http.StatusNotFound, wantCode: "NotAuthorizedOrNotFound"},
		{name: "global transparent", mutate: func(d *ocidnssdk.CreateZoneDetails) {
			d.ResolutionMode = ocidnssdk.ZoneResolutionModeTransparent
		}, status: http.StatusBadRequest, wantCode: "InvalidParameter"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, envOpts{})
			_, err := e.client.CreateZone(
				t.Context(),
				ocidnssdk.CreateZoneRequest{CreateZoneDetails: ocidnssdk.CreateZoneDetails{
					Name: new("example.com"), CompartmentId: new(testCompartment),
				}},
			)
			require.NoError(t, err)

			d := ocidnssdk.CreateZoneDetails{Name: new("other.example.org"), CompartmentId: new(testCompartment)}
			tt.mutate(&d)

			_, err = e.client.CreateZone(t.Context(), ocidnssdk.CreateZoneRequest{CreateZoneDetails: d})
			se := svcErr(t, err)
			assert.Equal(t, tt.status, se.GetHTTPStatusCode())
			assert.Equal(t, tt.wantCode, se.GetCode())
		})
	}
}

func TestZoneDuplicateName(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	e.createZone(t, "dup.example", nil)

	_, err := e.client.CreateZone(
		t.Context(),
		ocidnssdk.CreateZoneRequest{CreateZoneDetails: ocidnssdk.CreateZoneDetails{
			Name: new("DUP.example."), CompartmentId: new(otherCompart),
		}},
	)
	se := svcErr(t, err)
	assert.Equal(t, http.StatusConflict, se.GetHTTPStatusCode())
	assert.Equal(t, "Conflict", se.GetCode())
}

func TestZoneGetByNameAndID(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	z := e.createZone(t, "get.example", nil)

	byName := e.getZone(t, "get.example")
	byID := e.getZone(t, *z.Id)
	assert.Equal(t, *z.Id, *byName.Id)
	assert.Equal(t, *byName.ETag, *byID.ETag)

	for _, ref := range []string{"missing.example", "ocid1.dns-zone.oc1..nope"} {
		_, err := e.client.GetZone(t.Context(), ocidnssdk.GetZoneRequest{ZoneNameOrId: new(ref)})
		assert.Equal(t, http.StatusNotFound, svcErr(t, err).GetHTTPStatusCode(), ref)
	}

	_, err := e.client.GetZone(t.Context(), ocidnssdk.GetZoneRequest{
		ZoneNameOrId: new("get.example"), Scope: ocidnssdk.GetZoneScopePrivate,
	})
	assert.Equal(t, "MissingParameter", svcErr(t, err).GetCode())
}

func TestZoneListFilters(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	e.createZone(t, "a.example", nil)
	e.clock.Advance(1e9)
	e.createZone(t, "b.example", func(d *ocidnssdk.CreateZoneDetails) { d.CompartmentId = new(otherCompart) })
	e.clock.Advance(1e9)
	e.createZone(t, "c.test", nil)
	e.clock.Advance(1e9)

	e.createZone(t, "d.example", func(d *ocidnssdk.CreateZoneDetails) {
		d.ZoneType = ocidnssdk.CreateZoneDetailsZoneTypeSecondary
		d.ExternalMasters = []ocidnssdk.ExternalMaster{{Address: new("192.0.2.9")}}
	})

	names := func(r ocidnssdk.ListZonesRequest) []string {
		resp, err := e.client.ListZones(t.Context(), r)
		require.NoError(t, err)

		out := make([]string, 0, len(resp.Items))
		for _, z := range resp.Items {
			out = append(out, *z.Name)
		}

		return out
	}

	comp := new(testCompartment)

	tests := []struct {
		req  ocidnssdk.ListZonesRequest
		name string
		want []string
	}{
		{
			name: "default newest first",
			req:  ocidnssdk.ListZonesRequest{CompartmentId: comp},
			want: []string{"d.example", "c.test", "a.example"},
		},
		{
			name: "name asc",
			req: ocidnssdk.ListZonesRequest{
				CompartmentId: comp,
				SortBy:        ocidnssdk.ListZonesSortByName,
				SortOrder:     ocidnssdk.ListZonesSortOrderAsc,
			},
			want: []string{"a.example", "c.test", "d.example"},
		},
		{
			name: "exact name",
			req:  ocidnssdk.ListZonesRequest{CompartmentId: comp, Name: new("C.TEST")},
			want: []string{"c.test"},
		},
		{
			name: "contains",
			req:  ocidnssdk.ListZonesRequest{CompartmentId: comp, NameContains: new("EXAM")},
			want: []string{"d.example", "a.example"},
		},
		{
			name: "secondary",
			req:  ocidnssdk.ListZonesRequest{CompartmentId: comp, ZoneType: ocidnssdk.ListZonesZoneTypeSecondary},
			want: []string{"d.example"},
		},
		{
			name: "other compartment",
			req:  ocidnssdk.ListZonesRequest{CompartmentId: new(otherCompart)},
			want: []string{"b.example"},
		},
		{
			name: "private scope empty",
			req:  ocidnssdk.ListZonesRequest{CompartmentId: comp, Scope: ocidnssdk.ListZonesScopePrivate},
			want: []string{},
		},
		{
			name: "dnssec enabled",
			req:  ocidnssdk.ListZonesRequest{CompartmentId: comp, DnssecState: ocidnssdk.ListZonesDnssecStateEnabled},
			want: []string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.want, names(tt.req))
		})
	}

	status, body, _ := rawGet(t, e.base+"/20180115/zones", nil)
	assert.Equal(t, http.StatusBadRequest, status)
	assert.Contains(t, body, "MissingParameter")

	pg1, err := e.client.ListZones(t.Context(), ocidnssdk.ListZonesRequest{CompartmentId: comp, Limit: common.Int64(2)})
	require.NoError(t, err)
	require.Len(t, pg1.Items, 2)
	assert.Equal(t, 3, *pg1.OpcTotalItems)
	require.NotNil(t, pg1.OpcNextPage)

	pg2, err := e.client.ListZones(
		t.Context(),
		ocidnssdk.ListZonesRequest{CompartmentId: comp, Limit: common.Int64(2), Page: pg1.OpcNextPage},
	)
	require.NoError(t, err)
	require.Len(t, pg2.Items, 1)
	assert.Nil(t, pg2.OpcNextPage)
	assert.Equal(t, "a.example", *pg2.Items[0].Name)

	_, err = e.client.ListZones(t.Context(), ocidnssdk.ListZonesRequest{CompartmentId: comp, Page: new("!!")})
	assert.Equal(t, "InvalidParameter", svcErr(t, err).GetCode())
}

func TestZoneUpdateDeleteChangeCompartment(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	z := e.createZone(t, "upd.example", nil)

	resp, err := e.client.UpdateZone(t.Context(), ocidnssdk.UpdateZoneRequest{
		ZoneNameOrId: z.Name,
		FreeformTags: map[string]string{"a": "b"},
	})
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"a": "b"}, resp.FreeformTags)

	assert.Equal(t, *e.getZone(t, "upd.example").ETag, *resp.ETag)

	_, err = e.client.UpdateZone(t.Context(), ocidnssdk.UpdateZoneRequest{
		ZoneNameOrId:    z.Name,
		ExternalMasters: []ocidnssdk.ExternalMaster{{Address: new("192.0.2.1")}},
	})
	assert.Equal(t, "InvalidParameter", svcErr(t, err).GetCode())

	_, err = e.client.UpdateZone(t.Context(), ocidnssdk.UpdateZoneRequest{
		ZoneNameOrId: z.Name, DnssecState: ocidnssdk.ZoneDnssecStateEnabled,
	})
	require.NoError(t, err)

	_, err = e.client.ChangeZoneCompartment(t.Context(), ocidnssdk.ChangeZoneCompartmentRequest{
		ZoneId: z.Id, CompartmentId: new(otherCompart),
	})
	require.NoError(t, err)
	assert.Equal(t, otherCompart, *e.getZone(t, "upd.example").CompartmentId)

	_, err = e.client.ChangeZoneCompartment(t.Context(), ocidnssdk.ChangeZoneCompartmentRequest{
		ZoneId: z.Id, CompartmentId: new("ocid1.compartment.oc1..nope"),
	})
	assert.Equal(t, http.StatusNotFound, svcErr(t, err).GetHTTPStatusCode())

	_, err = e.client.DeleteZone(t.Context(), ocidnssdk.DeleteZoneRequest{ZoneNameOrId: z.Name})
	require.NoError(t, err)

	_, err = e.client.GetZone(t.Context(), ocidnssdk.GetZoneRequest{ZoneNameOrId: z.Name})
	assert.Equal(t, http.StatusNotFound, svcErr(t, err).GetHTTPStatusCode())

	_, err = e.client.DeleteZone(t.Context(), ocidnssdk.DeleteZoneRequest{ZoneNameOrId: z.Name})
	assert.Equal(t, http.StatusNotFound, svcErr(t, err).GetHTTPStatusCode())
}

func TestZoneConditionalHeaders(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		ifMatch  string
		unmod    time.Duration
		wantCode int
	}{
		{name: "stale etag", ifMatch: "deadbeef", wantCode: http.StatusPreconditionFailed},
		{name: "star", ifMatch: "*", wantCode: http.StatusOK},
		{name: "list with match", ifMatch: `"x", "ETAG"`, wantCode: http.StatusOK},
		{name: "unmodified since before", unmod: -time.Hour, wantCode: http.StatusPreconditionFailed},
		{name: "unmodified since after", unmod: time.Hour, wantCode: http.StatusOK},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, envOpts{})
			z := e.createZone(t, "cond.example", nil)

			r := ocidnssdk.UpdateZoneRequest{ZoneNameOrId: z.Name, FreeformTags: map[string]string{"t": tt.name}}
			if tt.ifMatch != "" {
				r.IfMatch = new(strings.ReplaceAll(tt.ifMatch, "ETAG", *e.getZone(t, "cond.example").ETag))
			}

			if tt.unmod != 0 {
				r.IfUnmodifiedSince = new(e.clock.Now().Add(tt.unmod).Format(http.TimeFormat))
			}

			_, err := e.client.UpdateZone(t.Context(), r)
			if tt.wantCode == http.StatusOK {
				require.NoError(t, err)

				return
			}

			se := svcErr(t, err)
			assert.Equal(t, tt.wantCode, se.GetHTTPStatusCode())
			assert.Equal(t, "NoEtagMatch", se.GetCode())
		})
	}
}

func TestZoneConditionalGet(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	e.createZone(t, "get304.example", nil)
	etag := *e.getZone(t, "get304.example").ETag
	url := e.base + "/20180115/zones/get304.example"

	status, _, _ := rawGet(t, url, map[string]string{"If-None-Match": etag})
	assert.Equal(t, http.StatusNotModified, status)

	status, _, _ = rawGet(t, url, map[string]string{"If-None-Match": "other"})
	assert.Equal(t, http.StatusOK, status)

	since := func(d time.Duration) map[string]string {
		return map[string]string{"If-Modified-Since": e.clock.Now().Add(d).Format(http.TimeFormat)}
	}

	status, _, _ = rawGet(t, url, since(time.Hour))
	assert.Equal(t, http.StatusNotModified, status)

	status, _, _ = rawGet(t, url, since(-time.Hour))
	assert.Equal(t, http.StatusOK, status)
}

func TestZoneFileRoundTrip(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	e.createZone(t, "file.example", nil)
	e.mustPatch(t, "file.example",
		add("www.file.example", "A", "192.0.2.10", 300),
		add("file.example", "MX", "10 mail.file.example.", 600),
		add("file.example", "TXT", `"v=spf1 -all"`, 120),
	)

	resp, err := e.client.GetZoneContent(
		t.Context(),
		ocidnssdk.GetZoneContentRequest{ZoneNameOrId: new("file.example")},
	)
	require.NoError(t, err)

	buf := new(strings.Builder)
	_, err = io.Copy(buf, resp.Content)
	require.NoError(t, err)
	require.Contains(t, buf.String(), "$ORIGIN file.example.")
	require.Contains(t, buf.String(), "www.file.example.\t300\tIN\tA\t192.0.2.10")

	created, err := e.client.CreateZoneFromZoneFile(t.Context(), ocidnssdk.CreateZoneFromZoneFileRequest{
		CompartmentId:                 new(otherCompart),
		CreateZoneFromZoneFileDetails: readCloser(buf.String()),
		Scope:                         ocidnssdk.CreateZoneFromZoneFileScopeGlobal,
	})
	assert.Equal(t, http.StatusConflict, svcErr(t, err).GetHTTPStatusCode(), "same global name")
	_ = created

	_, err = e.client.DeleteZone(t.Context(), ocidnssdk.DeleteZoneRequest{ZoneNameOrId: new("file.example")})
	require.NoError(t, err)

	created, err = e.client.CreateZoneFromZoneFile(t.Context(), ocidnssdk.CreateZoneFromZoneFileRequest{
		CompartmentId:                 new(otherCompart),
		CreateZoneFromZoneFileDetails: readCloser(buf.String()),
	})
	require.NoError(t, err)
	assert.Equal(t, "file.example", *created.Name)
	assert.Equal(t, otherCompart, *created.CompartmentId)
	assert.ElementsMatch(t, []string{
		"www.file.example A 300 192.0.2.10", "file.example MX 600 10 mail.file.example.",
		`file.example TXT 120 "v=spf1 -all"`,
	}, userRecords(e.records(t, "file.example")))

	_, err = e.client.CreateZoneFromZoneFile(t.Context(), ocidnssdk.CreateZoneFromZoneFileRequest{
		CompartmentId: new(testCompartment), CreateZoneFromZoneFileDetails: readCloser("garbage ( unbalanced"),
	})
	assert.Equal(t, http.StatusBadRequest, svcErr(t, err).GetHTTPStatusCode())

	_, err = e.client.CreateZoneFromZoneFile(t.Context(), ocidnssdk.CreateZoneFromZoneFileRequest{
		CompartmentId: new(testCompartment), CreateZoneFromZoneFileDetails: readCloser("www 60 IN A 192.0.2.1\n"),
	})
	assert.Equal(t, http.StatusBadRequest, svcErr(t, err).GetHTTPStatusCode(), "no SOA")
}

func TestZoneTransferServers(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})

	resp, err := e.client.ListZoneTransferServers(
		t.Context(),
		ocidnssdk.ListZoneTransferServersRequest{CompartmentId: new(testCompartment)},
	)
	require.NoError(t, err)
	require.Len(t, resp.Items, 2)
	assert.True(t, *resp.Items[0].IsTransferSource || *resp.Items[1].IsTransferSource)

	_, err = e.client.ListZoneTransferServers(
		t.Context(),
		ocidnssdk.ListZoneTransferServersRequest{CompartmentId: new("ocid1.compartment.oc1..nope")},
	)
	assert.Equal(t, http.StatusNotFound, svcErr(t, err).GetHTTPStatusCode())

	z := e.createZone(t, "xfer.example", func(d *ocidnssdk.CreateZoneDetails) {
		d.ExternalDownstreams = []ocidnssdk.ExternalDownstream{{Address: new("192.0.2.77"), Port: new(5353)}}
	})
	assert.Len(t, z.ZoneTransferServers, 2)
	require.Len(t, z.ExternalDownstreams, 1)
	assert.Equal(t, 5353, *z.ExternalDownstreams[0].Port)
}

func TestSignedRequestsEnforced(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{enforce: true, register: true})
	e.createZone(t, "signed.example", nil)

	status, body, _ := rawGet(t, e.base+"/20180115/zones/signed.example", nil)
	assert.Equal(t, http.StatusUnauthorized, status)
	assert.Contains(t, body, "NotAuthenticated")
}

func TestUnknownRoute(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	status, body, hdr := rawGet(t, e.base+"/20180115/nothing", nil)
	assert.Equal(t, http.StatusNotFound, status)
	assert.Contains(t, body, `"code":"NotFound"`)
	assert.NotEmpty(t, hdr.Get("Opc-Request-Id"))
}

func TestZoneUpdateIsAtomic(t *testing.T) {
	t.Parallel()

	tests := []struct {
		update ocidnssdk.UpdateZoneRequest
		name   string
	}{
		{
			name: "valid dnssec then invalid masters",
			update: ocidnssdk.UpdateZoneRequest{
				DnssecState:     ocidnssdk.ZoneDnssecStateEnabled,
				ExternalMasters: []ocidnssdk.ExternalMaster{{Address: new("192.0.2.1")}},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, envOpts{})
			z := e.createZone(t, "atomic.example", nil)
			before := e.getZone(t, *z.Name).Zone

			tt.update.ZoneNameOrId = z.Name
			_, err := e.client.UpdateZone(t.Context(), tt.update)
			assert.Equal(t, "InvalidParameter", svcErr(t, err).GetCode())

			after := e.getZone(t, *z.Name).Zone
			assert.Equal(t, before.DnssecState, after.DnssecState)
			assert.Equal(t, before.ExternalMasters, after.ExternalMasters)
			assert.Equal(t, before.Version, after.Version)
		})
	}
}

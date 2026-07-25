package route53_test

// delegationset_linkage_test.go covers linking hosted zones to reusable
// delegation sets. Before this pass, CreateHostedZoneRequest's
// DelegationSetId field was parsed off the wire and then silently dropped —
// every zone got the same hardcoded default name servers regardless of what
// was requested, DeleteReusableDelegationSet never checked for zones still
// using the set, and CountZonesByReusableDelegationSet always returned 0.

import (
	"encoding/xml"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/route53"
)

// createDelegationSetViaHTTP creates a reusable delegation set and returns
// its bare ID (without the "/delegationset/" prefix) plus its name servers.
func createDelegationSetViaHTTP(t *testing.T, h *route53.Handler, callerRef string) (string, []string) {
	t.Helper()

	body := `<?xml version="1.0" encoding="UTF-8"?>
<CreateReusableDelegationSetRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/">
  <CallerReference>` + callerRef + `</CallerReference>
</CreateReusableDelegationSetRequest>`

	rec := send(t, h, http.MethodPost, "/2013-04-01/delegationset", body)
	require.Equal(t, http.StatusCreated, rec.Code)

	type dsResp struct {
		DelegationSet struct {
			ID          string   `xml:"Id"`
			NameServers []string `xml:"NameServers>NameServer"`
		} `xml:"DelegationSet"`
	}

	var cr dsResp
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &cr))
	require.NotEmpty(t, cr.DelegationSet.ID)
	require.NotEmpty(t, cr.DelegationSet.NameServers)

	return strings.TrimPrefix(cr.DelegationSet.ID, "/delegationset/"), cr.DelegationSet.NameServers
}

func createHostedZoneWithDelegationSetXML(name, callerRef, delegationSetID string) string {
	dsElem := ""
	if delegationSetID != "" {
		dsElem = "<DelegationSetId>" + delegationSetID + "</DelegationSetId>"
	}

	return `<?xml version="1.0" encoding="UTF-8"?>
<CreateHostedZoneRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/">
  <Name>` + name + `</Name>
  <CallerReference>` + callerRef + `</CallerReference>
  ` + dsElem + `
</CreateHostedZoneRequest>`
}

func Test_CreateHostedZone_WithReusableDelegationSet(t *testing.T) {
	t.Parallel()

	h := newHandler(t)
	dsID, nameServers := createDelegationSetViaHTTP(t, h, "ds-ref-linked")

	rec := send(t, h, http.MethodPost, "/2013-04-01/hostedzone",
		createHostedZoneWithDelegationSetXML("linked.example.com", "zone-ref-linked", dsID))
	require.Equal(t, http.StatusCreated, rec.Code)

	body := rec.Body.String()
	// CreateHostedZone response's DelegationSet element carries the
	// reusable set's own Id and NameServers, not the fixed defaults.
	assert.Contains(t, body, "<Id>/delegationset/"+dsID+"</Id>")

	for _, ns := range nameServers {
		assert.Contains(t, body, ns)
	}

	zoneID := extractZoneID(t, body)

	// GetHostedZone must report the same linkage.
	rec = send(t, h, http.MethodGet, "/2013-04-01/hostedzone/"+zoneID, "")
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "<Id>/delegationset/"+dsID+"</Id>")
}

func Test_CreateHostedZone_UnknownDelegationSet(t *testing.T) {
	t.Parallel()

	h := newHandler(t)

	rec := send(t, h, http.MethodPost, "/2013-04-01/hostedzone",
		createHostedZoneWithDelegationSetXML("nosuch.example.com", "zone-ref-nosuch", "NBOGUS123456"))
	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "NoSuchDelegationSet")
}

func Test_CreateHostedZone_WithoutDelegationSet_UsesDefaults(t *testing.T) {
	t.Parallel()

	h := newHandler(t)

	rec := send(t, h, http.MethodPost, "/2013-04-01/hostedzone",
		createHostedZoneWithDelegationSetXML("plain.example.com", "zone-ref-plain", ""))
	require.Equal(t, http.StatusCreated, rec.Code)

	// No reusable set was requested, so the response must not carry a
	// DelegationSet Id (system-assigned delegation sets have none).
	assert.NotContains(t, rec.Body.String(), "<DelegationSet><Id>")
}

func Test_DeleteReusableDelegationSet_InUse(t *testing.T) {
	t.Parallel()

	h := newHandler(t)
	dsID, _ := createDelegationSetViaHTTP(t, h, "ds-ref-inuse")

	rec := send(t, h, http.MethodPost, "/2013-04-01/hostedzone",
		createHostedZoneWithDelegationSetXML("inuse.example.com", "zone-ref-inuse", dsID))
	require.Equal(t, http.StatusCreated, rec.Code)
	zoneID := extractZoneID(t, rec.Body.String())

	// Deleting the still-linked delegation set must fail.
	rec = send(t, h, http.MethodDelete, "/2013-04-01/delegationset/"+dsID, "")
	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "DelegationSetInUse")

	// Delete the zone, then the delegation set delete succeeds.
	rec = send(t, h, http.MethodDelete, "/2013-04-01/hostedzone/"+zoneID, "")
	require.Equal(t, http.StatusOK, rec.Code)

	rec = send(t, h, http.MethodDelete, "/2013-04-01/delegationset/"+dsID, "")
	assert.Equal(t, http.StatusOK, rec.Code)
}

func Test_CountZonesByReusableDelegationSet(t *testing.T) {
	t.Parallel()

	b := route53.NewInMemoryBackend()

	ds, err := b.CreateReusableDelegationSet("ds-ref-count", "")
	require.NoError(t, err)

	count, err := b.CountZonesByReusableDelegationSet(ds.ID)
	require.NoError(t, err)
	assert.Equal(t, 0, count)

	_, err = b.CreateHostedZone("a.example.com", "zone-ref-a", "", false, ds.ID)
	require.NoError(t, err)
	_, err = b.CreateHostedZone("b.example.com", "zone-ref-b", "", false, ds.ID)
	require.NoError(t, err)
	// A zone with no delegation set must not be counted.
	_, err = b.CreateHostedZone("c.example.com", "zone-ref-c", "", false, "")
	require.NoError(t, err)

	count, err = b.CountZonesByReusableDelegationSet(ds.ID)
	require.NoError(t, err)
	assert.Equal(t, 2, count)
}

func Test_CountZonesByReusableDelegationSet_NotFound(t *testing.T) {
	t.Parallel()

	b := route53.NewInMemoryBackend()

	_, err := b.CountZonesByReusableDelegationSet("/delegationset/NBOGUS")
	require.Error(t, err)
	assert.ErrorIs(t, err, route53.ErrDelegationSetNotFound)
}

// The tests below cover CreateReusableDelegationSet's HostedZoneId
// parameter: real AWS's "mark an existing hosted zone's delegation set as
// reusable" mode, distinct from the always-used CallerReference-only "brand
// new reusable set" mode covered above. Before this pass the parameter was
// parsed off the wire and then silently ignored.

func Test_CreateReusableDelegationSet_FromHostedZone(t *testing.T) {
	t.Parallel()

	b := route53.NewInMemoryBackend()

	// Give the source zone distinct (non-default) name servers via an
	// existing reusable delegation set, so the assertion below can't pass
	// by coincidence against the hardcoded default pair.
	source, err := b.CreateReusableDelegationSet("ds-ref-source", "")
	require.NoError(t, err)

	zone, err := b.CreateHostedZone("source.example.com", "zone-ref-source", "", false, source.ID)
	require.NoError(t, err)
	require.Equal(t, source.NameServers, zone.NameServers)

	extracted, err := b.CreateReusableDelegationSet("ds-ref-extracted", zone.ID)
	require.NoError(t, err)
	assert.Equal(t, zone.NameServers, extracted.NameServers)
	assert.NotEqual(t, source.ID, extracted.ID, "extraction must create a distinct delegation set")
}

func Test_CreateReusableDelegationSet_FromHostedZone_NotFound(t *testing.T) {
	t.Parallel()

	b := route53.NewInMemoryBackend()

	_, err := b.CreateReusableDelegationSet("ds-ref-nf", "ZBOGUSZONE")
	require.Error(t, err)
	assert.ErrorIs(t, err, route53.ErrHostedZoneNotFoundForDelegationSet)
}

func Test_CreateReusableDelegationSet_FromHostedZone_PrivateZoneRejected(t *testing.T) {
	t.Parallel()

	b := route53.NewInMemoryBackend()

	zone, err := b.CreateHostedZone("private.example.com", "zone-ref-priv", "", true, "")
	require.NoError(t, err)

	_, err = b.CreateReusableDelegationSet("ds-ref-priv", zone.ID)
	require.Error(t, err)
	assert.ErrorIs(t, err, route53.ErrInvalidInput)
}

func Test_CreateReusableDelegationSet_FromHostedZone_AlreadyReusable(t *testing.T) {
	t.Parallel()

	b := route53.NewInMemoryBackend()

	zone, err := b.CreateHostedZone("dup.example.com", "zone-ref-dup", "", false, "")
	require.NoError(t, err)

	_, err = b.CreateReusableDelegationSet("ds-ref-dup-1", zone.ID)
	require.NoError(t, err)

	_, err = b.CreateReusableDelegationSet("ds-ref-dup-2", zone.ID)
	require.Error(t, err)
	assert.ErrorIs(t, err, route53.ErrDelegationSetAlreadyReusable)
}

func Test_CreateReusableDelegationSet_DuplicateCallerReference(t *testing.T) {
	t.Parallel()

	b := route53.NewInMemoryBackend()

	_, err := b.CreateReusableDelegationSet("ds-ref-samecaller", "")
	require.NoError(t, err)

	_, err = b.CreateReusableDelegationSet("ds-ref-samecaller", "")
	require.Error(t, err)
	assert.ErrorIs(t, err, route53.ErrDelegationSetAlreadyCreated)
}

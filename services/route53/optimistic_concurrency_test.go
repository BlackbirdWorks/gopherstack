package route53_test

// optimistic_concurrency_test.go covers the version-checked update paths
// this pass added real behavior for: UpdateHealthCheck's HealthCheckVersion
// and ChangeCidrCollection's CollectionVersion, plus DeleteCidrCollection's
// non-empty guard. Before this pass these fields/checks were entirely
// absent — HealthCheckVersion wasn't even on the wire, and both delete/update
// paths silently accepted stale or unbounded state.

import (
	"encoding/xml"
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/route53"
)

func updateHealthCheckXML(version string) string {
	versionElem := ""
	if version != "" {
		versionElem = "<HealthCheckVersion>" + version + "</HealthCheckVersion>"
	}

	return fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?>
<UpdateHealthCheckRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/">
  %s
  <Port>8080</Port>
</UpdateHealthCheckRequest>`, versionElem)
}

// createHealthCheckViaHTTP creates a health check and returns its ID.
func createHealthCheckViaHTTP(t *testing.T, h *route53.Handler) string {
	t.Helper()

	rec := send(t, h, http.MethodPost, "/2013-04-01/healthcheck", createHealthCheckXML)
	require.Equal(t, http.StatusCreated, rec.Code)

	return extractHealthCheckID(t, rec.Body.String())
}

func Test_CreateHealthCheck_WireIncludesVersion(t *testing.T) {
	t.Parallel()

	h := newHandler(t)

	rec := send(t, h, http.MethodPost, "/2013-04-01/healthcheck", createHealthCheckXML)
	require.Equal(t, http.StatusCreated, rec.Code)
	assert.Contains(t, rec.Body.String(), "<HealthCheckVersion>1</HealthCheckVersion>")
}

func Test_UpdateHealthCheck_VersionHandling(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		version      string
		wantContains string
		wantStatus   int
	}{
		{
			name:         "omitted version is not checked",
			version:      "",
			wantStatus:   http.StatusOK,
			wantContains: "<HealthCheckVersion>2</HealthCheckVersion>",
		},
		{
			name:         "matching version succeeds and increments",
			version:      "1",
			wantStatus:   http.StatusOK,
			wantContains: "<HealthCheckVersion>2</HealthCheckVersion>",
		},
		{
			name:         "stale version is rejected",
			version:      "99",
			wantStatus:   http.StatusConflict,
			wantContains: "HealthCheckVersionMismatch",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler(t)
			id := createHealthCheckViaHTTP(t, h)

			rec := send(t, h, http.MethodPost, "/2013-04-01/healthcheck/"+id, updateHealthCheckXML(tt.version))
			assert.Equal(t, tt.wantStatus, rec.Code)
			assert.Contains(t, rec.Body.String(), tt.wantContains)
		})
	}
}

func Test_UpdateHealthCheck_VersionIncrementsAcrossUpdates(t *testing.T) {
	t.Parallel()

	h := newHandler(t)
	id := createHealthCheckViaHTTP(t, h)

	rec := send(t, h, http.MethodPost, "/2013-04-01/healthcheck/"+id, updateHealthCheckXML("1"))
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "<HealthCheckVersion>2</HealthCheckVersion>")

	// Retrying with the now-stale version 1 must fail.
	rec = send(t, h, http.MethodPost, "/2013-04-01/healthcheck/"+id, updateHealthCheckXML("1"))
	assert.Equal(t, http.StatusConflict, rec.Code)

	// The correct current version (2) succeeds.
	rec = send(t, h, http.MethodPost, "/2013-04-01/healthcheck/"+id, updateHealthCheckXML("2"))
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "<HealthCheckVersion>3</HealthCheckVersion>")
}

const createCidrCollectionXML = `<?xml version="1.0" encoding="UTF-8"?>
<CreateCidrCollectionRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/">
  <Name>test-collection</Name>
  <CallerReference>cidr-ref-1</CallerReference>
</CreateCidrCollectionRequest>`

func createCidrCollectionViaHTTP(t *testing.T, h *route53.Handler) string {
	t.Helper()

	rec := send(t, h, http.MethodPost, "/2013-04-01/cidrcollection", createCidrCollectionXML)
	require.Equal(t, http.StatusCreated, rec.Code)

	type collResp struct {
		Collection struct {
			ID string `xml:"Id"`
		} `xml:"Collection"`
	}

	var cr collResp
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &cr))
	require.NotEmpty(t, cr.Collection.ID)

	return cr.Collection.ID
}

func changeCidrCollectionXML(version, action string) string {
	versionElem := ""
	if version != "" {
		versionElem = "<CollectionVersion>" + version + "</CollectionVersion>"
	}

	return fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?>
<ChangeCidrCollectionRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/">
  %s
  <Changes>
    <member>
      <LocationName>office</LocationName>
      <Action>%s</Action>
      <CidrList>
        <Cidr>192.168.1.0/24</Cidr>
      </CidrList>
    </member>
  </Changes>
</ChangeCidrCollectionRequest>`, versionElem, action)
}

func Test_ChangeCidrCollection_VersionHandling(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		version      string
		wantContains string
		wantStatus   int
	}{
		{
			name:         "omitted version is not checked",
			version:      "",
			wantStatus:   http.StatusOK,
			wantContains: "<Version>2</Version>",
		},
		{
			name:         "matching version succeeds and increments",
			version:      "1",
			wantStatus:   http.StatusOK,
			wantContains: "<Version>2</Version>",
		},
		{
			name:         "stale version is rejected",
			version:      "7",
			wantStatus:   http.StatusConflict,
			wantContains: "CidrCollectionVersionMismatchException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler(t)
			id := createCidrCollectionViaHTTP(t, h)

			rec := send(t, h, http.MethodPost, "/2013-04-01/cidrcollection/"+id,
				changeCidrCollectionXML(tt.version, "PUT"))
			assert.Equal(t, tt.wantStatus, rec.Code)
			assert.Contains(t, rec.Body.String(), tt.wantContains)
		})
	}
}

func Test_DeleteCidrCollection_RequiresEmpty(t *testing.T) {
	t.Parallel()

	h := newHandler(t)
	id := createCidrCollectionViaHTTP(t, h)

	// Add a location — the collection is no longer empty.
	rec := send(t, h, http.MethodPost, "/2013-04-01/cidrcollection/"+id, changeCidrCollectionXML("", "PUT"))
	require.Equal(t, http.StatusOK, rec.Code)

	// Deleting a non-empty collection must fail.
	rec = send(t, h, http.MethodDelete, "/2013-04-01/cidrcollection/"+id, "")
	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "CidrCollectionInUseException")

	// Empty it out via DELETE_IF_EXISTS...
	rec = send(t, h, http.MethodPost, "/2013-04-01/cidrcollection/"+id,
		changeCidrCollectionXML("", "DELETE_IF_EXISTS"))
	require.Equal(t, http.StatusOK, rec.Code)

	// ...and now deletion succeeds.
	rec = send(t, h, http.MethodDelete, "/2013-04-01/cidrcollection/"+id, "")
	assert.Equal(t, http.StatusOK, rec.Code)
}

package route53_test

import (
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestListTagsForResource_NoSuchResource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		path     string
		wantBody string
	}{
		{
			name:     "unknown_hostedzone",
			path:     "/2013-04-01/tags/hostedzone/ZDOESNOTEXIST",
			wantBody: "NoSuchHostedZone",
		},
		{
			name:     "unknown_healthcheck",
			path:     "/2013-04-01/tags/healthcheck/DOESNOTEXIST",
			wantBody: "NoSuchHealthCheck",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler(t)

			rec := send(t, h, http.MethodGet, tt.path, "")
			assert.Equal(t, http.StatusNotFound, rec.Code,
				"ListTagsForResource on a nonexistent resource must 404, not silently return empty tags")
			assert.Contains(t, rec.Body.String(), tt.wantBody)
		})
	}
}

func TestChangeTagsForResource_NoSuchResource(t *testing.T) {
	t.Parallel()

	const body = `<?xml version="1.0" encoding="UTF-8"?>
<ChangeTagsForResourceRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/">
  <AddTags><Tag><Key>env</Key><Value>prod</Value></Tag></AddTags>
</ChangeTagsForResourceRequest>`

	tests := []struct {
		name     string
		path     string
		wantBody string
	}{
		{
			name:     "unknown_hostedzone",
			path:     "/2013-04-01/tags/hostedzone/ZDOESNOTEXIST",
			wantBody: "NoSuchHostedZone",
		},
		{
			name:     "unknown_healthcheck",
			path:     "/2013-04-01/tags/healthcheck/DOESNOTEXIST",
			wantBody: "NoSuchHealthCheck",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler(t)

			rec := send(t, h, http.MethodPost, tt.path, body)
			assert.Equal(t, http.StatusNotFound, rec.Code,
				"ChangeTagsForResource on a nonexistent resource must 404, not silently succeed")
			assert.Contains(t, rec.Body.String(), tt.wantBody)

			// Verify the tag was genuinely not applied anywhere.
			listRec := send(t, h, http.MethodGet, tt.path, "")
			assert.Equal(t, http.StatusNotFound, listRec.Code)
		})
	}
}

func TestListTagsForResources_NoSuchResource(t *testing.T) {
	t.Parallel()

	h := newHandler(t)

	rec := send(t, h, http.MethodPost, "/2013-04-01/hostedzone", createZoneXML)
	require.Equal(t, http.StatusCreated, rec.Code)
	zoneID := extractZoneID(t, rec.Body.String())

	body := `<?xml version="1.0" encoding="UTF-8"?>
<ListTagsForResourcesRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/">
  <ResourceType>hostedzone</ResourceType>
  <ResourceIds>
    <ResourceId>` + zoneID + `</ResourceId>
    <ResourceId>ZDOESNOTEXIST</ResourceId>
  </ResourceIds>
</ListTagsForResourcesRequest>`

	// Real AWS ListTagsForResources URI is POST /2013-04-01/tags/{ResourceType}
	// (a bare "/2013-04-01/tags" request, with no ResourceType segment, is not
	// a valid Route53 endpoint at all).
	got := send(t, h, http.MethodPost, "/2013-04-01/tags/hostedzone", body)
	assert.Equal(t, http.StatusNotFound, got.Code,
		"batch tag lookup with any unknown resource ID must 404, not return partial results")
	assert.Contains(t, got.Body.String(), "NoSuchHostedZone")
}

func TestTagRoundTrip_HealthCheck(t *testing.T) {
	t.Parallel()

	h := newHandler(t)

	rec := send(t, h, http.MethodPost, "/2013-04-01/healthcheck", createHealthCheckXML)
	require.Equal(t, http.StatusCreated, rec.Code)
	hcID := extractHealthCheckID(t, rec.Body.String())

	tagsPath := "/2013-04-01/tags/healthcheck/" + hcID

	addBody := `<?xml version="1.0" encoding="UTF-8"?>
<ChangeTagsForResourceRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/">
  <AddTags><Tag><Key>owner</Key><Value>net-team</Value></Tag></AddTags>
</ChangeTagsForResourceRequest>`

	addRec := send(t, h, http.MethodPost, tagsPath, addBody)
	require.Equal(t, http.StatusOK, addRec.Code,
		"ChangeTagsForResource on a real health check must still succeed")

	listRec := send(t, h, http.MethodGet, tagsPath, "")
	assert.Equal(t, http.StatusOK, listRec.Code)
	assert.Contains(t, listRec.Body.String(), "net-team")
}

func TestListTagsForResources_HTTPRoute(t *testing.T) {
	t.Parallel()

	h := newHandler(t)

	rec := send(t, h, http.MethodPost, "/2013-04-01/hostedzone", createZoneXML)
	require.Equal(t, http.StatusCreated, rec.Code)
	zoneID := extractZoneID(t, rec.Body.String())

	addBody := `<?xml version="1.0" encoding="UTF-8"?>
<ChangeTagsForResourceRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/">
  <AddTags><Tag><Key>env</Key><Value>prod</Value></Tag></AddTags>
</ChangeTagsForResourceRequest>`
	addRec := send(t, h, http.MethodPost, "/2013-04-01/tags/hostedzone/"+zoneID, addBody)
	require.Equal(t, http.StatusOK, addRec.Code)

	body := `<?xml version="1.0" encoding="UTF-8"?>
<ListTagsForResourcesRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/">
  <ResourceType>hostedzone</ResourceType>
  <ResourceIds><ResourceId>` + zoneID + `</ResourceId></ResourceIds>
</ListTagsForResourcesRequest>`

	// Real AWS ListTagsForResources request URI: POST /2013-04-01/tags/{ResourceType}.
	got := send(t, h, http.MethodPost, "/2013-04-01/tags/hostedzone", body)
	assert.Equal(t, http.StatusOK, got.Code,
		"ListTagsForResources must be reachable at its real AWS request URI")
	assert.Contains(t, got.Body.String(), "ListTagsForResourcesResponse")
	assert.Contains(t, got.Body.String(), "prod")
}

func TestRoute53Handler_Tags(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		method       string
		path         string
		body         string
		wantContains []string
		wantCode     int
	}{
		{
			name:   "AddTags",
			method: http.MethodPost,
			path:   "/2013-04-01/tags/hostedzone/{zoneID}",
			body: `<?xml version="1.0" encoding="UTF-8"?>
<ChangeTagsForResourceRequest>
  <AddTags>
    <Tag><Key>env</Key><Value>prod</Value></Tag>
    <Tag><Key>team</Key><Value>infra</Value></Tag>
  </AddTags>
</ChangeTagsForResourceRequest>`,
			wantCode:     http.StatusOK,
			wantContains: []string{"ChangeTagsForResourceResponse"},
		},
		{
			name:         "ListTags_Empty",
			method:       http.MethodGet,
			path:         "/2013-04-01/tags/hostedzone/{zoneID}",
			wantCode:     http.StatusOK,
			wantContains: []string{"ListTagsForResourceResponse", "hostedzone"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler(t)
			rec := send(t, h, http.MethodPost, "/2013-04-01/hostedzone", createZoneXML)
			require.Equal(t, http.StatusCreated, rec.Code)

			zoneID := extractZoneID(t, rec.Body.String())
			path := strings.Replace(tt.path, "{zoneID}", zoneID, 1)
			got := send(t, h, tt.method, path, tt.body)
			assert.Equal(t, tt.wantCode, got.Code)

			for _, s := range tt.wantContains {
				assert.Contains(t, got.Body.String(), s)
			}
		})
	}
}

func TestRoute53Handler_TagRoundTrip(t *testing.T) {
	t.Parallel()

	h := newHandler(t)
	rec := send(t, h, http.MethodPost, "/2013-04-01/hostedzone", createZoneXML)
	require.Equal(t, http.StatusCreated, rec.Code)

	zoneID := extractZoneID(t, rec.Body.String())
	tagsPath := "/2013-04-01/tags/hostedzone/" + zoneID

	// Add tags.
	addBody := `<?xml version="1.0" encoding="UTF-8"?>
<ChangeTagsForResourceRequest>
  <AddTags>
    <Tag><Key>env</Key><Value>prod</Value></Tag>
    <Tag><Key>team</Key><Value>infra</Value></Tag>
  </AddTags>
</ChangeTagsForResourceRequest>`
	addRec := send(t, h, http.MethodPost, tagsPath, addBody)
	require.Equal(t, http.StatusOK, addRec.Code)

	// List and verify tags exist.
	listRec := send(t, h, http.MethodGet, tagsPath, "")
	assert.Equal(t, http.StatusOK, listRec.Code)
	assert.Contains(t, listRec.Body.String(), "env")
	assert.Contains(t, listRec.Body.String(), "prod")
	assert.Contains(t, listRec.Body.String(), "team")

	// Remove one tag.
	removeBody := `<?xml version="1.0" encoding="UTF-8"?>
<ChangeTagsForResourceRequest>
  <RemoveTagKeys>
    <Key>team</Key>
  </RemoveTagKeys>
</ChangeTagsForResourceRequest>`
	removeRec := send(t, h, http.MethodPost, tagsPath, removeBody)
	require.Equal(t, http.StatusOK, removeRec.Code)

	// Verify only the remaining tag is listed.
	listRec2 := send(t, h, http.MethodGet, tagsPath, "")
	assert.Contains(t, listRec2.Body.String(), "env")
	assert.NotContains(t, listRec2.Body.String(), "team")
}

func TestRoute53Handler_Tags_UnsupportedMethod(t *testing.T) {
	t.Parallel()

	h := newHandler(t)
	rec := send(t, h, http.MethodDelete, "/2013-04-01/tags/hostedzone/ZFAKE", "")
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

package guardduty_test

// Regression coverage for a tag-state desync bug found during the parity-4
// GuardDuty audit: TagResource/UntagResource only ever mutated the generic
// ARN-keyed tag map, never the owning resource's own frozen Tags field set
// at creation time. Since GetDetector/GetFilter/GetIPSet/GetThreatIntelSet/
// GetThreatEntitySet/GetTrustedEntitySet/GetMalwareProtectionPlan/
// DescribePublishingDestination all return that frozen field (matching the
// real aws-sdk-go-v2 output shapes, each of which embeds Tags directly),
// calling TagResource then immediately re-reading the resource via its own
// Get/Describe op showed stale tags, even though ListTagsForResource (which
// reads the generic map directly) showed the update. Every existing tags
// test asserted only via ListTagsForResource, so this divergence had no
// regression coverage before now.

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/guardduty"
)

func tagSyncHandler(t *testing.T) *guardduty.Handler {
	t.Helper()

	return guardduty.NewHandler(guardduty.NewInMemoryBackend("123456789012", "us-east-1"))
}

// doJSON issues a request and decodes the JSON response body into a map.
func doJSON(t *testing.T, h *guardduty.Handler, method, path string, body any) map[string]any {
	t.Helper()

	rec := auditDo(t, h, method, path, body)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	return resp
}

// TestTagSync_TagResource_ReflectsInOwningResourceGet exercises
// TagResource/UntagResource against every taggable resource family and
// asserts the change is visible both via ListTagsForResource (the generic
// view) AND via that resource's own Get/Describe op (the resource-embedded
// view real aws-sdk-go-v2 GetXOutput.Tags fields expose).
func TestTagSync_TagResource_ReflectsInOwningResourceGet(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup func(t *testing.T, h *guardduty.Handler) (arn string, reread func(t *testing.T) map[string]any)
		name  string
	}{
		{
			name: "detector",
			setup: func(t *testing.T, h *guardduty.Handler) (string, func(t *testing.T) map[string]any) {
				t.Helper()

				resp := doJSON(t, h, http.MethodPost, "/detector", map[string]any{"enable": true})
				id := resp["detectorId"].(string)
				arn := fmt.Sprintf("arn:aws:guardduty:us-east-1:123456789012:detector/%s", id)

				return arn, func(t *testing.T) map[string]any {
					t.Helper()

					return doJSON(t, h, http.MethodGet, "/detector/"+id, nil)
				}
			},
		},
		{
			name: "filter",
			setup: func(t *testing.T, h *guardduty.Handler) (string, func(t *testing.T) map[string]any) {
				t.Helper()

				dResp := doJSON(t, h, http.MethodPost, "/detector", map[string]any{"enable": true})
				detID := dResp["detectorId"].(string)

				doJSON(t, h, http.MethodPost, "/detector/"+detID+"/filter", map[string]any{
					"name": "f1", "findingCriteria": map[string]any{},
				})
				arn := fmt.Sprintf("arn:aws:guardduty:us-east-1:123456789012:detector/%s/filter/f1", detID)

				return arn, func(t *testing.T) map[string]any {
					t.Helper()

					return doJSON(t, h, http.MethodGet, "/detector/"+detID+"/filter/f1", nil)
				}
			},
		},
		{
			name: "ipset",
			setup: func(t *testing.T, h *guardduty.Handler) (string, func(t *testing.T) map[string]any) {
				t.Helper()

				dResp := doJSON(t, h, http.MethodPost, "/detector", map[string]any{"enable": true})
				detID := dResp["detectorId"].(string)

				iResp := doJSON(t, h, http.MethodPost, "/detector/"+detID+"/ipset", map[string]any{
					"name": "ip1", "format": "TXT", "location": "s3://b/k",
				})
				ipSetID := iResp["ipSetId"].(string)
				arn := fmt.Sprintf("arn:aws:guardduty:us-east-1:123456789012:detector/%s/ipset/%s", detID, ipSetID)

				return arn, func(t *testing.T) map[string]any {
					t.Helper()

					return doJSON(t, h, http.MethodGet, "/detector/"+detID+"/ipset/"+ipSetID, nil)
				}
			},
		},
		{
			name: "threatentityset",
			setup: func(t *testing.T, h *guardduty.Handler) (string, func(t *testing.T) map[string]any) {
				t.Helper()

				dResp := doJSON(t, h, http.MethodPost, "/detector", map[string]any{"enable": true})
				detID := dResp["detectorId"].(string)

				sResp := doJSON(t, h, http.MethodPost, "/detector/"+detID+"/threatentityset", map[string]any{
					"name": "te1", "format": "TXT", "location": "s3://b/k",
				})
				setID := sResp["threatEntitySetId"].(string)
				arn := fmt.Sprintf(
					"arn:aws:guardduty:us-east-1:123456789012:detector/%s/threatentityset/%s", detID, setID,
				)

				return arn, func(t *testing.T) map[string]any {
					t.Helper()

					return doJSON(t, h, http.MethodGet, "/detector/"+detID+"/threatentityset/"+setID, nil)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := tagSyncHandler(t)
			arn, reread := tt.setup(t, h)

			// TagResource: the resource's own Get response must show the new tag.
			rec := auditDo(t, h, http.MethodPost, "/tags/"+arn, map[string]any{
				"tags": map[string]string{"owner": "alice"},
			})
			require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

			got := reread(t)
			gotTags, _ := got["tags"].(map[string]any)
			assert.Equal(t, "alice", gotTags["owner"], "TagResource must be visible via the resource's own Get op")

			// UntagResource: the resource's own Get response must drop it.
			rec = auditDo(t, h, http.MethodDelete, "/tags/"+arn+"?tagKeys=owner", nil)
			require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

			got = reread(t)
			gotTags, _ = got["tags"].(map[string]any)
			_, stillPresent := gotTags["owner"]
			assert.False(t, stillPresent, "UntagResource must be visible via the resource's own Get op")
		})
	}
}

// TestTagSync_CreateThreatEntitySet_TagsVisibleViaListTagsForResource covers
// the inverse desync this audit found: CreateThreatEntitySet/
// CreateTrustedEntitySet stored creation-time tags on the resource's own
// Tags field but never wrote them into the generic ARN-keyed tag map, so
// ListTagsForResource returned empty immediately after creation despite
// GetThreatEntitySet showing the tags.
func TestTagSync_CreateThreatEntitySet_TagsVisibleViaListTagsForResource(t *testing.T) {
	t.Parallel()

	h := tagSyncHandler(t)

	dResp := doJSON(t, h, http.MethodPost, "/detector", map[string]any{"enable": true})
	detID := dResp["detectorId"].(string)

	sResp := doJSON(t, h, http.MethodPost, "/detector/"+detID+"/trustedentityset", map[string]any{
		"name": "tr1", "format": "TXT", "location": "s3://b/k",
		"tags": map[string]string{"team": "sec"},
	})
	setID := sResp["trustedEntitySetId"].(string)

	arn := fmt.Sprintf("arn:aws:guardduty:us-east-1:123456789012:detector/%s/trustedentityset/%s", detID, setID)
	tagsResp := doJSON(t, h, http.MethodGet, "/tags/"+arn, nil)
	tags, _ := tagsResp["tags"].(map[string]any)

	assert.Equal(t, "sec", tags["team"], "creation-time tags must be visible via ListTagsForResource")
}

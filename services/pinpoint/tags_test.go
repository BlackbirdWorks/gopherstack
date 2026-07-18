package pinpoint_test

import (
	"encoding/json"
	"net/http"
	"net/url"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// pinpointEncodeARN URL-encodes an ARN for use in URL paths.
func pinpointEncodeARN(input string) string {
	var b strings.Builder

	for _, c := range input {
		switch c {
		case ':':
			b.WriteString("%3A")
		case '/':
			b.WriteString("%2F")
		default:
			b.WriteRune(c)
		}
	}

	return b.String()
}

func TestUntagResource_EmptyTagKeys(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		tagKeys    string
		wantStatus int
	}{
		{
			name:       "missing_tagkeys_returns_400",
			tagKeys:    "",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "with_tagkeys_returns_204",
			tagKeys:    "?tagKeys=env",
			wantStatus: http.StatusNoContent,
		},
		{
			name:       "multiple_tagkeys_returns_204",
			tagKeys:    "?tagKeys=env&tagKeys=team",
			wantStatus: http.StatusNoContent,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newHandlerForTest(t)
			appID := createTestApp(t, h, "untag-test-app")

			// Get the app ARN from the app response.
			getRec := doPinpointRequest(t, h, http.MethodGet, "/v1/apps/"+appID, nil)
			require.Equal(t, http.StatusOK, getRec.Code)

			var appResp map[string]any
			require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &appResp))

			arn, _ := appResp["Arn"].(string)
			require.NotEmpty(t, arn, "app ARN must be present")

			// URL-encode the ARN for use in path.
			encodedARN := pinpointEncodeARN(arn)

			// Add tags so the resource has some to remove.
			tagBody := map[string]any{"tags": map[string]any{"env": "test", "team": "eng"}}
			addTagRec := doPinpointRequest(t, h, http.MethodPost, "/v1/tags/"+encodedARN, tagBody)
			require.Equal(t, http.StatusNoContent, addTagRec.Code)

			untagPath := "/v1/tags/" + encodedARN + tc.tagKeys
			untagRec := doPinpointRequest(t, h, http.MethodDelete, untagPath, nil)
			assert.Equal(t, tc.wantStatus, untagRec.Code, "untag path: %s", untagPath)
		})
	}
}

func TestUntagResource_RemovesSpecificTags(t *testing.T) {
	t.Parallel()

	tests := []struct {
		initialTags   map[string]any
		remainingTags map[string]any
		name          string
		removeKeys    string
	}{
		{
			name:          "remove_one_tag",
			initialTags:   map[string]any{"tags": map[string]any{"env": "prod", "team": "eng"}},
			removeKeys:    "?tagKeys=env",
			remainingTags: map[string]any{"team": "eng"},
		},
		{
			name:          "remove_multiple_tags",
			initialTags:   map[string]any{"tags": map[string]any{"env": "prod", "team": "eng", "region": "us"}},
			removeKeys:    "?tagKeys=env&tagKeys=team",
			remainingTags: map[string]any{"region": "us"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newHandlerForTest(t)
			appID := createTestApp(t, h, "untag-specific-app")

			// Get app ARN.
			getRec := doPinpointRequest(t, h, http.MethodGet, "/v1/apps/"+appID, nil)
			require.Equal(t, http.StatusOK, getRec.Code)

			var appResp map[string]any
			require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &appResp))

			arn, _ := appResp["Arn"].(string)
			encodedARN := pinpointEncodeARN(arn)

			// Tag the resource.
			tagRec := doPinpointRequest(t, h, http.MethodPost,
				"/v1/tags/"+encodedARN, tc.initialTags)
			require.Equal(t, http.StatusNoContent, tagRec.Code)

			// Remove specific tags.
			untagRec := doPinpointRequest(t, h, http.MethodDelete,
				"/v1/tags/"+encodedARN+tc.removeKeys, nil)
			assert.Equal(t, http.StatusNoContent, untagRec.Code)

			// Verify remaining tags.
			listTagRec := doPinpointRequest(t, h, http.MethodGet,
				"/v1/tags/"+encodedARN, nil)
			require.Equal(t, http.StatusOK, listTagRec.Code)

			var tagsResp map[string]any
			require.NoError(t, json.Unmarshal(listTagRec.Body.Bytes(), &tagsResp))

			remaining, _ := tagsResp["tags"].(map[string]any)

			for k, v := range tc.remainingTags {
				assert.Equal(t, v, remaining[k], "tag %s should remain", k)
			}
		})
	}
}

func TestBackend_TagOperations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		tagKey   string
		tagValue string
	}{
		{
			name:     "tag_and_untag_resource",
			tagKey:   "owner",
			tagValue: "team-a",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()

			app, err := b.CreateApp("us-east-1", "123456789012", "tagtest", nil)
			require.NoError(t, err)

			err = b.TagResource(app.ARN, map[string]string{tt.tagKey: tt.tagValue})
			require.NoError(t, err)

			tags, err := b.ListTagsForResource(app.ARN)
			require.NoError(t, err)
			assert.Equal(t, tt.tagValue, tags[tt.tagKey])

			err = b.UntagResource(app.ARN, []string{tt.tagKey})
			require.NoError(t, err)

			tags, err = b.ListTagsForResource(app.ARN)
			require.NoError(t, err)
			assert.Empty(t, tags[tt.tagKey])
		})
	}
}

func TestTagCampaignViaARN(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)
	appID := createTestApp(t, h, "tag-campaign-app")

	recC := doPinpointRequest(t, h, http.MethodPost, "/v1/apps/"+appID+"/campaigns",
		map[string]any{"Name": "taggable-campaign", "SegmentId": "seg-1"})
	require.Equal(t, http.StatusCreated, recC.Code)

	var cResp map[string]any
	require.NoError(t, json.NewDecoder(recC.Body).Decode(&cResp))
	campaignARN, _ := cResp["Arn"].(string)
	require.NotEmpty(t, campaignARN)

	// Tag the campaign via /v1/tags/{arn}
	tagRec := doPinpointRequest(t, h, http.MethodPost, "/v1/tags/"+campaignARN,
		map[string]any{"tags": map[string]string{"cost-center": "42"}})
	assert.Equal(t, http.StatusNoContent, tagRec.Code)

	// List tags
	listRec := doPinpointRequest(t, h, http.MethodGet, "/v1/tags/"+campaignARN, nil)
	assert.Equal(t, http.StatusOK, listRec.Code)

	var tagsBody map[string]any
	require.NoError(t, json.NewDecoder(listRec.Body).Decode(&tagsBody))
	tagsMap, _ := tagsBody["tags"].(map[string]any)
	assert.Equal(t, "42", tagsMap["cost-center"])
}

func TestTagJourneyViaARN(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)
	appID := createTestApp(t, h, "tag-journey-app")

	recJ := doPinpointRequest(t, h, http.MethodPost, "/v1/apps/"+appID+"/journeys",
		map[string]any{"Name": "taggable-journey"})
	require.Equal(t, http.StatusCreated, recJ.Code)

	var jResp map[string]any
	require.NoError(t, json.NewDecoder(recJ.Body).Decode(&jResp))
	journeyARN, _ := jResp["Arn"].(string)
	require.NotEmpty(t, journeyARN)

	tagRec := doPinpointRequest(t, h, http.MethodPost, "/v1/tags/"+journeyARN,
		map[string]any{"tags": map[string]string{"team": "growth"}})
	assert.Equal(t, http.StatusNoContent, tagRec.Code)

	listRec := doPinpointRequest(t, h, http.MethodGet, "/v1/tags/"+journeyARN, nil)
	assert.Equal(t, http.StatusOK, listRec.Code)
}

func TestTagSegmentViaARN(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)
	appID := createTestApp(t, h, "tag-segment-app")

	recS := doPinpointRequest(t, h, http.MethodPost, "/v1/apps/"+appID+"/segments",
		map[string]any{"Name": "taggable-segment"})
	require.Equal(t, http.StatusCreated, recS.Code)

	var sResp map[string]any
	require.NoError(t, json.NewDecoder(recS.Body).Decode(&sResp))
	segmentARN, _ := sResp["Arn"].(string)
	require.NotEmpty(t, segmentARN)

	tagRec := doPinpointRequest(t, h, http.MethodPost, "/v1/tags/"+segmentARN,
		map[string]any{"tags": map[string]string{"segment-owner": "analytics"}})
	assert.Equal(t, http.StatusNoContent, tagRec.Code)
}

// ──────────────────────────────────────────────────
// Template uniqueness (409 Conflict)
// ──────────────────────────────────────────────────

func TestTagEmailTemplateViaARN(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)

	// Create email template
	rec := doPinpointRequest(t, h, http.MethodPost, "/v1/templates/tag-email/email",
		map[string]any{"Subject": "tag-me"})
	require.Equal(t, http.StatusCreated, rec.Code)

	var tResp map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&tResp))
	templateARN, _ := tResp["Arn"].(string)
	require.NotEmpty(t, templateARN)

	// Tag it
	tagRec := doPinpointRequest(t, h, http.MethodPost, "/v1/tags/"+templateARN,
		map[string]any{"tags": map[string]string{"type": "email"}})
	assert.Equal(t, http.StatusNoContent, tagRec.Code)

	// List tags
	listRec := doPinpointRequest(t, h, http.MethodGet, "/v1/tags/"+templateARN, nil)
	assert.Equal(t, http.StatusOK, listRec.Code)

	var listBody map[string]any
	require.NoError(t, json.NewDecoder(listRec.Body).Decode(&listBody))
	tagsMap, _ := listBody["tags"].(map[string]any)
	assert.Equal(t, "email", tagsMap["type"])

	// Untag
	untagRec := doPinpointRequest(t, h, http.MethodDelete, "/v1/tags/"+templateARN+"?tagKeys=type", nil)
	assert.Equal(t, http.StatusNoContent, untagRec.Code)
}

func TestHandler_TagOperations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		encodeARN bool
	}{
		{
			name:      "tag_list_untag",
			encodeARN: false,
		},
		{
			name:      "tag_list_untag_percent_encoded_arn",
			encodeARN: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandlerForTest(t)

			rec := doPinpointRequest(t, h, http.MethodPost, "/v1/apps", map[string]any{"Name": "tag-test-app"})
			require.Equal(t, http.StatusCreated, rec.Code)

			var appResp map[string]any
			require.NoError(t, json.NewDecoder(rec.Body).Decode(&appResp))

			appARN, _ := appResp["Arn"].(string)
			require.NotEmpty(t, appARN)

			tagPathARN := appARN
			if tt.encodeARN {
				tagPathARN = url.PathEscape(appARN)
			}

			tagRec := doPinpointRequest(t, h, http.MethodPost, "/v1/tags/"+tagPathARN, map[string]any{
				"tags": map[string]string{"owner": "integration"},
			})
			assert.Equal(t, http.StatusNoContent, tagRec.Code)

			listRec := doPinpointRequest(t, h, http.MethodGet, "/v1/tags/"+tagPathARN, nil)
			assert.Equal(t, http.StatusOK, listRec.Code)

			var tagsResp map[string]any
			require.NoError(t, json.NewDecoder(listRec.Body).Decode(&tagsResp))

			tagsMap, _ := tagsResp["tags"].(map[string]any)
			assert.Equal(t, "integration", tagsMap["owner"])

			untagPath := "/v1/tags/" + tagPathARN + "?tagKeys=owner"
			untagRec := doPinpointRequest(t, h, http.MethodDelete, untagPath, nil)
			assert.Equal(t, http.StatusNoContent, untagRec.Code)
		})
	}
}

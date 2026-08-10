package mq_test

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/mq"
)

func TestTagsLifecycle(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create a broker to get an ARN.
	rec := doRequest(t, h, http.MethodPost, "/v1/brokers", map[string]any{
		"brokerName": "tagged-broker",
		"engineType": mq.EngineTypeActiveMQ,
	})
	require.Equal(t, http.StatusOK, rec.Code)
	resourceARN := parseResponse(t, rec)["brokerArn"].(string)

	// Create tags.
	rec = doRequest(t, h, http.MethodPost, "/v1/tags/"+resourceARN, map[string]any{
		"tags": map[string]string{"env": "test", "owner": "alice"},
	})
	assert.Equal(t, http.StatusNoContent, rec.Code)

	// List tags.
	rec = doRequest(t, h, http.MethodGet, "/v1/tags/"+resourceARN, nil)
	require.Equal(t, http.StatusOK, rec.Code)

	tags, ok := parseResponse(t, rec)["tags"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "test", tags["env"])
	assert.Equal(t, "alice", tags["owner"])

	// Delete tags.
	rec = doRequest(t, h, http.MethodDelete, "/v1/tags/"+resourceARN+"?tagKeys=env", nil)
	assert.Equal(t, http.StatusNoContent, rec.Code)
}

func TestTags_UnknownARN_Returns404(t *testing.T) {
	t.Parallel()

	bogusARN := "arn:aws:mq:us-east-1:123456789012:broker:does-not-exist"

	tests := []struct {
		do   func(t *testing.T, h *mq.Handler) *httptest.ResponseRecorder
		name string
	}{
		{
			name: "createtags",
			do: func(t *testing.T, h *mq.Handler) *httptest.ResponseRecorder {
				t.Helper()

				return doRequest(t, h, http.MethodPost, "/v1/tags/"+bogusARN, map[string]any{
					"tags": map[string]string{"env": "test"},
				})
			},
		},
		{
			name: "listtags",
			do: func(t *testing.T, h *mq.Handler) *httptest.ResponseRecorder {
				t.Helper()

				return doRequest(t, h, http.MethodGet, "/v1/tags/"+bogusARN, nil)
			},
		},
		{
			name: "deletetags",
			do: func(t *testing.T, h *mq.Handler) *httptest.ResponseRecorder {
				t.Helper()

				return doRequest(t, h, http.MethodDelete, "/v1/tags/"+bogusARN+"?tagKeys=env", nil)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := tt.do(t, h)
			require.Equal(t, http.StatusNotFound, rec.Code, "body: %s", rec.Body.String())
			assert.Equal(t, "NotFoundException", parseResponse(t, rec)["__type"])
		})
	}
}

func TestCreateTags_OnConfiguration(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	configID := createTestConfig(t, h, "tagged-config", mq.EngineTypeActiveMQ)

	desc := doRequest(t, h, http.MethodGet, "/v1/configurations/"+configID, nil)
	require.Equal(t, http.StatusOK, desc.Code)
	configARN := parseResponse(t, desc)["arn"].(string)

	// Create tags on configuration.
	rec := doRequest(t, h, http.MethodPost, "/v1/tags/"+configARN, map[string]any{
		"tags": map[string]string{"tier": "free"},
	})
	assert.Equal(t, http.StatusNoContent, rec.Code)

	// Delete tags on configuration.
	rec = doRequest(t, h, http.MethodDelete, "/v1/tags/"+configARN+"?tagKeys=tier", nil)
	assert.Equal(t, http.StatusNoContent, rec.Code)
}

func TestCreateTags_ReturnsHTTP204(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodPost, "/v1/brokers", map[string]any{
		"brokerName": "create-tags-status-broker",
		"engineType": mq.EngineTypeActiveMQ,
	})
	require.Equal(t, http.StatusOK, rec.Code)
	brokerARN := parseResponse(t, rec)["brokerArn"].(string)

	tagRec := doRequest(t, h, http.MethodPost, "/v1/tags/"+brokerARN, map[string]any{
		"tags": map[string]string{"env": "prod"},
	})
	assert.Equal(t, http.StatusNoContent, tagRec.Code, "CreateTags must return 204, not 200: %s", tagRec.Body.String())
}

func TestListTags_EmptyTagMap(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	brokerID := createTestBroker(t, h, "no-tags-broker", mq.EngineTypeActiveMQ)
	brokerARN := describeTestBroker(t, h, brokerID)["brokerArn"].(string)

	rec := doRequest(t, h, http.MethodGet, "/v1/tags/"+brokerARN, nil)
	require.Equal(t, http.StatusOK, rec.Code)

	tags, ok := parseResponse(t, rec)["tags"].(map[string]any)
	require.True(t, ok, "tags field must be present")
	assert.Empty(t, tags, "tags must be empty for broker with no tags")
}

func TestListTags_EmptyObject(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	bid := createTestBroker(t, h, "listtags-broker", mq.EngineTypeActiveMQ)
	brokerARN := describeTestBroker(t, h, bid)["brokerArn"].(string)

	rec := doRequest(t, h, http.MethodGet, "/v1/tags/"+brokerARN, nil)
	require.Equal(t, http.StatusOK, rec.Code)
	resp := parseResponse(t, rec)

	tags, hasTags := resp["tags"]
	assert.True(t, hasTags, "ListTags must include 'tags' key")
	assert.IsType(t, map[string]any{}, tags, "'tags' must be an object, not null")
	assert.Empty(t, tags, "'tags' must be {} when no tags")
}

func TestCreateTags_AppendsToExisting(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodPost, "/v1/brokers", map[string]any{
		"brokerName": "add-tags-broker",
		"engineType": mq.EngineTypeActiveMQ,
		"tags":       map[string]string{"initial": "yes"},
	})
	require.Equal(t, http.StatusOK, rec.Code)
	brokerID := parseResponse(t, rec)["brokerId"].(string)
	brokerARN := describeTestBroker(t, h, brokerID)["brokerArn"].(string)

	// Add another tag.
	addRec := doRequest(t, h, http.MethodPost, "/v1/tags/"+brokerARN, map[string]any{
		"tags": map[string]string{"added": "later"},
	})
	assert.Equal(t, http.StatusNoContent, addRec.Code)

	listRec := doRequest(t, h, http.MethodGet, "/v1/tags/"+brokerARN, nil)
	require.Equal(t, http.StatusOK, listRec.Code)

	tags := parseResponse(t, listRec)["tags"].(map[string]any)
	assert.Equal(t, "yes", tags["initial"])
	assert.Equal(t, "later", tags["added"])
}

func TestDeleteTags_RemovesSpecified(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodPost, "/v1/brokers", map[string]any{
		"brokerName": "del-tags-broker",
		"engineType": mq.EngineTypeActiveMQ,
		"tags":       map[string]string{"keep": "yes", "remove": "no"},
	})
	require.Equal(t, http.StatusOK, rec.Code)
	brokerID := parseResponse(t, rec)["brokerId"].(string)
	brokerARN := describeTestBroker(t, h, brokerID)["brokerArn"].(string)

	// Delete the "remove" tag via query param.
	e := echo.New()
	req := httptest.NewRequest(http.MethodDelete, "/v1/tags/"+brokerARN+"?tagKeys=remove", nil)
	req.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=test/20240101/us-east-1/mq/aws4_request")
	rec2 := httptest.NewRecorder()
	c := e.NewContext(req, rec2)
	require.NoError(t, h.Handler()(c))
	assert.Equal(t, http.StatusNoContent, rec2.Code)

	listRec := doRequest(t, h, http.MethodGet, "/v1/tags/"+brokerARN, nil)
	tags := parseResponse(t, listRec)["tags"].(map[string]any)
	assert.Equal(t, "yes", tags["keep"])
	_, hasRemove := tags["remove"]
	assert.False(t, hasRemove, "deleted tag must not appear in ListTags")
}

func TestTagResource_MergesWithCreationTimeTags(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, http.MethodPost, "/v1/brokers", map[string]any{
		"brokerName": "merge-broker",
		"engineType": mq.EngineTypeActiveMQ,
		"tags":       map[string]string{"env": "prod"},
	})
	require.Equal(t, http.StatusOK, rec.Code)
	brokerARN := describeTestBroker(t, h, parseResponse(t, rec)["brokerId"].(string))["brokerArn"].(string)

	rec = doRequest(t, h, http.MethodPost, "/v1/tags/"+brokerARN,
		map[string]any{"tags": map[string]string{"team": "infra"}})
	assert.Equal(t, http.StatusNoContent, rec.Code)

	rec = doRequest(t, h, http.MethodGet, "/v1/tags/"+brokerARN, nil)
	require.Equal(t, http.StatusOK, rec.Code)
	tags := parseResponse(t, rec)["tags"].(map[string]any)
	assert.Equal(t, "prod", tags["env"], "original tag must persist")
	assert.Equal(t, "infra", tags["team"], "new tag must be added")
}

func TestUntagResource_RemovesOnlySpecifiedKey(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, http.MethodPost, "/v1/brokers", map[string]any{
		"brokerName": "untag-broker",
		"engineType": mq.EngineTypeActiveMQ,
		"tags":       map[string]string{"key1": "v1", "key2": "v2"},
	})
	require.Equal(t, http.StatusOK, rec.Code)
	brokerARN := describeTestBroker(t, h, parseResponse(t, rec)["brokerId"].(string))["brokerArn"].(string)

	rec = doRequest(t, h, http.MethodDelete, "/v1/tags/"+brokerARN+"?tagKeys=key1", nil)
	assert.Equal(t, http.StatusNoContent, rec.Code)

	rec = doRequest(t, h, http.MethodGet, "/v1/tags/"+brokerARN, nil)
	require.Equal(t, http.StatusOK, rec.Code)
	tags := parseResponse(t, rec)["tags"].(map[string]any)
	assert.NotContains(t, tags, "key1", "key1 must be removed")
	assert.Equal(t, "v2", tags["key2"], "key2 must remain")
}

func TestCreationTimeTags_VisibleViaListTags(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, http.MethodPost, "/v1/brokers", map[string]any{
		"brokerName": "ctag-broker",
		"engineType": mq.EngineTypeActiveMQ,
		"tags":       map[string]string{"created-with": "yes"},
	})
	require.Equal(t, http.StatusOK, rec.Code)
	brokerARN := describeTestBroker(t, h, parseResponse(t, rec)["brokerId"].(string))["brokerArn"].(string)

	rec = doRequest(t, h, http.MethodGet, "/v1/tags/"+brokerARN, nil)
	require.Equal(t, http.StatusOK, rec.Code)
	tags := parseResponse(t, rec)["tags"].(map[string]any)
	assert.Equal(t, "yes", tags["created-with"], "creation-time tags must be visible via ListTags")
}

func TestCreateTags_KeyValueValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		tags     map[string]string
		name     string
		wantCode int
	}{
		{
			name:     "valid key and value accepted",
			tags:     map[string]string{"env": "prod"},
			wantCode: http.StatusNoContent,
		},
		{
			name:     "key at 128 chars accepted",
			tags:     map[string]string{strings.Repeat("k", 128): "v"},
			wantCode: http.StatusNoContent,
		},
		{
			name:     "key over 128 chars rejected",
			tags:     map[string]string{strings.Repeat("k", 129): "v"},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "empty key rejected",
			tags:     map[string]string{"": "v"},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "value at 256 chars accepted",
			tags:     map[string]string{"k": strings.Repeat("v", 256)},
			wantCode: http.StatusNoContent,
		},
		{
			name:     "value over 256 chars rejected",
			tags:     map[string]string{"k": strings.Repeat("v", 257)},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "empty value accepted",
			tags:     map[string]string{"k": ""},
			wantCode: http.StatusNoContent,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			bid := createTestBroker(t, h, "tag-broker", mq.EngineTypeActiveMQ)
			brokerARN := describeTestBroker(t, h, bid)["brokerArn"].(string)

			rec := doRequest(t, h, http.MethodPost, "/v1/tags/"+brokerARN,
				map[string]any{"tags": tt.tags})
			assert.Equal(t, tt.wantCode, rec.Code, "tags=%v: %s", tt.tags, rec.Body.String())

			if tt.wantCode == http.StatusBadRequest {
				resp := parseResponse(t, rec)
				assert.Equal(t, "BadRequestException", resp["__type"])
			}
		})
	}
}

func TestCreateTags_MaxTagsPerResource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		addTags  map[string]string
		name     string
		wantCode int
		existing int
	}{
		{
			name:     "adding tags when at 49 (to reach 50) succeeds",
			existing: 49,
			addTags:  map[string]string{"tag50": "v"},
			wantCode: http.StatusNoContent,
		},
		{
			name:     "adding tag to resource already at 50 fails",
			existing: 50,
			addTags:  map[string]string{"tag51": "v"},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "overwriting existing key does not count as new tag",
			existing: 50,
			addTags:  map[string]string{"tag0": "updated"},
			wantCode: http.StatusNoContent,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend(t)
			h := mq.NewHandler(b)

			bid := createTestBroker(t, h, "maxtag-broker", mq.EngineTypeActiveMQ)
			brokerARN := describeTestBroker(t, h, bid)["brokerArn"].(string)

			// Seed existing tags directly via backend.
			existing := make(map[string]string, tt.existing)
			for i := range tt.existing {
				existing[fmt.Sprintf("tag%d", i)] = "v"
			}

			if tt.existing > 0 {
				err := b.CreateTags(brokerARN, existing)
				require.NoError(t, err, "seeding %d tags failed", tt.existing)
			}

			rec := doRequest(t, h, http.MethodPost, "/v1/tags/"+brokerARN,
				map[string]any{"tags": tt.addTags})
			assert.Equal(t, tt.wantCode, rec.Code, "existing=%d add=%v: %s", tt.existing, tt.addTags, rec.Body.String())

			if tt.wantCode == http.StatusBadRequest {
				resp := parseResponse(t, rec)
				assert.Equal(t, "BadRequestException", resp["__type"])
			}
		})
	}
}

func TestCreateBroker_Tags_KeyValueValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		tags     map[string]string
		name     string
		wantCode int
	}{
		{
			name:     "valid tags accepted at create time",
			tags:     map[string]string{"env": "prod"},
			wantCode: http.StatusOK,
		},
		{
			name:     "key over 128 chars rejected at create time",
			tags:     map[string]string{strings.Repeat("k", 129): "v"},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "value over 256 chars rejected at create time",
			tags:     map[string]string{"k": strings.Repeat("v", 257)},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "empty tag key rejected at create time",
			tags:     map[string]string{"": "v"},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			rec := doRequest(t, h, http.MethodPost, "/v1/brokers", map[string]any{
				"brokerName": "tagged-broker",
				"engineType": mq.EngineTypeActiveMQ,
				"tags":       tt.tags,
			})
			assert.Equal(t, tt.wantCode, rec.Code, "tags=%v: %s", tt.tags, rec.Body.String())

			if tt.wantCode == http.StatusBadRequest {
				resp := parseResponse(t, rec)
				assert.Equal(t, "BadRequestException", resp["__type"])
			}
		})
	}
}

func TestTagCount(t *testing.T) {
	t.Parallel()

	b := newTestBackend(t)
	assert.Equal(t, 0, mq.TagCount(b))

	_, err := b.CreateBroker(
		"tag-count-broker", mq.DeploymentModeSingleInstance,
		mq.EngineTypeActiveMQ, "", "",
		false, false, nil, nil, nil,
		map[string]string{"k": "v"},
	)
	require.NoError(t, err)

	assert.Equal(t, 1, mq.TagCount(b))
}

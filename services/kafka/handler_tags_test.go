package kafka_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestKafka_TagOperations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		op         string
		wantStatus int
	}{
		{name: "list_tags", op: "list", wantStatus: http.StatusOK},
		{name: "tag_resource", op: "tag", wantStatus: http.StatusOK},
		{name: "untag_resource", op: "untag", wantStatus: http.StatusOK},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			createRec := doKafkaRequest(t, h, http.MethodPost, "/v1/clusters", map[string]any{
				"clusterName":         "tagged-cluster",
				"kafkaVersion":        "2.8.0",
				"numberOfBrokerNodes": 3,
				"brokerNodeGroupInfo": map[string]any{
					"instanceType":  "kafka.m5.large",
					"clientSubnets": []string{"subnet-1"},
				},
			})
			require.Equal(t, http.StatusOK, createRec.Code)

			var createResp map[string]any
			require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))
			clusterArn := createResp["clusterArn"].(string)
			encodedArn := url.PathEscape(clusterArn)
			tagPath := "/v1/tags/" + encodedArn

			var rec *httptest.ResponseRecorder

			switch tt.op {
			case "list":
				rec = doKafkaRequest(t, h, http.MethodGet, tagPath, nil)
			case "tag":
				rec = doKafkaRequest(t, h, http.MethodPost, tagPath, map[string]any{
					"tags": map[string]string{"env": "prod"},
				})
			case "untag":
				// First add a tag, then remove it
				doKafkaRequest(t, h, http.MethodPost, tagPath, map[string]any{
					"tags": map[string]string{"env": "prod"},
				})

				e := echo.New()
				req := httptest.NewRequest(http.MethodDelete, tagPath+"?tagKeys=env", http.NoBody)
				rec2 := httptest.NewRecorder()
				c := e.NewContext(req, rec2)
				err := h.Handler()(c)
				require.NoError(t, err)
				rec = rec2
			}

			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// ----------------------------------------
// V2 API tests
// ----------------------------------------

func TestTag_CRUD(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	clusterArn := createTestClusterWithStorage(t, h, "tag-crud-cluster")
	encoded := url.PathEscape(clusterArn)

	// TagResource.
	tagRec := doKafkaRequest(t, h, http.MethodPost, "/v1/tags/"+encoded,
		map[string]any{"tags": map[string]string{"env": "test", "team": "infra"}})
	assert.Equal(t, http.StatusOK, tagRec.Code)

	// ListTagsForResource — both tags present.
	listRec := doKafkaRequest(t, h, http.MethodGet, "/v1/tags/"+encoded, nil)
	require.Equal(t, http.StatusOK, listRec.Code)

	var listResp map[string]any
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &listResp))
	tags, _ := listResp["tags"].(map[string]any)
	assert.Equal(t, "test", tags["env"])
	assert.Equal(t, "infra", tags["team"])

	// UntagResource — remove "team" tag.
	untagValues := url.Values{"tagKeys": []string{"team"}}
	untagRec := doKafkaRequest(
		t,
		h,
		http.MethodDelete,
		"/v1/tags/"+encoded+"?"+untagValues.Encode(),
		nil,
	)
	assert.Equal(t, http.StatusOK, untagRec.Code)

	// ListTagsForResource — only "env" remains.
	listRec2 := doKafkaRequest(t, h, http.MethodGet, "/v1/tags/"+encoded, nil)
	require.Equal(t, http.StatusOK, listRec2.Code)

	var listResp2 map[string]any
	require.NoError(t, json.Unmarshal(listRec2.Body.Bytes(), &listResp2))
	tags2, _ := listResp2["tags"].(map[string]any)
	assert.Equal(t, "test", tags2["env"])
	_, hasTeam := tags2["team"]
	assert.False(t, hasTeam, "team tag should have been removed")
}

func TestTag_ListEmpty(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	clusterArn := createTestClusterWithStorage(t, h, "tag-empty-cluster")
	encoded := url.PathEscape(clusterArn)

	// No tags added — ListTagsForResource returns empty map.
	listRec := doKafkaRequest(t, h, http.MethodGet, "/v1/tags/"+encoded, nil)
	require.Equal(t, http.StatusOK, listRec.Code)

	var listResp map[string]any
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &listResp))
	tags, _ := listResp["tags"].(map[string]any)
	assert.Empty(t, tags, "freshly created cluster should have no tags")
}

func TestTag_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	_, code := doKafkaRequestJSON(t, h, http.MethodGet,
		"/v1/tags/arn%3Aaws%3Akafka%3Aus-east-1%3A000000000000%3Acluster%2Fmissing%2F1", nil)
	assert.Equal(t, http.StatusNotFound, code)
}

// ----------------------------------------
// UpdateClusterConfiguration persists via HTTP (real V1 update path), verified
// through the DescribeClusterV2 read path.
// ----------------------------------------

package applicationautoscaling_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandler_TagResource(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "RegisterScalableTarget", map[string]any{
		"ServiceNamespace":  "ecs",
		"ResourceId":        "service/default/my-svc",
		"ScalableDimension": "ecs:service:DesiredCount",
		"MinCapacity":       int32(1),
		"MaxCapacity":       int32(10),
	})
	require.Equal(t, http.StatusOK, rec.Code)
	var createResp map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
	resourceARN := createResp["ScalableTargetARN"]

	tagRec := doRequest(t, h, "TagResource", map[string]any{
		"ResourceARN": resourceARN,
		"Tags":        map[string]string{"env": "prod", "team": "platform"},
	})
	assert.Equal(t, http.StatusOK, tagRec.Code)
}

func TestHandler_TagResource_Validation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     map[string]any
		name     string
		wantCode int
	}{
		{
			name:     "missing_arn",
			body:     map[string]any{"ResourceARN": "", "Tags": map[string]string{"k": "v"}},
			wantCode: http.StatusBadRequest,
		},
		{
			name: "not_found",
			body: map[string]any{
				"ResourceARN": "arn:aws:autoscaling:us-east-1:000000000000:scalable-target/no-such",
				"Tags":        map[string]string{"k": "v"},
			},
			wantCode: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, "TagResource", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestHandler_TagResource_ExceedsLimit(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Register a scalable target
	regRec := doRequest(t, h, "RegisterScalableTarget", map[string]any{
		"ServiceNamespace":  "ecs",
		"ResourceId":        "service/default/my-svc",
		"ScalableDimension": "ecs:service:DesiredCount",
		"MinCapacity":       int32(1),
		"MaxCapacity":       int32(5),
	})
	require.Equal(t, http.StatusOK, regRec.Code)

	var regResp map[string]any
	require.NoError(t, json.Unmarshal(regRec.Body.Bytes(), &regResp))
	targetARN, _ := regResp["ScalableTargetARN"].(string)
	require.NotEmpty(t, targetARN)

	// Add 50 tags (the maximum allowed)
	tags := make(map[string]string, 50)
	for i := range 50 {
		tags[fmt.Sprintf("key-%d", i)] = "value"
	}
	tagRec := doRequest(t, h, "TagResource", map[string]any{
		"ResourceARN": targetARN,
		"Tags":        tags,
	})
	require.Equal(t, http.StatusOK, tagRec.Code)

	// Adding one more distinct tag should fail with 400
	overRec := doRequest(t, h, "TagResource", map[string]any{
		"ResourceARN": targetARN,
		"Tags":        map[string]string{"new-key": "value"},
	})
	assert.Equal(t, http.StatusBadRequest, overRec.Code)
}

func TestHandler_ListTagsForResource(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "RegisterScalableTarget", map[string]any{
		"ServiceNamespace":  "ecs",
		"ResourceId":        "service/default/my-svc",
		"ScalableDimension": "ecs:service:DesiredCount",
		"MinCapacity":       int32(1),
		"MaxCapacity":       int32(10),
	})
	require.Equal(t, http.StatusOK, rec.Code)
	var createResp map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
	resourceARN := createResp["ScalableTargetARN"]

	doRequest(t, h, "TagResource", map[string]any{
		"ResourceARN": resourceARN,
		"Tags":        map[string]string{"env": "prod"},
	})

	listRec := doRequest(t, h, "ListTagsForResource", map[string]any{"ResourceARN": resourceARN})
	require.Equal(t, http.StatusOK, listRec.Code)

	var listResp map[string]any
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &listResp))
	tags, ok := listResp["Tags"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "prod", tags["env"])
}

func TestHandler_ListTagsForResource_EmptyARN(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "ListTagsForResource", map[string]any{"ResourceARN": ""})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_UntagResource(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "RegisterScalableTarget", map[string]any{
		"ServiceNamespace":  "ecs",
		"ResourceId":        "service/default/my-svc",
		"ScalableDimension": "ecs:service:DesiredCount",
		"MinCapacity":       int32(1),
		"MaxCapacity":       int32(10),
	})
	require.Equal(t, http.StatusOK, rec.Code)
	var createResp map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
	resourceARN := createResp["ScalableTargetARN"]

	doRequest(t, h, "TagResource", map[string]any{
		"ResourceARN": resourceARN,
		"Tags":        map[string]string{"env": "prod", "team": "platform"},
	})

	untagRec := doRequest(t, h, "UntagResource", map[string]any{
		"ResourceARN": resourceARN,
		"TagKeys":     []string{"env"},
	})
	require.Equal(t, http.StatusOK, untagRec.Code)

	listRec := doRequest(t, h, "ListTagsForResource", map[string]any{"ResourceARN": resourceARN})
	var listResp map[string]any
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &listResp))
	tags, ok := listResp["Tags"].(map[string]any)
	require.True(t, ok)
	_, hasEnv := tags["env"]
	assert.False(t, hasEnv)
	assert.Equal(t, "platform", tags["team"])
}

func TestHandler_UntagResource_Validation(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "UntagResource", map[string]any{
		"ResourceARN": "",
		"TagKeys":     []string{"key"},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_TagResource_RoundTrip(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	arn := seedTarget(t, h, "service/default/tagged", 1, 5)

	tagRec := doRequest(t, h, "TagResource", map[string]any{
		"ResourceARN": arn,
		"Tags":        map[string]any{"env": "prod", "team": "platform"},
	})
	assert.Equal(t, http.StatusOK, tagRec.Code)

	listRec := doRequest(t, h, "ListTagsForResource", map[string]any{
		"ResourceARN": arn,
	})
	require.Equal(t, http.StatusOK, listRec.Code)
	var listOut map[string]any
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &listOut))
	tags := listOut["Tags"].(map[string]any)
	assert.Equal(t, "prod", tags["env"])
	assert.Equal(t, "platform", tags["team"])

	untagRec := doRequest(t, h, "UntagResource", map[string]any{
		"ResourceARN": arn,
		"TagKeys":     []string{"env"},
	})
	assert.Equal(t, http.StatusOK, untagRec.Code)

	afterRec := doRequest(t, h, "ListTagsForResource", map[string]any{
		"ResourceARN": arn,
	})
	var afterOut map[string]any
	require.NoError(t, json.Unmarshal(afterRec.Body.Bytes(), &afterOut))
	afterTags := afterOut["Tags"].(map[string]any)
	assert.NotContains(t, afterTags, "env")
	assert.Equal(t, "platform", afterTags["team"])
}

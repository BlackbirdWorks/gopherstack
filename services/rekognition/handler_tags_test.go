package rekognition_test

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/rekognition"
)

func TestRekognition_Tags(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(h *rekognition.Handler) string
		bodyFn   func(arn string) any
		check    func(t *testing.T, body []byte)
		name     string
		action   string
		wantCode int
	}{
		{
			name:   "TagResource and ListTagsForResource round-trip",
			action: "TagResource",
			setup: func(h *rekognition.Handler) string {
				rec := doRequest(t, h, "CreateCollection", map[string]any{"CollectionId": "tag-coll"})
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

				return resp["CollectionArn"].(string)
			},
			bodyFn: func(arn string) any {
				return map[string]any{"ResourceArn": arn, "Tags": map[string]string{"env": "test"}}
			},
			wantCode: http.StatusOK,
		},
		{
			name:   "ListTagsForResource returns tags",
			action: "ListTagsForResource",
			setup: func(h *rekognition.Handler) string {
				rec := doRequest(
					t,
					h,
					"CreateCollection",
					map[string]any{"CollectionId": "ltfr-coll", "Tags": map[string]string{"k": "v"}},
				)
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

				return resp["CollectionArn"].(string)
			},
			bodyFn: func(arn string) any {
				return map[string]any{"ResourceArn": arn}
			},
			wantCode: http.StatusOK,
			check: func(t *testing.T, body []byte) {
				t.Helper()
				var resp map[string]any
				require.NoError(t, json.Unmarshal(body, &resp))
				tags, _ := resp["Tags"].(map[string]any)
				assert.Equal(t, "v", tags["k"])
			},
		},
		{
			name:   "UntagResource removes tag",
			action: "UntagResource",
			setup: func(h *rekognition.Handler) string {
				rec := doRequest(
					t,
					h,
					"CreateCollection",
					map[string]any{"CollectionId": "untag-coll", "Tags": map[string]string{"k": "v"}},
				)
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				arn := resp["CollectionArn"].(string)
				doRequest(t, h, "TagResource", map[string]any{"ResourceArn": arn, "Tags": map[string]string{"k": "v"}})

				return arn
			},
			bodyFn: func(arn string) any {
				return map[string]any{"ResourceArn": arn, "TagKeys": []string{"k"}}
			},
			wantCode: http.StatusOK,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)

			var arn string
			if tc.setup != nil {
				arn = tc.setup(h)
			}

			body := tc.bodyFn(arn)
			rec := doRequest(t, h, tc.action, body)
			assert.Equal(t, tc.wantCode, rec.Code)

			if tc.check != nil {
				tc.check(t, rec.Body.Bytes())
			}
		})
	}
}

// ---------------------------------------------------------------------------
// UntagResource on unknown ARN returns ResourceNotFoundException
// ---------------------------------------------------------------------------

func TestUntagResource_UnknownARN_Returns400(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "UntagResource", map[string]any{
		"ResourceArn": "arn:aws:rekognition:us-east-1:000000000000:collection/no-such",
		"TagKeys":     []string{"k"},
	})
	require.Equal(t, http.StatusBadRequest, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "ResourceNotFoundException", resp["__type"])
}

// ---------------------------------------------------------------------------
// ListTagsForResource on stream processor ARN returns tags
// ---------------------------------------------------------------------------

func TestListTagsForResource_StreamProcessor_ReturnsTags(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "CreateStreamProcessor", map[string]any{
		"Name":    "tagged-proc",
		"RoleArn": "arn:aws:iam::000000000000:role/r",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
	arn := createResp["StreamProcessorArn"].(string)

	// Tag the stream processor.
	doRequest(t, h, "TagResource", map[string]any{
		"ResourceArn": arn,
		"Tags":        map[string]string{"env": "staging", "team": "ml"},
	})

	// List tags via ARN.
	rec = doRequest(t, h, "ListTagsForResource", map[string]any{"ResourceArn": arn})
	require.Equal(t, http.StatusOK, rec.Code)

	var tagResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &tagResp))
	tags, _ := tagResp["Tags"].(map[string]any)
	assert.Equal(t, "staging", tags["env"])
	assert.Equal(t, "ml", tags["team"])
}

// ---------------------------------------------------------------------------
// Tag key/value validation on TagResource
// ---------------------------------------------------------------------------

func TestTagResource_KeyValueValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		tags     map[string]string
		name     string
		wantCode int
	}{
		{
			name:     "valid tags accepted",
			tags:     map[string]string{"env": "prod"},
			wantCode: http.StatusOK,
		},
		{
			name:     "empty key rejected",
			tags:     map[string]string{"": "value"},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "key at limit (128) accepted",
			tags:     map[string]string{strings.Repeat("k", 128): "v"},
			wantCode: http.StatusOK,
		},
		{
			name:     "key over limit (129) rejected",
			tags:     map[string]string{strings.Repeat("k", 129): "v"},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "value at limit (256) accepted",
			tags:     map[string]string{"k": strings.Repeat("v", 256)},
			wantCode: http.StatusOK,
		},
		{
			name:     "value over limit (257) rejected",
			tags:     map[string]string{"k": strings.Repeat("v", 257)},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "empty value accepted",
			tags:     map[string]string{"k": ""},
			wantCode: http.StatusOK,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, "CreateCollection", map[string]any{"CollectionId": "tag-val-coll"})
			require.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			arn := resp["CollectionArn"].(string)

			rec = doRequest(t, h, "TagResource", map[string]any{"ResourceArn": arn, "Tags": tc.tags})
			assert.Equal(t, tc.wantCode, rec.Code, tc.name)
		})
	}
}

func TestTagResource_KeyValidation_ErrorType(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "CreateCollection", map[string]any{"CollectionId": "tag-err-coll"})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	arn := resp["CollectionArn"].(string)

	rec = doRequest(t, h, "TagResource", map[string]any{
		"ResourceArn": arn,
		"Tags":        map[string]string{"": "v"},
	})
	require.Equal(t, http.StatusBadRequest, rec.Code)

	var errResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &errResp))
	assert.Equal(t, "InvalidParameterException", errResp["__type"])
}

// ---------------------------------------------------------------------------
// Tag count limit per resource (max 200)
// ---------------------------------------------------------------------------

func TestTagResource_CountLimit_Enforced(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "CreateCollection", map[string]any{"CollectionId": "tag-limit-coll"})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	arn := resp["CollectionArn"].(string)

	// Add 200 tags in batches of 50 (keys are "tag-000" through "tag-199").
	for batch := range 4 {
		batchTags := make(map[string]string, 50)
		for i := range 50 {
			n := batch*50 + i
			key := "tag-" + string(rune('0'+n/100)) + string(rune('0'+(n/10)%10)) + string(rune('0'+n%10))
			batchTags[key] = "v"
		}

		rec = doRequest(t, h, "TagResource", map[string]any{"ResourceArn": arn, "Tags": batchTags})
		require.Equal(t, http.StatusOK, rec.Code, "batch %d should succeed", batch)
	}

	// Adding one more tag should fail.
	rec = doRequest(t, h, "TagResource", map[string]any{
		"ResourceArn": arn,
		"Tags":        map[string]string{"extra-key": "v"},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestTagResource_UpdateExisting_DoesNotCountAgain(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "CreateCollection", map[string]any{"CollectionId": "tag-update-coll"})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	arn := resp["CollectionArn"].(string)

	// Add one tag, then update it — should not fail due to count.
	doRequest(t, h, "TagResource", map[string]any{"ResourceArn": arn, "Tags": map[string]string{"k": "v1"}})

	rec = doRequest(t, h, "TagResource", map[string]any{"ResourceArn": arn, "Tags": map[string]string{"k": "v2"}})
	assert.Equal(t, http.StatusOK, rec.Code)
}

// ---------------------------------------------------------------------------
// TagResource on unknown ARN
// ---------------------------------------------------------------------------

func TestTagResource_UnknownARN_Returns400(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "TagResource", map[string]any{
		"ResourceArn": "arn:aws:rekognition:us-east-1:000000000000:collection/no-such",
		"Tags":        map[string]string{"k": "v"},
	})
	require.Equal(t, http.StatusBadRequest, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "ResourceNotFoundException", resp["__type"])
}

// ---------------------------------------------------------------------------
// StreamProcessor tag validation via TagResource
// ---------------------------------------------------------------------------

func TestTagResource_StreamProcessor_TagsValidated(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "CreateStreamProcessor", map[string]any{
		"Name":    "tag-proc",
		"RoleArn": "arn:aws:iam::000000000000:role/r",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	arn := resp["StreamProcessorArn"].(string)

	// Valid tag.
	rec = doRequest(t, h, "TagResource", map[string]any{
		"ResourceArn": arn,
		"Tags":        map[string]string{"team": "ml"},
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	// Invalid tag key.
	rec = doRequest(t, h, "TagResource", map[string]any{
		"ResourceArn": arn,
		"Tags":        map[string]string{strings.Repeat("x", 129): "v"},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

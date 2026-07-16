package eventbridge_test

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/eventbridge"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTags_Rule(t *testing.T) {
	t.Parallel()
	e := echo.New()
	b := newBackend()
	h := eventbridge.NewHandler(b)

	_, err := b.PutRule(context.Background(), eventbridge.PutRuleInput{
		Name:         "tagged-rule",
		EventPattern: `{"source":["x"]}`,
	})
	require.NoError(t, err)

	ruleARN := "arn:aws:events:us-east-1:123456789012:rule/default/tagged-rule"

	rec := auditMakeRequest(t, h, e, "TagResource", map[string]any{
		"ResourceARN": ruleARN,
		"Tags":        []map[string]string{{"Key": "owner", "Value": "alice"}},
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = auditMakeRequest(t, h, e, "ListTagsForResource", map[string]any{
		"ResourceARN": ruleARN,
	})
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "alice")
}

func TestHandler_TagOperations(t *testing.T) {
	t.Parallel()

	const resourceARN = "arn:aws:events:us-east-1:123456789012:rule/my-rule"

	tests := []struct {
		wantTags     map[string]string
		name         string
		setupTags    string
		untagKeys    string
		wantTagCount int
	}{
		{
			name: "tag resource then list shows all tags",
			setupTags: `[{"Key":"env","Value":"prod"},` +
				`{"Key":"team","Value":"platform"}]`,
			wantTagCount: 2,
			wantTags:     map[string]string{"env": "prod", "team": "platform"},
		},
		{
			name: "tag then untag one key leaves remaining tag",
			setupTags: `[{"Key":"env","Value":"prod"},` +
				`{"Key":"team","Value":"platform"}]`,
			untagKeys:    `["env"]`,
			wantTagCount: 1,
			wantTags:     map[string]string{"team": "platform"},
		},
		{
			name:         "list tags for resource with no tags returns empty",
			wantTagCount: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := echo.New()
			backend := eventbridge.NewInMemoryBackend()
			handler := eventbridge.NewHandler(backend)

			if tt.setupTags != "" {
				rec := makeRequestWithHandler(t, handler, e, "TagResource",
					`{"ResourceARN":"`+resourceARN+`","Tags":`+tt.setupTags+`}`)
				assert.Equal(t, http.StatusOK, rec.Code)
			}

			if tt.untagKeys != "" {
				rec := makeRequestWithHandler(t, handler, e, "UntagResource",
					`{"ResourceARN":"`+resourceARN+`","TagKeys":`+tt.untagKeys+`}`)
				assert.Equal(t, http.StatusOK, rec.Code)
			}

			rec := makeRequestWithHandler(t, handler, e, "ListTagsForResource",
				`{"ResourceARN":"`+resourceARN+`"}`)
			require.Equal(t, http.StatusOK, rec.Code)

			var listResp struct {
				Tags []struct {
					Key   string `json:"Key"`
					Value string `json:"Value"`
				} `json:"Tags"`
			}
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
			assert.Len(t, listResp.Tags, tt.wantTagCount)

			if len(tt.wantTags) > 0 {
				tagMap := make(map[string]string, tt.wantTagCount)
				for _, tag := range listResp.Tags {
					tagMap[tag.Key] = tag.Value
				}
				for k, v := range tt.wantTags {
					assert.Equal(t, v, tagMap[k])
				}
			}
		})
	}
}

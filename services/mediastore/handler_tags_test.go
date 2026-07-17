package mediastore_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandler_Tags(t *testing.T) {
	t.Parallel()

	type tagOp struct {
		name       string
		op         string
		wantStatus int
		withTag    bool
	}

	tests := []tagOp{
		{
			name:       "tag resource",
			op:         "TagResource",
			wantStatus: http.StatusOK,
		},
		{
			name:       "list tags for resource",
			op:         "ListTagsForResource",
			withTag:    true,
			wantStatus: http.StatusOK,
		},
		{
			name:       "untag resource",
			op:         "UntagResource",
			withTag:    true,
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			setupRec := doRequest(t, h, "CreateContainer", map[string]any{"ContainerName": "tags-test"})
			require.Equal(t, http.StatusOK, setupRec.Code)

			var createResp map[string]any
			require.NoError(t, json.Unmarshal(setupRec.Body.Bytes(), &createResp))
			containerMap, _ := createResp["Container"].(map[string]any)
			containerARN, _ := containerMap["ARN"].(string)

			if tt.withTag {
				tagRec := doRequest(t, h, "TagResource", map[string]any{
					"Resource": containerARN,
					"Tags":     []any{map[string]any{"Key": "env", "Value": "test"}},
				})
				require.Equal(t, http.StatusOK, tagRec.Code)
			}

			var body map[string]any
			switch tt.op {
			case "TagResource":
				body = map[string]any{
					"Resource": containerARN,
					"Tags":     []any{map[string]any{"Key": "env", "Value": "test"}},
				}
			case "UntagResource":
				body = map[string]any{"Resource": containerARN, "TagKeys": []any{"env"}}
			default:
				body = map[string]any{"Resource": containerARN}
			}

			result := doRequest(t, h, tt.op, body)
			assert.Equal(t, tt.wantStatus, result.Code)
		})
	}
}

// TestHandler_TagResource_RoundTrip verifies add, list, partial remove, verify.
// Moved (and de-prefixed) from the former parity_audit1_test.go's
// TestParity_TagResource_RoundTrip.
func TestHandler_TagResource_RoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct { //nolint:govet // test-only struct; layout not performance-critical
		addTags   []any
		removKeys []any
		wantAfter map[string]string
		name      string
	}{
		{
			name: "add_two_remove_one",
			addTags: []any{
				map[string]any{"Key": "a", "Value": "1"},
				map[string]any{"Key": "b", "Value": "2"},
			},
			removKeys: []any{"a"},
			wantAfter: map[string]string{"b": "2"},
		},
		{
			name: "add_then_update",
			addTags: []any{
				map[string]any{"Key": "env", "Value": "staging"},
			},
			removKeys: nil,
			wantAfter: map[string]string{"env": "staging"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			containerARN := createTestContainer(t, h, "tag-roundtrip")

			rec := doRequest(t, h, "TagResource", map[string]any{
				"Resource": containerARN,
				"Tags":     tt.addTags,
			})
			require.Equal(t, http.StatusOK, rec.Code)

			if len(tt.removKeys) > 0 {
				rec = doRequest(t, h, "UntagResource", map[string]any{
					"Resource": containerARN,
					"TagKeys":  tt.removKeys,
				})
				require.Equal(t, http.StatusOK, rec.Code)
			}

			rec = doRequest(t, h, "ListTagsForResource", map[string]any{"Resource": containerARN})
			require.Equal(t, http.StatusOK, rec.Code)

			tagList := unmarshalBody(t, rec)["Tags"].([]any)
			got := make(map[string]string, len(tagList))
			for _, entry := range tagList {
				e := entry.(map[string]any)
				got[e["Key"].(string)] = e["Value"].(string)
			}

			assert.Equal(t, tt.wantAfter, got)
		})
	}
}

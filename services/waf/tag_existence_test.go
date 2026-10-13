package waf_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTagOps_RequireExistingResource(t *testing.T) {
	t.Parallel()

	const missing = "arn:aws:waf::123456789012:webacl/00000000-0000-0000-0000-000000000000"

	tests := []struct {
		body   map[string]any
		name   string
		action string
	}{
		{name: "tag", action: "TagResource", body: map[string]any{
			"ResourceARN": missing, "Tags": []map[string]any{{"Key": "k", "Value": "v"}},
		}},
		{
			name:   "untag",
			action: "UntagResource",
			body:   map[string]any{"ResourceARN": missing, "TagKeys": []string{"k"}},
		},
		{name: "list", action: "ListTagsForResource", body: map[string]any{"ResourceARN": missing}},
		{name: "list_malformed_arn", action: "ListTagsForResource", body: map[string]any{"ResourceARN": "not-an-arn"}},
		{name: "list_unknown_kind", action: "ListTagsForResource", body: map[string]any{
			"ResourceARN": "arn:aws:waf::123456789012:widget/abc",
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newWAFHandler(t)
			rec := wafDo(t, h, tt.action, tt.body)

			require.Equal(t, http.StatusBadRequest, rec.Code, rec.Body.String())
			assert.Contains(t, rec.Body.String(), "WAFNonexistentItemException")
		})
	}
}

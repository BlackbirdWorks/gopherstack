package bedrockruntime_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestGetAsyncInvoke_NoInventedTagsKey_RealClient covers gopherstack-y1zn.
// buildAsyncInvokeResponse emitted "tags" whenever Tags was non-empty;
// neither GetAsyncInvokeOutput nor AsyncInvokeSummary (bedrockruntime@v1.57.1
// api_op_GetAsyncInvoke.go / types/types.go) has a Tags member, and this
// service has no TagResource/ListTagsForResource op at all -- real AWS gives
// no way to read an async invoke's tags back. A typed client silently
// ignores the unknown key, so the proof is the raw body.
func TestGetAsyncInvoke_NoInventedTagsKey_RealClient(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	b := h.Backend

	inv, err := b.StartAsyncInvoke(
		"anthropic.claude-v2", "s3://bucket/out/", "", map[string]string{"env": "test"},
	)
	require.NoError(t, err)

	rec := doRequest(t, h, http.MethodGet, "/async-invoke/"+inv.InvocationArn, nil)
	require.Equal(t, http.StatusOK, rec.Code)

	body := rec.Body.String()
	assert.NotContains(t, body, `"tags"`,
		"neither GetAsyncInvokeOutput nor AsyncInvokeSummary has a tags member")

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	_, hasTags := out["tags"]
	assert.False(t, hasTags)
}

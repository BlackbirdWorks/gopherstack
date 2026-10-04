package bedrockruntime_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	bedrockruntimesdk "github.com/aws/aws-sdk-go-v2/service/bedrockruntime"
	brtypes "github.com/aws/aws-sdk-go-v2/service/bedrockruntime/types"
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

// GetAsyncInvoke declares no ResourceNotFoundException (bedrockruntime@v1.57.1
// deserializers.go), so a missing ARN must surface as the declared ValidationException.
func TestGetAsyncInvoke_UnknownArnIsDeclaredValidation_RealClient(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		arn  string
	}{
		{name: "well formed arn", arn: "arn:aws:bedrock:us-east-1:000000000000:async-invoke/nonexistent"},
		{name: "bare id", arn: "nonexistent-id"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestBedrockRuntimeSDKClient(t, newTestHandler(t))

			_, err := client.GetAsyncInvoke(t.Context(), &bedrockruntimesdk.GetAsyncInvokeInput{
				InvocationArn: aws.String(tt.arn),
			})
			require.Error(t, err)

			var ve *brtypes.ValidationException

			require.ErrorAs(t, err, &ve)
		})
	}
}

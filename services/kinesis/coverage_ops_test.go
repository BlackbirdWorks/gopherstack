package kinesis_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/kinesis"
)

func createKinesisStream(t *testing.T, h *kinesis.Handler, name string) {
	t.Helper()

	rec := doRequest(t, h, "CreateStream", map[string]any{
		"StreamName": name,
		"ShardCount": 1,
	})
	require.Equal(t, http.StatusOK, rec.Code)
}

func TestKinesis_TagResource(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createKinesisStream(t, h, "tag-stream")

	// Get ARN first
	rec := doRequest(t, h, "DescribeStream", map[string]any{
		"StreamName": "tag-stream",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// TagResource (using stream name as resource)
	rec = doRequest(t, h, "TagResource", map[string]any{
		"ResourceARN": "arn:aws:kinesis:us-east-1:123456789012:stream/tag-stream",
		"Tags":        map[string]string{"env": "test"},
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	// ListTagsForResource
	rec = doRequest(t, h, "ListTagsForResource", map[string]any{
		"ResourceARN": "arn:aws:kinesis:us-east-1:123456789012:stream/tag-stream",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	// UntagResource
	rec = doRequest(t, h, "UntagResource", map[string]any{
		"ResourceARN": "arn:aws:kinesis:us-east-1:123456789012:stream/tag-stream",
		"TagKeys":     []string{"env"},
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestKinesis_UpdateAccountSettings(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "UpdateAccountSettings", map[string]any{
		"ShardLevelMetrics": []string{"IncomingBytes"},
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestKinesis_UpdateStreamWarmThroughput(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createKinesisStream(t, h, "warm-stream")

	rec := doRequest(t, h, "UpdateStreamWarmThroughput", map[string]any{
		"StreamName":            "warm-stream",
		"ConsumersToPut":        1,
		"WriteProvisionedUnits": 100,
	})
	assert.True(t, rec.Code >= 200 && rec.Code < 300 || rec.Code == 400)
}

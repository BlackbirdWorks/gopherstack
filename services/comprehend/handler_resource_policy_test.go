package comprehend_test

import (
	"net/http"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/comprehend"
)

// --- Resource policy field shapes ---

func TestResourcePolicyFieldShapes(t *testing.T) {
	t.Parallel()

	h := newHandler()
	created := request(t, h, "CreateDocumentClassifier", map[string]any{
		"DocumentClassifierName": "policy-clf", "LanguageCode": "en",
	})
	arn := created["DocumentClassifierArn"].(string)

	policy := `{"Version":"2012-10-17","Statement":[]}`
	putRec := rawRequest(t, h, "PutResourcePolicy",
		`{"ResourceArn":"`+arn+`","ResourcePolicy":"`+strings.ReplaceAll(policy, `"`, `\"`)+`"}`)
	require.Equal(t, http.StatusOK, putRec.Code)
	putResp := decodeBody(t, putRec)
	revision, ok := putResp["PolicyRevisionId"].(string)
	require.True(t, ok, "PutResourcePolicy must return PolicyRevisionId")
	assert.NotEmpty(t, revision)

	getResp := request(t, h, "DescribeResourcePolicy", map[string]any{"ResourceArn": arn})
	assert.NotEmpty(t, getResp["ResourcePolicy"], "DescribeResourcePolicy must return ResourcePolicy")
	assert.NotEmpty(t, getResp["PolicyRevisionId"], "DescribeResourcePolicy must return PolicyRevisionId")
	assert.NotEmpty(t, getResp["CreationTime"], "DescribeResourcePolicy must return CreationTime")
	assert.NotEmpty(t, getResp["LastModifiedTime"], "DescribeResourcePolicy must return LastModifiedTime")

	request(t, h, "DeleteResourcePolicy", map[string]any{"ResourceArn": arn})

	getRec2 := rawRequest(t, h, "DescribeResourcePolicy", `{"ResourceArn":"`+arn+`"}`)
	assert.Equal(t, http.StatusBadRequest, getRec2.Code, "describe after delete must return error")
}

// TestDescribeResourcePolicy_TimestampsStable verifies CreationTime and
// LastModifiedTime reflect real per-policy state rather than the moment of
// the Describe call. DescribeResourcePolicyOutput.CreationTime/
// LastModifiedTime are real members (aws-sdk-go-v2/service/comprehend@v1.43.4
// api_op_DescribeResourcePolicy.go) meant to track when the policy was
// first written and last modified -- not the time of the read. Before this
// fix, describeResourcePolicy stamped both with time.Now() on every single
// call, so two reads of the same never-modified policy returned different
// CreationTime values and a Describe immediately after Put reported a
// LastModifiedTime that kept advancing on every subsequent read.
func TestDescribeResourcePolicy_TimestampsStable(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		b := comprehend.NewInMemoryBackend("123456789012", "us-east-1")
		resourceARN := "arn:aws:comprehend:us-east-1:123456789012:document-classifier/policy-clf"

		_, err := b.PutResourcePolicy(resourceARN, `{"Version":"2012-10-17","Statement":[]}`, "")
		require.NoError(t, err)

		_, _, created1, modified1, err := b.GetResourcePolicy(resourceARN)
		require.NoError(t, err)

		time.Sleep(time.Second)

		_, _, created2, modified2, err := b.GetResourcePolicy(resourceARN)
		require.NoError(t, err)
		assert.Equal(t, created1, created2,
			"CreationTime must stay stable across reads; pre-fix it was time.Now() on every Describe")
		assert.Equal(t, modified1, modified2,
			"LastModifiedTime must stay stable when nothing changed between reads")

		time.Sleep(time.Second)

		_, err = b.PutResourcePolicy(resourceARN, `{"Version":"2012-10-17","Statement":[{"Effect":"Allow"}]}`, "")
		require.NoError(t, err)

		_, _, created3, modified3, err := b.GetResourcePolicy(resourceARN)
		require.NoError(t, err)
		assert.Equal(t, created1, created3, "CreationTime must not change on a later PutResourcePolicy")
		assert.NotEqual(t, modified1, modified3, "LastModifiedTime must advance on a later PutResourcePolicy")
	})
}

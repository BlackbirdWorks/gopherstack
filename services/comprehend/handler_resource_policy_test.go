package comprehend_test

import (
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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

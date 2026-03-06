package integration_test

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// taggingAPIPost sends a POST to the Resource Groups Tagging API with the given
// X-Amz-Target action and JSON body.
func taggingAPIPost(t *testing.T, action string, body any) *http.Response {
	t.Helper()

	bodyBytes, err := json.Marshal(body)
	require.NoError(t, err)

	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, endpoint, bytes.NewReader(bodyBytes))
	require.NoError(t, err)

	req.Header.Set("Content-Type", "application/x-amz-json-1.1")
	req.Header.Set("X-Amz-Target", "ResourceGroupsTaggingAPI_20170126."+action)

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)

	return resp
}

func taggingAPIBody(t *testing.T, resp *http.Response) string {
	t.Helper()
	defer resp.Body.Close()

	data, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	return string(data)
}

func TestIntegration_TaggingAPI_GetResources_Empty(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	resp := taggingAPIPost(t, "GetResources", map[string]any{})
	body := taggingAPIBody(t, resp)

	assert.Equal(t, http.StatusOK, resp.StatusCode, "body: %s", body)
	assert.Contains(t, body, "ResourceTagMappingList")
}

func TestIntegration_TaggingAPI_GetTagKeys(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	resp := taggingAPIPost(t, "GetTagKeys", map[string]any{})
	body := taggingAPIBody(t, resp)

	assert.Equal(t, http.StatusOK, resp.StatusCode, "body: %s", body)
	assert.Contains(t, body, "TagKeys")
}

func TestIntegration_TaggingAPI_GetTagValues(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	resp := taggingAPIPost(t, "GetTagValues", map[string]any{"Key": "env"})
	body := taggingAPIBody(t, resp)

	assert.Equal(t, http.StatusOK, resp.StatusCode, "body: %s", body)
	assert.Contains(t, body, "TagValues")
}

func TestIntegration_TaggingAPI_TagResources(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	// Create a queue so we have a real ARN to tag.
	sqsClient := createSQSClient(t)
	ctx := t.Context()

	queueName := "tag-api-test-queue-" + t.Name()
	createOut, err := sqsClient.CreateQueue(ctx, &sqs.CreateQueueInput{
		QueueName: aws.String(queueName),
	})
	require.NoError(t, err)
	require.NotNil(t, createOut.QueueUrl)

	// Retrieve the queue ARN.
	attrOut, err := sqsClient.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
		QueueUrl:       createOut.QueueUrl,
		AttributeNames: []sqstypes.QueueAttributeName{sqstypes.QueueAttributeNameQueueArn},
	})
	require.NoError(t, err)

	queueARN := attrOut.Attributes[string(sqstypes.QueueAttributeNameQueueArn)]
	require.NotEmpty(t, queueARN)

	// Tag the queue via the Tagging API.
	resp := taggingAPIPost(t, "TagResources", map[string]any{
		"ResourceARNList": []string{queueARN},
		"Tags":            map[string]string{"integration-test": "true", "service": "sqs"},
	})
	body := taggingAPIBody(t, resp)

	require.Equal(t, http.StatusOK, resp.StatusCode, "body: %s", body)

	// Decode the response and assert no failures.
	var tagOut map[string]any
	require.NoError(t, json.Unmarshal([]byte(body), &tagOut))
	assert.Empty(t, tagOut["FailedResourcesMap"], "expected no tag failures")

	// Verify via GetResources that the tagged queue appears when filtering by tag.
	getResp := taggingAPIPost(t, "GetResources", map[string]any{
		"TagFilters": []map[string]any{
			{"Key": "integration-test", "Values": []string{"true"}},
		},
		"ResourceTypeFilters": []string{"sqs:queue"},
	})
	getBody := taggingAPIBody(t, getResp)

	require.Equal(t, http.StatusOK, getResp.StatusCode, "body: %s", getBody)
	assert.Contains(t, getBody, queueARN, "expected tagged queue to appear in GetResources")
}

func TestIntegration_TaggingAPI_UntagResources(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	resp := taggingAPIPost(t, "UntagResources", map[string]any{
		"ResourceARNList": []string{"arn:aws:sqs:us-east-1:000000000000:nonexistent"},
		"TagKeys":         []string{"test-key"},
	})
	body := taggingAPIBody(t, resp)

	assert.Equal(t, http.StatusOK, resp.StatusCode, "body: %s", body)
}

func TestIntegration_TaggingAPI_GetResources_ByResourceType(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	resp := taggingAPIPost(t, "GetResources", map[string]any{
		"ResourceTypeFilters": []string{"sqs:queue"},
	})
	body := taggingAPIBody(t, resp)

	assert.Equal(t, http.StatusOK, resp.StatusCode, "body: %s", body)
	assert.Contains(t, body, "ResourceTagMappingList")
}

func TestIntegration_TaggingAPI_GetResources_ByTagFilter(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	resp := taggingAPIPost(t, "GetResources", map[string]any{
		"TagFilters": []map[string]any{
			{"Key": "env", "Values": []string{"prod"}},
		},
	})
	body := taggingAPIBody(t, resp)

	assert.Equal(t, http.StatusOK, resp.StatusCode, "body: %s", body)
	assert.Contains(t, body, "ResourceTagMappingList")
}

func TestIntegration_TaggingAPI_UnknownOperation(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	resp := taggingAPIPost(t, "BogusOperation", map[string]any{})
	body := taggingAPIBody(t, resp)

	assert.Equal(t, http.StatusBadRequest, resp.StatusCode, "body: %s", body)
}

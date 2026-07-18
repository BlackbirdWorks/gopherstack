package cloudformation_test

import (
	"net/http"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestHookResults covers GetHookResult, ListHookResults, DescribeChangeSetHooks.
func TestHookResults(t *testing.T) {
	t.Parallel()

	h := newHandler()

	// GetHookResult — unknown token returns SUCCEEDED (no error)
	rec := postForm(t, h, url.Values{
		"Action":          []string{"GetHookResult"},
		"HookResultToken": []string{"unknown-token"},
	}.Encode())
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "SUCCEEDED")

	// ListHookResults
	rec = postForm(t, h, url.Values{
		"Action":          []string{"ListHookResults"},
		"HookResultToken": []string{"unknown-token"},
	}.Encode())
	require.Equal(t, http.StatusOK, rec.Code)

	// DescribeChangeSetHooks
	rec = postForm(t, h, url.Values{
		"Action":        []string{"DescribeChangeSetHooks"},
		"StackName":     []string{"my-stack"},
		"ChangeSetName": []string{"my-changeset"},
	}.Encode())
	require.Equal(t, http.StatusOK, rec.Code)
}

// TestMisc covers SignalResource, RecordHandlerProgress, DescribeEvents.
func TestSignalResourceRecordProgressAndEvents(t *testing.T) {
	t.Parallel()

	h := newHandler()

	// Create a stack to use with SignalResource
	rec := postForm(t, h, url.Values{
		"Action":    []string{"CreateStack"},
		"StackName": []string{"misc-test-stack"},
		"TemplateBody": []string{
			`{"AWSTemplateFormatVersion":"2010-09-09","Resources":{"MyBucket":{"Type":"AWS::S3::Bucket","Properties":{}}}}`,
		},
	}.Encode())
	require.Equal(t, http.StatusOK, rec.Code)

	// SignalResource — valid stack
	rec = postForm(t, h, url.Values{
		"Action":            []string{"SignalResource"},
		"StackName":         []string{"misc-test-stack"},
		"LogicalResourceId": []string{"MyBucket"},
		"UniqueId":          []string{"signal-001"},
		"Status":            []string{"SUCCESS"},
	}.Encode())
	require.Equal(t, http.StatusOK, rec.Code)

	// RecordHandlerProgress
	rec = postForm(t, h, url.Values{
		"Action":          []string{"RecordHandlerProgress"},
		"BearerToken":     []string{"test-bearer-token"},
		"OperationStatus": []string{"IN_PROGRESS"},
	}.Encode())
	require.Equal(t, http.StatusOK, rec.Code)

	// DescribeEvents — returns events across all stacks
	rec = postForm(t, h, url.Values{
		"Action": []string{"DescribeEvents"},
	}.Encode())
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "DescribeEventsResponse")
}

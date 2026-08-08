package cloudformation_test

import (
	"encoding/xml"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cloudformation"
)

type describeStackResourcesResponse struct {
	XMLName xml.Name `xml:"DescribeStackResourcesResponse"`
	Result  struct {
		StackResources []struct {
			LogicalResourceID  string `xml:"LogicalResourceId"`
			PhysicalResourceID string `xml:"PhysicalResourceId"`
			ResourceType       string `xml:"ResourceType"`
		} `xml:"StackResources>member"`
	} `xml:"DescribeStackResourcesResult"`
}

func describeStackResources(
	t *testing.T, h *cloudformation.Handler, stackName string,
) describeStackResourcesResponse {
	t.Helper()
	rec := postForm(t, h, url.Values{
		"Action":    {"DescribeStackResources"},
		"StackName": {stackName},
	}.Encode())
	require.Equal(t, 200, rec.Code, "body: %s", rec.Body.String())

	var resp describeStackResourcesResponse
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))

	return resp
}

// TestExecuteStackRefactor_MovesResourceBetweenStacks drives a real
// CreateStackRefactor + ExecuteStackRefactor request pair through the
// handler and verifies the moved resource leaves the source stack and
// appears in the destination stack via DescribeStackResources on both --
// not merely a status flip.
func TestExecuteStackRefactor_MovesResourceBetweenStacks(t *testing.T) {
	t.Parallel()

	h := newHandler()
	postForm(t, h, url.Values{
		"Action":       {"CreateStack"},
		"StackName":    {"refactor-src"},
		"TemplateBody": {simpleTemplate},
	}.Encode())
	postForm(t, h, url.Values{
		"Action":    {"CreateStack"},
		"StackName": {"refactor-dst"},
		"TemplateBody": {
			`{"AWSTemplateFormatVersion":"2010-09-09",` +
				`"Resources":{"OtherQueue":{"Type":"AWS::SQS::Queue","Properties":{}}}}`,
		},
	}.Encode())

	// Sanity: the resource starts in the source stack only.
	before := describeStackResources(t, h, "refactor-src")
	require.Len(t, before.Result.StackResources, 1)
	physicalID := before.Result.StackResources[0].PhysicalResourceID
	resourceType := before.Result.StackResources[0].ResourceType

	rec := postForm(t, h, url.Values{
		"Action":      {"CreateStackRefactor"},
		"Description": {"move MyBucket to refactor-dst"},
		"ResourceMappings.member.1.Source.StackName":              {"refactor-src"},
		"ResourceMappings.member.1.Source.LogicalResourceId":      {"MyBucket"},
		"ResourceMappings.member.1.Destination.StackName":         {"refactor-dst"},
		"ResourceMappings.member.1.Destination.LogicalResourceId": {"MovedBucket"},
	}.Encode())
	require.Equal(t, 200, rec.Code, "body: %s", rec.Body.String())
	refactorID := extractField(rec.Body.String(), "StackRefactorId")
	require.NotEmpty(t, refactorID)

	rec = postForm(t, h, url.Values{
		"Action":          {"ExecuteStackRefactor"},
		"StackRefactorId": {refactorID},
	}.Encode())
	require.Equal(t, 200, rec.Code, "body: %s", rec.Body.String())

	rec = postForm(t, h, url.Values{
		"Action":          {"DescribeStackRefactor"},
		"StackRefactorId": {refactorID},
	}.Encode())
	require.Equal(t, 200, rec.Code)
	assert.Contains(t, rec.Body.String(), "EXECUTE_COMPLETE")

	after := describeStackResources(t, h, "refactor-src")
	assert.Empty(t, after.Result.StackResources, "resource must have left the source stack")

	dst := describeStackResources(t, h, "refactor-dst")
	require.Len(
		t, dst.Result.StackResources, 2,
		"destination must now have its original resource plus the moved one",
	)

	var moved *struct {
		LogicalResourceID  string `xml:"LogicalResourceId"`
		PhysicalResourceID string `xml:"PhysicalResourceId"`
		ResourceType       string `xml:"ResourceType"`
	}
	for i := range dst.Result.StackResources {
		if dst.Result.StackResources[i].LogicalResourceID == "MovedBucket" {
			moved = &dst.Result.StackResources[i]
		}
	}
	require.NotNil(t, moved, "MovedBucket must be present in the destination stack")
	assert.Equal(t, physicalID, moved.PhysicalResourceID, "the moved resource keeps its physical identity")
	assert.Equal(t, resourceType, moved.ResourceType)
}

// TestExecuteStackRefactor_UnknownRefactorErrors ensures execute on an
// unknown ID fails instead of silently succeeding.
func TestExecuteStackRefactor_UnknownRefactorErrors(t *testing.T) {
	t.Parallel()

	h := newHandler()
	rec := postForm(t, h, url.Values{
		"Action":          {"ExecuteStackRefactor"},
		"StackRefactorId": {"does-not-exist"},
	}.Encode())
	assert.NotEqual(t, 200, rec.Code)
	assert.Contains(t, rec.Body.String(), "StackRefactorNotFoundException")
}

// TestExecuteStackRefactor_MissingSourceResourceErrors ensures execute fails
// (rather than reporting a hollow success) when the source resource does not
// exist, so a partial/invalid refactor cannot report EXECUTE_COMPLETE.
func TestExecuteStackRefactor_MissingSourceResourceErrors(t *testing.T) {
	t.Parallel()

	h := newHandler()
	postForm(t, h, url.Values{
		"Action":       {"CreateStack"},
		"StackName":    {"refactor-missing-src"},
		"TemplateBody": {simpleTemplate},
	}.Encode())
	postForm(t, h, url.Values{
		"Action":       {"CreateStack"},
		"StackName":    {"refactor-missing-dst"},
		"TemplateBody": {simpleTemplate},
	}.Encode())

	rec := postForm(t, h, url.Values{
		"Action":      {"CreateStackRefactor"},
		"Description": {"bad mapping"},
		"ResourceMappings.member.1.Source.StackName":              {"refactor-missing-src"},
		"ResourceMappings.member.1.Source.LogicalResourceId":      {"NoSuchResource"},
		"ResourceMappings.member.1.Destination.StackName":         {"refactor-missing-dst"},
		"ResourceMappings.member.1.Destination.LogicalResourceId": {"NoSuchResource"},
	}.Encode())
	require.Equal(t, 200, rec.Code)
	refactorID := extractField(rec.Body.String(), "StackRefactorId")
	require.NotEmpty(t, refactorID)

	rec = postForm(t, h, url.Values{
		"Action":          {"ExecuteStackRefactor"},
		"StackRefactorId": {refactorID},
	}.Encode())
	assert.NotEqual(t, 200, rec.Code, "executing a refactor whose source resource does not exist must error")

	// Status must remain CREATE_COMPLETE, not silently flip to EXECUTE_COMPLETE.
	rec = postForm(t, h, url.Values{
		"Action":          {"DescribeStackRefactor"},
		"StackRefactorId": {refactorID},
	}.Encode())
	assert.Contains(t, rec.Body.String(), "CREATE_COMPLETE")
	assert.NotContains(t, rec.Body.String(), "EXECUTE_COMPLETE")
}

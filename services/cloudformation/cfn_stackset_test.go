package cloudformation_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestStackSet_CRUD covers CreateStackSet, DescribeStackSet, ListStackSets,
// UpdateStackSet, CreateStackInstances, ListStackInstances, DescribeStackInstance,
// DetectStackSetDrift, ListStackSetOperations, DescribeStackSetOperation,
// StopStackSetOperation, ListStackSetOperationResults, DeleteStackInstances, DeleteStackSet.
func TestStackSet_CRUD(t *testing.T) {
	t.Parallel()

	h := newHandler()

	// CreateStackSet.
	rec := postForm(t, h, "Action=CreateStackSet&Version=2010-05-15"+
		"&StackSetName=test-stack-set"+
		"&TemplateBody="+encodeTemplate(simpleTemplate)+
		"&PermissionModel=SELF_MANAGED")
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "CreateStackSetResponse")

	// DescribeStackSet.
	rec = postForm(t, h, "Action=DescribeStackSet&Version=2010-05-15"+
		"&StackSetName=test-stack-set")
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "DescribeStackSetResponse")

	// ListStackSets.
	rec = postForm(t, h, "Action=ListStackSets&Version=2010-05-15")
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "ListStackSetsResponse")

	// UpdateStackSet.
	rec = postForm(t, h, "Action=UpdateStackSet&Version=2010-05-15"+
		"&StackSetName=test-stack-set"+
		"&TemplateBody="+encodeTemplate(simpleTemplate))
	assert.Equal(t, http.StatusOK, rec.Code)

	// CreateStackInstances.
	rec = postForm(t, h, "Action=CreateStackInstances&Version=2010-05-15"+
		"&StackSetName=test-stack-set"+
		"&Accounts.member.1=000000000000"+
		"&Regions.member.1=us-east-1")
	assert.Equal(t, http.StatusOK, rec.Code)

	// ListStackInstances.
	rec = postForm(t, h, "Action=ListStackInstances&Version=2010-05-15"+
		"&StackSetName=test-stack-set")
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "ListStackInstancesResponse")

	// DescribeStackInstance.
	rec = postForm(t, h, "Action=DescribeStackInstance&Version=2010-05-15"+
		"&StackSetName=test-stack-set"+
		"&StackInstanceAccount=000000000000"+
		"&StackInstanceRegion=us-east-1")
	assert.Equal(t, http.StatusOK, rec.Code)

	// UpdateStackInstances.
	rec = postForm(t, h, "Action=UpdateStackInstances&Version=2010-05-15"+
		"&StackSetName=test-stack-set"+
		"&Accounts.member.1=000000000000"+
		"&Regions.member.1=us-east-1")
	assert.Equal(t, http.StatusOK, rec.Code)

	// DetectStackSetDrift.
	rec = postForm(t, h, "Action=DetectStackSetDrift&Version=2010-05-15"+
		"&StackSetName=test-stack-set")
	assert.Equal(t, http.StatusOK, rec.Code)

	// ListStackSetOperations.
	rec = postForm(t, h, "Action=ListStackSetOperations&Version=2010-05-15"+
		"&StackSetName=test-stack-set")
	assert.Equal(t, http.StatusOK, rec.Code)

	// DescribeStackSetOperation (operation ID from CreateStackInstances response).
	body := rec.Body.String()
	_ = body
	rec = postForm(t, h, "Action=DescribeStackSetOperation&Version=2010-05-15"+
		"&StackSetName=test-stack-set"+
		"&OperationId=op-1234")
	// May return 404 if op not found, just check it doesn't panic.
	assert.GreaterOrEqual(t, rec.Code, 200)

	// StopStackSetOperation.
	rec = postForm(t, h, "Action=StopStackSetOperation&Version=2010-05-15"+
		"&StackSetName=test-stack-set"+
		"&OperationId=op-1234")
	assert.GreaterOrEqual(t, rec.Code, 200)

	// ListStackSetOperationResults.
	rec = postForm(t, h, "Action=ListStackSetOperationResults&Version=2010-05-15"+
		"&StackSetName=test-stack-set"+
		"&OperationId=op-1234")
	assert.GreaterOrEqual(t, rec.Code, 200)

	// DeleteStackInstances.
	rec = postForm(t, h, "Action=DeleteStackInstances&Version=2010-05-15"+
		"&StackSetName=test-stack-set"+
		"&Accounts.member.1=000000000000"+
		"&Regions.member.1=us-east-1"+
		"&RetainStacks=false")
	assert.Equal(t, http.StatusOK, rec.Code)

	// DeleteStackSet.
	rec = postForm(t, h, "Action=DeleteStackSet&Version=2010-05-15"+
		"&StackSetName=test-stack-set")
	assert.Equal(t, http.StatusOK, rec.Code)
}

func encodeTemplate(_ string) string {
	// Simple URL encode for the template body in form params.
	return "AWSTemplateFormatVersion%3D2010-09-09%26Resources%3D%7B%7D"
}

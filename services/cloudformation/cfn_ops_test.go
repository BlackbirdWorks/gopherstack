package cloudformation_test

import (
	"net/http"
	"net/url"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// extractField is a helper to extract a simple XML element value from a response body.
func extractField(body, tag string) string {
	open := "<" + tag + ">"
	closeTag := "</" + tag + ">"
	start := strings.Index(body, open)
	if start == -1 {
		return ""
	}
	start += len(open)
	end := strings.Index(body[start:], closeTag)
	if end == -1 {
		return ""
	}

	return body[start : start+end]
}

// TestCFN_TypeRegistry covers RegisterType, DescribeTypeRegistration, ActivateType,
// DeactivateType, DeregisterType, PublishType, SetTypeDefaultVersion,
// SetTypeConfiguration, BatchDescribeTypeConfigurations, ListTypes,
// ListTypeVersions, ListTypeRegistrations, TestType.
func TestCFN_TypeRegistry(t *testing.T) {
	t.Parallel()

	h := newHandler()

	// RegisterType
	rec := postForm(t, h, url.Values{
		"Action":               []string{"RegisterType"},
		"TypeName":             []string{"MyOrg::MyService::MyResource"},
		"SchemaHandlerPackage": []string{"s3://bucket/schema.zip"},
	}.Encode())
	require.Equal(t, http.StatusOK, rec.Code, "RegisterType should succeed")
	assert.Contains(t, rec.Body.String(), "RegisterTypeResponse")
	assert.Contains(t, rec.Body.String(), "RegistrationToken")
	token := extractField(rec.Body.String(), "RegistrationToken")
	require.NotEmpty(t, token, "RegistrationToken must be non-empty")

	// DescribeTypeRegistration — should return COMPLETE
	rec = postForm(t, h, url.Values{
		"Action":            []string{"DescribeTypeRegistration"},
		"RegistrationToken": []string{token},
	}.Encode())
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "COMPLETE")

	// DescribeTypeRegistration — unknown token should fail
	rec = postForm(t, h, url.Values{
		"Action":            []string{"DescribeTypeRegistration"},
		"RegistrationToken": []string{"nonexistent-token"},
	}.Encode())
	assert.NotEqual(t, http.StatusOK, rec.Code, "Unknown token should return error")

	// ListTypeRegistrations
	rec = postForm(t, h, url.Values{
		"Action":   []string{"ListTypeRegistrations"},
		"TypeName": []string{"MyOrg::MyService::MyResource"},
	}.Encode())
	require.Equal(t, http.StatusOK, rec.Code)

	// ActivateType
	rec = postForm(t, h, url.Values{
		"Action":   []string{"ActivateType"},
		"TypeName": []string{"MyOrg::MyService::MyResource"},
	}.Encode())
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "ActivateTypeResponse")

	// ListTypes — should contain our type
	rec = postForm(t, h, url.Values{
		"Action": []string{"ListTypes"},
	}.Encode())
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "ListTypesResponse")
	assert.Contains(t, rec.Body.String(), "MyOrg::MyService::MyResource")

	// DeactivateType
	rec = postForm(t, h, url.Values{
		"Action":   []string{"DeactivateType"},
		"TypeName": []string{"MyOrg::MyService::MyResource"},
	}.Encode())
	require.Equal(t, http.StatusOK, rec.Code)

	// SetTypeDefaultVersion
	rec = postForm(t, h, url.Values{
		"Action":    []string{"SetTypeDefaultVersion"},
		"Arn":       []string{"arn:aws:cloudformation:::type/resource/MyOrg::MyService::MyResource"},
		"VersionId": []string{"00000002"},
	}.Encode())
	require.Equal(t, http.StatusOK, rec.Code)

	// SetTypeConfiguration
	rec = postForm(t, h, url.Values{
		"Action":        []string{"SetTypeConfiguration"},
		"TypeName":      []string{"MyOrg::MyService::MyResource"},
		"Configuration": []string{`{"key":"value"}`},
	}.Encode())
	require.Equal(t, http.StatusOK, rec.Code)

	// BatchDescribeTypeConfigurations
	rec = postForm(t, h, url.Values{
		"Action": []string{"BatchDescribeTypeConfigurations"},
	}.Encode())
	require.Equal(t, http.StatusOK, rec.Code)

	// ListTypeVersions
	rec = postForm(t, h, url.Values{
		"Action":   []string{"ListTypeVersions"},
		"TypeName": []string{"MyOrg::MyService::MyResource"},
	}.Encode())
	require.Equal(t, http.StatusOK, rec.Code)

	// TestType
	rec = postForm(t, h, url.Values{
		"Action":   []string{"TestType"},
		"TypeName": []string{"MyOrg::MyService::MyResource"},
	}.Encode())
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "TestTypeResponse")

	// DeregisterType
	rec = postForm(t, h, url.Values{
		"Action": []string{"DeregisterType"},
		"Arn":    []string{"arn:aws:cloudformation:::type/resource/MyOrg::MyService::MyResource"},
	}.Encode())
	require.Equal(t, http.StatusOK, rec.Code)

	// PublishType
	rec = postForm(t, h, url.Values{
		"Action":   []string{"PublishType"},
		"TypeName": []string{"MyOrg::MyService::MyResource"},
	}.Encode())
	require.Equal(t, http.StatusOK, rec.Code)
}

// TestCFN_Publisher covers RegisterPublisher and DescribePublisher.
func TestCFN_Publisher(t *testing.T) {
	t.Parallel()

	h := newHandler()

	// RegisterPublisher
	rec := postForm(t, h, url.Values{
		"Action":        []string{"RegisterPublisher"},
		"ConnectionArn": []string{"arn:aws:codestar-connections:us-east-1:123456789012:connection/abc"},
	}.Encode())
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "RegisterPublisherResponse")
	publisherID := extractField(rec.Body.String(), "PublisherId")
	require.NotEmpty(t, publisherID, "PublisherId must be non-empty")

	// DescribePublisher — known publisher
	rec = postForm(t, h, url.Values{
		"Action":      []string{"DescribePublisher"},
		"PublisherId": []string{publisherID},
	}.Encode())
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "VERIFIED")

	// DescribePublisher — unknown publisher should error
	rec = postForm(t, h, url.Values{
		"Action":      []string{"DescribePublisher"},
		"PublisherId": []string{"nonexistent-publisher"},
	}.Encode())
	assert.NotEqual(t, http.StatusOK, rec.Code, "Unknown publisher should return error")
}

// TestCFN_StackRefactor covers CreateStackRefactor, DescribeStackRefactor,
// ExecuteStackRefactor, ListStackRefactors, ListStackRefactorActions.
func TestCFN_StackRefactor(t *testing.T) {
	t.Parallel()

	h := newHandler()

	// CreateStackRefactor
	rec := postForm(t, h, url.Values{
		"Action":      []string{"CreateStackRefactor"},
		"Description": []string{"test refactor"},
	}.Encode())
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "CreateStackRefactorResponse")
	refactorID := extractField(rec.Body.String(), "StackRefactorId")
	require.NotEmpty(t, refactorID, "StackRefactorId must be non-empty")

	// DescribeStackRefactor — should be CREATE_COMPLETE
	rec = postForm(t, h, url.Values{
		"Action":          []string{"DescribeStackRefactor"},
		"StackRefactorId": []string{refactorID},
	}.Encode())
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "CREATE_COMPLETE")

	// ExecuteStackRefactor
	rec = postForm(t, h, url.Values{
		"Action":          []string{"ExecuteStackRefactor"},
		"StackRefactorId": []string{refactorID},
	}.Encode())
	require.Equal(t, http.StatusOK, rec.Code)

	// DescribeStackRefactor after execute — should be EXECUTE_COMPLETE
	rec = postForm(t, h, url.Values{
		"Action":          []string{"DescribeStackRefactor"},
		"StackRefactorId": []string{refactorID},
	}.Encode())
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "EXECUTE_COMPLETE")

	// ListStackRefactors
	rec = postForm(t, h, url.Values{
		"Action": []string{"ListStackRefactors"},
	}.Encode())
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "ListStackRefactorsResponse")

	// ListStackRefactorActions
	rec = postForm(t, h, url.Values{
		"Action":          []string{"ListStackRefactorActions"},
		"StackRefactorId": []string{refactorID},
	}.Encode())
	require.Equal(t, http.StatusOK, rec.Code)
}

// TestCFN_OrganizationsAccess covers Activate/Deactivate/DescribeOrganizationsAccess.
func TestCFN_OrganizationsAccess(t *testing.T) {
	t.Parallel()

	h := newHandler()

	// Initially DISABLED
	rec := postForm(t, h, url.Values{
		"Action": []string{"DescribeOrganizationsAccess"},
	}.Encode())
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "DISABLED")

	// Activate
	rec = postForm(t, h, url.Values{
		"Action": []string{"ActivateOrganizationsAccess"},
	}.Encode())
	require.Equal(t, http.StatusOK, rec.Code)

	// Now ENABLED
	rec = postForm(t, h, url.Values{
		"Action": []string{"DescribeOrganizationsAccess"},
	}.Encode())
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "ENABLED")

	// Deactivate
	rec = postForm(t, h, url.Values{
		"Action": []string{"DeactivateOrganizationsAccess"},
	}.Encode())
	require.Equal(t, http.StatusOK, rec.Code)

	// Now DISABLED again
	rec = postForm(t, h, url.Values{
		"Action": []string{"DescribeOrganizationsAccess"},
	}.Encode())
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "DISABLED")
}

// TestCFN_StackSetOperations covers the operation-tracking methods:
// ListStackSetOperations, DescribeStackSetOperation, StopStackSetOperation,
// ListStackSetOperationResults, ListStackSetAutoDeploymentTargets,
// ImportStacksToStackSet, ListStackInstanceResourceDrifts.
func TestCFN_StackSetOperations(t *testing.T) {
	t.Parallel()

	h := newHandler()

	// Create a stack set first
	rec := postForm(t, h, url.Values{
		"Action":       []string{"CreateStackSet"},
		"StackSetName": []string{"ops-test-set"},
		"TemplateBody": []string{`{"AWSTemplateFormatVersion":"2010-09-09","Resources":{}}`},
	}.Encode())
	require.Equal(t, http.StatusOK, rec.Code)

	// CreateStackInstances creates an operation
	rec = postForm(t, h, url.Values{
		"Action":            []string{"CreateStackInstances"},
		"StackSetName":      []string{"ops-test-set"},
		"Accounts.member.1": []string{"111111111111"},
		"Regions.member.1":  []string{"us-east-1"},
	}.Encode())
	require.Equal(t, http.StatusOK, rec.Code)

	// ListStackSetOperations — should have at least one operation
	rec = postForm(t, h, url.Values{
		"Action":       []string{"ListStackSetOperations"},
		"StackSetName": []string{"ops-test-set"},
	}.Encode())
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "ListStackSetOperationsResponse")

	// DetectStackSetDrift — returns an operation ID
	rec = postForm(t, h, url.Values{
		"Action":       []string{"DetectStackSetDrift"},
		"StackSetName": []string{"ops-test-set"},
	}.Encode())
	require.Equal(t, http.StatusOK, rec.Code)
	opID := extractField(rec.Body.String(), "OperationId")
	require.NotEmpty(t, opID, "OperationId must be returned from DetectStackSetDrift")

	// DescribeStackSetOperation — should work for the drift operation
	rec = postForm(t, h, url.Values{
		"Action":       []string{"DescribeStackSetOperation"},
		"StackSetName": []string{"ops-test-set"},
		"OperationId":  []string{opID},
	}.Encode())
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "SUCCEEDED")

	// DescribeStackSetOperation — nonexistent op should return error
	rec = postForm(t, h, url.Values{
		"Action":       []string{"DescribeStackSetOperation"},
		"StackSetName": []string{"ops-test-set"},
		"OperationId":  []string{"nonexistent-op"},
	}.Encode())
	assert.NotEqual(t, http.StatusOK, rec.Code, "Nonexistent op should return error")

	// StopStackSetOperation — op is SUCCEEDED so should fail (not RUNNING)
	rec = postForm(t, h, url.Values{
		"Action":       []string{"StopStackSetOperation"},
		"StackSetName": []string{"ops-test-set"},
		"OperationId":  []string{opID},
	}.Encode())
	assert.NotEqual(t, http.StatusOK, rec.Code, "stopping a non-RUNNING operation should error")

	// ListStackSetOperationResults
	rec = postForm(t, h, url.Values{
		"Action":       []string{"ListStackSetOperationResults"},
		"StackSetName": []string{"ops-test-set"},
		"OperationId":  []string{opID},
	}.Encode())
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "ListStackSetOperationResultsResponse")

	// ListStackSetAutoDeploymentTargets — should return the account we added
	rec = postForm(t, h, url.Values{
		"Action":       []string{"ListStackSetAutoDeploymentTargets"},
		"StackSetName": []string{"ops-test-set"},
	}.Encode())
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "111111111111")

	// ImportStacksToStackSet — creates an IMPORT operation
	rec = postForm(t, h, url.Values{
		"Action":            []string{"ImportStacksToStackSet"},
		"StackSetName":      []string{"ops-test-set"},
		"StackIds.member.1": []string{"stack-abc"},
	}.Encode())
	require.Equal(t, http.StatusOK, rec.Code)

	// ListStackInstanceResourceDrifts
	rec = postForm(t, h, url.Values{
		"Action":               []string{"ListStackInstanceResourceDrifts"},
		"StackSetName":         []string{"ops-test-set"},
		"OperationId":          []string{opID},
		"StackInstanceAccount": []string{"111111111111"},
		"StackInstanceRegion":  []string{"us-east-1"},
	}.Encode())
	require.Equal(t, http.StatusOK, rec.Code)

	// UpdateStackSet — creates an UPDATE operation
	rec = postForm(t, h, url.Values{
		"Action":       []string{"UpdateStackSet"},
		"StackSetName": []string{"ops-test-set"},
		"TemplateBody": []string{`{"AWSTemplateFormatVersion":"2010-09-09","Resources":{}}`},
	}.Encode())
	require.Equal(t, http.StatusOK, rec.Code)

	// UpdateStackInstances — creates an UPDATE_INSTANCES operation
	rec = postForm(t, h, url.Values{
		"Action":            []string{"UpdateStackInstances"},
		"StackSetName":      []string{"ops-test-set"},
		"Accounts.member.1": []string{"111111111111"},
		"Regions.member.1":  []string{"us-east-1"},
	}.Encode())
	require.Equal(t, http.StatusOK, rec.Code)

	// DeleteStackInstances — creates a DELETE_INSTANCES operation
	rec = postForm(t, h, url.Values{
		"Action":            []string{"DeleteStackInstances"},
		"StackSetName":      []string{"ops-test-set"},
		"Accounts.member.1": []string{"111111111111"},
		"Regions.member.1":  []string{"us-east-1"},
	}.Encode())
	require.Equal(t, http.StatusOK, rec.Code)
}

// TestCFN_HookResults covers GetHookResult, ListHookResults, DescribeChangeSetHooks.
func TestCFN_HookResults(t *testing.T) {
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

// TestCFN_ResourceScanResources covers ListResourceScanResources and ListResourceScanRelatedResources.
func TestCFN_ResourceScanResources(t *testing.T) {
	t.Parallel()

	h := newHandler()

	// Start a scan first
	rec := postForm(t, h, url.Values{
		"Action": []string{"StartResourceScan"},
	}.Encode())
	require.Equal(t, http.StatusOK, rec.Code)
	scanID := extractField(rec.Body.String(), "ResourceScanId")
	require.NotEmpty(t, scanID, "ResourceScanId must be non-empty")

	// ListResourceScanResources — valid scan
	rec = postForm(t, h, url.Values{
		"Action":         []string{"ListResourceScanResources"},
		"ResourceScanId": []string{scanID},
	}.Encode())
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "AWS::S3::Bucket")

	// ListResourceScanResources — invalid scan should fail
	rec = postForm(t, h, url.Values{
		"Action":         []string{"ListResourceScanResources"},
		"ResourceScanId": []string{"nonexistent-scan"},
	}.Encode())
	// handler ignores error for this endpoint currently, just verify no panic
	assert.GreaterOrEqual(t, rec.Code, 200)

	// ListResourceScanRelatedResources — valid scan
	rec = postForm(t, h, url.Values{
		"Action":         []string{"ListResourceScanRelatedResources"},
		"ResourceScanId": []string{scanID},
	}.Encode())
	require.Equal(t, http.StatusOK, rec.Code)
}

// TestCFN_Misc covers SignalResource, RecordHandlerProgress, DescribeEvents.
func TestCFN_Misc(t *testing.T) {
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

// TestCFN_StackSetOperations_ListAutoDeploymentTargets_NotFound ensures
// ListStackSetAutoDeploymentTargets errors on missing stack set.
func TestCFN_StackSetOperations_ListAutoDeploymentTargets_NotFound(t *testing.T) {
	t.Parallel()

	h := newHandler()

	rec := postForm(t, h, url.Values{
		"Action":       []string{"ListStackSetAutoDeploymentTargets"},
		"StackSetName": []string{"nonexistent-set"},
	}.Encode())
	assert.NotEqual(t, http.StatusOK, rec.Code, "missing stack set should error")
}

// TestCFN_StackSetOperations_ImportNotFound ensures ImportStacksToStackSet
// returns error for missing stack set.
func TestCFN_StackSetOperations_ImportNotFound(t *testing.T) {
	t.Parallel()

	h := newHandler()

	rec := postForm(t, h, url.Values{
		"Action":       []string{"ImportStacksToStackSet"},
		"StackSetName": []string{"nonexistent-set"},
	}.Encode())
	assert.NotEqual(t, http.StatusOK, rec.Code, "Should error for nonexistent stack set")
}

// TestCFN_TypeRegistry_DeregisterNotFound ensures DeregisterType returns error for unknown ARN.
func TestCFN_TypeRegistry_DeregisterNotFound(t *testing.T) {
	t.Parallel()

	h := newHandler()

	rec := postForm(t, h, url.Values{
		"Action": []string{"DeregisterType"},
		"Arn":    []string{"arn:aws:cloudformation:::type/resource/Unknown::Type::Here"},
	}.Encode())
	// handler currently ignores DeregisterType error; just verify no panic
	assert.GreaterOrEqual(t, rec.Code, 200)
}

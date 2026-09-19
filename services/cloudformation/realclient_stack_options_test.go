package cloudformation_test

import (
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	cfnsdk "github.com/aws/aws-sdk-go-v2/service/cloudformation"
	"github.com/aws/aws-sdk-go-v2/service/cloudformation/types"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/cloudformation"
)

// TestRealClient_StackOptions proves the CreateStack/UpdateStack/
// CreateChangeSet/ExecuteChangeSet/GetTemplate/ListResourceScan*/
// ListTypeRegistrations/ListTypes/RegisterPublisher/TestType fields the
// reqfielddiff tier-1 sweep found dropped (gopherstack-xhu2t) are now
// declared, applied and observable through the real typed SDK client.
func TestRealClient_StackOptions(t *testing.T) {
	t.Parallel()

	cases := []struct {
		run  func(t *testing.T)
		name string
	}{
		{testCreateStackResourceTypesAllowlist, "create_stack_resource_types_allowlist"},
		{testCreateStackDisableValidation, "create_stack_disable_validation"},
		{testCreateStackEnableTerminationProtection, "create_stack_enable_termination_protection"},
		{testCreateStackAutomatedRetentionValidated, "create_stack_automated_retention_validated"},
		{testExecuteChangeSetDisableRollback, "execute_change_set_disable_rollback"},
		{testCreateChangeSetType, "create_change_set_type"},
		{testGetTemplateStage, "get_template_stage"},
		{testListResourceScansScanTypeFilter, "list_resource_scans_scan_type_filter"},
		{testListResourceScanResourcesPagination, "list_resource_scan_resources_pagination"},
		{testListTypeRegistrationsStatusFilter, "list_type_registrations_status_filter"},
		{testListTypesVisibilityAndProvisioningType, "list_types_visibility_and_provisioning_type"},
		{testRegisterPublisherRequiresAcceptTerms, "register_publisher_requires_accept_terms"},
		{testTestTypeVersionIDValidated, "test_type_version_id_validated"},
		{testCreateStackRetainExceptOnCreate, "create_stack_retain_except_on_create"},
		{testCreateChangeSetResourceTypesThreadedToExecute, "create_change_set_resource_types_threaded_to_execute"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.run(t)
		})
	}
}

func testCreateStackResourceTypesAllowlist(t *testing.T) {
	t.Helper()

	client := newTestHandlerAndClient(t)
	ctx := t.Context()

	// CreateStack itself always succeeds synchronously (real AWS semantics:
	// a create/update/rollback failure surfaces via stack status, not the
	// API call's own error return -- this backend's provisioning is
	// synchronous but still follows that same contract). AWS::SQS::Queue is
	// not covered by an S3-only allowlist, so this stack must roll back.
	_, err := client.CreateStack(ctx, &cfnsdk.CreateStackInput{
		StackName:     aws.String("resourcetypes-denied"),
		TemplateBody:  aws.String(modifiedTemplate),
		ResourceTypes: []string{"AWS::S3::*"},
	})
	require.NoError(t, err)

	descDenied, err := client.DescribeStacks(ctx, &cfnsdk.DescribeStacksInput{
		StackName: aws.String("resourcetypes-denied"),
	})
	require.NoError(t, err)
	require.Len(t, descDenied.Stacks, 1)
	assert.Equal(t, types.StackStatusRollbackComplete, descDenied.Stacks[0].StackStatus)

	out, err := client.CreateStack(ctx, &cfnsdk.CreateStackInput{
		StackName:     aws.String("resourcetypes-allowed"),
		TemplateBody:  aws.String(modifiedTemplate),
		ResourceTypes: []string{"AWS::S3::*", "AWS::SQS::*"},
	})
	require.NoError(t, err)
	assert.NotEmpty(t, aws.ToString(out.StackId))

	descAllowed, err := client.DescribeStacks(ctx, &cfnsdk.DescribeStacksInput{
		StackName: aws.String("resourcetypes-allowed"),
	})
	require.NoError(t, err)
	require.Len(t, descAllowed.Stacks, 1)
	assert.NotEqual(t, types.StackStatusRollbackComplete, descAllowed.Stacks[0].StackStatus)
}

func testCreateStackDisableValidation(t *testing.T) {
	t.Helper()

	client := newTestHandlerAndClient(t)
	ctx := t.Context()

	badTypeTemplate := `{"AWSTemplateFormatVersion":"2010-09-09",` +
		`"Resources":{"Thing":{"Type":"NotARealType","Properties":{}}}}`

	// Without DisableValidation, the unsupported resource type fails
	// intrinsics validation and the stack rolls back.
	_, err := client.CreateStack(ctx, &cfnsdk.CreateStackInput{
		StackName:    aws.String("disablevalidation-off"),
		TemplateBody: aws.String(badTypeTemplate),
	})
	require.NoError(t, err)

	descOff, err := client.DescribeStacks(ctx, &cfnsdk.DescribeStacksInput{
		StackName: aws.String("disablevalidation-off"),
	})
	require.NoError(t, err)
	require.Len(t, descOff.Stacks, 1)
	assert.Equal(t, types.StackStatusRollbackComplete, descOff.Stacks[0].StackStatus)

	// With DisableValidation, the intrinsics check is skipped and the stack
	// completes (the resource itself is created via this backend's stub
	// fallback path for an unrecognized type).
	_, err = client.CreateStack(ctx, &cfnsdk.CreateStackInput{
		StackName:         aws.String("disablevalidation-on"),
		TemplateBody:      aws.String(badTypeTemplate),
		DisableValidation: aws.Bool(true),
	})
	require.NoError(t, err)

	descOn, err := client.DescribeStacks(ctx, &cfnsdk.DescribeStacksInput{
		StackName: aws.String("disablevalidation-on"),
	})
	require.NoError(t, err)
	require.Len(t, descOn.Stacks, 1)
	assert.NotEqual(t, types.StackStatusRollbackComplete, descOn.Stacks[0].StackStatus)
}

func testCreateStackEnableTerminationProtection(t *testing.T) {
	t.Helper()

	client := newTestHandlerAndClient(t)
	ctx := t.Context()

	_, err := client.CreateStack(ctx, &cfnsdk.CreateStackInput{
		StackName:                   aws.String("termprotect-stack"),
		TemplateBody:                aws.String(simpleTemplate),
		EnableTerminationProtection: aws.Bool(true),
	})
	require.NoError(t, err)

	_, err = client.DeleteStack(ctx, &cfnsdk.DeleteStackInput{
		StackName: aws.String("termprotect-stack"),
	})
	require.Error(t, err)

	desc, err := client.DescribeStacks(ctx, &cfnsdk.DescribeStacksInput{
		StackName: aws.String("termprotect-stack"),
	})
	require.NoError(t, err)
	require.Len(t, desc.Stacks, 1)
	assert.True(t, aws.ToBool(desc.Stacks[0].EnableTerminationProtection))
}

func testCreateStackAutomatedRetentionValidated(t *testing.T) {
	t.Helper()

	client := newTestHandlerAndClient(t)
	ctx := t.Context()

	// ResourceTypes/DisableValidation/EnableTerminationProtection all also
	// reachable via UpdateStack -- exercised here for RetainExceptOnCreate's
	// own sibling, ResourceTypes, on the update path specifically.
	_, err := client.CreateStack(ctx, &cfnsdk.CreateStackInput{
		StackName:    aws.String("update-resourcetypes-stack"),
		TemplateBody: aws.String(simpleTemplate),
	})
	require.NoError(t, err)

	_, err = client.UpdateStack(ctx, &cfnsdk.UpdateStackInput{
		StackName:     aws.String("update-resourcetypes-stack"),
		TemplateBody:  aws.String(modifiedTemplate),
		ResourceTypes: []string{"AWS::S3::*"},
	})
	require.NoError(t, err) // UpdateStack itself always succeeds synchronously; see status check below

	desc, err := client.DescribeStacks(ctx, &cfnsdk.DescribeStacksInput{
		StackName: aws.String("update-resourcetypes-stack"),
	})
	require.NoError(t, err)
	require.Len(t, desc.Stacks, 1)
	assert.Equal(t, types.StackStatusUpdateRollbackComplete, desc.Stacks[0].StackStatus)
}

// testExecuteChangeSetDisableRollback drives ExecuteChangeSet against a
// stack name that does not exist yet, so its internal fallback creates a
// brand new stack (change_sets.go's ExecuteChangeSet: UpdateStack fails
// with ErrStackNotFound, then CreateStack runs) -- the one path where
// DisableRollback has a real, provable effect (this backend's UpdateStack
// path has no resource-level rollback machinery at all to suppress).
func testExecuteChangeSetDisableRollback(t *testing.T) {
	t.Helper()

	client := newTestHandlerAndClient(t)
	ctx := t.Context()

	badTypeTemplate := `{"AWSTemplateFormatVersion":"2010-09-09",` +
		`"Resources":{"MyBucket":{"Type":"AWS::S3::Bucket","Properties":{}},` +
		`"Thing":{"Type":"NotARealType","Properties":{}}}}`

	_, err := client.CreateChangeSet(ctx, &cfnsdk.CreateChangeSetInput{
		StackName:     aws.String("execchangeset-disablerollback"),
		ChangeSetName: aws.String("cs-disablerollback"),
		TemplateBody:  aws.String(badTypeTemplate),
	})
	require.NoError(t, err)

	_, err = client.ExecuteChangeSet(ctx, &cfnsdk.ExecuteChangeSetInput{
		StackName:       aws.String("execchangeset-disablerollback"),
		ChangeSetName:   aws.String("cs-disablerollback"),
		DisableRollback: aws.Bool(true),
	})
	require.NoError(t, err) // ExecuteChangeSet itself always succeeds synchronously; see status check below

	desc, err := client.DescribeStacks(ctx, &cfnsdk.DescribeStacksInput{
		StackName: aws.String("execchangeset-disablerollback"),
	})
	require.NoError(t, err)
	require.Len(t, desc.Stacks, 1)
	assert.Equal(t, types.StackStatusCreateFailed, desc.Stacks[0].StackStatus)
}

func testCreateChangeSetType(t *testing.T) {
	t.Helper()

	client := newTestHandlerAndClient(t)
	ctx := t.Context()

	// CREATE against an existing stack is rejected.
	_, err := client.CreateStack(ctx, &cfnsdk.CreateStackInput{
		StackName:    aws.String("changesettype-existing"),
		TemplateBody: aws.String(simpleTemplate),
	})
	require.NoError(t, err)

	_, err = client.CreateChangeSet(ctx, &cfnsdk.CreateChangeSetInput{
		StackName:     aws.String("changesettype-existing"),
		ChangeSetName: aws.String("cs-wrong-create"),
		TemplateBody:  aws.String(modifiedTemplate),
		ChangeSetType: types.ChangeSetTypeCreate,
	})
	require.Error(t, err)

	// UPDATE against a missing stack is rejected.
	_, err = client.CreateChangeSet(ctx, &cfnsdk.CreateChangeSetInput{
		StackName:     aws.String("changesettype-missing"),
		ChangeSetName: aws.String("cs-wrong-update"),
		TemplateBody:  aws.String(simpleTemplate),
		ChangeSetType: types.ChangeSetTypeUpdate,
	})
	require.Error(t, err)

	// IMPORT is rejected outright (unsupported).
	_, err = client.CreateChangeSet(ctx, &cfnsdk.CreateChangeSetInput{
		StackName:     aws.String("changesettype-import"),
		ChangeSetName: aws.String("cs-import"),
		TemplateBody:  aws.String(simpleTemplate),
		ChangeSetType: types.ChangeSetTypeImport,
	})
	require.Error(t, err)

	// The matching, correct type succeeds.
	out, err := client.CreateChangeSet(ctx, &cfnsdk.CreateChangeSetInput{
		StackName:     aws.String("changesettype-existing"),
		ChangeSetName: aws.String("cs-right-update"),
		TemplateBody:  aws.String(modifiedTemplate),
		ChangeSetType: types.ChangeSetTypeUpdate,
	})
	require.NoError(t, err)
	assert.NotEmpty(t, aws.ToString(out.Id))
}

func testGetTemplateStage(t *testing.T) {
	t.Helper()

	client := newTestHandlerAndClient(t)
	ctx := t.Context()

	_, err := client.CreateStack(ctx, &cfnsdk.CreateStackInput{
		StackName:    aws.String("gettemplate-stage-stack"),
		TemplateBody: aws.String(simpleTemplate),
	})
	require.NoError(t, err)

	_, err = client.GetTemplate(ctx, &cfnsdk.GetTemplateInput{
		StackName:     aws.String("gettemplate-stage-stack"),
		TemplateStage: types.TemplateStage("BOGUS"),
	})
	require.Error(t, err)

	out, err := client.GetTemplate(ctx, &cfnsdk.GetTemplateInput{
		StackName:     aws.String("gettemplate-stage-stack"),
		TemplateStage: types.TemplateStageOriginal,
	})
	require.NoError(t, err)
	assert.NotEmpty(t, aws.ToString(out.TemplateBody))
}

func testListResourceScansScanTypeFilter(t *testing.T) {
	t.Helper()

	client := newTestHandlerAndClient(t)
	ctx := t.Context()

	_, err := client.StartResourceScan(ctx, &cfnsdk.StartResourceScanInput{})
	require.NoError(t, err)

	fullOut, err := client.ListResourceScans(ctx, &cfnsdk.ListResourceScansInput{
		ScanTypeFilter: types.ScanTypeFull,
	})
	require.NoError(t, err)
	assert.NotEmpty(t, fullOut.ResourceScanSummaries)

	partialOut, err := client.ListResourceScans(ctx, &cfnsdk.ListResourceScansInput{
		ScanTypeFilter: types.ScanTypePartial,
	})
	require.NoError(t, err)
	assert.Empty(t, partialOut.ResourceScanSummaries)
}

func testListResourceScanResourcesPagination(t *testing.T) {
	t.Helper()

	client := newTestHandlerAndClient(t)
	ctx := t.Context()

	for i := range 5 {
		_, err := client.CreateStack(ctx, &cfnsdk.CreateStackInput{
			StackName:    aws.String("scanresources-stack-" + string(rune('a'+i))),
			TemplateBody: aws.String(simpleTemplate),
		})
		require.NoError(t, err)
	}

	scanOut, err := client.StartResourceScan(ctx, &cfnsdk.StartResourceScanInput{})
	require.NoError(t, err)

	page1, err := client.ListResourceScanResources(ctx, &cfnsdk.ListResourceScanResourcesInput{
		ResourceScanId: scanOut.ResourceScanId,
		MaxResults:     aws.Int32(2),
	})
	require.NoError(t, err)
	assert.Len(t, page1.Resources, 2)
	require.NotNil(t, page1.NextToken)

	page2, err := client.ListResourceScanResources(ctx, &cfnsdk.ListResourceScanResourcesInput{
		ResourceScanId: scanOut.ResourceScanId,
		MaxResults:     aws.Int32(2),
		NextToken:      page1.NextToken,
	})
	require.NoError(t, err)
	assert.Len(t, page2.Resources, 2)
}

func testListTypeRegistrationsStatusFilter(t *testing.T) {
	t.Helper()

	client := newTestHandlerAndClient(t)
	ctx := t.Context()

	_, err := client.RegisterType(ctx, &cfnsdk.RegisterTypeInput{
		TypeName:             aws.String("Acme::ListRegFilter::Resource"),
		SchemaHandlerPackage: aws.String("s3://bucket/schema.zip"),
	})
	require.NoError(t, err)

	completeOut, err := client.ListTypeRegistrations(ctx, &cfnsdk.ListTypeRegistrationsInput{
		TypeName:                 aws.String("Acme::ListRegFilter::Resource"),
		RegistrationStatusFilter: types.RegistrationStatusComplete,
	})
	require.NoError(t, err)
	assert.NotEmpty(t, completeOut.RegistrationTokenList)

	inProgressOut, err := client.ListTypeRegistrations(ctx, &cfnsdk.ListTypeRegistrationsInput{
		TypeName:                 aws.String("Acme::ListRegFilter::Resource"),
		RegistrationStatusFilter: types.RegistrationStatusInProgress,
	})
	require.NoError(t, err)
	assert.Empty(t, inProgressOut.RegistrationTokenList)
}

func testListTypesVisibilityAndProvisioningType(t *testing.T) {
	t.Helper()

	client := newTestHandlerAndClient(t)
	ctx := t.Context()

	_, err := client.ActivateType(ctx, &cfnsdk.ActivateTypeInput{
		TypeName:      aws.String("Acme::ListTypesFilter::Resource"),
		PublicTypeArn: aws.String("arn:aws:cloudformation:us-east-1::type/resource/Acme-ListTypesFilter-Resource"),
	})
	require.NoError(t, err)

	privateOut, err := client.ListTypes(ctx, &cfnsdk.ListTypesInput{Visibility: types.VisibilityPrivate})
	require.NoError(t, err)

	found := false

	for _, ts := range privateOut.TypeSummaries {
		if aws.ToString(ts.TypeName) == "Acme::ListTypesFilter::Resource" {
			found = true
		}
	}

	assert.True(t, found, "activated type must appear under Visibility=PRIVATE")

	publicOut, err := client.ListTypes(ctx, &cfnsdk.ListTypesInput{Visibility: types.VisibilityPublic})
	require.NoError(t, err)

	for _, ts := range publicOut.TypeSummaries {
		assert.NotEqual(t, "Acme::ListTypesFilter::Resource", aws.ToString(ts.TypeName))
	}

	immutableOut, err := client.ListTypes(
		ctx, &cfnsdk.ListTypesInput{ProvisioningType: types.ProvisioningTypeImmutable},
	)
	require.NoError(t, err)
	assert.Empty(t, immutableOut.TypeSummaries)
}

func testRegisterPublisherRequiresAcceptTerms(t *testing.T) {
	t.Helper()

	client := newTestHandlerAndClient(t)
	ctx := t.Context()

	_, err := client.RegisterPublisher(ctx, &cfnsdk.RegisterPublisherInput{
		ConnectionArn: aws.String("arn:aws:codestar-connections:us-east-1:123456789012:connection/nope"),
	})
	require.Error(t, err)

	out, err := client.RegisterPublisher(ctx, &cfnsdk.RegisterPublisherInput{
		ConnectionArn:            aws.String("arn:aws:codestar-connections:us-east-1:123456789012:connection/yes"),
		AcceptTermsAndConditions: aws.Bool(true),
	})
	require.NoError(t, err)
	assert.NotEmpty(t, aws.ToString(out.PublisherId))
}

func testTestTypeVersionIDValidated(t *testing.T) {
	t.Helper()

	client := newTestHandlerAndClient(t)
	ctx := t.Context()

	regOut, err := client.RegisterType(ctx, &cfnsdk.RegisterTypeInput{
		TypeName:             aws.String("Acme::TestTypeVersion::Resource"),
		SchemaHandlerPackage: aws.String("s3://bucket/schema.zip"),
	})
	require.NoError(t, err)

	regStatus, err := client.DescribeTypeRegistration(ctx, &cfnsdk.DescribeTypeRegistrationInput{
		RegistrationToken: regOut.RegistrationToken,
	})
	require.NoError(t, err)
	require.NotNil(t, regStatus.TypeVersionArn)

	// An unknown VersionId is rejected.
	_, err = client.TestType(ctx, &cfnsdk.TestTypeInput{
		TypeName:  aws.String("Acme::TestTypeVersion::Resource"),
		VersionId: aws.String("99999999"),
	})
	require.Error(t, err)

	// Omitting VersionId (default version) still succeeds.
	testOut, err := client.TestType(ctx, &cfnsdk.TestTypeInput{
		TypeName: aws.String("Acme::TestTypeVersion::Resource"),
	})
	require.NoError(t, err)
	assert.NotEmpty(t, aws.ToString(testOut.TypeVersionArn))
}

// testCreateStackRetainExceptOnCreate proves the CreateStack.
// RetainExceptOnCreate fix directly: this backend's own rollback-on-create-
// failure path (rollbackCreateResources) previously force-deleted every
// newly-created resource regardless of DeletionPolicy, silently behaving as
// if RetainExceptOnCreate were always true. With the fix, a Retain-policy
// resource created before a later resource's creation fails is left in
// place unless RetainExceptOnCreate=true is explicitly requested.
//
// Deterministically forcing a mid-provisioning creation failure (as opposed
// to a pre-flight template validation failure, which never reaches
// rollbackCreateResources at all) requires the ResourceCreator.
// InjectCreateHook test-only mechanism this package's own
// TestBackend_CreateStack_RollbackDeleteFails already established for the
// identical class of scenario -- there is no way to trigger it through
// well-formed request input alone. The request itself is still driven
// through the real typed SDK client end to end.
func testCreateStackRetainExceptOnCreate(t *testing.T) {
	t.Helper()

	runRetainExceptOnCreate(t, false, true)
	runRetainExceptOnCreate(t, true, false)
}

func runRetainExceptOnCreate(t *testing.T, retainExceptOnCreate, wantBucketSurvives bool) {
	t.Helper()

	backends := newServiceBackends()
	creator := cloudformation.NewResourceCreator(backends)
	backend := cloudformation.NewInMemoryBackendWithConfig("000000000000", rtTestRegion, creator)

	creator.InjectCreateHook(func(resourceType string) error {
		if resourceType == "AWS::SQS::Queue" {
			return errSimulatedCreate
		}

		return nil
	})

	h := cloudformation.NewHandler(backend)
	e := echo.New()
	registry := service.NewRegistry()
	require.NoError(t, registry.Register(h))
	e.Use(service.NewServiceRouter(registry).RouteHandler())
	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	cfg, err := awscfg.LoadDefaultConfig(
		t.Context(),
		awscfg.WithRegion(rtTestRegion),
		awscfg.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	require.NoError(t, err)

	client := cfnsdk.NewFromConfig(cfg, func(o *cfnsdk.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
	})

	// MyBucket sorts before MyQueue in topoSortResources' alphabetical
	// tie-break, so it is created (with DeletionPolicy=Retain) before
	// MyQueue's injected failure triggers rollback.
	tmpl := `{"AWSTemplateFormatVersion":"2010-09-09","Resources":{` +
		`"MyBucket":{"Type":"AWS::S3::Bucket","DeletionPolicy":"Retain","Properties":{}},` +
		`"MyQueue":{"Type":"AWS::SQS::Queue","Properties":{}}}}`

	_, err = client.CreateStack(t.Context(), &cfnsdk.CreateStackInput{
		StackName:            aws.String("retainexceptoncreate-stack"),
		TemplateBody:         aws.String(tmpl),
		RetainExceptOnCreate: aws.Bool(retainExceptOnCreate),
	})
	require.NoError(t, err)

	// A Retain-policy resource is deregistered from the stack on rollback
	// either way (matching this backend's existing DeleteStack/
	// deleteStaleResources convention for Retain -- see stacks.go), so
	// DescribeStackResource can't distinguish "retained" from "deleted";
	// recover the bucket's physical name from its CREATE_COMPLETE event and
	// check the underlying S3 resource directly, the way
	// TestBackend_CreateStack_RollbackDeleteFails already does for the
	// sibling ROLLBACK_FAILED scenario.
	events, err := backend.DescribeEvents("retainexceptoncreate-stack", "", false)
	require.NoError(t, err)

	var bucketPhysicalID string

	for _, e := range events.Data {
		if e.LogicalResourceID == "MyBucket" && e.ResourceStatus == "CREATE_COMPLETE" {
			bucketPhysicalID = e.PhysicalResourceID

			break
		}
	}

	require.NotEmpty(t, bucketPhysicalID, "MyBucket must have been created before MyQueue's injected failure")

	_, headErr := backends.S3.Backend.HeadBucket(t.Context(), &awss3.HeadBucketInput{
		Bucket: aws.String(bucketPhysicalID),
	})

	if wantBucketSurvives {
		assert.NoError(t, headErr, "Retain-policy bucket must survive rollback by default")
	} else {
		assert.Error(t, headErr, "RetainExceptOnCreate=true must force-delete the Retain-policy bucket on rollback")
	}
}

// testCreateChangeSetResourceTypesThreadedToExecute proves the fix for a
// previously-dropped CreateChangeSetInput field: ResourceTypes (and
// DisableValidation) were read nowhere by handleCreateChangeSet, unlike
// CreateStack/UpdateStack which already honor them -- so a change set's own
// resource-type allowlist had no effect once executed. ResourceTypes is now
// stored on the ChangeSet and threaded into ExecuteChangeSet's internal
// CreateStack/UpdateStack call, the same way Capabilities already is.
func testCreateChangeSetResourceTypesThreadedToExecute(t *testing.T) {
	t.Helper()

	client := newTestHandlerAndClient(t)
	ctx := t.Context()

	// modifiedTemplate has an S3 bucket and an SQS queue; an S3-only
	// allowlist must deny it once executed against a brand-new stack.
	_, err := client.CreateChangeSet(ctx, &cfnsdk.CreateChangeSetInput{
		StackName:     aws.String("changeset-resourcetypes-denied"),
		ChangeSetName: aws.String("cs-resourcetypes-denied"),
		TemplateBody:  aws.String(modifiedTemplate),
		ResourceTypes: []string{"AWS::S3::*"},
	})
	require.NoError(t, err)

	_, err = client.ExecuteChangeSet(ctx, &cfnsdk.ExecuteChangeSetInput{
		StackName:     aws.String("changeset-resourcetypes-denied"),
		ChangeSetName: aws.String("cs-resourcetypes-denied"),
	})
	require.NoError(t, err) // ExecuteChangeSet itself always succeeds synchronously; see status check below

	descDenied, err := client.DescribeStacks(ctx, &cfnsdk.DescribeStacksInput{
		StackName: aws.String("changeset-resourcetypes-denied"),
	})
	require.NoError(t, err)
	require.Len(t, descDenied.Stacks, 1)
	assert.Equal(t, types.StackStatusRollbackComplete, descDenied.Stacks[0].StackStatus)

	// Without the allowlist, the identical template succeeds -- proving the
	// denial above came from ResourceTypes, not the template itself.
	_, err = client.CreateChangeSet(ctx, &cfnsdk.CreateChangeSetInput{
		StackName:     aws.String("changeset-resourcetypes-allowed"),
		ChangeSetName: aws.String("cs-resourcetypes-allowed"),
		TemplateBody:  aws.String(modifiedTemplate),
	})
	require.NoError(t, err)

	_, err = client.ExecuteChangeSet(ctx, &cfnsdk.ExecuteChangeSetInput{
		StackName:     aws.String("changeset-resourcetypes-allowed"),
		ChangeSetName: aws.String("cs-resourcetypes-allowed"),
	})
	require.NoError(t, err)

	descAllowed, err := client.DescribeStacks(ctx, &cfnsdk.DescribeStacksInput{
		StackName: aws.String("changeset-resourcetypes-allowed"),
	})
	require.NoError(t, err)
	require.Len(t, descAllowed.Stacks, 1)
	assert.Equal(t, types.StackStatusCreateComplete, descAllowed.Stacks[0].StackStatus)
}

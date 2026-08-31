package cloudformation_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	cfnsdk "github.com/aws/aws-sdk-go-v2/service/cloudformation"
	cfnsdktypes "github.com/aws/aws-sdk-go-v2/service/cloudformation/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestListStacks_ItemFields_RealClient drives ListStacks and DescribeStacks
// through the real aws-sdk-go-v2 client (gopherstack-21my). types.StackSummary
// (cloudformation@v1.76.1 types/types.go:3102) carries StackStatusReason,
// LastUpdatedTime and DeletionTime; gopherstack's ListStacks item shape
// dropped all three even though the backend's Stack/StackSummary models
// already track them (models.go's stack.StackStatusReason is set on failure,
// stack.LastUpdatedTime on UpdateStack, stack.DeletionTime on DeleteStack).
// DescribeStacks dropped LastUpdatedTime and DeletionTime too (StackStatusReason
// was already correct there) -- a shared gap between the singular and plural
// forms, confirmed by hand-reverting.
func TestListStacks_ItemFields_RealClient(t *testing.T) {
	t.Parallel()

	client := newTestHandlerAndClient(t)

	failTemplate := `{
		"AWSTemplateFormatVersion": "2010-09-09",
		"Resources": {
			"Bucket": {
				"Type": "AWS::S3::Bucket",
				"Properties": {"BucketName": {"Fn::ImportValue": "cfn21my-nonexistent-export"}}
			}
		}
	}`
	_, err := client.CreateStack(t.Context(), &cfnsdk.CreateStackInput{
		StackName:    aws.String("cfn21my-fail-stack"),
		TemplateBody: aws.String(failTemplate),
	})
	require.NoError(t, err)

	okTemplate := `{"Resources":{"Bucket":{"Type":"AWS::S3::Bucket"}}}`
	_, err = client.CreateStack(t.Context(), &cfnsdk.CreateStackInput{
		StackName:    aws.String("cfn21my-updated-stack"),
		TemplateBody: aws.String(okTemplate),
	})
	require.NoError(t, err)
	_, err = client.UpdateStack(t.Context(), &cfnsdk.UpdateStackInput{
		StackName:    aws.String("cfn21my-updated-stack"),
		TemplateBody: aws.String(okTemplate),
		Tags:         []cfnsdktypes.Tag{{Key: aws.String("k"), Value: aws.String("v")}},
	})
	require.NoError(t, err)

	_, err = client.CreateStack(t.Context(), &cfnsdk.CreateStackInput{
		StackName:    aws.String("cfn21my-deleted-stack"),
		TemplateBody: aws.String(okTemplate),
	})
	require.NoError(t, err)
	_, err = client.DeleteStack(t.Context(), &cfnsdk.DeleteStackInput{
		StackName: aws.String("cfn21my-deleted-stack"),
	})
	require.NoError(t, err)

	listOut, err := client.ListStacks(t.Context(), &cfnsdk.ListStacksInput{})
	require.NoError(t, err)

	byName := make(map[string]int)
	for i, s := range listOut.StackSummaries {
		byName[aws.ToString(s.StackName)] = i
	}

	failIdx, ok := byName["cfn21my-fail-stack"]
	require.True(t, ok, "cfn21my-fail-stack missing from ListStacks")
	assert.NotEmpty(t, aws.ToString(listOut.StackSummaries[failIdx].StackStatusReason),
		"ListStacks: StackStatusReason empty on a CREATE_FAILED stack")

	updIdx, ok := byName["cfn21my-updated-stack"]
	require.True(t, ok, "cfn21my-updated-stack missing from ListStacks")
	require.NotNil(t, listOut.StackSummaries[updIdx].LastUpdatedTime,
		"ListStacks: LastUpdatedTime nil on an updated stack")
	assert.False(t, listOut.StackSummaries[updIdx].LastUpdatedTime.IsZero())

	delIdx, ok := byName["cfn21my-deleted-stack"]
	require.True(t, ok, "cfn21my-deleted-stack missing from ListStacks")
	require.NotNil(t, listOut.StackSummaries[delIdx].DeletionTime,
		"ListStacks: DeletionTime nil on a deleted stack")
	assert.False(t, listOut.StackSummaries[delIdx].DeletionTime.IsZero())

	descUpd, err := client.DescribeStacks(t.Context(), &cfnsdk.DescribeStacksInput{
		StackName: aws.String("cfn21my-updated-stack"),
	})
	require.NoError(t, err)
	require.Len(t, descUpd.Stacks, 1)
	require.NotNil(t, descUpd.Stacks[0].LastUpdatedTime,
		"DescribeStacks: LastUpdatedTime nil on an updated stack")

	descDel, err := client.DescribeStacks(t.Context(), &cfnsdk.DescribeStacksInput{
		StackName: aws.String("cfn21my-deleted-stack"),
	})
	require.NoError(t, err)
	require.Len(t, descDel.Stacks, 1)
	require.NotNil(t, descDel.Stacks[0].DeletionTime,
		"DescribeStacks: DeletionTime nil on a deleted stack")
}

// TestListChangeSets_ItemFields_RealClient drives ListChangeSets through the
// real client (gopherstack-21my). types.ChangeSetSummary (cloudformation@v1.76.1
// types/types.go:257) carries ExecutionStatus and StatusReason; gopherstack's
// ListChangeSets item shape dropped both even though DescribeChangeSet (the
// singular sibling) emits them correctly and the backend's ChangeSet model
// already tracks both (change_sets.go sets ExecutionStatus="AVAILABLE" at
// creation and both ExecutionStatus="UNAVAILABLE"/StatusReason on a no-op
// change set), confirmed by hand-reverting.
func TestListChangeSets_ItemFields_RealClient(t *testing.T) {
	t.Parallel()

	client := newTestHandlerAndClient(t)

	stackName := "cfn21my-cs-stack"
	okTemplate := `{"Resources":{"Bucket":{"Type":"AWS::S3::Bucket"}}}`
	_, err := client.CreateStack(t.Context(), &cfnsdk.CreateStackInput{
		StackName:    aws.String(stackName),
		TemplateBody: aws.String(okTemplate),
	})
	require.NoError(t, err)

	changedTemplate := `{"Resources":{"Bucket":{"Type":"AWS::S3::Bucket"},` +
		`"Queue":{"Type":"AWS::SQS::Queue"}}}`
	_, err = client.CreateChangeSet(t.Context(), &cfnsdk.CreateChangeSetInput{
		StackName:     aws.String(stackName),
		ChangeSetName: aws.String("cfn21my-cs-available"),
		TemplateBody:  aws.String(changedTemplate),
	})
	require.NoError(t, err)

	_, err = client.CreateChangeSet(t.Context(), &cfnsdk.CreateChangeSetInput{
		StackName:     aws.String(stackName),
		ChangeSetName: aws.String("cfn21my-cs-unavailable"),
		TemplateBody:  aws.String(okTemplate),
	})
	require.NoError(t, err)

	out, err := client.ListChangeSets(t.Context(), &cfnsdk.ListChangeSetsInput{
		StackName: aws.String(stackName),
	})
	require.NoError(t, err)

	byName := make(map[string]int)
	for i, s := range out.Summaries {
		byName[aws.ToString(s.ChangeSetName)] = i
	}

	availIdx, ok := byName["cfn21my-cs-available"]
	require.True(t, ok, "cfn21my-cs-available missing from ListChangeSets")
	assert.Equal(t, "AVAILABLE", string(out.Summaries[availIdx].ExecutionStatus))

	unavailIdx, ok := byName["cfn21my-cs-unavailable"]
	require.True(t, ok, "cfn21my-cs-unavailable missing from ListChangeSets")
	assert.Equal(t, "UNAVAILABLE", string(out.Summaries[unavailIdx].ExecutionStatus))
	assert.NotEmpty(t, aws.ToString(out.Summaries[unavailIdx].StatusReason),
		"ListChangeSets: StatusReason empty on a no-op change set")
}

// TestStackSet_ItemFields_RealClient drives ListStackSets and
// DescribeStackSet through the real client (gopherstack-21my).
// types.StackSetSummary carries Description; gopherstack's own
// StackSetSummary model already tracks it (backend.ListStackSets populates
// ss.Description) but the ListStackSets handler's local summXML struct never
// declared or mapped the field, so it never reached the wire. Separately,
// types.StackSet (the DescribeStackSet sibling) carries TemplateBody, which
// this backend's StackSet model also tracks (set at CreateStackSet and
// UpdateStackSet) but the handler's ssXML -- despite a comment claiming it
// was "field-diffed against ... awsAwsquery_deserializeDocumentStackSet" --
// never emitted it. Both confirmed by hand-reverting.
func TestStackSet_ItemFields_RealClient(t *testing.T) {
	t.Parallel()

	client := newTestHandlerAndClient(t)

	stackSetName := "cfn21my-stackset"
	description := "cfn21my distinguishable description"
	templateBody := `{"Resources":{"Bucket":{"Type":"AWS::S3::Bucket"}}}`

	_, err := client.CreateStackSet(t.Context(), &cfnsdk.CreateStackSetInput{
		StackSetName: aws.String(stackSetName),
		Description:  aws.String(description),
		TemplateBody: aws.String(templateBody),
	})
	require.NoError(t, err)

	listOut, err := client.ListStackSets(t.Context(), &cfnsdk.ListStackSetsInput{})
	require.NoError(t, err)
	require.Len(t, listOut.Summaries, 1)
	assert.Equal(t, description, aws.ToString(listOut.Summaries[0].Description),
		"ListStackSets: Description missing")

	descOut, err := client.DescribeStackSet(t.Context(), &cfnsdk.DescribeStackSetInput{
		StackSetName: aws.String(stackSetName),
	})
	require.NoError(t, err)
	assert.Equal(t, templateBody, aws.ToString(descOut.StackSet.TemplateBody),
		"DescribeStackSet: TemplateBody missing")
}

// TestStackInstance_ItemFields_RealClient drives ListStackInstances and
// DescribeStackInstance through the real client (gopherstack-21my).
// types.StackInstance/StackInstanceSummary have no StackSetName member at
// all -- only StackSetId. gopherstack's local instXML in both handlers
// emitted a "StackSetName" element instead, so a real client's StackSetId
// was unconditionally empty despite this backend's own StackInstance model
// (models.go) already tracking StackSetID under the correct "StackSetId" xml
// tag; StackId, StatusReason, DriftStatus and LastOperationId were also
// tracked on the model but never reached either handler's wire shape.
// Confirmed by hand-reverting.
func TestStackInstance_ItemFields_RealClient(t *testing.T) {
	t.Parallel()

	client := newTestHandlerAndClient(t)

	stackSetName := "cfn21my-instances-stackset"
	templateBody := `{"Resources":{"Bucket":{"Type":"AWS::S3::Bucket"}}}`

	createOut, err := client.CreateStackSet(t.Context(), &cfnsdk.CreateStackSetInput{
		StackSetName: aws.String(stackSetName),
		TemplateBody: aws.String(templateBody),
	})
	require.NoError(t, err)
	stackSetID := aws.ToString(createOut.StackSetId)
	require.NotEmpty(t, stackSetID)

	_, err = client.CreateStackInstances(t.Context(), &cfnsdk.CreateStackInstancesInput{
		StackSetName: aws.String(stackSetName),
		Accounts:     []string{"123456789012"},
		Regions:      []string{"us-east-1"},
	})
	require.NoError(t, err)

	listOut, err := client.ListStackInstances(t.Context(), &cfnsdk.ListStackInstancesInput{
		StackSetName: aws.String(stackSetName),
	})
	require.NoError(t, err)
	require.Len(t, listOut.Summaries, 1)
	assert.Equal(t, stackSetID, aws.ToString(listOut.Summaries[0].StackSetId),
		"ListStackInstances: StackSetId empty/wrong")
	assert.NotEmpty(t, aws.ToString(listOut.Summaries[0].StackId),
		"ListStackInstances: StackId empty")

	descOut, err := client.DescribeStackInstance(t.Context(), &cfnsdk.DescribeStackInstanceInput{
		StackSetName:         aws.String(stackSetName),
		StackInstanceAccount: aws.String("123456789012"),
		StackInstanceRegion:  aws.String("us-east-1"),
	})
	require.NoError(t, err)
	require.NotNil(t, descOut.StackInstance)
	assert.Equal(t, stackSetID, aws.ToString(descOut.StackInstance.StackSetId),
		"DescribeStackInstance: StackSetId empty/wrong")
	assert.NotEmpty(t, aws.ToString(descOut.StackInstance.StackId),
		"DescribeStackInstance: StackId empty")
}

// TestListTypes_ItemFields_RealClient drives ListTypes through the real
// client (gopherstack-21my). types.TypeSummary carries DefaultVersionId and
// IsActivated; DescribeType (the singular sibling) already emits both
// correctly, and this backend's type registry already tracks both
// (RegisteredType.DefaultVersion is set at RegisterType, .IsActivated is
// set by ActivateType/DeactivateType) but the ListTypes handler's local
// typeXML struct never declared or mapped either, so a real client's
// IsActivated was always false and DefaultVersionId always empty regardless
// of activation state. Confirmed by hand-reverting. (TypeSummary.Description
// -- mapped in the backend from RegisteredType.Configuration, which no
// backend path ever sets -- is recorded separately as an unbacked gap, not
// fixed here: it is unconditionally empty upstream of the handler too.)
func TestListTypes_ItemFields_RealClient(t *testing.T) {
	t.Parallel()

	client := newTestHandlerAndClient(t)

	_, err := client.RegisterType(t.Context(), &cfnsdk.RegisterTypeInput{
		TypeName:             aws.String("AWS::Cfn21my::Inactive"),
		SchemaHandlerPackage: aws.String("s3://bucket/inactive.zip"),
	})
	require.NoError(t, err)

	_, err = client.RegisterType(t.Context(), &cfnsdk.RegisterTypeInput{
		TypeName:             aws.String("AWS::Cfn21my::Active"),
		SchemaHandlerPackage: aws.String("s3://bucket/active.zip"),
	})
	require.NoError(t, err)
	_, err = client.ActivateType(t.Context(), &cfnsdk.ActivateTypeInput{
		TypeName: aws.String("AWS::Cfn21my::Active"),
	})
	require.NoError(t, err)

	out, err := client.ListTypes(t.Context(), &cfnsdk.ListTypesInput{})
	require.NoError(t, err)

	byName := make(map[string]int)
	for i, ts := range out.TypeSummaries {
		byName[aws.ToString(ts.TypeName)] = i
	}

	inactiveIdx, ok := byName["AWS::Cfn21my::Inactive"]
	require.True(t, ok, "AWS::Cfn21my::Inactive missing from ListTypes")
	assert.False(t, aws.ToBool(out.TypeSummaries[inactiveIdx].IsActivated))
	assert.Equal(t, "00000001", aws.ToString(out.TypeSummaries[inactiveIdx].DefaultVersionId),
		"ListTypes: DefaultVersionId empty")

	activeIdx, ok := byName["AWS::Cfn21my::Active"]
	require.True(t, ok, "AWS::Cfn21my::Active missing from ListTypes")
	assert.True(t, aws.ToBool(out.TypeSummaries[activeIdx].IsActivated))
}

// TestDescribeEvents_RealClient locks in a WRAPPER-KEY bug in DescribeEvents
// (gopherstack-21my continuation): DescribeEventsOutput wraps its collection
// under "OperationEvents" holding []types.OperationEvent
// (cloudformation@v1.76.1 deserializers.go:27818,
// awsAwsquery_deserializeOpDocumentDescribeEventsOutput reads
// strings.EqualFold("OperationEvents", ...)), a completely different type
// from DescribeStackEvents' "StackEvents"/types.StackEvent. gopherstack's
// handleDescribeEvents emitted its items under "StackEvents" -- a real
// client's OperationEvents slice decoded empty regardless of how many events
// existed, since the deserializer never finds an OperationEvents element.
func TestDescribeEvents_RealClient(t *testing.T) {
	t.Parallel()

	client := newTestHandlerAndClient(t)

	okTemplate := `{"Resources":{"Bucket":{"Type":"AWS::S3::Bucket"}}}`
	createOut, err := client.CreateStack(t.Context(), &cfnsdk.CreateStackInput{
		StackName:    aws.String("cfn21my-describeevents-stack"),
		TemplateBody: aws.String(okTemplate),
	})
	require.NoError(t, err)

	out, err := client.DescribeEvents(t.Context(), &cfnsdk.DescribeEventsInput{
		StackName: aws.String("cfn21my-describeevents-stack"),
	})
	require.NoError(t, err)

	require.NotEmpty(t, out.OperationEvents,
		"DescribeEvents: OperationEvents empty -- wrapper key regression")
	found := false
	for _, e := range out.OperationEvents {
		if aws.ToString(e.StackId) == aws.ToString(createOut.StackId) {
			found = true
			assert.NotEmpty(t, aws.ToString(e.EventId))
			assert.NotEmpty(t, string(e.ResourceStatus))
		}
	}
	assert.True(t, found, "no OperationEvents entry for the created stack's StackId")
}

// TestListStackInstanceResourceDrifts_ItemFields_RealClient (gopherstack-21my
// continuation): the real item type is types.StackInstanceResourceDriftsSummary
// (cloudformation@v1.76.1 types/types.go:1975), not types.StackResourceDrift --
// a distinct sibling type with the same required members (LogicalResourceId,
// ResourceType, StackId, StackResourceDriftStatus, Timestamp). gopherstack's
// backend.ListStackInstanceResourceDrifts rebuilt a bare StackResourceDrift
// from resourceDriftStatus (status only) instead of reusing
// resourceDriftDetail -- the same fuller map DescribeStackResourceDrifts
// already prefers -- so ResourceType, PhysicalResourceId and Timestamp were
// always empty/zero even though DetectStackResourceDrift had already
// populated resourceDriftDetail for the same resource.
func TestListStackInstanceResourceDrifts_ItemFields_RealClient(t *testing.T) {
	t.Parallel()

	backend, client := newTestHandlerAndClientWithBackend(t)

	stackSetName := "cfn21my-drift-stackset"
	templateBody := `{"Resources":{"MyBucket":{"Type":"AWS::S3::Bucket",` +
		`"Properties":{"BucketName":"cfn21my-drift-bucket"}}}}`

	_, err := client.CreateStackSet(t.Context(), &cfnsdk.CreateStackSetInput{
		StackSetName: aws.String(stackSetName),
		TemplateBody: aws.String(templateBody),
	})
	require.NoError(t, err)

	_, err = client.CreateStackInstances(t.Context(), &cfnsdk.CreateStackInstancesInput{
		StackSetName: aws.String(stackSetName),
		Accounts:     []string{"123456789012"},
		Regions:      []string{"us-east-1"},
	})
	require.NoError(t, err)

	descOut, err := client.DescribeStackInstance(t.Context(), &cfnsdk.DescribeStackInstanceInput{
		StackSetName:         aws.String(stackSetName),
		StackInstanceAccount: aws.String("123456789012"),
		StackInstanceRegion:  aws.String("us-east-1"),
	})
	require.NoError(t, err)
	childStackID := aws.ToString(descOut.StackInstance.StackId)
	require.NotEmpty(t, childStackID)

	listStacksOut, err := client.ListStacks(t.Context(), &cfnsdk.ListStacksInput{})
	require.NoError(t, err)
	var childStackName string
	for _, s := range listStacksOut.StackSummaries {
		if aws.ToString(s.StackId) == childStackID {
			childStackName = aws.ToString(s.StackName)
		}
	}
	require.NotEmpty(t, childStackName, "could not find the stack-instance's child stack in ListStacks")

	backend.ForceModifyResourceProperties(childStackName, "MyBucket", map[string]any{
		"BucketName":              "cfn21my-drift-bucket",
		"VersioningConfiguration": map[string]any{"Status": "Enabled"},
	})
	_, err = backend.DetectStackResourceDrift(childStackName, "MyBucket")
	require.NoError(t, err)

	driftOut, err := client.ListStackInstanceResourceDrifts(t.Context(), &cfnsdk.ListStackInstanceResourceDriftsInput{
		StackSetName:         aws.String(stackSetName),
		StackInstanceAccount: aws.String("123456789012"),
		StackInstanceRegion:  aws.String("us-east-1"),
		OperationId:          aws.String("cfn21my-op"),
	})
	require.NoError(t, err)
	require.Len(t, driftOut.Summaries, 1)
	s := driftOut.Summaries[0]
	assert.Equal(t, "MyBucket", aws.ToString(s.LogicalResourceId))
	assert.Equal(t, cfnsdktypes.StackResourceDriftStatusModified, s.StackResourceDriftStatus)
	assert.Equal(t, "AWS::S3::Bucket", aws.ToString(s.ResourceType),
		"ListStackInstanceResourceDrifts: ResourceType empty")
	assert.NotEmpty(t, aws.ToString(s.PhysicalResourceId),
		"ListStackInstanceResourceDrifts: PhysicalResourceId empty")
	require.NotNil(t, s.Timestamp, "ListStackInstanceResourceDrifts: Timestamp nil")
	assert.False(t, s.Timestamp.IsZero(), "ListStackInstanceResourceDrifts: Timestamp is the zero value")
	assert.NotEmpty(t, s.PropertyDifferences,
		"ListStackInstanceResourceDrifts: PropertyDifferences empty -- PropertyDifferences>member wrapping regression")
}

// TestStackSetOperation_ItemFields_RealClient (gopherstack-21my
// continuation): types.StackSetOperation and its sibling
// types.StackSetOperationSummary both carry CreationTimestamp
// (cloudformation@v1.76.1 types/types.go:2733,2985); the backend's own
// StackSetOperation model already tracks the equivalent (CreatedAt, set at
// recordStackSetOperation) but neither DescribeStackSetOperation nor
// ListStackSetOperations ever emitted it -- a shared gap across the singular
// and plural forms, invisible to a sibling-only diff. types.StackSetOperation
// also carries StackSetId, which gopherstack's DescribeStackSetOperation
// never emitted despite the StackSet's own StackSetID being available via
// DescribeStackSet.
func TestStackSetOperation_ItemFields_RealClient(t *testing.T) {
	t.Parallel()

	client := newTestHandlerAndClient(t)

	stackSetName := "cfn21my-ssop-stackset"
	templateBody := `{"Resources":{"Bucket":{"Type":"AWS::S3::Bucket"}}}`

	createOut, err := client.CreateStackSet(t.Context(), &cfnsdk.CreateStackSetInput{
		StackSetName: aws.String(stackSetName),
		TemplateBody: aws.String(templateBody),
	})
	require.NoError(t, err)
	stackSetID := aws.ToString(createOut.StackSetId)
	require.NotEmpty(t, stackSetID)

	instOut, err := client.CreateStackInstances(t.Context(), &cfnsdk.CreateStackInstancesInput{
		StackSetName: aws.String(stackSetName),
		Accounts:     []string{"123456789012"},
		Regions:      []string{"us-east-1"},
	})
	require.NoError(t, err)
	opID := aws.ToString(instOut.OperationId)
	require.NotEmpty(t, opID)

	descOut, err := client.DescribeStackSetOperation(t.Context(), &cfnsdk.DescribeStackSetOperationInput{
		StackSetName: aws.String(stackSetName),
		OperationId:  aws.String(opID),
	})
	require.NoError(t, err)
	require.NotNil(t, descOut.StackSetOperation)
	require.NotNil(t, descOut.StackSetOperation.CreationTimestamp,
		"DescribeStackSetOperation: CreationTimestamp nil")
	assert.False(t, descOut.StackSetOperation.CreationTimestamp.IsZero())
	assert.Equal(t, stackSetID, aws.ToString(descOut.StackSetOperation.StackSetId),
		"DescribeStackSetOperation: StackSetId empty/wrong")

	listOut, err := client.ListStackSetOperations(t.Context(), &cfnsdk.ListStackSetOperationsInput{
		StackSetName: aws.String(stackSetName),
	})
	require.NoError(t, err)
	found := false
	for _, s := range listOut.Summaries {
		if aws.ToString(s.OperationId) == opID {
			found = true
			require.NotNil(t, s.CreationTimestamp, "ListStackSetOperations: CreationTimestamp nil")
			assert.False(t, s.CreationTimestamp.IsZero())
		}
	}
	assert.True(t, found, "no ListStackSetOperations entry for the created operation")
}

// TestStackRefactor_ItemFields_RealClient (gopherstack-21my continuation):
// types.StackRefactorAction has no StackName, LogicalResourceId or
// ResourceType member at all -- it carries the source/destination location
// nested under ResourceMapping.Source/.Destination, each a ResourceLocation
// (cloudformation@v1.76.1 types/types.go:2118,1195,1178). gopherstack emitted
// flat top-level StackName/LogicalResourceId/ResourceType elements instead --
// none of them a real member, so a real client's ResourceMapping was
// unconditionally nil despite the backend already tracking both the source
// and destination ResourceLocation for every mapping. Separately,
// DescribeStackRefactorOutput carries Description and StackRefactorId, both
// already tracked by the backend's StackRefactor model but never emitted.
func TestStackRefactor_ItemFields_RealClient(t *testing.T) {
	t.Parallel()

	client := newTestHandlerAndClient(t)

	okTemplate := `{"Resources":{"Bucket":{"Type":"AWS::S3::Bucket"}}}`
	_, err := client.CreateStack(t.Context(), &cfnsdk.CreateStackInput{
		StackName:    aws.String("cfn21my-refactor-src"),
		TemplateBody: aws.String(okTemplate),
	})
	require.NoError(t, err)
	_, err = client.CreateStack(t.Context(), &cfnsdk.CreateStackInput{
		StackName:    aws.String("cfn21my-refactor-dst"),
		TemplateBody: aws.String(okTemplate),
	})
	require.NoError(t, err)

	createOut, err := client.CreateStackRefactor(t.Context(), &cfnsdk.CreateStackRefactorInput{
		StackDefinitions: []cfnsdktypes.StackDefinition{},
		Description:      aws.String("cfn21my distinguishable refactor"),
		ResourceMappings: []cfnsdktypes.ResourceMapping{
			{
				Source: &cfnsdktypes.ResourceLocation{
					StackName:         aws.String("cfn21my-refactor-src"),
					LogicalResourceId: aws.String("Bucket"),
				},
				Destination: &cfnsdktypes.ResourceLocation{
					StackName:         aws.String("cfn21my-refactor-dst"),
					LogicalResourceId: aws.String("MovedBucket"),
				},
			},
		},
	})
	require.NoError(t, err)
	refactorID := aws.ToString(createOut.StackRefactorId)
	require.NotEmpty(t, refactorID)

	descOut, err := client.DescribeStackRefactor(t.Context(), &cfnsdk.DescribeStackRefactorInput{
		StackRefactorId: aws.String(refactorID),
	})
	require.NoError(t, err)
	assert.Equal(t, "cfn21my distinguishable refactor", aws.ToString(descOut.Description),
		"DescribeStackRefactor: Description empty")
	assert.Equal(t, refactorID, aws.ToString(descOut.StackRefactorId),
		"DescribeStackRefactor: StackRefactorId empty")

	actionsOut, err := client.ListStackRefactorActions(t.Context(), &cfnsdk.ListStackRefactorActionsInput{
		StackRefactorId: aws.String(refactorID),
	})
	require.NoError(t, err)
	require.Len(t, actionsOut.StackRefactorActions, 1)
	a := actionsOut.StackRefactorActions[0]
	require.NotNil(t, a.ResourceMapping, "ListStackRefactorActions: ResourceMapping nil")
	require.NotNil(t, a.ResourceMapping.Source)
	require.NotNil(t, a.ResourceMapping.Destination)
	assert.Equal(t, "cfn21my-refactor-src", aws.ToString(a.ResourceMapping.Source.StackName))
	assert.Equal(t, "Bucket", aws.ToString(a.ResourceMapping.Source.LogicalResourceId))
	assert.Equal(t, "cfn21my-refactor-dst", aws.ToString(a.ResourceMapping.Destination.StackName))
	assert.Equal(t, "MovedBucket", aws.ToString(a.ResourceMapping.Destination.LogicalResourceId))
}

// TestDescribePublisher_PublisherId_RealClient (gopherstack-21my
// continuation): DescribePublisherOutput carries PublisherId
// (cloudformation@v1.76.1 api_op_DescribePublisher.go); gopherstack's
// handleDescribePublisher already resolves the publisher by that exact ID
// but never echoed it back onto the wire.
func TestDescribePublisher_PublisherId_RealClient(t *testing.T) {
	t.Parallel()

	client := newTestHandlerAndClient(t)

	regOut, err := client.RegisterPublisher(t.Context(), &cfnsdk.RegisterPublisherInput{
		ConnectionArn: aws.String("arn:aws:codestar-connections:us-east-1:123456789012:connection/cfn21my"),
	})
	require.NoError(t, err)
	publisherID := aws.ToString(regOut.PublisherId)
	require.NotEmpty(t, publisherID)

	descOut, err := client.DescribePublisher(t.Context(), &cfnsdk.DescribePublisherInput{
		PublisherId: aws.String(publisherID),
	})
	require.NoError(t, err)
	assert.Equal(t, publisherID, aws.ToString(descOut.PublisherId),
		"DescribePublisher: PublisherId empty/wrong")
}

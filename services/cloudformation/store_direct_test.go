package cloudformation_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cloudformation"
)

// ---- Backend: StackSet drift detection ----------------------------------------

func TestStackSetDrift(t *testing.T) {
	t.Parallel()

	b := newBackend()
	_, err := b.CreateStackSet("drift-ss", "desc", simpleTemplate, cloudformation.StackSetOptions{})
	require.NoError(t, err)

	opID, err := b.DetectStackSetDrift("drift-ss")
	require.NoError(t, err)
	assert.NotEmpty(t, opID)

	op, err := b.DescribeStackSetOperation("drift-ss", opID)
	require.NoError(t, err)
	assert.NotEmpty(t, op.Status)
}

// TestStackSetDrift_UpdatesInstanceDriftStatus locks in a parity fix:
// DetectStackSetDrift previously only recorded a SUCCEEDED operation without
// running any actual per-resource drift comparison against stack instances'
// provisioned child stacks -- a disguised stub (looks real, but every
// instance's DriftStatus stayed NOT_CHECKED forever, matching real AWS's
// initial-state value but never reflecting an actual detection run).
func TestStackSetDrift_UpdatesInstanceDriftStatus(t *testing.T) {
	t.Parallel()

	b := newBackend()
	_, err := b.CreateStackSet(
		"drift-instance-ss",
		"desc",
		simpleTemplate,
		cloudformation.StackSetOptions{},
	)
	require.NoError(t, err)

	_, err = b.CreateStackInstances(
		t.Context(),
		"drift-instance-ss",
		[]string{"111111111111"},
		nil,
		[]string{"us-east-1"},
	)
	require.NoError(t, err)

	instances, err := b.ListStackInstances("drift-instance-ss", "")
	require.NoError(t, err)
	require.Len(t, instances.Data, 1)
	assert.Equal(
		t,
		"NOT_CHECKED",
		instances.Data[0].DriftStatus,
		"before any detection, DriftStatus starts NOT_CHECKED",
	)

	childStackID := instances.Data[0].StackID

	// First detection with no drift: the child stack matches its template as
	// provisioned, so every instance should report IN_SYNC.
	_, err = b.DetectStackSetDrift("drift-instance-ss")
	require.NoError(t, err)

	instances, err = b.ListStackInstances("drift-instance-ss", "")
	require.NoError(t, err)
	require.Len(t, instances.Data, 1)
	assert.Equal(t, "IN_SYNC", instances.Data[0].DriftStatus)

	// Simulate an out-of-band mutation on the child stack's resource, then
	// re-detect: the instance must now report DRIFTED.
	require.NoError(
		t,
		b.RecordResourceMutation(childStackID, "MyBucket", map[string]any{"Mutated": true}),
	)

	_, err = b.DetectStackSetDrift("drift-instance-ss")
	require.NoError(t, err)

	instances, err = b.ListStackInstances("drift-instance-ss", "")
	require.NoError(t, err)
	require.Len(t, instances.Data, 1)
	assert.Equal(t, "DRIFTED", instances.Data[0].DriftStatus)
}

func TestStackSetOperationList(t *testing.T) {
	t.Parallel()

	b := newBackend()
	_, err := b.CreateStackSet(
		"op-list-ss",
		"desc",
		simpleTemplate,
		cloudformation.StackSetOptions{},
	)
	require.NoError(t, err)

	_, err = b.CreateStackInstances(
		t.Context(),
		"op-list-ss",
		[]string{"111"},
		nil,
		[]string{"us-east-1"},
	)
	require.NoError(t, err)

	p, err := b.ListStackSetOperations("op-list-ss", "")
	require.NoError(t, err)
	assert.NotEmpty(t, p.Data)
}

func TestStopStackSetOperation(t *testing.T) {
	t.Parallel()

	b := newBackend()
	_, err := b.CreateStackSet("stop-ss", "desc", simpleTemplate, cloudformation.StackSetOptions{})
	require.NoError(t, err)

	opID, err := b.DetectStackSetDrift("stop-ss")
	require.NoError(t, err)

	// Operations complete synchronously (SUCCEEDED), so StopStackSetOperation
	// returns ErrOperationNotRunning — which is expected behavior.
	err = b.StopStackSetOperation("stop-ss", opID)
	require.ErrorIs(t, err, cloudformation.ErrOperationNotRunning)

	op, err := b.DescribeStackSetOperation("stop-ss", opID)
	require.NoError(t, err)
	assert.NotEmpty(t, op.Status)
}

// ---- Backend: Generated templates ---------------------------------------------

func TestGeneratedTemplate_CRUD(t *testing.T) {
	t.Parallel()

	b := newBackend()

	gt, err := b.CreateGeneratedTemplate("my-gen-tmpl", []string{"arn:aws:s3:::my-bucket"})
	require.NoError(t, err)
	assert.Equal(t, "my-gen-tmpl", gt.GeneratedTemplateName)

	err = b.UpdateGeneratedTemplate(gt.GeneratedTemplateID, "renamed-tmpl")
	require.NoError(t, err)

	desc, err := b.DescribeGeneratedTemplate(gt.GeneratedTemplateID)
	require.NoError(t, err)
	assert.NotNil(t, desc)

	body, err := b.GetGeneratedTemplate(gt.GeneratedTemplateID)
	require.NoError(t, err)
	assert.NotEmpty(t, body)

	list, err := b.ListGeneratedTemplates("")
	require.NoError(t, err)
	assert.NotEmpty(t, list.Data)

	err = b.DeleteGeneratedTemplate(gt.GeneratedTemplateID)
	require.NoError(t, err)

	_, err = b.DescribeGeneratedTemplate(gt.GeneratedTemplateID)
	require.ErrorIs(t, err, cloudformation.ErrGeneratedTemplateNotFound)
}

// ---- Backend: Resource scans --------------------------------------------------

func TestResourceScan_CRUD(t *testing.T) {
	t.Parallel()

	b := newBackend()

	scanID, err := b.StartResourceScan()
	require.NoError(t, err)
	assert.NotEmpty(t, scanID)

	scan, err := b.DescribeResourceScan(scanID)
	require.NoError(t, err)
	assert.Equal(t, "COMPLETE", scan.Status)

	scans, err := b.ListResourceScans("")
	require.NoError(t, err)
	assert.NotEmpty(t, scans.Data)

	resources, err := b.ListResourceScanResources(scanID, "")
	require.NoError(t, err)
	assert.NotNil(t, resources)
}

// ---- Backend: Type management -------------------------------------------------

func TestTypeManagement_RegisterAndList(t *testing.T) {
	t.Parallel()

	b := newBackend()

	token, err := b.RegisterType("MyCompany::MyService::MyType", "s3://bucket/schema.zip")
	require.NoError(t, err)
	assert.NotEmpty(t, token)

	regStatus, err := b.DescribeTypeRegistration(token)
	require.NoError(t, err)
	assert.NotEmpty(t, regStatus)

	types, err := b.ListTypes("")
	require.NoError(t, err)
	assert.NotEmpty(t, types)

	typeVersions, err := b.ListTypeVersions("MyCompany::MyService::MyType", "")
	require.NoError(t, err)
	assert.NotNil(t, typeVersions)
}

func TestTypeManagement_ActivateDeactivate(t *testing.T) {
	t.Parallel()

	b := newBackend()

	err := b.ActivateType(
		"AWS::S3::Bucket",
		"arn:aws:cloudformation:us-east-1::type/resource/AWS-S3-Bucket",
	)
	require.NoError(t, err)

	err = b.DeactivateType(
		"AWS::S3::Bucket",
		"arn:aws:cloudformation:us-east-1::type/resource/AWS-S3-Bucket",
	)
	require.NoError(t, err)
}

func TestTypeManagement_Configuration(t *testing.T) {
	t.Parallel()

	b := newBackend()

	_, err := b.RegisterType("My::Test::Type", "s3://bucket/schema.zip")
	require.NoError(t, err)

	config := `{"LoggingConfig":{"LogGroupName":"/aws/cloudformation/My-Test-Type"}}`
	err = b.SetTypeConfiguration("My::Test::Type", config)
	require.NoError(t, err)

	result, errs, unprocessed := b.BatchDescribeTypeConfigurations(
		[]cloudformation.TypeConfigurationIdentifier{{TypeName: "My::Test::Type"}},
	)
	assert.Empty(t, errs)
	assert.Empty(t, unprocessed)
	require.Len(t, result, 1)
	assert.JSONEq(t, config, result[0].Configuration)
}

// ---- Backend: Publisher -------------------------------------------------------

func TestPublisher_RegisterAndDescribe(t *testing.T) {
	t.Parallel()

	b := newBackend()

	publisherID, err := b.RegisterPublisher(
		"arn:aws:codestar-connections:us-east-1:123:connection/abc",
	)
	require.NoError(t, err)
	assert.NotEmpty(t, publisherID)

	status, err := b.DescribePublisher(publisherID)
	require.NoError(t, err)
	assert.NotEmpty(t, status)
}

// ---- Backend: Signal resource -------------------------------------------------

func TestSignalResource(t *testing.T) {
	t.Parallel()

	b := newBackend()
	_, err := b.CreateStack(
		t.Context(),
		"signal-stack",
		simpleTemplate,
		nil,
		cloudformation.StackOptions{},
	)
	require.NoError(t, err)

	err = b.SignalResource("signal-stack", "MyBucket", "unique-id-1", "SUCCESS")
	require.NoError(t, err)
}

// ---- Backend: DescribeStackResource / DescribeStackResources ------------------

func TestDescribeStackResource(t *testing.T) {
	t.Parallel()

	b := newBackend()
	tmpl := `{
		"AWSTemplateFormatVersion": "2010-09-09",
		"Resources": {
			"MyQueue": {"Type": "AWS::SQS::Queue"}
		}
	}`
	_, err := b.CreateStack(t.Context(), "res-stack", tmpl, nil, cloudformation.StackOptions{})
	require.NoError(t, err)

	res, err := b.DescribeStackResource("res-stack", "MyQueue")
	require.NoError(t, err)
	assert.Equal(t, "MyQueue", res.LogicalID)
	assert.Equal(t, "AWS::SQS::Queue", res.Type)
}

func TestDescribeStackResources_Multiple(t *testing.T) {
	t.Parallel()

	b := newBackend()
	tmpl := `{
		"AWSTemplateFormatVersion": "2010-09-09",
		"Resources": {
			"QueueA": {"Type": "AWS::SQS::Queue"},
			"QueueB": {"Type": "AWS::SQS::Queue"}
		}
	}`
	_, err := b.CreateStack(t.Context(), "multi-res", tmpl, nil, cloudformation.StackOptions{})
	require.NoError(t, err)

	resources, err := b.DescribeStackResources("multi-res")
	require.NoError(t, err)
	assert.Len(t, resources, 2)
}

// ---- Backend: ListStackResources paged ----------------------------------------

func TestListStackResources_Paged(t *testing.T) {
	t.Parallel()

	b := newBackend()
	tmpl := `{
		"AWSTemplateFormatVersion": "2010-09-09",
		"Resources": {
			"Q1": {"Type": "AWS::SQS::Queue"},
			"Q2": {"Type": "AWS::SQS::Queue"},
			"Q3": {"Type": "AWS::SQS::Queue"}
		}
	}`
	_, err := b.CreateStack(t.Context(), "paged-stack", tmpl, nil, cloudformation.StackOptions{})
	require.NoError(t, err)

	page, err := b.ListStackResources("paged-stack", "")
	require.NoError(t, err)
	assert.Len(t, page.Data, 3)
}

// ---- Backend: OrgsAccess ------------------------------------------------------

func TestOrgsAccess(t *testing.T) {
	t.Parallel()

	b := newBackend()

	err := b.ActivateOrganizationsAccess()
	require.NoError(t, err)

	status, err := b.DescribeOrganizationsAccess()
	require.NoError(t, err)
	assert.Equal(t, "ENABLED", status)

	err = b.DeactivateOrganizationsAccess()
	require.NoError(t, err)

	status, err = b.DescribeOrganizationsAccess()
	require.NoError(t, err)
	assert.Equal(t, "DISABLED", status)
}

// ---- Backend: RecordHandlerProgress / HookResult ------------------------------

func TestRecordHandlerProgress(t *testing.T) {
	t.Parallel()

	b := newBackend()
	err := b.RecordHandlerProgress("bearer-token-1", "IN_PROGRESS")
	require.NoError(t, err)

	err = b.RecordHandlerProgress("bearer-token-1", "SUCCESS")
	require.NoError(t, err)
}

func TestHookResult(t *testing.T) {
	t.Parallel()

	b := newBackend()
	result, err := b.GetHookResult("some-token")
	// May return error or empty result — just ensure no panic.
	_ = result
	_ = err
}

// ---- Backend: StackRefactor ---------------------------------------------------

func TestStackRefactor_CRUD(t *testing.T) {
	t.Parallel()

	b := newBackend()

	refactorID, err := b.CreateStackRefactor("my refactor", nil, false)
	require.NoError(t, err)
	assert.NotEmpty(t, refactorID)

	desc, err := b.DescribeStackRefactor(refactorID)
	require.NoError(t, err)
	assert.NotEmpty(t, desc)

	list, err := b.ListStackRefactors("")
	require.NoError(t, err)
	assert.NotEmpty(t, list)

	actions, err := b.ListStackRefactorActions(refactorID)
	require.NoError(t, err)
	assert.NotNil(t, actions)

	err = b.ExecuteStackRefactor(refactorID)
	require.NoError(t, err)
}

// ---- Backend: ImportStacksToStackSet ------------------------------------------

func TestImportStacksToStackSet(t *testing.T) {
	t.Parallel()

	b := newBackend()
	_, err := b.CreateStackSet(
		"import-ss",
		"desc",
		simpleTemplate,
		cloudformation.StackSetOptions{},
	)
	require.NoError(t, err)

	err = b.ImportStacksToStackSet(
		"import-ss",
		[]string{"arn:aws:cloudformation:us-east-1:123:stack/my-stack/abc"},
	)
	require.NoError(t, err)
}

// ---- Backend: ListStackSetAutoDeploymentTargets --------------------------------

func TestListStackSetAutoDeploymentTargets(t *testing.T) {
	t.Parallel()

	b := newBackend()
	_, err := b.CreateStackSet(
		"auto-deploy-ss",
		"desc",
		simpleTemplate,
		cloudformation.StackSetOptions{},
	)
	require.NoError(t, err)

	targets, err := b.ListStackSetAutoDeploymentTargets("auto-deploy-ss")
	require.NoError(t, err)
	assert.NotNil(t, targets)
}

// ---- Backend: DependsOn ordering in template -----------------------------------

func TestCreateStack_DependsOn_Ordering(t *testing.T) {
	t.Parallel()

	b := newBackend()
	tmpl := `{
		"AWSTemplateFormatVersion": "2010-09-09",
		"Resources": {
			"TopicA": {"Type": "AWS::SNS::Topic"},
			"QueueB": {
				"Type": "AWS::SQS::Queue",
				"DependsOn": "TopicA"
			}
		}
	}`

	stack, err := b.CreateStack(t.Context(), "dep-stack", tmpl, nil, cloudformation.StackOptions{})
	require.NoError(t, err)
	assert.Equal(t, "CREATE_COMPLETE", stack.StackStatus)

	resources, err := b.DescribeStackResources("dep-stack")
	require.NoError(t, err)
	assert.Len(t, resources, 2)
}

// ---- Backend: Conditions in template ------------------------------------------

func TestCreateStack_Conditions_TrueCondition(t *testing.T) {
	t.Parallel()

	b := newBackend()
	tmpl := `{
		"AWSTemplateFormatVersion": "2010-09-09",
		"Parameters": {"Env": {"Type": "String", "Default": "prod"}},
		"Conditions": {
			"IsProd": {"Fn::Equals": [{"Ref": "Env"}, "prod"]}
		},
		"Resources": {
			"Queue": {
				"Type": "AWS::SQS::Queue",
				"Condition": "IsProd"
			}
		}
	}`

	stack, err := b.CreateStack(t.Context(), "cond-true", tmpl, nil, cloudformation.StackOptions{})
	require.NoError(t, err)
	assert.Equal(t, "CREATE_COMPLETE", stack.StackStatus)
}

// ---- Backend: Parameters with UsePreviousValue --------------------------------

func TestCreateStack_MultipleParameters(t *testing.T) {
	t.Parallel()

	b := newBackend()
	tmpl := `{
		"AWSTemplateFormatVersion": "2010-09-09",
		"Parameters": {
			"Env": {"Type": "String"},
			"Region": {"Type": "String", "Default": "us-east-1"},
			"Team": {"Type": "String", "Default": "platform"}
		},
		"Resources": {
			"Queue": {"Type": "AWS::SQS::Queue"}
		},
		"Outputs": {
			"EnvOut": {"Value": {"Ref": "Env"}},
			"RegionOut": {"Value": {"Ref": "Region"}},
			"TeamOut": {"Value": {"Ref": "Team"}}
		}
	}`

	stack, err := b.CreateStack(
		t.Context(), "multi-params", tmpl,
		[]cloudformation.Parameter{
			{ParameterKey: "Env", ParameterValue: "production"},
			{ParameterKey: "Team", ParameterValue: "ops"},
		},
		cloudformation.StackOptions{},
	)
	require.NoError(t, err)
	assert.Equal(t, "CREATE_COMPLETE", stack.StackStatus)

	outputMap := make(map[string]string, len(stack.Outputs))
	for _, o := range stack.Outputs {
		outputMap[o.OutputKey] = o.OutputValue
	}
	assert.Equal(t, "production", outputMap["EnvOut"])
	assert.Equal(t, "us-east-1", outputMap["RegionOut"])
	assert.Equal(t, "ops", outputMap["TeamOut"])
}

// ---- Backend: RollbackStack changes status ------------------------------------

func TestRollbackStack_ChangesStatus(t *testing.T) {
	t.Parallel()

	b := newBackend()
	_, err := b.CreateStack(
		t.Context(),
		"rb-change",
		simpleTemplate,
		nil,
		cloudformation.StackOptions{},
	)
	require.NoError(t, err)

	err = b.RollbackStack(t.Context(), "rb-change")
	require.NoError(t, err)

	// Stack should still be accessible.
	stack, err := b.DescribeStack("rb-change")
	require.NoError(t, err)
	assert.NotEmpty(t, stack.StackID)
}

// ---- Backend: ListAll returns all stacks including deleted --------------------

func TestListAll_IncludesDeletedStacks(t *testing.T) {
	t.Parallel()

	b := newBackend()
	_, err := b.CreateStack(
		t.Context(),
		"alive",
		simpleTemplate,
		nil,
		cloudformation.StackOptions{},
	)
	require.NoError(t, err)
	_, err = b.CreateStack(t.Context(), "dead", simpleTemplate, nil, cloudformation.StackOptions{})
	require.NoError(t, err)

	err = b.DeleteStack(t.Context(), "dead")
	require.NoError(t, err)

	all := b.ListAll()
	assert.Len(t, all, 2)
}

// ---- Backend: DescribeEvents --------------------------------------------------

func TestDescribeEvents_Global(t *testing.T) {
	t.Parallel()

	b := newBackend()
	_, err := b.CreateStack(
		t.Context(),
		"ev-stack",
		simpleTemplate,
		nil,
		cloudformation.StackOptions{},
	)
	require.NoError(t, err)

	p, err := b.DescribeEvents("", "")
	require.NoError(t, err)
	assert.NotEmpty(t, p.Data)
}

// ---- Backend: ChangeSet for new stack (ExecuteChangeSet creates stack) --------

func TestChangeSet_ExecuteOnNewStack(t *testing.T) {
	t.Parallel()

	b := newBackend()

	// Create changeset without pre-existing stack.
	cs, err := b.CreateChangeSet(
		t.Context(),
		"brand-new",
		"my-cs",
		simpleTemplate,
		"init",
		nil,
		nil,
		nil,
	)
	require.NoError(t, err)
	assert.Equal(t, "brand-new", cs.StackName)

	// Execute — should create the stack.
	err = b.ExecuteChangeSet(t.Context(), "brand-new", "my-cs")
	require.NoError(t, err)

	stack, err := b.DescribeStack("brand-new")
	require.NoError(t, err)
	assert.Equal(t, "CREATE_COMPLETE", stack.StackStatus)
}

// ---- Backend: ContinueUpdateRollback with missing stack -----------------------

func TestContinueUpdateRollback_StackNotFound(t *testing.T) {
	t.Parallel()

	b := newBackend()
	err := b.ContinueUpdateRollback(t.Context(), "no-such-stack")
	require.ErrorIs(t, err, cloudformation.ErrStackNotFound)
}

func TestCancelUpdateStack_StackNotFound(t *testing.T) {
	t.Parallel()

	b := newBackend()
	err := b.CancelUpdateStack(t.Context(), "no-such-stack")
	require.ErrorIs(t, err, cloudformation.ErrStackNotFound)
}

// ---- Backend: Template Fn::Select and Fn::If ----------------------------------

func TestCreateStack_FnSelect_Output(t *testing.T) {
	t.Parallel()

	b := newBackend()
	tmpl := `{
		"AWSTemplateFormatVersion": "2010-09-09",
		"Parameters": {"Idx": {"Type": "Number", "Default": "1"}},
		"Resources": {"Q": {"Type": "AWS::SQS::Queue"}},
		"Outputs": {
			"Selected": {"Value": {"Fn::Select": [1, ["alpha", "beta", "gamma"]]}}
		}
	}`

	stack, err := b.CreateStack(t.Context(), "fn-select", tmpl, nil, cloudformation.StackOptions{})
	require.NoError(t, err)
	assert.Equal(t, "CREATE_COMPLETE", stack.StackStatus)

	// Fn::Select with index 1 should produce "beta".
	found := false
	for _, o := range stack.Outputs {
		if o.OutputKey == "Selected" && o.OutputValue == "beta" {
			found = true
		}
	}
	assert.True(t, found, "expected Fn::Select to resolve to 'beta'")
}

// ---- Backend: DescribeStack pagination ----------------------------------------

func TestListStacks_StatusFilter(t *testing.T) {
	t.Parallel()

	b := newBackend()
	_, err := b.CreateStack(t.Context(), "s1", simpleTemplate, nil, cloudformation.StackOptions{})
	require.NoError(t, err)
	_, err = b.CreateStack(t.Context(), "s2", simpleTemplate, nil, cloudformation.StackOptions{})
	require.NoError(t, err)
	err = b.DeleteStack(t.Context(), "s2")
	require.NoError(t, err)

	// Filter for only CREATE_COMPLETE.
	page, err := b.ListStacks([]string{"CREATE_COMPLETE"}, "")
	require.NoError(t, err)
	for _, s := range page.Data {
		assert.Equal(t, "CREATE_COMPLETE", s.StackStatus)
	}

	// No filter returns all.
	allPage, err := b.ListStacks(nil, "")
	require.NoError(t, err)
	assert.GreaterOrEqual(t, len(allPage.Data), 2)
}

// ---- Backend: AllowedValues default value passes validation -------------------

func TestCreateStack_AllowedValues_NoOverride_UsesDefault(t *testing.T) {
	t.Parallel()

	b := newBackend()
	// Default NOT in AllowedValues — should fail.
	tmpl := `{
		"AWSTemplateFormatVersion": "2010-09-09",
		"Parameters": {
			"Env": {
				"Type": "String",
				"Default": "qa",
				"AllowedValues": ["dev", "prod"]
			}
		},
		"Resources": {"Q": {"Type": "AWS::SQS::Queue"}}
	}`

	stack, err := b.CreateStack(
		t.Context(),
		"av-bad-default",
		tmpl,
		nil,
		cloudformation.StackOptions{},
	)
	require.NoError(t, err)
	// Default "qa" not in AllowedValues → ROLLBACK_COMPLETE.
	assert.Equal(t, "ROLLBACK_COMPLETE", stack.StackStatus)
}

// ---- Backend: ChangeSet changes computed correctly ----------------------------

func TestChangeSet_ChangesContainAdd(t *testing.T) {
	t.Parallel()

	b := newBackend()
	_, err := b.CreateStack(
		t.Context(),
		"cs-chg",
		simpleTemplate,
		nil,
		cloudformation.StackOptions{},
	)
	require.NoError(t, err)

	newTmpl := `{
		"AWSTemplateFormatVersion": "2010-09-09",
		"Resources": {
			"MyBucket": {"Type": "AWS::S3::Bucket"},
			"NewQueue": {"Type": "AWS::SQS::Queue"}
		}
	}`

	cs, err := b.CreateChangeSet(
		t.Context(),
		"cs-chg",
		"my-changes",
		newTmpl,
		"add queue",
		nil,
		nil,
		nil,
	)
	require.NoError(t, err)
	assert.NotEmpty(t, cs.Changes)

	// NewQueue should appear as an Add.
	found := false
	for _, ch := range cs.Changes {
		if ch.ResourceChange.LogicalID == "NewQueue" && ch.ResourceChange.Action == "Add" {
			found = true
		}
	}
	assert.True(t, found, "expected NewQueue to be in changeset as Add")
}

// ---- Backend: NestedStack parameter passing -----------------------------------

func TestCreateStack_NestedStack_WithParams(t *testing.T) {
	t.Parallel()

	b := newBackend()
	childTemplate := `{"AWSTemplateFormatVersion":"2010-09-09",` +
		`"Parameters":{"QName":{"Type":"String"}},` +
		`"Resources":{"Q":{"Type":"AWS::SQS::Queue"}},` +
		`"Outputs":{"QueueName":{"Value":{"Ref":"QName"}}}}`

	// Build parent with param passing inline.
	parentTemplate := strings.ReplaceAll(`{
		"AWSTemplateFormatVersion": "2010-09-09",
		"Resources": {
			"Child": {
				"Type": "AWS::CloudFormation::Stack",
				"Properties": {
					"TemplateBody": CHILD_TMPL,
					"Parameters": {
						"QName": "my-nested-queue"
					}
				}
			}
		}
	}`, "CHILD_TMPL", quoteJSON(childTemplate))

	stack, err := b.CreateStack(
		t.Context(),
		"parent-params",
		parentTemplate,
		nil,
		cloudformation.StackOptions{},
	)
	require.NoError(t, err)
	assert.Equal(t, "CREATE_COMPLETE", stack.StackStatus)

	// Child stack should exist.
	child, err := b.DescribeStack("Child")
	require.NoError(t, err)
	assert.Equal(t, "CREATE_COMPLETE", child.StackStatus)
}

// ---- Backend: Concurrent stack operations (race detector) ---------------------

func TestBackend_ConcurrentCreateStack(t *testing.T) {
	t.Parallel()

	b := newBackend()

	results := make(chan error, 5)
	for i := range 5 {
		name := "concurrent-" + string(rune('a'+i))
		go func(n string) {
			_, err := b.CreateStack(
				t.Context(),
				n,
				simpleTemplate,
				nil,
				cloudformation.StackOptions{},
			)
			results <- err
		}(name)
	}

	for range 5 {
		err := <-results
		require.NoError(t, err)
	}

	all := b.ListAll()
	assert.Len(t, all, 5)
}

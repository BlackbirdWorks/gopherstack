package cloudformation_test

import (
	"encoding/xml"
	"net/http"
	"net/url"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cloudformation"
)

// postFormValues posts a url.Values form to the handler.
func postFormValues(t *testing.T, h *cloudformation.Handler, values url.Values) *httpResponse {
	t.Helper()

	return postFormBody(t, h, values.Encode())
}

type httpResponse struct {
	Body   string
	Status int
}

func postFormBody(t *testing.T, h *cloudformation.Handler, body string) *httpResponse {
	t.Helper()
	rec := postForm(t, h, body)

	return &httpResponse{Body: rec.Body.String(), Status: rec.Code}
}

func (r *httpResponse) mustOK(t *testing.T) {
	t.Helper()
	assert.Equal(t, http.StatusOK, r.Status, "body: %s", r.Body)
}

// ---- Handler: CreateStack with Capabilities -----------------------------------

func TestHandler_CreateStack_WithCapabilities(t *testing.T) {
	t.Parallel()

	h := newHandler()
	v := url.Values{
		"Action":                {"CreateStack"},
		"StackName":             {"cap-stack"},
		"TemplateBody":          {simpleTemplate},
		"Capabilities.member.1": {"CAPABILITY_IAM"},
		"Capabilities.member.2": {"CAPABILITY_NAMED_IAM"},
	}
	resp := postFormValues(t, h, v)
	resp.mustOK(t)

	// Verify capabilities stored on stack.
	stack, err := h.Backend.(*cloudformation.InMemoryBackend).DescribeStack("cap-stack")
	require.NoError(t, err)
	assert.Contains(t, stack.Capabilities, "CAPABILITY_IAM")
	assert.Contains(t, stack.Capabilities, "CAPABILITY_NAMED_IAM")
}

func TestHandler_CreateStack_WithOnFailureDelete(t *testing.T) {
	t.Parallel()

	h := newHandler()
	failTemplate := `{
		"AWSTemplateFormatVersion": "2010-09-09",
		"Resources": {
			"B": {"Type": "AWS::S3::Bucket", "Properties": {"BucketName": {"Fn::ImportValue": "no-export"}}}
		}
	}`
	v := url.Values{
		"Action":       {"CreateStack"},
		"StackName":    {"fail-del"},
		"TemplateBody": {failTemplate},
		"OnFailure":    {"DELETE"},
	}
	resp := postFormValues(t, h, v)
	resp.mustOK(t)
}

func TestHandler_CreateStack_WithRoleARN(t *testing.T) {
	t.Parallel()

	h := newHandler()
	v := url.Values{
		"Action":       {"CreateStack"},
		"StackName":    {"role-stack"},
		"TemplateBody": {simpleTemplate},
		"RoleARN":      {"arn:aws:iam::123456789012:role/CFNRole"},
	}
	resp := postFormValues(t, h, v)
	resp.mustOK(t)

	stack, err := h.Backend.(*cloudformation.InMemoryBackend).DescribeStack("role-stack")
	require.NoError(t, err)
	assert.Equal(t, "arn:aws:iam::123456789012:role/CFNRole", stack.RoleARN)
}

func TestHandler_CreateStack_WithNotificationARNs(t *testing.T) {
	t.Parallel()

	h := newHandler()
	v := url.Values{
		"Action":                    {"CreateStack"},
		"StackName":                 {"notif-stack"},
		"TemplateBody":              {simpleTemplate},
		"NotificationARNs.member.1": {"arn:aws:sns:us-east-1:123:MyTopic"},
	}
	resp := postFormValues(t, h, v)
	resp.mustOK(t)

	stack, err := h.Backend.(*cloudformation.InMemoryBackend).DescribeStack("notif-stack")
	require.NoError(t, err)
	assert.Len(t, stack.NotificationARNs, 1)
	assert.Equal(t, "arn:aws:sns:us-east-1:123:MyTopic", stack.NotificationARNs[0])
}

func TestHandler_CreateStack_WithTimeoutInMinutes(t *testing.T) {
	t.Parallel()

	h := newHandler()
	v := url.Values{
		"Action":           {"CreateStack"},
		"StackName":        {"timeout-stack"},
		"TemplateBody":     {simpleTemplate},
		"TimeoutInMinutes": {"45"},
	}
	resp := postFormValues(t, h, v)
	resp.mustOK(t)

	stack, err := h.Backend.(*cloudformation.InMemoryBackend).DescribeStack("timeout-stack")
	require.NoError(t, err)
	assert.Equal(t, 45, stack.TimeoutInMinutes)
}

func TestHandler_CreateStack_WithDisableRollback(t *testing.T) {
	t.Parallel()

	h := newHandler()
	v := url.Values{
		"Action":          {"CreateStack"},
		"StackName":       {"noroll-stack"},
		"TemplateBody":    {simpleTemplate},
		"DisableRollback": {"true"},
	}
	resp := postFormValues(t, h, v)
	resp.mustOK(t)

	stack, err := h.Backend.(*cloudformation.InMemoryBackend).DescribeStack("noroll-stack")
	require.NoError(t, err)
	assert.True(t, stack.DisableRollback)
}

func TestHandler_CreateStack_WithRollbackConfiguration(t *testing.T) {
	t.Parallel()

	h := newHandler()
	v := url.Values{
		"Action":       {"CreateStack"},
		"StackName":    {"rc-stack"},
		"TemplateBody": {simpleTemplate},
		"RollbackConfiguration.MonitoringTimeInMinutes":        {"10"},
		"RollbackConfiguration.RollbackTriggers.member.1.Arn":  {"arn:aws:cloudwatch:us-east-1:123:alarm/MyAlarm"},
		"RollbackConfiguration.RollbackTriggers.member.1.Type": {"AWS::CloudWatch::Alarm"},
	}
	resp := postFormValues(t, h, v)
	resp.mustOK(t)

	stack, err := h.Backend.(*cloudformation.InMemoryBackend).DescribeStack("rc-stack")
	require.NoError(t, err)
	require.NotNil(t, stack.RollbackConfiguration)
	assert.Equal(t, 10, stack.RollbackConfiguration.MonitoringTimeInMinutes)
	assert.Len(t, stack.RollbackConfiguration.RollbackTriggers, 1)
}

// ---- Handler: UpdateStack with new fields -------------------------------------

func TestHandler_UpdateStack_WithCapabilities(t *testing.T) {
	t.Parallel()

	h := newHandler()

	// Create first.
	postFormValues(t, h, url.Values{
		"Action": {"CreateStack"}, "StackName": {"us-cap"}, "TemplateBody": {simpleTemplate},
	})

	v := url.Values{
		"Action":                {"UpdateStack"},
		"StackName":             {"us-cap"},
		"TemplateBody":          {simpleTemplate},
		"Capabilities.member.1": {"CAPABILITY_AUTO_EXPAND"},
	}
	resp := postFormValues(t, h, v)
	resp.mustOK(t)

	stack, err := h.Backend.(*cloudformation.InMemoryBackend).DescribeStack("us-cap")
	require.NoError(t, err)
	assert.Contains(t, stack.Capabilities, "CAPABILITY_AUTO_EXPAND")
}

// ---- Handler: UpdateTerminationProtection -------------------------------------

func TestHandler_UpdateTerminationProtection_Enable(t *testing.T) {
	t.Parallel()

	h := newHandler()
	postFormValues(t, h, url.Values{
		"Action": {"CreateStack"}, "StackName": {"prot"}, "TemplateBody": {simpleTemplate},
	})

	v := url.Values{
		"Action":                      {"UpdateTerminationProtection"},
		"StackName":                   {"prot"},
		"EnableTerminationProtection": {"true"},
	}
	resp := postFormValues(t, h, v)
	resp.mustOK(t)

	stack, err := h.Backend.(*cloudformation.InMemoryBackend).DescribeStack("prot")
	require.NoError(t, err)
	assert.True(t, stack.EnableTerminationProtection)
}

func TestHandler_DeleteStack_TerminationProtected_Returns403(t *testing.T) {
	t.Parallel()

	h := newHandler()
	postFormValues(t, h, url.Values{
		"Action": {"CreateStack"}, "StackName": {"prot-del"}, "TemplateBody": {simpleTemplate},
	})
	postFormValues(t, h, url.Values{
		"Action": {"UpdateTerminationProtection"}, "StackName": {"prot-del"}, "EnableTerminationProtection": {"true"},
	})

	resp := postFormValues(t, h, url.Values{
		"Action": {"DeleteStack"}, "StackName": {"prot-del"},
	})
	// Should return an error (non-200).
	assert.NotEqual(t, http.StatusOK, resp.Status)
}

// ---- Handler: ValidateTemplate returns AllowedValues --------------------------

func TestHandler_ValidateTemplate_ReturnsCapabilities(t *testing.T) {
	t.Parallel()

	h := newHandler()
	tmpl := `{
		"AWSTemplateFormatVersion": "2010-09-09",
		"Parameters": {
			"Env": {"Type": "String", "AllowedValues": ["dev", "prod"]}
		},
		"Resources": {
			"Role": {"Type": "AWS::IAM::Role"}
		}
	}`
	v := url.Values{
		"Action":       {"ValidateTemplate"},
		"TemplateBody": {tmpl},
	}
	resp := postFormValues(t, h, v)
	resp.mustOK(t)
}

// ---- Handler: GetTemplateSummary returns AllowedValues / NoEcho ---------------

func TestHandler_GetTemplateSummary_AllowedValues(t *testing.T) {
	t.Parallel()

	h := newHandler()
	tmpl := `{
		"AWSTemplateFormatVersion": "2010-09-09",
		"Parameters": {
			"Env": {
				"Type": "String",
				"AllowedValues": ["dev", "staging", "prod"],
				"NoEcho": false
			},
			"Secret": {
				"Type": "String",
				"NoEcho": true
			}
		},
		"Resources": {"B": {"Type": "AWS::S3::Bucket"}}
	}`
	v := url.Values{
		"Action":       {"GetTemplateSummary"},
		"TemplateBody": {tmpl},
	}
	resp := postFormValues(t, h, v)
	resp.mustOK(t)
	// Response should contain AllowedValues members.
	assert.Contains(t, resp.Body, "dev")
	assert.Contains(t, resp.Body, "staging")
	assert.Contains(t, resp.Body, "prod")
}

// ---- Handler: DescribeStacks returns new fields --------------------------------

func TestHandler_DescribeStacks_ReturnsCapabilities(t *testing.T) {
	t.Parallel()

	h := newHandler()
	postFormValues(t, h, url.Values{
		"Action":                {"CreateStack"},
		"StackName":             {"cap-describe"},
		"TemplateBody":          {simpleTemplate},
		"Capabilities.member.1": {"CAPABILITY_IAM"},
	})

	v := url.Values{
		"Action":    {"DescribeStacks"},
		"StackName": {"cap-describe"},
	}
	resp := postFormValues(t, h, v)
	resp.mustOK(t)
	assert.Contains(t, resp.Body, "CAPABILITY_IAM")
}

// ---- Handler: StackSet operations via HTTP ------------------------------------

func TestHandler_StackSetCRUD_HTTP(t *testing.T) {
	t.Parallel()

	h := newHandler()

	// Create.
	resp := postFormValues(t, h, url.Values{
		"Action":       {"CreateStackSet"},
		"StackSetName": {"my-http-ss"},
		"Description":  {"http test set"},
		"TemplateBody": {simpleTemplate},
	})
	resp.mustOK(t)

	// Describe.
	resp = postFormValues(t, h, url.Values{
		"Action":       {"DescribeStackSet"},
		"StackSetName": {"my-http-ss"},
	})
	resp.mustOK(t)
	assert.Contains(t, resp.Body, "my-http-ss")

	// List.
	resp = postFormValues(t, h, url.Values{"Action": {"ListStackSets"}})
	resp.mustOK(t)

	// Delete.
	resp = postFormValues(t, h, url.Values{
		"Action":       {"DeleteStackSet"},
		"StackSetName": {"my-http-ss"},
	})
	resp.mustOK(t)
}

func TestHandler_StackInstances_HTTP(t *testing.T) {
	t.Parallel()

	h := newHandler()

	postFormValues(t, h, url.Values{
		"Action": {"CreateStackSet"}, "StackSetName": {"inst-ss"}, "TemplateBody": {simpleTemplate},
	})

	// Create instances.
	resp := postFormValues(t, h, url.Values{
		"Action":            {"CreateStackInstances"},
		"StackSetName":      {"inst-ss"},
		"Accounts.member.1": {"111111111111"},
		"Regions.member.1":  {"us-east-1"},
		"Regions.member.2":  {"us-west-2"},
	})
	resp.mustOK(t)

	// List instances.
	resp = postFormValues(t, h, url.Values{
		"Action":       {"ListStackInstances"},
		"StackSetName": {"inst-ss"},
	})
	resp.mustOK(t)

	// Describe instance.
	resp = postFormValues(t, h, url.Values{
		"Action":               {"DescribeStackInstance"},
		"StackSetName":         {"inst-ss"},
		"StackInstanceAccount": {"111111111111"},
		"StackInstanceRegion":  {"us-east-1"},
	})
	resp.mustOK(t)
}

// ---- Handler: Drift detection via HTTP ----------------------------------------

func TestHandler_DetectStackDrift_HTTP(t *testing.T) {
	t.Parallel()

	h := newHandler()
	postFormValues(t, h, url.Values{
		"Action": {"CreateStack"}, "StackName": {"drift-http"}, "TemplateBody": {simpleTemplate},
	})

	resp := postFormValues(t, h, url.Values{
		"Action":    {"DetectStackDrift"},
		"StackName": {"drift-http"},
	})
	resp.mustOK(t)
	assert.Contains(t, resp.Body, "StackDriftDetectionId")
}

func TestHandler_DescribeStackDriftDetectionStatus_HTTP(t *testing.T) {
	t.Parallel()

	h := newHandler()
	postFormValues(t, h, url.Values{
		"Action": {"CreateStack"}, "StackName": {"drift-status"}, "TemplateBody": {simpleTemplate},
	})

	// Start drift detection.
	respDrift := postFormValues(t, h, url.Values{
		"Action":    {"DetectStackDrift"},
		"StackName": {"drift-status"},
	})
	respDrift.mustOK(t)

	// Parse detection ID.
	var detResp struct {
		XMLName xml.Name `xml:"DetectStackDriftResponse"`
		Result  struct {
			ID string `xml:"StackDriftDetectionId"`
		} `xml:"DetectStackDriftResult"`
	}
	err := xml.Unmarshal([]byte(respDrift.Body), &detResp)
	require.NoError(t, err)

	// Describe status.
	resp := postFormValues(t, h, url.Values{
		"Action":                {"DescribeStackDriftDetectionStatus"},
		"StackDriftDetectionId": {detResp.Result.ID},
	})
	resp.mustOK(t)
	assert.Contains(t, resp.Body, "DETECTION_COMPLETE")
}

// ---- Handler: ListExports via HTTP --------------------------------------------

func TestHandler_ListExports_HTTP(t *testing.T) {
	t.Parallel()

	h := newHandler()

	// Create an exporter stack.
	exportTmpl := `{
		"AWSTemplateFormatVersion": "2010-09-09",
		"Resources": {"Q": {"Type": "AWS::SQS::Queue"}},
		"Outputs": {"QueueURL": {"Value": {"Ref": "Q"}, "Export": {"Name": "http-queue-url"}}}
	}`
	postFormValues(t, h, url.Values{
		"Action": {"CreateStack"}, "StackName": {"exp-http"}, "TemplateBody": {exportTmpl},
	})

	resp := postFormValues(t, h, url.Values{"Action": {"ListExports"}})
	resp.mustOK(t)
	assert.Contains(t, resp.Body, "http-queue-url")
}

func TestHandler_ListImports_HTTP(t *testing.T) {
	t.Parallel()

	h := newHandler()

	exportTmpl := `{
		"AWSTemplateFormatVersion": "2010-09-09",
		"Resources": {"Q": {"Type": "AWS::SQS::Queue"}},
		"Outputs": {"U": {"Value": {"Ref": "Q"}, "Export": {"Name": "imp-queue"}}}
	}`
	importTmpl := `{
		"AWSTemplateFormatVersion": "2010-09-09",
		"Resources": {"Q": {"Type": "AWS::SQS::Queue"}},
		"Outputs": {"U": {"Value": {"Fn::ImportValue": "imp-queue"}}}
	}`

	postFormValues(t, h, url.Values{
		"Action": {"CreateStack"}, "StackName": {"exp-imp-src"}, "TemplateBody": {exportTmpl},
	})
	postFormValues(t, h, url.Values{
		"Action": {"CreateStack"}, "StackName": {"exp-imp-dst"}, "TemplateBody": {importTmpl},
	})

	resp := postFormValues(t, h, url.Values{
		"Action":     {"ListImports"},
		"ExportName": {"imp-queue"},
	})
	resp.mustOK(t)
}

// ---- Backend: StackSet drift detection ----------------------------------------

func TestStackSetDrift(t *testing.T) {
	t.Parallel()

	b := newBackend()
	_, err := b.CreateStackSet("drift-ss", "desc", simpleTemplate)
	require.NoError(t, err)

	opID, err := b.DetectStackSetDrift("drift-ss")
	require.NoError(t, err)
	assert.NotEmpty(t, opID)

	op, err := b.DescribeStackSetOperation("drift-ss", opID)
	require.NoError(t, err)
	assert.NotEmpty(t, op.Status)
}

func TestStackSetOperationList(t *testing.T) {
	t.Parallel()

	b := newBackend()
	_, err := b.CreateStackSet("op-list-ss", "desc", simpleTemplate)
	require.NoError(t, err)

	_, err = b.CreateStackInstances("op-list-ss", []string{"111"}, []string{"us-east-1"})
	require.NoError(t, err)

	ops, err := b.ListStackSetOperations("op-list-ss", "")
	require.NoError(t, err)
	assert.NotEmpty(t, ops)
}

func TestStopStackSetOperation(t *testing.T) {
	t.Parallel()

	b := newBackend()
	_, err := b.CreateStackSet("stop-ss", "desc", simpleTemplate)
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
	assert.NotEmpty(t, list)

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
	assert.NotEmpty(t, scans)

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

	err := b.ActivateType("AWS::S3::Bucket", "arn:aws:cloudformation:us-east-1::type/resource/AWS-S3-Bucket")
	require.NoError(t, err)

	err = b.DeactivateType("AWS::S3::Bucket", "arn:aws:cloudformation:us-east-1::type/resource/AWS-S3-Bucket")
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

	result, err := b.BatchDescribeTypeConfigurations([]string{"My::Test::Type"})
	require.NoError(t, err)
	assert.NotNil(t, result)
}

// ---- Backend: Publisher -------------------------------------------------------

func TestPublisher_RegisterAndDescribe(t *testing.T) {
	t.Parallel()

	b := newBackend()

	publisherID, err := b.RegisterPublisher("arn:aws:codestar-connections:us-east-1:123:connection/abc")
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
	_, err := b.CreateStack(t.Context(), "signal-stack", simpleTemplate, nil, cloudformation.StackOptions{})
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

	refactorID, err := b.CreateStackRefactor("my refactor", []string{"stack-a", "stack-b"})
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
	_, err := b.CreateStackSet("import-ss", "desc", simpleTemplate)
	require.NoError(t, err)

	err = b.ImportStacksToStackSet("import-ss", []string{"arn:aws:cloudformation:us-east-1:123:stack/my-stack/abc"})
	require.NoError(t, err)
}

// ---- Backend: ListStackSetAutoDeploymentTargets --------------------------------

func TestListStackSetAutoDeploymentTargets(t *testing.T) {
	t.Parallel()

	b := newBackend()
	_, err := b.CreateStackSet("auto-deploy-ss", "desc", simpleTemplate)
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

	stack, err := b.CreateStack(t.Context(), "multi-params", tmpl,
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
	_, err := b.CreateStack(t.Context(), "rb-change", simpleTemplate, nil, cloudformation.StackOptions{})
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
	_, err := b.CreateStack(t.Context(), "alive", simpleTemplate, nil, cloudformation.StackOptions{})
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
	_, err := b.CreateStack(t.Context(), "ev-stack", simpleTemplate, nil, cloudformation.StackOptions{})
	require.NoError(t, err)

	events, err := b.DescribeEvents("")
	require.NoError(t, err)
	assert.NotEmpty(t, events)
}

// ---- Backend: ChangeSet for new stack (ExecuteChangeSet creates stack) --------

func TestChangeSet_ExecuteOnNewStack(t *testing.T) {
	t.Parallel()

	b := newBackend()

	// Create changeset without pre-existing stack.
	cs, err := b.CreateChangeSet(t.Context(), "brand-new", "my-cs", simpleTemplate, "init", nil)
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

// ---- Handler: DescribeStackResources HTTP -------------------------------------

func TestHandler_DescribeStackResources_HTTP(t *testing.T) {
	t.Parallel()

	h := newHandler()

	tmpl := `{
		"AWSTemplateFormatVersion": "2010-09-09",
		"Resources": {
			"Q": {"Type": "AWS::SQS::Queue"},
			"Topic": {"Type": "AWS::SNS::Topic"}
		}
	}`
	postFormValues(t, h, url.Values{
		"Action": {"CreateStack"}, "StackName": {"res-http"}, "TemplateBody": {tmpl},
	})

	resp := postFormValues(t, h, url.Values{
		"Action":    {"DescribeStackResources"},
		"StackName": {"res-http"},
	})
	resp.mustOK(t)
	assert.Contains(t, resp.Body, "AWS::SQS::Queue")
}

// ---- Handler: DescribeAccountLimits HTTP --------------------------------------

func TestHandler_DescribeAccountLimits_HTTP(t *testing.T) {
	t.Parallel()

	h := newHandler()
	resp := postFormValues(t, h, url.Values{"Action": {"DescribeAccountLimits"}})
	resp.mustOK(t)
	assert.Contains(t, resp.Body, "AccountLimitsResult")
}

// ---- Handler: SetStackPolicy / GetStackPolicy HTTP ----------------------------

func TestHandler_StackPolicy_HTTP(t *testing.T) {
	t.Parallel()

	h := newHandler()
	postFormValues(t, h, url.Values{
		"Action": {"CreateStack"}, "StackName": {"pol-http"}, "TemplateBody": {simpleTemplate},
	})

	policy := `{"Statement":[{"Effect":"Allow","Action":"Update:*","Principal":"*","Resource":"*"}]}`
	resp := postFormValues(t, h, url.Values{
		"Action":          {"SetStackPolicy"},
		"StackName":       {"pol-http"},
		"StackPolicyBody": {policy},
	})
	resp.mustOK(t)

	resp = postFormValues(t, h, url.Values{
		"Action":    {"GetStackPolicy"},
		"StackName": {"pol-http"},
	})
	resp.mustOK(t)
	assert.Contains(t, resp.Body, "Effect")
}

// ---- Handler: ContinueUpdateRollback / CancelUpdateStack HTTP -----------------

func TestHandler_ContinueUpdateRollback_HTTP(t *testing.T) {
	t.Parallel()

	h := newHandler()
	postFormValues(t, h, url.Values{
		"Action": {"CreateStack"}, "StackName": {"cur-http"}, "TemplateBody": {simpleTemplate},
	})

	resp := postFormValues(t, h, url.Values{
		"Action":    {"ContinueUpdateRollback"},
		"StackName": {"cur-http"},
	})
	resp.mustOK(t)
}

func TestHandler_CancelUpdateStack_HTTP(t *testing.T) {
	t.Parallel()

	h := newHandler()
	postFormValues(t, h, url.Values{
		"Action": {"CreateStack"}, "StackName": {"cancel-http"}, "TemplateBody": {simpleTemplate},
	})

	resp := postFormValues(t, h, url.Values{
		"Action":    {"CancelUpdateStack"},
		"StackName": {"cancel-http"},
	})
	resp.mustOK(t)
}

// ---- Handler: EstimateTemplateCost HTTP ----------------------------------------

func TestHandler_EstimateTemplateCost_HTTP(t *testing.T) {
	t.Parallel()

	h := newHandler()
	resp := postFormValues(t, h, url.Values{
		"Action":       {"EstimateTemplateCost"},
		"TemplateBody": {simpleTemplate},
	})
	resp.mustOK(t)
	assert.Contains(t, resp.Body, "EstimateTemplateCostResult")
}

// ---- Handler: RollbackStack HTTP ----------------------------------------------

func TestHandler_RollbackStack_HTTP(t *testing.T) {
	t.Parallel()

	h := newHandler()
	postFormValues(t, h, url.Values{
		"Action": {"CreateStack"}, "StackName": {"rb-http"}, "TemplateBody": {simpleTemplate},
	})

	resp := postFormValues(t, h, url.Values{
		"Action":    {"RollbackStack"},
		"StackName": {"rb-http"},
	})
	resp.mustOK(t)
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

	stack, err := b.CreateStack(t.Context(), "av-bad-default", tmpl, nil, cloudformation.StackOptions{})
	require.NoError(t, err)
	// Default "qa" not in AllowedValues → ROLLBACK_COMPLETE.
	assert.Equal(t, "ROLLBACK_COMPLETE", stack.StackStatus)
}

// ---- Backend: ChangeSet changes computed correctly ----------------------------

func TestChangeSet_ChangesContainAdd(t *testing.T) {
	t.Parallel()

	b := newBackend()
	_, err := b.CreateStack(t.Context(), "cs-chg", simpleTemplate, nil, cloudformation.StackOptions{})
	require.NoError(t, err)

	newTmpl := `{
		"AWSTemplateFormatVersion": "2010-09-09",
		"Resources": {
			"MyBucket": {"Type": "AWS::S3::Bucket"},
			"NewQueue": {"Type": "AWS::SQS::Queue"}
		}
	}`

	cs, err := b.CreateChangeSet(t.Context(), "cs-chg", "my-changes", newTmpl, "add queue", nil)
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

	stack, err := b.CreateStack(t.Context(), "parent-params", parentTemplate, nil, cloudformation.StackOptions{})
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
			_, err := b.CreateStack(t.Context(), n, simpleTemplate, nil, cloudformation.StackOptions{})
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

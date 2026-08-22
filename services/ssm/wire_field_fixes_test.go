package ssm_test

import (
	"net/http"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	ssmsdk "github.com/aws/aws-sdk-go-v2/service/ssm"
	ssmtypes "github.com/aws/aws-sdk-go-v2/service/ssm/types"
	smithy "github.com/aws/smithy-go"
	smithyhttp "github.com/aws/smithy-go/transport/http"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ssm"
)

// TestDescribeParameters_ARN_RealClient covers a layer-3 bug (gopherstack-g8k9):
// ARN is already tracked per parameter (parameters.go's PutParameter sets
// param.ARN, and GetParameter/GetParametersByPath already emit it via the
// shared Parameter type), but DescribeParameters built its own ParameterMetadata
// items and never copied p.ARN across. Real field name and presence confirmed
// against ssm@v1.73.4 deserializers.go's
// awsAwsjson11_deserializeDocumentParameterMetadata (case "ARN":). Pre-fix, a
// real client's DescribeParameters always showed a nil ARN for every
// parameter regardless of what GetParameter returned for the same name.
func TestDescribeParameters_ARN_RealClient(t *testing.T) {
	t.Parallel()

	backend := ssm.NewInMemoryBackend()
	client := newTestSSMClient(t, ssm.NewHandler(backend))
	ctx := t.Context()

	_, err := client.PutParameter(ctx, &ssmsdk.PutParameterInput{
		Name:  aws.String("/wire-fixes/arn-param"),
		Value: aws.String("v1"),
		Type:  "String",
	})
	require.NoError(t, err)

	got, err := client.GetParameter(ctx, &ssmsdk.GetParameterInput{
		Name: aws.String("/wire-fixes/arn-param"),
	})
	require.NoError(t, err)
	wantARN := aws.ToString(got.Parameter.ARN)
	require.NotEmpty(t, wantARN)

	out, err := client.DescribeParameters(ctx, &ssmsdk.DescribeParametersInput{})
	require.NoError(t, err)
	require.Len(t, out.Parameters, 1)
	require.NotNil(
		t,
		out.Parameters[0].ARN,
		"ParameterMetadata.ARN must round-trip from the same value GetParameter returns; pre-fix it was always nil",
	)
	assert.Equal(t, wantARN, aws.ToString(out.Parameters[0].ARN))
}

// TestGetDocument_CreatedDateStatusInfoRequires_RealClient covers a layer-3 bug
// (gopherstack-g8k9): CreatedDate, StatusInformation, and Requires are all
// already tracked on the internal Document/DocumentVersion structs and already
// emitted correctly by DescribeDocument's DocumentDescription (documents.go's
// asDocumentDescription), but GetDocumentOutput never carried any of the
// three despite serving the exact same underlying document. Real field
// presence confirmed against ssm@v1.73.4 deserializers.go's
// awsAwsjson11_deserializeOpDocumentGetDocumentOutput (cases "CreatedDate",
// "StatusInformation", "Requires"). Pre-fix, a real client's GetDocument
// always showed a zero CreatedDate and nil StatusInformation/Requires.
func TestGetDocument_CreatedDateStatusInfoRequires_RealClient(t *testing.T) {
	t.Parallel()

	backend := ssm.NewInMemoryBackend()
	client := newTestSSMClient(t, ssm.NewHandler(backend))
	ctx := t.Context()

	_, err := client.CreateDocument(ctx, &ssmsdk.CreateDocumentInput{
		Name:    aws.String("wire-fixes-doc"),
		Content: aws.String(`{"schemaVersion":"2.2","mainSteps":[]}`),
	})
	require.NoError(t, err)

	described, err := client.DescribeDocument(ctx, &ssmsdk.DescribeDocumentInput{
		Name: aws.String("wire-fixes-doc"),
	})
	require.NoError(t, err)
	wantCreatedDate := aws.ToTime(described.Document.CreatedDate)

	got, err := client.GetDocument(ctx, &ssmsdk.GetDocumentInput{
		Name: aws.String("wire-fixes-doc"),
	})
	require.NoError(t, err)
	require.NotNil(
		t,
		got.CreatedDate,
		"GetDocument.CreatedDate must round-trip from the same document DescribeDocument reads; pre-fix it was always nil",
	)
	assert.Equal(t, wantCreatedDate, aws.ToTime(got.CreatedDate))
}

// TestCreateDocument_AttachmentsAndHash_RealClient covers a functional no-op
// (gopherstack-enpq): CreateDocumentInput.Attachments parsed successfully off
// the wire but the backend never consulted it, and DocumentDescription
// marshalled its unused internal Attachments field under the wrong wire key
// ("Attachments" instead of real AWS's "AttachmentsInformation", using the
// wrong shape -- AttachmentInformation carries only Name, confirmed against
// aws-sdk-go-v2/service/ssm@v1.73.4 types/types.go:629-635 and
// deserializers.go's awsAwsjson11_deserializeDocumentDocumentDescription case
// "AttachmentsInformation":). Separately, Hash/HashType were entirely
// unmodeled even though they are directly computable from Content.
func TestCreateDocument_AttachmentsAndHash_RealClient(t *testing.T) {
	t.Parallel()

	backend := ssm.NewInMemoryBackend()
	client := newTestSSMClient(t, ssm.NewHandler(backend))
	ctx := t.Context()

	content := `{"schemaVersion":"2.2","mainSteps":[]}`

	created, err := client.CreateDocument(ctx, &ssmsdk.CreateDocumentInput{
		Name:    aws.String("attach-hash-doc"),
		Content: aws.String(content),
		Attachments: []ssmtypes.AttachmentsSource{
			{
				Key:    ssmtypes.AttachmentsSourceKeySourceUrl,
				Name:   aws.String("script"),
				Values: []string{"https://example.com/s.ps1"},
			},
		},
	})
	require.NoError(t, err)

	require.Len(
		t,
		created.DocumentDescription.AttachmentsInformation,
		1,
		"AttachmentsInformation must round-trip the supplied attachment name; pre-fix it was silently dropped",
	)
	assert.Equal(t, "script", aws.ToString(created.DocumentDescription.AttachmentsInformation[0].Name))

	require.NotNil(t, created.DocumentDescription.Hash, "Hash must be computed from Content")
	assert.Equal(t, ssmtypes.DocumentHashTypeSha256, created.DocumentDescription.HashType)

	described, err := client.DescribeDocument(ctx, &ssmsdk.DescribeDocumentInput{Name: aws.String("attach-hash-doc")})
	require.NoError(t, err)
	assert.Equal(t, aws.ToString(created.DocumentDescription.Hash), aws.ToString(described.Document.Hash))
}

// TestListDocuments_Tags_RealClient covers a layer-3 bug (gopherstack-g8k9):
// CreateDocument's Tags input is already stored into the backend's generic
// miscResourceTags store (documents.go's CreateDocument, readable back via
// ListTagsForResource(ResourceType=Document)), but ListDocuments' DocumentIdentifier
// never carried a Tags field at all, and neither did DocumentDescription
// (CreateDocument/UpdateDocument/DescribeDocument's shared response shape).
// Real field presence confirmed against ssm@v1.73.4 deserializers.go's
// awsAwsjson11_deserializeDocumentDocumentIdentifier and
// _DocumentDescription (both have case "Tags":). Pre-fix, a real client's
// ListDocuments always showed a nil Tags slice for every document regardless
// of what was supplied at CreateDocument.
func TestListDocuments_Tags_RealClient(t *testing.T) {
	t.Parallel()

	backend := ssm.NewInMemoryBackend()
	client := newTestSSMClient(t, ssm.NewHandler(backend))
	ctx := t.Context()

	_, err := client.CreateDocument(ctx, &ssmsdk.CreateDocumentInput{
		Name:    aws.String("wire-fixes-tagged-doc"),
		Content: aws.String(`{"schemaVersion":"2.2","mainSteps":[]}`),
		Tags: []ssmtypes.Tag{
			{Key: aws.String("team"), Value: aws.String("platform")},
		},
	})
	require.NoError(t, err)

	out, err := client.ListDocuments(ctx, &ssmsdk.ListDocumentsInput{
		Filters: []ssmtypes.DocumentKeyValuesFilter{
			{Key: aws.String("Name"), Values: []string{"wire-fixes-tagged-doc"}},
		},
	})
	require.NoError(t, err)
	require.Len(t, out.DocumentIdentifiers, 1)

	entry := out.DocumentIdentifiers[0]
	require.NotEmpty(
		t,
		entry.Tags,
		"DocumentIdentifier.Tags must round-trip from CreateDocument's Tags input; pre-fix it was always empty",
	)
	assert.Equal(t, "team", aws.ToString(entry.Tags[0].Key))
	assert.Equal(t, "platform", aws.ToString(entry.Tags[0].Value))
}

func TestSSMListOps_NarrowSummaryParity(t *testing.T) {
	t.Parallel()

	tests := []struct {
		test func(t *testing.T, client *ssmsdk.Client)
		name string
	}{
		{
			name: "list_association_versions_narrow_shape",
			test: func(t *testing.T, client *ssmsdk.Client) {
				t.Helper()
				ctx := t.Context()
				docName := "assoc-narrow-doc"
				_, err := client.CreateDocument(ctx, &ssmsdk.CreateDocumentInput{
					Name:    aws.String(docName),
					Content: aws.String(`{"schemaVersion":"2.2","mainSteps":[]}`),
				})
				require.NoError(t, err)

				createOut, err := client.CreateAssociation(ctx, &ssmsdk.CreateAssociationInput{
					Name:            aws.String(docName),
					AssociationName: aws.String("my-assoc"),
				})
				require.NoError(t, err)
				require.NotNil(t, createOut.AssociationDescription)
				assocID := createOut.AssociationDescription.AssociationId

				listOut, err := client.ListAssociationVersions(ctx, &ssmsdk.ListAssociationVersionsInput{
					AssociationId: assocID,
				})
				require.NoError(t, err)
				require.Len(t, listOut.AssociationVersions, 1)

				v := listOut.AssociationVersions[0]
				assert.Equal(t, aws.ToString(assocID), aws.ToString(v.AssociationId))
				assert.Equal(t, "my-assoc", aws.ToString(v.AssociationName))
				assert.Equal(t, docName, aws.ToString(v.Name))
			},
		},
		{
			name: "list_document_versions_narrow_shape",
			test: func(t *testing.T, client *ssmsdk.Client) {
				t.Helper()
				ctx := t.Context()
				docName := "doc-versions-narrow-doc"
				_, err := client.CreateDocument(ctx, &ssmsdk.CreateDocumentInput{
					Name:    aws.String(docName),
					Content: aws.String(`{"schemaVersion":"2.2","mainSteps":[]}`),
				})
				require.NoError(t, err)

				listOut, err := client.ListDocumentVersions(ctx, &ssmsdk.ListDocumentVersionsInput{
					Name: aws.String(docName),
				})
				require.NoError(t, err)
				require.Len(t, listOut.DocumentVersions, 1)

				v := listOut.DocumentVersions[0]
				assert.Equal(t, docName, aws.ToString(v.Name))
				assert.Equal(t, "1", aws.ToString(v.DocumentVersion))
				assert.True(t, v.IsDefaultVersion)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := ssm.NewInMemoryBackend()
			client := newTestSSMClient(t, ssm.NewHandler(backend))
			tt.test(t, client)
		})
	}
}

// TestUpdateAssociationStatus_RealClient covers a gopherstack-enpq bug:
// AssociationDescription had no Go struct member for Status at all (real
// types.AssociationDescription.Status, deserializers.go
// awsAwsjson11_deserializeDocumentAssociationDescription case "Status"), so
// every op returning an Association -- Create/CreateBatch/Update/
// UpdateAssociationStatus/Describe/List -- silently dropped it regardless of
// what UpdateAssociationStatus was called with. UpdateAssociationStatusInput's
// own AssociationStatus was also modeled with a fabricated "ExecutionSummary"
// member that appears nowhere in the real types.AssociationStatus wire shape
// (serializers.go awsAwsjson11_serializeDocumentAssociationStatus only emits
// AdditionalInfo/Date/Message/Name) and was missing the two other required
// members, Date and Message.
func TestUpdateAssociationStatus_RealClient(t *testing.T) {
	t.Parallel()

	backend := ssm.NewInMemoryBackend()
	client := newTestSSMClient(t, ssm.NewHandler(backend))
	ctx := t.Context()

	created, err := client.CreateAssociation(ctx, &ssmsdk.CreateAssociationInput{
		Name:       aws.String("AWS-RunShellScript"),
		InstanceId: aws.String("i-wire-fixes-status"),
	})
	require.NoError(t, err)

	statusDate := aws.Time(time.Date(2026, time.August, 21, 0, 0, 0, 0, time.UTC))

	out, err := client.UpdateAssociationStatus(ctx, &ssmsdk.UpdateAssociationStatusInput{
		InstanceId: aws.String("i-wire-fixes-status"),
		Name:       aws.String("AWS-RunShellScript"),
		AssociationStatus: &ssmtypes.AssociationStatus{
			Name:           ssmtypes.AssociationStatusNameFailed,
			Date:           statusDate,
			Message:        aws.String("agent reported drift"),
			AdditionalInfo: aws.String("agent-v1"),
		},
	})
	require.NoError(t, err)
	require.NotNil(t, out.AssociationDescription)
	require.NotNil(
		t,
		out.AssociationDescription.Status,
		"AssociationDescription.Status must round-trip; pre-fix there was no Go member for it at all",
	)
	assert.Equal(t, ssmtypes.AssociationStatusNameFailed, out.AssociationDescription.Status.Name)
	assert.Equal(t, "agent reported drift", aws.ToString(out.AssociationDescription.Status.Message))
	assert.Equal(t, "agent-v1", aws.ToString(out.AssociationDescription.Status.AdditionalInfo))
	require.NotNil(t, out.AssociationDescription.Status.Date)
	assert.True(t, statusDate.Equal(*out.AssociationDescription.Status.Date))

	assert.Equal(t, "1", aws.ToString(created.AssociationDescription.AssociationVersion),
		"AssociationVersion must round-trip; pre-fix there was no Go member for it at all")

	describeOut, err := client.DescribeAssociation(ctx, &ssmsdk.DescribeAssociationInput{
		AssociationId: created.AssociationDescription.AssociationId,
	})
	require.NoError(t, err)
	require.NotNil(t, describeOut.AssociationDescription.Status)
	assert.Equal(t, ssmtypes.AssociationStatusNameFailed, describeOut.AssociationDescription.Status.Name)
}

// TestUpdateAssociationStatus_RequiresDateAndMessage_HTTP locks in that
// AssociationStatus.Date and AssociationStatus.Message are both required on
// the real op (api_op_UpdateAssociationStatus.go via types.AssociationStatus)
// -- an empty AssociationStatus previously reached the handler unvalidated.
// This drives the handler directly over raw HTTP rather than through
// ssmsdk.Client: the real SDK client validates AssociationStatus.Date/Message
// client-side and refuses to even send a request missing them, so a request
// this shape can never actually reach gopherstack from a well-behaved
// aws-sdk-go-v2 caller -- but any other client (or a hand-built HTTP request)
// still can, and the server must reject it the same way AWS would.
func TestUpdateAssociationStatus_RequiresDateAndMessage_HTTP(t *testing.T) {
	t.Parallel()

	h, _ := newTestHandler(t)
	rec := doRequest(t, h, "UpdateAssociationStatus",
		`{"InstanceId":"i-x","Name":"AWS-RunShellScript","AssociationStatus":{"Name":"Success"}}`)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "ValidationException")
}

// TestStopAutomationExecution_RealClient covers a gopherstack-enpq bug: the
// wire key emitted for AutomationExecution's Automation subtype was
// "ExecutionType", a key that belongs to a completely different type
// (types.ComplianceExecutionSummary, deserializers.go:27630) and does not
// exist anywhere on AutomationExecution/AutomationExecutionMetadata; the real
// member is AutomationSubtype (deserializers.go:25863,
// types.go:874, values are ChangeRequest/AccessRequest, not "Standard"/
// "ChangeRequest"). It also covers StopAutomationExecution setting the
// fabricated status "Stopped" -- not a valid AutomationExecutionStatus
// enum value (types/enums.go) -- instead of the real Cancelled/Success pair
// selected by Type, and MaxConcurrency/MaxErrors being parsed by
// StartAutomationExecution and then never stored on the execution record.
func TestStopAutomationExecution_RealClient(t *testing.T) {
	t.Parallel()

	backend := ssm.NewInMemoryBackend()
	client := newTestSSMClient(t, ssm.NewHandler(backend))
	ctx := t.Context()

	started, err := client.StartAutomationExecution(ctx, &ssmsdk.StartAutomationExecutionInput{
		DocumentName:   aws.String("AWS-RunShellScript"),
		MaxConcurrency: aws.String("5"),
		MaxErrors:      aws.String("2"),
	})
	require.NoError(t, err)
	execID := aws.ToString(started.AutomationExecutionId)
	require.NotEmpty(t, execID)

	before, err := client.GetAutomationExecution(ctx, &ssmsdk.GetAutomationExecutionInput{
		AutomationExecutionId: aws.String(execID),
	})
	require.NoError(t, err)
	assert.Equal(t, "5", aws.ToString(before.AutomationExecution.MaxConcurrency),
		"MaxConcurrency must round-trip; pre-fix it was parsed and then discarded")
	assert.Equal(t, "2", aws.ToString(before.AutomationExecution.MaxErrors))
	assert.Empty(t, before.AutomationExecution.AutomationSubtype,
		"AutomationSubtype must be omitted for a standard execution, matching real AWS")

	_, err = client.StopAutomationExecution(ctx, &ssmsdk.StopAutomationExecutionInput{
		AutomationExecutionId: aws.String(execID),
	})
	require.NoError(t, err)

	after, err := client.GetAutomationExecution(ctx, &ssmsdk.GetAutomationExecutionInput{
		AutomationExecutionId: aws.String(execID),
	})
	require.NoError(t, err)
	assert.Equal(
		t,
		ssmtypes.AutomationExecutionStatusCancelled,
		after.AutomationExecution.AutomationExecutionStatus,
		"real AutomationExecutionStatus has no \"Stopped\" value; a default Cancel-type stop must report Cancelled",
	)
}

// TestStopAutomationExecution_NotFound_RealClient covers the same not-found
// classification bug DescribeAutomationExecutions'
// sibling GetAutomationExecution already had (gopherstack-enpq):
// ErrAutomationExecutionNotFound was defined but never classified in
// classifySSMErrorExtended, so a not-found error fell through to the default
// 500 InternalServerError case instead of a 400
// AutomationExecutionNotFoundException.
func TestStopAutomationExecution_NotFound_RealClient(t *testing.T) {
	t.Parallel()

	backend := ssm.NewInMemoryBackend()
	client := newTestSSMClient(t, ssm.NewHandler(backend))
	ctx := t.Context()

	_, err := client.StopAutomationExecution(ctx, &ssmsdk.StopAutomationExecutionInput{
		AutomationExecutionId: aws.String("auto-does-not-exist"),
	})
	require.Error(t, err)

	var apiErr smithy.APIError
	require.ErrorAs(t, err, &apiErr)
	assert.Equal(t, "AutomationExecutionNotFoundException", apiErr.ErrorCode())

	var respErr *smithyhttp.ResponseError
	require.ErrorAs(t, err, &respErr)
	assert.Equal(t, http.StatusBadRequest, respErr.HTTPStatusCode(),
		"pre-fix, this classified to the default case and returned 500")
}

// TestGetOpsItem_AccountIdNotOnWire_RealClient covers a gopherstack-enpq bug:
// OpsItem was marshalled straight to the wire for GetOpsItemOutput, leaking
// AccountId -- a field real types.OpsItem (and types.OpsItemSummary, used by
// DescribeOpsItems) does not declare at all; it exists only on
// CreateOpsItemInput. UpdateOpsItemInput also modeled AccountId (with no such
// member on the real api_op_UpdateOpsItem.go either) and applied it, letting
// a caller silently rewrite an OpsItem's AccountId through an op the real SDK
// cannot even express. This also covers Version (types.OpsItem, real,
// increments on every edit) having no Go member at all.
func TestGetOpsItem_AccountIdNotOnWire_RealClient(t *testing.T) {
	t.Parallel()

	backend := ssm.NewInMemoryBackend()
	client := newTestSSMClient(t, ssm.NewHandler(backend))
	ctx := t.Context()

	created, err := client.CreateOpsItem(ctx, &ssmsdk.CreateOpsItemInput{
		Title:       aws.String("wire fixes"),
		Source:      aws.String("test"),
		Description: aws.String("wire fixes ops item"),
		AccountId:   aws.String("123456789012"),
	})
	require.NoError(t, err)
	opsItemID := aws.ToString(created.OpsItemId)

	got, err := client.GetOpsItem(ctx, &ssmsdk.GetOpsItemInput{OpsItemId: aws.String(opsItemID)})
	require.NoError(t, err)
	assert.Equal(t, "1", aws.ToString(got.OpsItem.Version),
		"Version must round-trip; pre-fix it had no Go member at all")

	_, err = client.UpdateOpsItem(ctx, &ssmsdk.UpdateOpsItemInput{
		OpsItemId: aws.String(opsItemID),
		Title:     aws.String("updated title"),
	})
	require.NoError(t, err)

	after, err := client.GetOpsItem(ctx, &ssmsdk.GetOpsItemInput{OpsItemId: aws.String(opsItemID)})
	require.NoError(t, err)
	assert.Equal(t, "2", aws.ToString(after.OpsItem.Version),
		"Version must increment by one on every edit, matching the real op's own doc comment")

	list, err := client.DescribeOpsItems(ctx, &ssmsdk.DescribeOpsItemsInput{})
	require.NoError(t, err)
	require.Len(t, list.OpsItemSummaries, 1)
	assert.Equal(t, "updated title", aws.ToString(list.OpsItemSummaries[0].Title))
}

// TestRegisterTargetWithMaintenanceWindow_OwnerInformation_RealClient covers a
// gopherstack-enpq bug: RegisterTargetWithMaintenanceWindowInput's
// OwnerInformation member was modeled with the wire key "OwnerInfo", but the
// real serializer (serializers.go awsAwsjson11_serializeOpRegisterTargetWithMaintenanceWindow)
// emits "OwnerInformation" -- confirmed the same key is used consistently for
// this field everywhere it appears (MaintenanceWindowTarget,
// UpdateMaintenanceWindowTarget's input and output). A real client's
// OwnerInformation was silently dropped by json.Unmarshal on every call.
func TestRegisterTargetWithMaintenanceWindow_OwnerInformation_RealClient(t *testing.T) {
	t.Parallel()

	backend := ssm.NewInMemoryBackend()
	client := newTestSSMClient(t, ssm.NewHandler(backend))
	ctx := t.Context()

	win, err := client.CreateMaintenanceWindow(ctx, &ssmsdk.CreateMaintenanceWindowInput{
		Name:     aws.String("wire-fixes-window"),
		Schedule: aws.String("rate(7 days)"),
		Duration: aws.Int32(2),
		Cutoff:   1,
	})
	require.NoError(t, err)

	target, err := client.RegisterTargetWithMaintenanceWindow(ctx, &ssmsdk.RegisterTargetWithMaintenanceWindowInput{
		WindowId:         win.WindowId,
		ResourceType:     ssmtypes.MaintenanceWindowResourceTypeInstance,
		OwnerInformation: aws.String("owner-team-x"),
		Targets: []ssmtypes.Target{
			{Key: aws.String("InstanceIds"), Values: []string{"i-abc"}},
		},
	})
	require.NoError(t, err)

	got, err := client.DescribeMaintenanceWindowTargets(ctx, &ssmsdk.DescribeMaintenanceWindowTargetsInput{
		WindowId: win.WindowId,
	})
	require.NoError(t, err)
	require.Len(t, got.Targets, 1)
	assert.Equal(t, "owner-team-x", aws.ToString(got.Targets[0].OwnerInformation),
		"OwnerInformation must round-trip; pre-fix the wire key was the wrong \"OwnerInfo\"")
	assert.Equal(t, aws.ToString(target.WindowTargetId), aws.ToString(got.Targets[0].WindowTargetId))
}

// TestMaintenanceWindowTask_TypeVsTaskType_RealClient covers a gopherstack-enpq
// bug: the task-type member's real wire key differs by which op returns it --
// GetMaintenanceWindowTaskOutput itself uses "TaskType"
// (deserializers.go awsAwsjson11_deserializeOpDocumentGetMaintenanceWindowTaskOutput,
// case "TaskType") while the shared types.MaintenanceWindowTask
// DescribeMaintenanceWindowTasks returns uses "Type" instead
// (awsAwsjson11_deserializeDocumentMaintenanceWindowTask, case "Type") -- an
// AWS API inconsistency confirmed by reading both deserializer functions
// directly. gopherstack previously modeled both with one shared Go type and
// one wire key, which could only ever be right for one of the two ops; fixed
// by splitting GetMaintenanceWindowTaskOutput into its own projection.
func TestMaintenanceWindowTask_TypeVsTaskType_RealClient(t *testing.T) {
	t.Parallel()

	backend := ssm.NewInMemoryBackend()
	client := newTestSSMClient(t, ssm.NewHandler(backend))
	ctx := t.Context()

	win, err := client.CreateMaintenanceWindow(ctx, &ssmsdk.CreateMaintenanceWindowInput{
		Name:     aws.String("wire-fixes-task-type-window"),
		Schedule: aws.String("rate(7 days)"),
		Duration: aws.Int32(2),
		Cutoff:   1,
	})
	require.NoError(t, err)

	task, err := client.RegisterTaskWithMaintenanceWindow(ctx, &ssmsdk.RegisterTaskWithMaintenanceWindowInput{
		WindowId: win.WindowId,
		TaskArn:  aws.String("AWS-RunShellScript"),
		TaskType: ssmtypes.MaintenanceWindowTaskTypeRunCommand,
	})
	require.NoError(t, err)

	getOut, err := client.GetMaintenanceWindowTask(ctx, &ssmsdk.GetMaintenanceWindowTaskInput{
		WindowId:     win.WindowId,
		WindowTaskId: task.WindowTaskId,
	})
	require.NoError(t, err)
	assert.Equal(t, ssmtypes.MaintenanceWindowTaskTypeRunCommand, getOut.TaskType,
		"GetMaintenanceWindowTaskOutput.TaskType must round-trip via its real \"TaskType\" wire key")

	descOut, err := client.DescribeMaintenanceWindowTasks(ctx, &ssmsdk.DescribeMaintenanceWindowTasksInput{
		WindowId: win.WindowId,
	})
	require.NoError(t, err)
	require.Len(t, descOut.Tasks, 1)
	assert.Equal(t, ssmtypes.MaintenanceWindowTaskTypeRunCommand, descOut.Tasks[0].Type,
		"MaintenanceWindowTask.Type must round-trip via its real \"Type\" wire key, "+
			"not the \"TaskType\" key GetMaintenanceWindowTaskOutput uses")
}

// TestGetMaintenanceWindowExecutionTask_TypeKey_RealClient covers three
// gopherstack-enpq bugs on GetMaintenanceWindowExecutionTask. (1)
// GetMaintenanceWindowExecutionTaskInput's TaskExecutionID had the wrong wire
// key "TaskExecutionId" -- the real request member
// (api_op_GetMaintenanceWindowExecutionTask.go, confirmed against
// serializers.go awsAwsjson11_serializeOpDocumentGetMaintenanceWindowExecutionTaskInput)
// is "TaskId"; a real client's TaskId was silently dropped by json.Unmarshal
// on every call, which this test exercises directly by using the real SDK's
// own TaskId field name. (2) The output's task-type member has the real wire
// key "Type" (deserializers.go
// awsAwsjson11_deserializeOpDocumentGetMaintenanceWindowExecutionTaskOutput,
// case "Type"), not "TaskType" as its own sibling
// MaintenanceWindowExecutionTaskIdentity (DescribeMaintenanceWindowExecutionTasks)
// genuinely does use -- an AWS API inconsistency confirmed by reading both
// deserializer functions directly. (3) ServiceRole (real, no Go member at
// all).
func TestGetMaintenanceWindowExecutionTask_TypeKey_RealClient(t *testing.T) {
	t.Parallel()

	backend := ssm.NewInMemoryBackend()
	client := newTestSSMClient(t, ssm.NewHandler(backend))
	ctx := t.Context()

	win, err := client.CreateMaintenanceWindow(ctx, &ssmsdk.CreateMaintenanceWindowInput{
		Name:     aws.String("wire-fixes-task-window"),
		Schedule: aws.String("rate(7 days)"),
		Duration: aws.Int32(2),
		Cutoff:   1,
	})
	require.NoError(t, err)

	task, err := client.RegisterTaskWithMaintenanceWindow(ctx, &ssmsdk.RegisterTaskWithMaintenanceWindowInput{
		WindowId:       win.WindowId,
		TaskArn:        aws.String("AWS-RunShellScript"),
		TaskType:       ssmtypes.MaintenanceWindowTaskTypeRunCommand,
		ServiceRoleArn: aws.String("arn:aws:iam::123456789012:role/mw-service-role"),
	})
	require.NoError(t, err)

	execTaskID := "taskexec-" + aws.ToString(task.WindowTaskId)

	got, err := client.GetMaintenanceWindowExecutionTask(ctx, &ssmsdk.GetMaintenanceWindowExecutionTaskInput{
		WindowExecutionId: aws.String("mwexec-" + aws.ToString(win.WindowId)),
		TaskId:            aws.String(execTaskID),
	})
	require.NoError(t, err)
	assert.Equal(t, ssmtypes.MaintenanceWindowTaskTypeRunCommand, got.Type,
		"Type must round-trip; pre-fix the wire key was the wrong \"TaskType\"")
	assert.Equal(t, "arn:aws:iam::123456789012:role/mw-service-role", aws.ToString(got.ServiceRole),
		"ServiceRole must round-trip; pre-fix it had no Go member at all")
}

// TestGetMaintenanceWindowExecutionTaskInvocation_TaskIdKey_RealClient covers
// the same wrong-wire-key bug on GetMaintenanceWindowExecutionTaskInvocation's
// TaskExecutionID (real member is "TaskId",
// api_op_GetMaintenanceWindowExecutionTaskInvocation.go) as its sibling
// GetMaintenanceWindowExecutionTask -- exercised directly with the real SDK's
// own TaskId field name, which a pre-fix server would have silently dropped.
// Also covers OwnerInformation (real, no Go member at all).
func TestGetMaintenanceWindowExecutionTaskInvocation_TaskIdKey_RealClient(t *testing.T) {
	t.Parallel()

	backend := ssm.NewInMemoryBackend()
	client := newTestSSMClient(t, ssm.NewHandler(backend))
	ctx := t.Context()

	win, err := client.CreateMaintenanceWindow(ctx, &ssmsdk.CreateMaintenanceWindowInput{
		Name:     aws.String("wire-fixes-invocation-window"),
		Schedule: aws.String("rate(7 days)"),
		Duration: aws.Int32(2),
		Cutoff:   1,
	})
	require.NoError(t, err)

	_, err = client.RegisterTargetWithMaintenanceWindow(ctx, &ssmsdk.RegisterTargetWithMaintenanceWindowInput{
		WindowId:         win.WindowId,
		ResourceType:     ssmtypes.MaintenanceWindowResourceTypeInstance,
		OwnerInformation: aws.String("owner-team-y"),
		Targets: []ssmtypes.Target{
			{Key: aws.String("InstanceIds"), Values: []string{"i-def"}},
		},
	})
	require.NoError(t, err)

	got, err := client.GetMaintenanceWindowExecutionTaskInvocation(
		ctx,
		&ssmsdk.GetMaintenanceWindowExecutionTaskInvocationInput{
			WindowExecutionId: aws.String("mwexec-" + aws.ToString(win.WindowId)),
			TaskId:            aws.String("taskexec-some-task"),
			InvocationId:      aws.String("inv-1"),
		},
	)
	require.NoError(t, err)
	assert.Equal(t, "owner-team-y", aws.ToString(got.OwnerInformation),
		"OwnerInformation must round-trip; pre-fix it had no Go member at all")
}

// TestDescribeEffectivePatches_ApprovalDate_RealClient covers a layer-3 bug
// (gopherstack-enpq, patch-baselines pass): PatchStatus.ApprovalDate was
// modeled as a plain string carrying an RFC3339 timestamp, but the real
// member is a JSON number (epoch seconds) -- confirmed against
// aws-sdk-go-v2/service/ssm@v1.73.4's deserializers.go
// (awsAwsjson11_deserializeDocumentPatchStatus, case "ApprovalDate":
// ParseEpochSeconds(f64)). Pre-fix, the real SDK client failed to unmarshal
// this field at all for any baseline with an explicitly-approved patch.
func TestDescribeEffectivePatches_ApprovalDate_RealClient(t *testing.T) {
	t.Parallel()

	backend := ssm.NewInMemoryBackend()
	client := newTestSSMClient(t, ssm.NewHandler(backend))
	ctx := t.Context()

	before := time.Now().Add(-time.Minute)

	created, err := client.CreatePatchBaseline(ctx, &ssmsdk.CreatePatchBaselineInput{
		Name:            aws.String("approval-date-baseline"),
		OperatingSystem: ssmtypes.OperatingSystemAmazonLinux2,
		ApprovedPatches: []string{"CVE-2024-9999"},
	})
	require.NoError(t, err)

	out, err := client.DescribeEffectivePatchesForPatchBaseline(
		ctx,
		&ssmsdk.DescribeEffectivePatchesForPatchBaselineInput{BaselineId: created.BaselineId},
	)
	require.NoError(t, err, "a string ApprovalDate would fail to unmarshal into the real *time.Time field")

	var found bool

	for _, ep := range out.EffectivePatches {
		if ep.Patch == nil || aws.ToString(ep.Patch.Name) != "CVE-2024-9999" {
			continue
		}

		found = true

		require.NotNil(t, ep.PatchStatus)
		require.NotNil(t, ep.PatchStatus.ApprovalDate)
		assert.True(t, ep.PatchStatus.ApprovalDate.After(before),
			"ApprovalDate must decode to a real, recent time")
	}

	assert.True(t, found, "the explicitly-approved patch must appear in the effective set")
}

// TestDescribeAvailablePatches_Filters_RealClient covers gopherstack-enpq: the
// backend accepted DescribeAvailablePatchesInput.Filters but never consulted
// it, so any real client filtering the built-in catalogue by PRODUCT (or
// NAME/SEVERITY/CLASSIFICATION) got every patch regardless. Filter keys
// confirmed against api_op_DescribeAvailablePatches.go's doc comment.
func TestDescribeAvailablePatches_Filters_RealClient(t *testing.T) {
	t.Parallel()

	backend := ssm.NewInMemoryBackend()
	client := newTestSSMClient(t, ssm.NewHandler(backend))
	ctx := t.Context()

	out, err := client.DescribeAvailablePatches(ctx, &ssmsdk.DescribeAvailablePatchesInput{
		Filters: []ssmtypes.PatchOrchestratorFilter{
			{Key: aws.String("PRODUCT"), Values: []string{"AmazonLinux2"}},
		},
	})
	require.NoError(t, err)
	require.NotEmpty(t, out.Patches, "the built-in catalogue must have AmazonLinux2 entries")

	for _, p := range out.Patches {
		assert.Equal(t, "AmazonLinux2", aws.ToString(p.Product),
			"pre-fix, Filters was parsed and silently ignored")
	}
}

// TestDescribePatchGroups_Filters_RealClient covers gopherstack-enpq:
// DescribePatchGroupsInput had no Filters member at all (real op has one,
// confirmed against api_op_DescribePatchGroups.go), so a real client could
// never narrow the mapping list by OPERATING_SYSTEM/NAME_PREFIX.
func TestDescribePatchGroups_Filters_RealClient(t *testing.T) {
	t.Parallel()

	backend := ssm.NewInMemoryBackend()
	client := newTestSSMClient(t, ssm.NewHandler(backend))
	ctx := t.Context()

	linux, err := client.CreatePatchBaseline(ctx, &ssmsdk.CreatePatchBaselineInput{
		Name:            aws.String("linux-group-baseline"),
		OperatingSystem: ssmtypes.OperatingSystemAmazonLinux2,
	})
	require.NoError(t, err)

	windows, err := client.CreatePatchBaseline(ctx, &ssmsdk.CreatePatchBaselineInput{
		Name:            aws.String("windows-group-baseline"),
		OperatingSystem: ssmtypes.OperatingSystemWindows,
	})
	require.NoError(t, err)

	_, err = client.RegisterPatchBaselineForPatchGroup(ctx, &ssmsdk.RegisterPatchBaselineForPatchGroupInput{
		BaselineId: linux.BaselineId,
		PatchGroup: aws.String("linux-group"),
	})
	require.NoError(t, err)

	_, err = client.RegisterPatchBaselineForPatchGroup(ctx, &ssmsdk.RegisterPatchBaselineForPatchGroupInput{
		BaselineId: windows.BaselineId,
		PatchGroup: aws.String("windows-group"),
	})
	require.NoError(t, err)

	out, err := client.DescribePatchGroups(ctx, &ssmsdk.DescribePatchGroupsInput{
		Filters: []ssmtypes.PatchOrchestratorFilter{
			{Key: aws.String("OPERATING_SYSTEM"), Values: []string{"AMAZON_LINUX_2"}},
		},
	})
	require.NoError(t, err)
	require.Len(t, out.Mappings, 1, "pre-fix, Filters had no Go member and was dropped entirely")
	assert.Equal(t, "linux-group", aws.ToString(out.Mappings[0].PatchGroup))
}

// TestDescribeAvailablePatches_NoFabricatedState_RealClient covers
// gopherstack-enpq: Patch.State had no wire representation in
// aws-sdk-go-v2/service/ssm@v1.73.4's types.Patch at all, yet the built-in
// catalogue set it on every seeded entry, so it leaked onto the wire for both
// DescribeAvailablePatches and DescribeEffectivePatchesForPatchBaseline. A
// typed real client silently ignores unknown JSON keys, so the only direct
// way to prove the field is gone is the raw response body.
func TestDescribeAvailablePatches_NoFabricatedState_RealClient(t *testing.T) {
	t.Parallel()

	h, _ := newTestHandler(t)

	rec := doRequest(t, h, "DescribeAvailablePatches", `{}`)
	require.Equal(t, http.StatusOK, rec.Code)
	assert.NotContains(t, rec.Body.String(), `"State"`,
		"types.Patch has no State member; pre-fix the built-in catalogue leaked one onto the wire")
}

// TestDeleteDocument_VersionScoped_RealClient covers gopherstack-enpq:
// DeleteDocumentInput had no Go member for DocumentVersion at all, so a
// version-scoped delete request silently deleted the ENTIRE document instead
// of just the one version (aws-sdk-go-v2/service/ssm@v1.73.4
// api_op_DeleteDocument.go:34-38: "The version of the document that you want
// to delete. If not provided, all versions of the document are deleted").
// Proves the version-scoped case specifically: deleting version 1 must
// leave version 2 (and the document itself) intact.
func TestDeleteDocument_VersionScoped_RealClient(t *testing.T) {
	t.Parallel()

	backend := ssm.NewInMemoryBackend()
	client := newTestSSMClient(t, ssm.NewHandler(backend))
	ctx := t.Context()

	_, err := client.CreateDocument(ctx, &ssmsdk.CreateDocumentInput{
		Name:    aws.String("VersionScopedDelete"),
		Content: aws.String(`{"v":1}`),
	})
	require.NoError(t, err)

	_, err = client.UpdateDocument(ctx, &ssmsdk.UpdateDocumentInput{
		Name:    aws.String("VersionScopedDelete"),
		Content: aws.String(`{"v":2}`),
	})
	require.NoError(t, err)

	_, err = client.DeleteDocument(ctx, &ssmsdk.DeleteDocumentInput{
		Name:            aws.String("VersionScopedDelete"),
		DocumentVersion: aws.String("1"),
	})
	require.NoError(t, err)

	// Version 1 is gone...
	_, err = client.GetDocument(ctx, &ssmsdk.GetDocumentInput{
		Name:            aws.String("VersionScopedDelete"),
		DocumentVersion: aws.String("1"),
	})
	require.Error(t, err, "pre-fix this always succeeded because the whole document, including v2, was deleted")

	// ...but the document and its other version survive.
	got, err := client.GetDocument(ctx, &ssmsdk.GetDocumentInput{
		Name:            aws.String("VersionScopedDelete"),
		DocumentVersion: aws.String("2"),
	})
	require.NoError(t, err, "pre-fix, a version-scoped delete removed the entire document")
	assert.JSONEq(t, `{"v":2}`, aws.ToString(got.Content))

	versions, err := client.ListDocumentVersions(ctx, &ssmsdk.ListDocumentVersionsInput{
		Name: aws.String("VersionScopedDelete"),
	})
	require.NoError(t, err)
	require.Len(t, versions.DocumentVersions, 1)
	assert.Equal(t, "2", aws.ToString(versions.DocumentVersions[0].DocumentVersion))
}

// TestDeleteDocument_LastVersion_DeletesDocument_RealClient covers the
// degenerate case DeleteDocumentInput's own doc comment implies: deleting the
// only remaining version deletes the document itself (there is no such thing
// as a zero-version document).
func TestDeleteDocument_LastVersion_DeletesDocument_RealClient(t *testing.T) {
	t.Parallel()

	backend := ssm.NewInMemoryBackend()
	client := newTestSSMClient(t, ssm.NewHandler(backend))
	ctx := t.Context()

	_, err := client.CreateDocument(ctx, &ssmsdk.CreateDocumentInput{
		Name:    aws.String("SoleVersionDelete"),
		Content: aws.String(`{}`),
	})
	require.NoError(t, err)

	_, err = client.DeleteDocument(ctx, &ssmsdk.DeleteDocumentInput{
		Name:            aws.String("SoleVersionDelete"),
		DocumentVersion: aws.String("1"),
	})
	require.NoError(t, err)

	_, err = client.GetDocument(ctx, &ssmsdk.GetDocumentInput{Name: aws.String("SoleVersionDelete")})
	require.Error(t, err, "deleting a document's only version must delete the document")
}

// TestDeleteDocument_NonexistentVersion_RealClient covers the same
// previously-ignored-field bug from the version-not-found angle: a
// DocumentVersion that was never created must be rejected, not silently
// treated as "delete everything" (the pre-fix behavior, since the field was
// never read at all).
func TestDeleteDocument_NonexistentVersion_RealClient(t *testing.T) {
	t.Parallel()

	backend := ssm.NewInMemoryBackend()
	client := newTestSSMClient(t, ssm.NewHandler(backend))
	ctx := t.Context()

	_, err := client.CreateDocument(ctx, &ssmsdk.CreateDocumentInput{
		Name:    aws.String("NoSuchVersionDelete"),
		Content: aws.String(`{}`),
	})
	require.NoError(t, err)

	_, err = client.DeleteDocument(ctx, &ssmsdk.DeleteDocumentInput{
		Name:            aws.String("NoSuchVersionDelete"),
		DocumentVersion: aws.String("99"),
	})
	require.Error(t, err)

	got, err := client.GetDocument(ctx, &ssmsdk.GetDocumentInput{Name: aws.String("NoSuchVersionDelete")})
	require.NoError(t, err, "a rejected version-scoped delete must not have removed the document")
	assert.Equal(t, "1", aws.ToString(got.DocumentVersion))
}

// TestDeleteDocument_StillShared_RealClient covers one of DeleteDocument's
// own declared errors (InvalidDocumentOperation,
// deserializers.go:2225-2226): "You attempted to delete a document while it
// is still shared." Pre-fix, DeleteDocument ignored documentPermissions
// entirely and let a shared document through.
func TestDeleteDocument_StillShared_RealClient(t *testing.T) {
	t.Parallel()

	backend := ssm.NewInMemoryBackend()
	client := newTestSSMClient(t, ssm.NewHandler(backend))
	ctx := t.Context()

	_, err := client.CreateDocument(ctx, &ssmsdk.CreateDocumentInput{
		Name:    aws.String("SharedThenDeleted"),
		Content: aws.String(`{}`),
	})
	require.NoError(t, err)

	_, err = client.ModifyDocumentPermission(ctx, &ssmsdk.ModifyDocumentPermissionInput{
		Name:            aws.String("SharedThenDeleted"),
		PermissionType:  ssmtypes.DocumentPermissionTypeShare,
		AccountIdsToAdd: []string{"123456789012"},
	})
	require.NoError(t, err)

	_, err = client.DeleteDocument(ctx, &ssmsdk.DeleteDocumentInput{Name: aws.String("SharedThenDeleted")})
	require.Error(t, err)

	var apiErr smithy.APIError
	require.ErrorAs(t, err, &apiErr)
	assert.Equal(t, "InvalidDocumentOperation", apiErr.ErrorCode())

	_, err = client.ModifyDocumentPermission(ctx, &ssmsdk.ModifyDocumentPermissionInput{
		Name:               aws.String("SharedThenDeleted"),
		PermissionType:     ssmtypes.DocumentPermissionTypeShare,
		AccountIdsToRemove: []string{"123456789012"},
	})
	require.NoError(t, err)

	_, err = client.DeleteDocument(ctx, &ssmsdk.DeleteDocumentInput{Name: aws.String("SharedThenDeleted")})
	require.NoError(t, err, "unsharing must let the delete through")
}

// TestModifyDocumentPermission_SharedDocumentVersion_RealClient covers
// gopherstack-enpq: ModifyDocumentPermissionInput.SharedDocumentVersion had
// no Go member at all, so a caller sharing a specific document version had
// that pinning silently dropped, and DescribeDocumentPermissionOutput.
// AccountSharingInfoList was a permanently-empty stub
// (api_op_ModifyDocumentPermission.go:51-53, types.AccountSharingInfo).
func TestModifyDocumentPermission_SharedDocumentVersion_RealClient(t *testing.T) {
	t.Parallel()

	backend := ssm.NewInMemoryBackend()
	client := newTestSSMClient(t, ssm.NewHandler(backend))
	ctx := t.Context()

	_, err := client.CreateDocument(ctx, &ssmsdk.CreateDocumentInput{
		Name:    aws.String("PinnedShareDoc"),
		Content: aws.String(`{"v":1}`),
	})
	require.NoError(t, err)

	_, err = client.UpdateDocument(ctx, &ssmsdk.UpdateDocumentInput{
		Name:    aws.String("PinnedShareDoc"),
		Content: aws.String(`{"v":2}`),
	})
	require.NoError(t, err)

	// UpdateDocument advances LatestVersion but never DefaultVersion -- move
	// the default to "2" explicitly so the two accounts below get distinct
	// pins and this test proves the pin tracks DefaultVersion, not LatestVersion.
	_, err = client.UpdateDocumentDefaultVersion(ctx, &ssmsdk.UpdateDocumentDefaultVersionInput{
		Name:            aws.String("PinnedShareDoc"),
		DocumentVersion: aws.String("2"),
	})
	require.NoError(t, err)

	_, err = client.ModifyDocumentPermission(ctx, &ssmsdk.ModifyDocumentPermissionInput{
		Name:                  aws.String("PinnedShareDoc"),
		PermissionType:        ssmtypes.DocumentPermissionTypeShare,
		AccountIdsToAdd:       []string{"111111111111"},
		SharedDocumentVersion: aws.String("1"),
	})
	require.NoError(t, err)

	// Sharing a second account with no SharedDocumentVersion must pin the
	// document's current DefaultVersion ("2"), not silently drop the field.
	_, err = client.ModifyDocumentPermission(ctx, &ssmsdk.ModifyDocumentPermissionInput{
		Name:            aws.String("PinnedShareDoc"),
		PermissionType:  ssmtypes.DocumentPermissionTypeShare,
		AccountIdsToAdd: []string{"222222222222"},
	})
	require.NoError(t, err)

	out, err := client.DescribeDocumentPermission(ctx, &ssmsdk.DescribeDocumentPermissionInput{
		Name:           aws.String("PinnedShareDoc"),
		PermissionType: ssmtypes.DocumentPermissionTypeShare,
	})
	require.NoError(t, err)
	require.Len(t, out.AccountSharingInfoList, 2,
		"pre-fix AccountSharingInfoList was a permanently-empty stub")

	pins := make(map[string]string, len(out.AccountSharingInfoList))
	for _, info := range out.AccountSharingInfoList {
		pins[aws.ToString(info.AccountId)] = aws.ToString(info.SharedDocumentVersion)
	}
	assert.Equal(t, "1", pins["111111111111"], "explicit SharedDocumentVersion must round-trip")
	assert.Equal(t, "2", pins["222222222222"], "an omitted SharedDocumentVersion must pin the current DefaultVersion")
}

// TestDescribeDocumentPermission_Pagination_RealClient covers
// gopherstack-enpq: DescribeDocumentPermissionInput had no MaxResults/
// NextToken members at all, so every call returned every shared account
// regardless of MaxResults.
func TestDescribeDocumentPermission_Pagination_RealClient(t *testing.T) {
	t.Parallel()

	backend := ssm.NewInMemoryBackend()
	client := newTestSSMClient(t, ssm.NewHandler(backend))
	ctx := t.Context()

	_, err := client.CreateDocument(ctx, &ssmsdk.CreateDocumentInput{
		Name:    aws.String("PaginatedShareDoc"),
		Content: aws.String(`{}`),
	})
	require.NoError(t, err)

	_, err = client.ModifyDocumentPermission(ctx, &ssmsdk.ModifyDocumentPermissionInput{
		Name:            aws.String("PaginatedShareDoc"),
		PermissionType:  ssmtypes.DocumentPermissionTypeShare,
		AccountIdsToAdd: []string{"111111111111", "222222222222", "333333333333"},
	})
	require.NoError(t, err)

	page1, err := client.DescribeDocumentPermission(ctx, &ssmsdk.DescribeDocumentPermissionInput{
		Name:           aws.String("PaginatedShareDoc"),
		PermissionType: ssmtypes.DocumentPermissionTypeShare,
		MaxResults:     aws.Int32(2),
	})
	require.NoError(t, err)
	require.Len(t, page1.AccountIds, 2, "pre-fix MaxResults had no Go member and was ignored")
	require.NotEmpty(t, aws.ToString(page1.NextToken))

	page2, err := client.DescribeDocumentPermission(ctx, &ssmsdk.DescribeDocumentPermissionInput{
		Name:           aws.String("PaginatedShareDoc"),
		PermissionType: ssmtypes.DocumentPermissionTypeShare,
		MaxResults:     aws.Int32(2),
		NextToken:      page1.NextToken,
	})
	require.NoError(t, err)
	require.Len(t, page2.AccountIds, 1)
	assert.Empty(t, aws.ToString(page2.NextToken))

	all := append(append([]string{}, page1.AccountIds...), page2.AccountIds...)
	assert.ElementsMatch(t, []string{"111111111111", "222222222222", "333333333333"}, all)
}

// TestSendCommand_TargetsOnly_RealClient covers a functional no-op bug
// (gopherstack-enpq): SendCommandInput.Targets is documented, verbatim, on
// types.Command.Targets (api_op_SendCommand.go / types/types.go) as
// "required if you don't provide one or more managed node IDs in the call"
// -- the recommended way to address commands at scale (api_op_SendCommand.go:
// InstanceIds' own doc comment: "we recommend using Targets instead"). The
// field round-tripped (echoed back on Command) but the invocation-building
// loop in commands.go only ever ranged over InstanceIds, so a Targets-only
// caller -- exactly AWS's documented pattern -- got a command with
// TargetCount 0 and zero CommandInvocations: nothing actually ran.
func TestSendCommand_TargetsOnly_RealClient(t *testing.T) {
	t.Parallel()

	backend := ssm.NewInMemoryBackend()
	client := newTestSSMClient(t, ssm.NewHandler(backend))
	ctx := t.Context()

	_, err := client.CreateDocument(ctx, &ssmsdk.CreateDocumentInput{
		Name:    aws.String("TargetsOnlyDoc"),
		Content: aws.String(`{"schemaVersion":"2.2"}`),
	})
	require.NoError(t, err)

	out, err := client.SendCommand(ctx, &ssmsdk.SendCommandInput{
		DocumentName: aws.String("TargetsOnlyDoc"),
		Targets: []ssmtypes.Target{
			{Key: aws.String("InstanceIds"), Values: []string{"i-targets-1", "i-targets-2"}},
		},
	})
	require.NoError(t, err)
	assert.EqualValues(t, 2, out.Command.TargetCount,
		"pre-fix: Targets round-tripped but the invocation loop never consulted it, so TargetCount stayed 0")

	invs, err := client.ListCommandInvocations(ctx, &ssmsdk.ListCommandInvocationsInput{
		CommandId: out.Command.CommandId,
	})
	require.NoError(t, err)
	require.Len(t, invs.CommandInvocations, 2)

	gotIDs := []string{
		aws.ToString(invs.CommandInvocations[0].InstanceId),
		aws.ToString(invs.CommandInvocations[1].InstanceId),
	}
	assert.ElementsMatch(t, []string{"i-targets-1", "i-targets-2"}, gotIDs)

	// Targets is still echoed back exactly as given, unmerged with InstanceIds.
	require.Len(t, out.Command.Targets, 1)
	assert.Equal(t, "InstanceIds", aws.ToString(out.Command.Targets[0].Key))
}

// TestSendCommand_InstanceIDsAndTargets_Dedup_RealClient covers the union
// path: a node named in both InstanceIds and Targets must be invoked once,
// not twice.
func TestSendCommand_InstanceIDsAndTargets_Dedup_RealClient(t *testing.T) {
	t.Parallel()

	backend := ssm.NewInMemoryBackend()
	client := newTestSSMClient(t, ssm.NewHandler(backend))
	ctx := t.Context()

	_, err := client.CreateDocument(ctx, &ssmsdk.CreateDocumentInput{
		Name:    aws.String("DedupDoc"),
		Content: aws.String(`{"schemaVersion":"2.2"}`),
	})
	require.NoError(t, err)

	out, err := client.SendCommand(ctx, &ssmsdk.SendCommandInput{
		DocumentName: aws.String("DedupDoc"),
		InstanceIds:  []string{"i-shared", "i-only-instanceids"},
		Targets: []ssmtypes.Target{
			{Key: aws.String("InstanceIds"), Values: []string{"i-shared", "i-only-targets"}},
		},
	})
	require.NoError(t, err)
	assert.EqualValues(t, 3, out.Command.TargetCount)

	invs, err := client.ListCommandInvocations(ctx, &ssmsdk.ListCommandInvocationsInput{
		CommandId: out.Command.CommandId,
	})
	require.NoError(t, err)
	gotIDs := make([]string, len(invs.CommandInvocations))
	for i, inv := range invs.CommandInvocations {
		gotIDs[i] = aws.ToString(inv.InstanceId)
	}
	assert.ElementsMatch(t, []string{"i-shared", "i-only-instanceids", "i-only-targets"}, gotIDs)
}

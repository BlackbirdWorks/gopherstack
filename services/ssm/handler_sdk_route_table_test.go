package ssm_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ssm"
)

// sdkRouteCases is the authoritative X-Amz-Target for every real SSM
// operation, extracted from ssm@v1.73.4 serializers.go: each op's
// awsAwsjson11_serializeOp<Op>.HandleSerialize sets
// httpBindingEncoder.SetHeader("X-Amz-Target").String("AmazonSSM.<Op>") and
// always request.Request.Method = "POST" against path "/" -- SSM is
// JSON-RPC 1.1 (services/_PROTOCOLS.md), so unlike a REST-family service
// there is no path template to get wrong: dispatch is entirely by this one
// header. ExtractOperation and Handler() both derive the action the same
// way (split on "."), so the class of bug this table can catch is a
// dispatch-table key that doesn't exactly match the real op name (typo,
// wrong case -- SSM is case-sensitive JSON-RPC), not a route-template
// mismatch.
//
// This table covers all 152 real SSM ops, which is also gopherstack's full
// implemented set (h.GetSupportedOperations(), 152/152) as of ssm@v1.73.4 --
// confirmed by diffing the dispatch-table keys against this exact list,
// zero mismatches either direction.
//
// Regenerate by grepping serializers.go for every
// `SetHeader("X-Amz-Target").String("AmazonSSM.` and pulling the suffix
// after the last dot.
func sdkRouteCases() []struct{ op, target string } {
	return []struct{ op, target string }{
		{"AddTagsToResource", "AmazonSSM.AddTagsToResource"},
		{"AssociateOpsItemRelatedItem", "AmazonSSM.AssociateOpsItemRelatedItem"},
		{"CancelCommand", "AmazonSSM.CancelCommand"},
		{"CancelMaintenanceWindowExecution", "AmazonSSM.CancelMaintenanceWindowExecution"},
		{"CreateActivation", "AmazonSSM.CreateActivation"},
		{"CreateAssociation", "AmazonSSM.CreateAssociation"},
		{"CreateAssociationBatch", "AmazonSSM.CreateAssociationBatch"},
		{"CreateCloudConnector", "AmazonSSM.CreateCloudConnector"},
		{"CreateDocument", "AmazonSSM.CreateDocument"},
		{"CreateMaintenanceWindow", "AmazonSSM.CreateMaintenanceWindow"},
		{"CreateOpsItem", "AmazonSSM.CreateOpsItem"},
		{"CreateOpsMetadata", "AmazonSSM.CreateOpsMetadata"},
		{"CreatePatchBaseline", "AmazonSSM.CreatePatchBaseline"},
		{"CreateResourceDataSync", "AmazonSSM.CreateResourceDataSync"},
		{"DeleteActivation", "AmazonSSM.DeleteActivation"},
		{"DeleteAssociation", "AmazonSSM.DeleteAssociation"},
		{"DeleteCloudConnector", "AmazonSSM.DeleteCloudConnector"},
		{"DeleteDocument", "AmazonSSM.DeleteDocument"},
		{"DeleteInventory", "AmazonSSM.DeleteInventory"},
		{"DeleteMaintenanceWindow", "AmazonSSM.DeleteMaintenanceWindow"},
		{"DeleteOpsItem", "AmazonSSM.DeleteOpsItem"},
		{"DeleteOpsMetadata", "AmazonSSM.DeleteOpsMetadata"},
		{"DeleteParameter", "AmazonSSM.DeleteParameter"},
		{"DeleteParameters", "AmazonSSM.DeleteParameters"},
		{"DeletePatchBaseline", "AmazonSSM.DeletePatchBaseline"},
		{"DeleteResourceDataSync", "AmazonSSM.DeleteResourceDataSync"},
		{"DeleteResourcePolicy", "AmazonSSM.DeleteResourcePolicy"},
		{"DeregisterManagedInstance", "AmazonSSM.DeregisterManagedInstance"},
		{"DeregisterPatchBaselineForPatchGroup", "AmazonSSM.DeregisterPatchBaselineForPatchGroup"},
		{"DeregisterTargetFromMaintenanceWindow", "AmazonSSM.DeregisterTargetFromMaintenanceWindow"},
		{"DeregisterTaskFromMaintenanceWindow", "AmazonSSM.DeregisterTaskFromMaintenanceWindow"},
		{"DescribeActivations", "AmazonSSM.DescribeActivations"},
		{"DescribeAssociation", "AmazonSSM.DescribeAssociation"},
		{"DescribeAssociationExecutions", "AmazonSSM.DescribeAssociationExecutions"},
		{"DescribeAssociationExecutionTargets", "AmazonSSM.DescribeAssociationExecutionTargets"},
		{"DescribeAutomationExecutions", "AmazonSSM.DescribeAutomationExecutions"},
		{"DescribeAutomationStepExecutions", "AmazonSSM.DescribeAutomationStepExecutions"},
		{"DescribeAvailablePatches", "AmazonSSM.DescribeAvailablePatches"},
		{"DescribeDocument", "AmazonSSM.DescribeDocument"},
		{"DescribeDocumentPermission", "AmazonSSM.DescribeDocumentPermission"},
		{"DescribeEffectiveInstanceAssociations", "AmazonSSM.DescribeEffectiveInstanceAssociations"},
		{"DescribeEffectivePatchesForPatchBaseline", "AmazonSSM.DescribeEffectivePatchesForPatchBaseline"},
		{"DescribeInstanceAssociationsStatus", "AmazonSSM.DescribeInstanceAssociationsStatus"},
		{"DescribeInstanceInformation", "AmazonSSM.DescribeInstanceInformation"},
		{"DescribeInstancePatches", "AmazonSSM.DescribeInstancePatches"},
		{"DescribeInstancePatchStates", "AmazonSSM.DescribeInstancePatchStates"},
		{"DescribeInstancePatchStatesForPatchGroup", "AmazonSSM.DescribeInstancePatchStatesForPatchGroup"},
		{"DescribeInstanceProperties", "AmazonSSM.DescribeInstanceProperties"},
		{"DescribeInventoryDeletions", "AmazonSSM.DescribeInventoryDeletions"},
		{"DescribeMaintenanceWindowExecutions", "AmazonSSM.DescribeMaintenanceWindowExecutions"},
		{
			"DescribeMaintenanceWindowExecutionTaskInvocations",
			"AmazonSSM.DescribeMaintenanceWindowExecutionTaskInvocations",
		},
		{"DescribeMaintenanceWindowExecutionTasks", "AmazonSSM.DescribeMaintenanceWindowExecutionTasks"},
		{"DescribeMaintenanceWindows", "AmazonSSM.DescribeMaintenanceWindows"},
		{"DescribeMaintenanceWindowSchedule", "AmazonSSM.DescribeMaintenanceWindowSchedule"},
		{"DescribeMaintenanceWindowsForTarget", "AmazonSSM.DescribeMaintenanceWindowsForTarget"},
		{"DescribeMaintenanceWindowTargets", "AmazonSSM.DescribeMaintenanceWindowTargets"},
		{"DescribeMaintenanceWindowTasks", "AmazonSSM.DescribeMaintenanceWindowTasks"},
		{"DescribeOpsItems", "AmazonSSM.DescribeOpsItems"},
		{"DescribeParameters", "AmazonSSM.DescribeParameters"},
		{"DescribePatchBaselines", "AmazonSSM.DescribePatchBaselines"},
		{"DescribePatchGroups", "AmazonSSM.DescribePatchGroups"},
		{"DescribePatchGroupState", "AmazonSSM.DescribePatchGroupState"},
		{"DescribePatchProperties", "AmazonSSM.DescribePatchProperties"},
		{"DescribeSessions", "AmazonSSM.DescribeSessions"},
		{"DisassociateOpsItemRelatedItem", "AmazonSSM.DisassociateOpsItemRelatedItem"},
		{"GetAccessToken", "AmazonSSM.GetAccessToken"},
		{"GetAutomationExecution", "AmazonSSM.GetAutomationExecution"},
		{"GetCalendarState", "AmazonSSM.GetCalendarState"},
		{"GetCloudConnector", "AmazonSSM.GetCloudConnector"},
		{"GetCommandInvocation", "AmazonSSM.GetCommandInvocation"},
		{"GetConnectionStatus", "AmazonSSM.GetConnectionStatus"},
		{"GetDefaultPatchBaseline", "AmazonSSM.GetDefaultPatchBaseline"},
		{"GetDeployablePatchSnapshotForInstance", "AmazonSSM.GetDeployablePatchSnapshotForInstance"},
		{"GetDocument", "AmazonSSM.GetDocument"},
		{"GetExecutionPreview", "AmazonSSM.GetExecutionPreview"},
		{"GetInventory", "AmazonSSM.GetInventory"},
		{"GetInventorySchema", "AmazonSSM.GetInventorySchema"},
		{"GetMaintenanceWindow", "AmazonSSM.GetMaintenanceWindow"},
		{"GetMaintenanceWindowExecution", "AmazonSSM.GetMaintenanceWindowExecution"},
		{"GetMaintenanceWindowExecutionTask", "AmazonSSM.GetMaintenanceWindowExecutionTask"},
		{"GetMaintenanceWindowExecutionTaskInvocation", "AmazonSSM.GetMaintenanceWindowExecutionTaskInvocation"},
		{"GetMaintenanceWindowTask", "AmazonSSM.GetMaintenanceWindowTask"},
		{"GetOpsItem", "AmazonSSM.GetOpsItem"},
		{"GetOpsMetadata", "AmazonSSM.GetOpsMetadata"},
		{"GetOpsSummary", "AmazonSSM.GetOpsSummary"},
		{"GetParameter", "AmazonSSM.GetParameter"},
		{"GetParameterHistory", "AmazonSSM.GetParameterHistory"},
		{"GetParameters", "AmazonSSM.GetParameters"},
		{"GetParametersByPath", "AmazonSSM.GetParametersByPath"},
		{"GetPatchBaseline", "AmazonSSM.GetPatchBaseline"},
		{"GetPatchBaselineForPatchGroup", "AmazonSSM.GetPatchBaselineForPatchGroup"},
		{"GetResourcePolicies", "AmazonSSM.GetResourcePolicies"},
		{"GetServiceSetting", "AmazonSSM.GetServiceSetting"},
		{"LabelParameterVersion", "AmazonSSM.LabelParameterVersion"},
		{"ListAssociations", "AmazonSSM.ListAssociations"},
		{"ListAssociationVersions", "AmazonSSM.ListAssociationVersions"},
		{"ListCloudConnectors", "AmazonSSM.ListCloudConnectors"},
		{"ListCommandInvocations", "AmazonSSM.ListCommandInvocations"},
		{"ListCommands", "AmazonSSM.ListCommands"},
		{"ListComplianceItems", "AmazonSSM.ListComplianceItems"},
		{"ListComplianceSummaries", "AmazonSSM.ListComplianceSummaries"},
		{"ListDocumentMetadataHistory", "AmazonSSM.ListDocumentMetadataHistory"},
		{"ListDocuments", "AmazonSSM.ListDocuments"},
		{"ListDocumentVersions", "AmazonSSM.ListDocumentVersions"},
		{"ListInventoryEntries", "AmazonSSM.ListInventoryEntries"},
		{"ListNodes", "AmazonSSM.ListNodes"},
		{"ListNodesSummary", "AmazonSSM.ListNodesSummary"},
		{"ListOpsItemEvents", "AmazonSSM.ListOpsItemEvents"},
		{"ListOpsItemRelatedItems", "AmazonSSM.ListOpsItemRelatedItems"},
		{"ListOpsMetadata", "AmazonSSM.ListOpsMetadata"},
		{"ListResourceComplianceSummaries", "AmazonSSM.ListResourceComplianceSummaries"},
		{"ListResourceDataSync", "AmazonSSM.ListResourceDataSync"},
		{"ListTagsForResource", "AmazonSSM.ListTagsForResource"},
		{"ModifyDocumentPermission", "AmazonSSM.ModifyDocumentPermission"},
		{"PutComplianceItems", "AmazonSSM.PutComplianceItems"},
		{"PutInventory", "AmazonSSM.PutInventory"},
		{"PutParameter", "AmazonSSM.PutParameter"},
		{"PutResourcePolicy", "AmazonSSM.PutResourcePolicy"},
		{"RegisterDefaultPatchBaseline", "AmazonSSM.RegisterDefaultPatchBaseline"},
		{"RegisterPatchBaselineForPatchGroup", "AmazonSSM.RegisterPatchBaselineForPatchGroup"},
		{"RegisterTargetWithMaintenanceWindow", "AmazonSSM.RegisterTargetWithMaintenanceWindow"},
		{"RegisterTaskWithMaintenanceWindow", "AmazonSSM.RegisterTaskWithMaintenanceWindow"},
		{"RemoveTagsFromResource", "AmazonSSM.RemoveTagsFromResource"},
		{"ResetServiceSetting", "AmazonSSM.ResetServiceSetting"},
		{"ResumeSession", "AmazonSSM.ResumeSession"},
		{"SendAutomationSignal", "AmazonSSM.SendAutomationSignal"},
		{"SendCommand", "AmazonSSM.SendCommand"},
		{"StartAccessRequest", "AmazonSSM.StartAccessRequest"},
		{"StartAssociationsOnce", "AmazonSSM.StartAssociationsOnce"},
		{"StartAutomationExecution", "AmazonSSM.StartAutomationExecution"},
		{"StartChangeRequestExecution", "AmazonSSM.StartChangeRequestExecution"},
		{"StartExecutionPreview", "AmazonSSM.StartExecutionPreview"},
		{"StartSession", "AmazonSSM.StartSession"},
		{"StopAutomationExecution", "AmazonSSM.StopAutomationExecution"},
		{"TerminateSession", "AmazonSSM.TerminateSession"},
		{"UnlabelParameterVersion", "AmazonSSM.UnlabelParameterVersion"},
		{"UpdateAssociation", "AmazonSSM.UpdateAssociation"},
		{"UpdateAssociationStatus", "AmazonSSM.UpdateAssociationStatus"},
		{"UpdateCloudConnector", "AmazonSSM.UpdateCloudConnector"},
		{"UpdateDocument", "AmazonSSM.UpdateDocument"},
		{"UpdateDocumentDefaultVersion", "AmazonSSM.UpdateDocumentDefaultVersion"},
		{"UpdateDocumentMetadata", "AmazonSSM.UpdateDocumentMetadata"},
		{"UpdateMaintenanceWindow", "AmazonSSM.UpdateMaintenanceWindow"},
		{"UpdateMaintenanceWindowTarget", "AmazonSSM.UpdateMaintenanceWindowTarget"},
		{"UpdateMaintenanceWindowTask", "AmazonSSM.UpdateMaintenanceWindowTask"},
		{"UpdateManagedInstanceRole", "AmazonSSM.UpdateManagedInstanceRole"},
		{"UpdateOpsItem", "AmazonSSM.UpdateOpsItem"},
		{"UpdateOpsMetadata", "AmazonSSM.UpdateOpsMetadata"},
		{"UpdatePatchBaseline", "AmazonSSM.UpdatePatchBaseline"},
		{"UpdateResourceDataSync", "AmazonSSM.UpdateResourceDataSync"},
		{"UpdateServiceSetting", "AmazonSSM.UpdateServiceSetting"},
		{"ValidateCloudConnector", "AmazonSSM.ValidateCloudConnector"},
	}
}

// TestExtractOperation_SDKRouteTable drives every real SSM operation's
// authoritative X-Amz-Target through ExtractOperation and Handler(),
// asserting the header resolves to the right op name and that Handler()
// does not fall through to the "UnknownOperationException" sentinel that a
// dispatch-table key mismatch would produce.
func TestExtractOperation_SDKRouteTable(t *testing.T) {
	t.Parallel()

	for _, tc := range sdkRouteCases() {
		t.Run(strings.ToLower(tc.op), func(t *testing.T) {
			t.Parallel()

			h := ssm.NewHandler(ssm.NewInMemoryBackend())
			e := echo.New()
			req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader("{}"))
			req.Header.Set("X-Amz-Target", tc.target)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			got := h.ExtractOperation(c)
			assert.Equal(t, tc.op, got)

			require.NoError(t, h.Handler()(c))
			assert.NotContains(t, rec.Body.String(), "UnknownOperationException",
				"target=%s op=%s: dispatched to the unmatched-route handler", tc.target, tc.op)
		})
	}
}

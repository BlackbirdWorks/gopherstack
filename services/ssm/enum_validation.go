package ssm

import (
	"errors"
	"fmt"
	"net/http"
	"slices"

	ssmtypes "github.com/aws/aws-sdk-go-v2/service/ssm/types"
)

var (
	ErrInvalidPermissionType = errors.New("InvalidPermissionType")
	ErrInvalidOption         = errors.New("InvalidOptionException")
)

type sdkEnum[T any] interface {
	~string
	Values() []T
}

// checkEnumAs rejects v when it is non-empty and outside the SDK enum's Values(), wrapping sentinel.
func checkEnumAs[T sdkEnum[T]](sentinel error, field string, v T) error {
	if v == "" || slices.Contains(v.Values(), v) {
		return nil
	}

	return fmt.Errorf("%w: invalid %s %q", sentinel, field, string(v))
}

func checkEnum[T sdkEnum[T]](field string, v T) error {
	return checkEnumAs(ErrValidationException, field, v)
}

func firstErr(errs ...error) error {
	for _, err := range errs {
		if err != nil {
			return err
		}
	}

	return nil
}

func checkPermissionType(v string) error {
	return checkEnumAs(ErrInvalidPermissionType, "PermissionType", ssmtypes.DocumentPermissionType(v))
}

func checkPatchBaselineEnums(level, status, action string) error {
	return firstErr(
		checkEnum("ApprovedPatchesComplianceLevel", ssmtypes.PatchComplianceLevel(level)),
		checkEnum("AvailableSecurityUpdatesComplianceStatus", ssmtypes.PatchComplianceStatus(status)),
		checkEnum("RejectedPatchesAction", ssmtypes.PatchAction(action)),
	)
}

func checkAssociationEnums(severity, sync string) error {
	return firstErr(
		checkEnum("ComplianceSeverity", ssmtypes.AssociationComplianceSeverity(severity)),
		checkEnum("SyncCompliance", ssmtypes.AssociationSyncCompliance(sync)),
	)
}

func checkOperatingSystem(v string) error {
	return checkEnum("OperatingSystem", ssmtypes.OperatingSystem(v))
}

func checkMaintenanceWindowResourceType(v string) error {
	return checkEnum("ResourceType", ssmtypes.MaintenanceWindowResourceType(v))
}

func checkCutoffBehavior(v string) error {
	return checkEnum("CutoffBehavior", ssmtypes.MaintenanceWindowTaskCutoffBehavior(v))
}

func checkDocumentFormat(v string) error {
	return checkEnum("DocumentFormat", ssmtypes.DocumentFormat(v))
}

func validateCreateAssociationEnums(in *CreateAssociationInput) error {
	return checkAssociationEnums(in.ComplianceSeverity, in.SyncCompliance)
}

func validateUpdateAssociationEnums(in *UpdateAssociationInput) error {
	return checkAssociationEnums(in.ComplianceSeverity, in.SyncCompliance)
}

func validateCreateDocumentEnums(in *CreateDocumentInput) error {
	return firstErr(
		checkDocumentFormat(in.DocumentFormat),
		checkEnum("DocumentType", ssmtypes.DocumentType(in.DocumentType)),
	)
}

func validateGetDocumentEnums(in *GetDocumentInput) error {
	return checkDocumentFormat(in.DocumentFormat)
}

func validateUpdateDocumentEnums(in *UpdateDocumentInput) error {
	return checkDocumentFormat(in.DocumentFormat)
}

func validateDescribeDocumentPermissionEnums(in *DescribeDocumentPermissionInput) error {
	return checkPermissionType(in.PermissionType)
}

func validateModifyDocumentPermissionEnums(in *ModifyDocumentPermissionInput) error {
	return checkPermissionType(in.PermissionType)
}

func validateListDocumentMetadataHistoryEnums(in *ListDocumentMetadataHistoryInput) error {
	return checkEnum("Metadata", ssmtypes.DocumentMetadataEnum(in.Metadata))
}

func validateCreatePatchBaselineEnums(in *CreatePatchBaselineInput) error {
	return firstErr(
		checkPatchBaselineEnums(
			in.ApprovedPatchesComplianceLevel, in.AvailableSecurityUpdatesComplianceStatus, in.RejectedPatchesAction),
		checkOperatingSystem(in.OperatingSystem),
	)
}

func validateUpdatePatchBaselineEnums(in *UpdatePatchBaselineInput) error {
	return checkPatchBaselineEnums(
		in.ApprovedPatchesComplianceLevel, in.AvailableSecurityUpdatesComplianceStatus, in.RejectedPatchesAction)
}

func validateDescribePatchPropertiesEnums(in *DescribePatchPropertiesInput) error {
	return firstErr(
		checkOperatingSystem(in.OperatingSystem),
		checkEnum("PatchSet", ssmtypes.PatchSet(in.PatchSet)),
		checkEnum("Property", ssmtypes.PatchProperty(in.Property)),
	)
}

func validateGetDefaultPatchBaselineEnums(in *GetDefaultPatchBaselineInput) error {
	return checkOperatingSystem(in.OperatingSystem)
}

func validateGetPatchBaselineForPatchGroupEnums(in *GetPatchBaselineForPatchGroupInput) error {
	return checkOperatingSystem(in.OperatingSystem)
}

func validateDeleteInventoryEnums(in *DeleteInventoryInput) error {
	return checkEnumAs(
		ErrInvalidOption,
		"SchemaDeleteOption",
		ssmtypes.InventorySchemaDeleteOption(in.SchemaDeleteOption),
	)
}

func validateDescribeMaintenanceWindowScheduleEnums(in *DescribeMaintenanceWindowScheduleInput) error {
	return checkMaintenanceWindowResourceType(in.ResourceType)
}

func validateDescribeMaintenanceWindowsForTargetEnums(in *DescribeMaintenanceWindowsForTargetInput) error {
	return checkMaintenanceWindowResourceType(in.ResourceType)
}

func validateRegisterTargetEnums(in *RegisterTargetWithMaintenanceWindowInput) error {
	return checkMaintenanceWindowResourceType(in.ResourceType)
}

func validateRegisterTaskEnums(in *RegisterTaskWithMaintenanceWindowInput) error {
	return firstErr(
		checkCutoffBehavior(in.CutoffBehavior),
		checkEnum("TaskType", ssmtypes.MaintenanceWindowTaskType(in.TaskType)),
	)
}

func validateUpdateMaintenanceWindowTaskEnums(in *UpdateMaintenanceWindowTaskInput) error {
	return checkCutoffBehavior(in.CutoffBehavior)
}

func validateDescribeSessionsEnums(in *DescribeSessionsInput) error {
	return checkEnum("State", ssmtypes.SessionState(in.State))
}

func validatePutComplianceItemsEnums(in *PutComplianceItemsInput) error {
	return checkEnum("UploadType", ssmtypes.ComplianceUploadType(in.UploadType))
}

func validateSendAutomationSignalEnums(in *SendAutomationSignalInput) error {
	return checkEnum("SignalType", ssmtypes.SignalType(in.SignalType))
}

func validateSendCommandEnums(in *SendCommandInput) error {
	return checkEnum("DocumentHashType", ssmtypes.DocumentHashType(in.DocumentHashType))
}

func validateStartAutomationExecutionEnums(in *StartAutomationExecutionInput) error {
	return checkEnum("Mode", ssmtypes.ExecutionMode(in.Mode))
}

func validateStopAutomationExecutionEnums(in *StopAutomationExecutionInput) error {
	return checkEnum("Type", ssmtypes.StopType(in.Type))
}

func validateUpdateOpsItemEnums(in *UpdateOpsItemInput) error {
	return checkEnum("Status", ssmtypes.OpsItemStatus(in.Status))
}

func classifySSMEnumError(reqErr error) (string, int, bool) {
	switch {
	case errors.Is(reqErr, ErrInvalidPermissionType):
		return "InvalidPermissionType", http.StatusBadRequest, true
	case errors.Is(reqErr, ErrInvalidOption):
		return "InvalidOptionException", http.StatusBadRequest, true
	default:
		return "", 0, false
	}
}

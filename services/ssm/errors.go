package ssm

import (
	"errors"
)

var (
	ErrParameterNotFound                  = errors.New("ParameterNotFound")
	ErrParameterVersionNotFound           = errors.New("ParameterVersionNotFound")
	ErrParameterAlreadyExists             = errors.New("ParameterAlreadyExists")
	ErrInvalidKeyID                       = errors.New("InvalidKeyId")
	ErrCiphertextTooShort                 = errors.New("ciphertext too short")
	ErrValidationException                = errors.New("ValidationException")
	ErrDocumentAlreadyExists              = errors.New("DocumentAlreadyExists")
	ErrDocumentNotFound                   = errors.New("DocumentNotFound")
	ErrInvalidDocumentVersion             = errors.New("InvalidDocumentVersion")
	ErrCommandNotFound                    = errors.New("CommandNotFound")
	ErrActivationNotFound                 = errors.New("ActivationNotFound")
	ErrAssociationNotFound                = errors.New("AssociationDoesNotExist")
	ErrMaintenanceWindowNotFound          = errors.New("DoesNotExistException")
	ErrMaintenanceWindowExecutionNotFound = errors.New("DoesNotExistException")
	ErrOpsItemNotFound                    = errors.New("OpsItemNotFoundException")
	ErrOpsMetadataNotFound                = errors.New("OpsMetadataNotFoundException")
	ErrPatchBaselineNotFound              = errors.New("DoesNotExistException")
	ErrOpsMetadataAlreadyExists           = errors.New("OpsMetadataAlreadyExistsException")
	ErrHierarchyLevelLimitExceeded        = errors.New("HierarchyLevelLimitExceededException")
	ErrParameterMaxVersionLimitExceeded   = errors.New("ParameterMaxVersionLimitExceeded")
)
var (
	ErrResourceDataSyncNotFound    = errors.New("ResourceDataSyncNotFoundException")
	ErrAutomationExecutionNotFound = errors.New("AutomationExecutionNotFoundException")
	ErrExecutionPreviewNotFound    = errors.New("ExecutionPreviewNotFoundException")
	ErrResourcePolicyNotFound      = errors.New("ResourcePolicyInvalidRequest")
	ErrResourceDataSyncExists      = errors.New("ResourceDataSyncAlreadyExistsException")
)
var (
	// ErrInventoryNotFound is returned when inventory for a type is not found.
	ErrInventoryNotFound = errors.New("InventoryTypeNotFound")
	// ErrDocumentVersionNotFound is returned when a document version is not found.
	ErrDocumentVersionNotFound = errors.New("InvalidDocumentVersion")
)

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
	// ErrAccessRequestNotFound is returned when GetAccessToken is called with
	// an AccessRequestId that was never created by StartAccessRequest.
	ErrAccessRequestNotFound = errors.New("ResourceNotFoundException")
)
var (
	ErrResourceDataSyncNotFound    = errors.New("ResourceDataSyncNotFoundException")
	ErrAutomationExecutionNotFound = errors.New("AutomationExecutionNotFoundException")
	ErrExecutionPreviewNotFound    = errors.New("ExecutionPreviewNotFoundException")
	// ErrResourcePolicyNotFound and ErrResourcePolicyConflict are the two real
	// exceptions declared for PutResourcePolicy/DeleteResourcePolicy
	// (ssm@v1.73.4 types/errors.go) around a PolicyId/PolicyHash mismatch.
	ErrResourcePolicyNotFound = errors.New("ResourcePolicyNotFoundException")
	ErrResourcePolicyConflict = errors.New("ResourcePolicyConflictException")
	ErrResourceDataSyncExists = errors.New("ResourceDataSyncAlreadyExistsException")
)
var (
	// ErrInventoryNotFound is returned when inventory for a type is not found.
	ErrInventoryNotFound = errors.New("InventoryTypeNotFound")
	// ErrDocumentVersionNotFound is returned when a document version is not found.
	ErrDocumentVersionNotFound = errors.New("InvalidDocumentVersion")
	// ErrInvalidAggregator is returned by ListNodesSummary when Aggregators is
	// missing or empty. InvalidAggregatorException is one of the op's own
	// declared exceptions (awsAwsjson11_deserializeOpErrorListNodesSummary,
	// ssm@v1.73.4 deserializers.go), not the generic ValidationException.
	ErrInvalidAggregator = errors.New("InvalidAggregatorException")
	// ErrDocumentStillShared is returned by DeleteDocument when the document
	// still has active AccountIdsToAdd shares. InvalidDocumentOperation is one
	// of DeleteDocument's own declared exceptions (ssm@v1.73.4
	// deserializers.go:2225-2226), whose message reads: you attempted to
	// delete a document while it is still shared, and must stop sharing it
	// first.
	ErrDocumentStillShared = errors.New("InvalidDocumentOperation")
)

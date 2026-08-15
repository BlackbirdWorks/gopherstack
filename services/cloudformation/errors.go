package cloudformation

import "errors"

var (
	ErrStackNotFound            = errors.New("stack with id does not exist")
	ErrStackAlreadyExists       = errors.New("stack already exists")
	ErrChangeSetNotFound        = errors.New("change set not found")
	ErrChangeSetExists          = errors.New("change set already exists")
	ErrChangeSetAlreadyExecuted = errors.New("change set has already been executed")
	ErrResourceNotFound         = errors.New("resource not found in stack")
	ErrExportNotFound           = errors.New("export with given name not found")
	ErrDuplicateExport          = errors.New("export already exists and is owned by another stack")
	ErrExportInUse              = errors.New("export cannot be removed while it is in use by another stack")
	ErrChangeSetNotExecutable   = errors.New("change set is not in an executable status")
	ErrDriftDetectionNotFound   = errors.New("drift detection not found")
	ErrStackSetNotFound         = errors.New("stack set not found")
	ErrStackSetAlreadyExists    = errors.New("stack set already exists")
	ErrStackSetNotEmpty         = errors.New(
		"stack set is not empty: delete all stack instances before deleting the stack set",
	)
	ErrStackInstanceNotFound      = errors.New("stack instance not found")
	ErrStackInstanceAlreadyExists = errors.New(
		"stack instance already exists in this account/region",
	)
	ErrGeneratedTemplateNotFound = errors.New("generated template not found")
	ErrResourceScanNotFound      = errors.New("resource scan not found")
	ErrOperationNotFound         = errors.New("operation not found in stack set")
	ErrOperationNotRunning       = errors.New("operation is not in RUNNING state")
	ErrTypeNotFound              = errors.New("type not found")
	ErrTypeVersionNotFound       = errors.New("type version not found")
	ErrRegistrationTokenNotFound = errors.New("registration token not found")
	ErrPublisherNotFound         = errors.New("publisher not found")
	ErrInvalidRoleARN            = errors.New("invalid IAM role ARN format")
	ErrInsufficientCapabilities  = errors.New(
		"requires capabilities: CAPABILITY_IAM or CAPABILITY_NAMED_IAM",
	)
	ErrStackRefactorNotFound = errors.New("stack refactor not found")
	ErrStackPolicyDenied     = errors.New("update action denied by stack policy")
)

// ErrTerminationProtectionEnabled is returned when deleting a termination-protected stack.
var ErrTerminationProtectionEnabled = errors.New("stack termination protection is enabled")

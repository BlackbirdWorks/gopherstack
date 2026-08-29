package codedeploy

import "github.com/blackbirdworks/gopherstack/pkgs/awserr"

// Sentinel errors returned by the CodeDeploy backend, mapped to AWS exception
// types and HTTP status codes by handler.go's errorMappings table.
var (
	ErrNotFound                      = awserr.New("ApplicationDoesNotExistException", awserr.ErrNotFound)
	ErrDeploymentGroupNotFound       = awserr.New("DeploymentGroupDoesNotExistException", awserr.ErrNotFound)
	ErrDeploymentNotFound            = awserr.New("DeploymentDoesNotExistException", awserr.ErrNotFound)
	ErrAlreadyExists                 = awserr.New("ApplicationAlreadyExistsException", awserr.ErrConflict)
	ErrDeploymentGroupAlreadyExists  = awserr.New("DeploymentGroupAlreadyExistsException", awserr.ErrConflict)
	ErrDeploymentConfigNotFound      = awserr.New("DeploymentConfigDoesNotExistException", awserr.ErrNotFound)
	ErrDeploymentConfigAlreadyExists = awserr.New("DeploymentConfigAlreadyExistsException", awserr.ErrConflict)
	ErrOnPremisesInstanceNotFound    = awserr.New("InstanceDoesNotExistException", awserr.ErrNotFound)
	ErrInvalidComputePlatform        = awserr.New("InvalidComputePlatformException", awserr.ErrInvalidParameter)
	ErrIamArnRequired                = awserr.New("IamArnRequiredException", awserr.ErrInvalidParameter)
	ErrMultipleIamArns               = awserr.New("MultipleIamArnsProvidedException", awserr.ErrInvalidParameter)
	// ErrDeploymentConfigIsDefault guards DeleteDeploymentConfig's built-in-config
	// case. DeleteDeploymentConfig's own deserializer models InvalidOperationException,
	// not DeploymentConfigInUseException (that code belongs to AddTagsToOnPremisesInstances/
	// RemoveTagsFromOnPremisesInstances/UpdateDeploymentGroup's tag-limit case instead).
	ErrDeploymentConfigIsDefault  = awserr.New("InvalidOperationException", awserr.ErrConflict)
	ErrGitHubAccountTokenNotFound = awserr.New("GitHubAccountTokenDoesNotExistException", awserr.ErrNotFound)
	ErrRevisionNotFound           = awserr.New("RevisionDoesNotExistException", awserr.ErrNotFound)
	ErrDeploymentTargetNotFound   = awserr.New("DeploymentTargetDoesNotExistException", awserr.ErrNotFound)
	ErrDeploymentAlreadyCompleted = awserr.New("DeploymentAlreadyCompletedException", awserr.ErrConflict)
	ErrDeploymentNotInReadyState  = awserr.New("DeploymentIsNotInReadyStateException", awserr.ErrConflict)
	ErrInvalidDeploymentWaitType  = awserr.New("InvalidDeploymentWaitTypeException", awserr.ErrInvalidParameter)
	ErrInvalidFileExistsBehavior  = awserr.New("InvalidFileExistsBehaviorException", awserr.ErrInvalidParameter)

	// ErrInvalidTagsToAdd covers every TagResource-rejected tag (reserved "aws:"
	// prefix, oversized key/value, or the 50-tag-per-resource cap): TagResource's
	// own deserializer models only InvalidTagsToAddException for tag content, not
	// TagLimitExceededException (that code belongs to AddTagsToOnPremisesInstances/
	// RemoveTagsFromOnPremisesInstances/UpdateDeploymentGroup instead).
	ErrInvalidTagsToAdd = awserr.New("InvalidTagsToAddException", awserr.ErrInvalidParameter)
	// ErrBatchLimitExceeded is BatchGetApplicationRevisions' own modeled code for
	// exceeding the 25-revision batch cap.
	ErrBatchLimitExceeded = awserr.New("BatchLimitExceededException", awserr.ErrInvalidParameter)
	// ErrInvalidInstanceName is RegisterOnPremisesInstance's own modeled code for
	// a malformed (not missing) on-premises instance name.
	ErrInvalidInstanceName = awserr.New("InvalidInstanceNameException", awserr.ErrInvalidParameter)

	// Required-field sentinels below back the generic "field is required"
	// validation every write/read op performs. Each op's own deserializer models
	// its own distinct Required exception per field -- there is no single generic
	// "InvalidRequestException" in the real SDK, so a shared sentinel here is
	// scoped per field name (shared correctly across every op that model the
	// exact same code, e.g. ApplicationNameRequiredException), never per op.
	ErrApplicationNameRequired      = awserr.New("ApplicationNameRequiredException", awserr.ErrInvalidParameter)
	ErrDeploymentGroupNameRequired  = awserr.New("DeploymentGroupNameRequiredException", awserr.ErrInvalidParameter)
	ErrDeploymentIDRequired         = awserr.New("DeploymentIdRequiredException", awserr.ErrInvalidParameter)
	ErrInstanceIDRequired           = awserr.New("InstanceIdRequiredException", awserr.ErrInvalidParameter)
	ErrDeploymentTargetIDRequired   = awserr.New("DeploymentTargetIdRequiredException", awserr.ErrInvalidParameter)
	ErrDeploymentConfigNameRequired = awserr.New("DeploymentConfigNameRequiredException", awserr.ErrInvalidParameter)
	ErrInstanceNameRequired         = awserr.New("InstanceNameRequiredException", awserr.ErrInvalidParameter)
	ErrResourceArnRequired          = awserr.New("ResourceArnRequiredException", awserr.ErrInvalidParameter)
	ErrGitHubTokenNameRequired      = awserr.New("GitHubAccountTokenNameRequiredException", awserr.ErrInvalidParameter)
)

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
	ErrValidation                    = awserr.New("InvalidParameterValueException", awserr.ErrInvalidParameter)
	ErrTagLimitExceeded              = awserr.New("TagLimitExceededException", awserr.ErrInvalidParameter)
	ErrInvalidComputePlatform        = awserr.New("InvalidComputePlatformException", awserr.ErrInvalidParameter)
	ErrIamArnRequired                = awserr.New("IamArnRequiredException", awserr.ErrInvalidParameter)
	ErrMultipleIamArns               = awserr.New("MultipleIamArnsProvidedException", awserr.ErrInvalidParameter)
	ErrDeploymentConfigInUse         = awserr.New("DeploymentConfigInUseException", awserr.ErrConflict)
	ErrGitHubAccountTokenNotFound    = awserr.New("GitHubAccountTokenDoesNotExistException", awserr.ErrNotFound)
	ErrRevisionNotFound              = awserr.New("RevisionDoesNotExistException", awserr.ErrNotFound)
	ErrDeploymentTargetNotFound      = awserr.New("DeploymentTargetDoesNotExistException", awserr.ErrNotFound)
)

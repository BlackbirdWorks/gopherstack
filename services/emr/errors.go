package emr

import (
	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
)

var ErrValidation = awserr.New(
	"ValidationException: required field is missing",
	awserr.ErrInvalidParameter,
)

var (
	ErrNotFound      = awserr.New("ClientException", awserr.ErrNotFound)
	ErrAlreadyExists = awserr.New("ClientException", awserr.ErrAlreadyExists)
)

var errTerminationProtected = awserr.New(
	"ValidationException: cluster has termination protection enabled",
	awserr.ErrInvalidParameter,
)

// errSessionClusterNotReady is returned by StartSession when the target
// cluster is not in a state that can host a session (real StartSession's
// doc: "The cluster must be in the RUNNING or WAITING state").
var errSessionClusterNotReady = awserr.New(
	"ValidationException: cluster is not in a state that can host a session",
	awserr.ErrInvalidParameter,
)

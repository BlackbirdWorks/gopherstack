package dlm

import (
	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
)

const (
	errResourceNotFound = "ResourceNotFoundException"
	errInvalidRequest   = "InvalidRequestException"
	errLimitExceeded    = "LimitExceededException"
)

var (
	// ErrPolicyNotFound is returned when a lifecycle policy does not exist.
	ErrPolicyNotFound = awserr.New(errResourceNotFound, awserr.ErrNotFound)
	// ErrInvalidRequest is returned on invalid input.
	ErrInvalidRequest = awserr.New(errInvalidRequest, awserr.ErrInvalidParameter)
	// ErrLimitExceeded is returned by CreateLifecyclePolicy once the account
	// already holds maxPoliciesPerRegion policies in this Region -- AWS's
	// documented default "Policies per Region" quota (adjustable; quota code
	// L-5407D8DA, docs.aws.amazon.com/general/latest/gr/dlm.html).
	ErrLimitExceeded = awserr.New(errLimitExceeded, awserr.ErrInvalidParameter)
)

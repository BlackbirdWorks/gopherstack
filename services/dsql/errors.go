package dsql

import (
	"errors"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
)

var (
	// ErrClusterNotFound is returned when a cluster identifier does not exist.
	ErrClusterNotFound = awserr.New("cluster not found", awserr.ErrNotFound)
	// ErrStreamNotFound is returned when a stream identifier does not exist.
	ErrStreamNotFound = awserr.New("stream not found", awserr.ErrNotFound)
	// ErrPolicyNotFound is returned when a cluster has no resource policy set.
	ErrPolicyNotFound = awserr.New("cluster policy not found", awserr.ErrNotFound)
	// ErrDeletionProtected is returned when DeleteCluster is called on a
	// cluster with deletionProtectionEnabled set.
	ErrDeletionProtected = awserr.New("cluster has deletion protection enabled", awserr.ErrInvalidParameter)
	// ErrValidation is returned when request input fails validation.
	ErrValidation = awserr.New("invalid argument", awserr.ErrInvalidParameter)
	// ErrPolicyVersionMismatch is returned when an expectedPolicyVersion does
	// not match the cluster's current policy version.
	ErrPolicyVersionMismatch = awserr.New("policy version mismatch", awserr.ErrConflict)
	// ErrClusterQuotaExceeded is returned when an account/region would exceed
	// the emulated per-region cluster quota.
	ErrClusterQuotaExceeded = errors.New("cluster quota exceeded")
	// ErrStreamQuotaExceeded is returned when a cluster would exceed the
	// emulated per-cluster stream quota.
	ErrStreamQuotaExceeded = errors.New("stream quota exceeded")
)

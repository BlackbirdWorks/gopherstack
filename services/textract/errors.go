package textract

import (
	"errors"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
)

var (
	// ErrJobNotFound is returned when a document job is not found. Only use
	// this on the Get*/Start*(idempotency-hit) paths that take a client
	// supplied job ID -- it maps to InvalidJobIdException in handleError,
	// which the Start* ops' own deserializeOpError<Op> switch (textract
	// deserializers.go) does not declare at all (gopherstack-uox6 sweep).
	ErrJobNotFound = awserr.New("InvalidJobIdException", awserr.ErrNotFound)
	// errJobEvictedBeforeReadback is returned when a just-written job is
	// gone by the time StartX's post-write readback runs (trimJobsIfNeeded
	// evicted it, e.g. maxJobs==0) -- a server-side invariant violation, not
	// a client-supplied bad job ID. It deliberately matches none of
	// handleError's errors.Is cases, so it falls through to the existing
	// default branch (InternalServerError, 500), which every Start* op does
	// declare.
	errJobEvictedBeforeReadback = errors.New("job evicted from history before post-write readback")
	// ErrAdapterNotFound is returned when an adapter is not found. Real AWS
	// returns ResourceNotFoundException (not InvalidParameterException) for
	// GetAdapter/UpdateAdapter/DeleteAdapter/CreateAdapterVersion/
	// ListAdapterVersions/Tag*Resource on a nonexistent adapter -- verified
	// against every deserializeOpError<Op> switch in the SDK.
	ErrAdapterNotFound = awserr.New("ResourceNotFoundException", awserr.ErrNotFound)
	// ErrAdapterVersionNotFound is returned when an adapter version is not
	// found. See ErrAdapterNotFound's doc comment for the error-code source.
	ErrAdapterVersionNotFound = awserr.New("ResourceNotFoundException", awserr.ErrNotFound)
	// ErrValidation is returned when request parameters fail validation.
	ErrValidation = awserr.New("ValidationException", awserr.ErrInvalidParameter)
)

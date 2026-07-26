package sagemaker

import (
	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
)

// ErrValidation is returned for invalid input parameters.
var ErrValidation = awserr.New("ValidationException", awserr.ErrInvalidParameter)

// ErrResourceNotFound is the shared base sentinel for the AIBenchmarkJob,
// AIRecommendationJob, AIWorkloadConfig, and generic Job families (all added
// in the SDK bump that introduced CreateJob et al.). Field-diffed against
// aws-sdk-go-v2/service/sagemaker's deserializers.go: every one of these
// ops' error deserializers recognizes a "ResourceNotFound" wire exception on
// not-found — distinct from "ValidationException", which handleError still
// emits for the generic awserr.ErrNotFound sentinel used by the rest of the
// service's (older, previously-audited) CRUD families. handleError
// special-cases errors.Is(err, ErrResourceNotFound) ahead of the generic
// ErrNotFound branch so only these new families get the accurate wire type;
// nothing else in the service constructs an error wrapping this sentinel.
var ErrResourceNotFound = awserr.New("ResourceNotFound", awserr.ErrNotFound)

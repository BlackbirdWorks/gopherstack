package sagemaker

import (
	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
)

// ErrValidation is returned for invalid input parameters.
var ErrValidation = awserr.New("ValidationException", awserr.ErrInvalidParameter)

// ErrResourceNotFound is the shared base sentinel for resource families whose
// relevant ops' error deserializers recognize a "ResourceNotFound" wire
// exception on not-found — distinct from "ValidationException", which
// handleError emits for the generic awserr.ErrNotFound sentinel used by
// families whose ops model no not-found exception at all (their Describe/
// Delete deserializers have an empty case switch, so any code -- including
// ValidationException, which matches real AWS's observed behavior for these
// -- lands on the same unmodeled smithy.GenericAPIError either way).
// Field-diffed op by op against aws-sdk-go-v2/service/sagemaker's
// deserializers.go, this base sentinel now also covers: AIBenchmarkJob,
// AIRecommendationJob, AIWorkloadConfig, and generic Job (added with
// CreateJob et al.); EdgeDeploymentPlan (Describe/Delete/Create*Stage);
// DeviceFleet and Device (Describe*/Update*); InferenceRecommendationsJob
// (Describe/Stop); HyperParameterTuningJob (Describe/Stop); TrainingJob
// (Describe/Stop/Delete/Update); TransformJob (Describe/Stop); and
// EdgePackagingJob (Describe). handleError special-cases
// errors.Is(err, ErrResourceNotFound) ahead of the generic ErrNotFound
// branch so these families get the accurate wire type.
var ErrResourceNotFound = awserr.New("ResourceNotFound", awserr.ErrNotFound)

// ErrConflictException is the shared base sentinel for the resources whose
// real conflict error is ConflictException rather than the ResourceInUse
// handleError emits for the generic awserr.ErrConflict sentinel: verified
// per-resource against botocore sagemaker/2017-07-24@1.43.56 service-2.json's
// operation error lists — ClusterSchedulerConfig and ComputeQuota
// (Create/UpdateClusterSchedulerConfig, Create/UpdateComputeQuota),
// ModelPackageGroup (DeleteModelPackageGroup), and Pipeline
// (Create/Update/DeletePipeline). Most other "already exists"/"in use"
// sentinels in this service do declare ResourceInUse and are unaffected;
// see gopherstack-kbxx for the resource-by-resource audit. handleError
// special-cases errors.Is(err, ErrConflictException) ahead of the generic
// ErrConflict branch, mirroring ErrResourceNotFound above.
var ErrConflictException = awserr.New("ConflictException", awserr.ErrConflict)

package emrserverless

import (
	"maps"
	"slices"
	"time"
)

// ApplicationStateCreating is the state when an application is being created.
const ApplicationStateCreating = "CREATING"

// ApplicationStateCreated is the state when an application has been created.
const ApplicationStateCreated = "CREATED"

// ApplicationStateStarting is the state when an application is starting.
const ApplicationStateStarting = "STARTING"

// ApplicationStateStarted is the state when an application is running.
const ApplicationStateStarted = "STARTED"

// ApplicationStateStopping is the state when an application is stopping.
const ApplicationStateStopping = "STOPPING"

// ApplicationStateStopped is the state when an application has stopped.
const ApplicationStateStopped = "STOPPED"

// ApplicationStateTerminated is the state when an application is terminated.
const ApplicationStateTerminated = "TERMINATED"

// ApplicationStateTerminatedWithError is the state when an application terminated with errors.
const ApplicationStateTerminatedWithError = "TERMINATED_WITH_ERRORS"

// JobRunStateSubmitted is the state when a job run has been submitted.
const JobRunStateSubmitted = "SUBMITTED"

// JobRunStatePending is the state when a job run is pending.
const JobRunStatePending = "PENDING"

// JobRunStateScheduled is the state when a job run is scheduled.
const JobRunStateScheduled = "SCHEDULED"

// JobRunStateRunning is the state when a job run is running.
const JobRunStateRunning = "RUNNING"

// JobRunStateSuccess is the state when a job run completed successfully.
const JobRunStateSuccess = "SUCCESS"

// JobRunStateFailed is the state when a job run has failed.
const JobRunStateFailed = "FAILED"

// JobRunStateCancelling is the state when a job run is being cancelled.
const JobRunStateCancelling = "CANCELLING"

// JobRunStateCancelled is the state when a job run has been cancelled.
const JobRunStateCancelled = "CANCELLED"

// Application represents an EMR Serverless application.
type Application struct {
	Tags map[string]string `json:"tags,omitempty"`
	// ExtraConfig holds application configuration sub-objects that this
	// in-memory backend does not interpret (initialCapacity, maximumCapacity,
	// autoStartConfiguration, autoStopConfiguration, networkConfiguration,
	// imageConfiguration, monitoringConfiguration, workerTypeSpecifications,
	// runtimeConfiguration, interactiveConfiguration) but must still store and
	// echo back verbatim on GetApplication/ListApplications -- AWS clients
	// (Terraform, drift-detection tooling) commonly round-trip these values,
	// and CreateApplication/UpdateApplication silently discarding them is a
	// disguised no-op. Keyed by AWS wire field name; see applicationToMap.
	ExtraConfig   map[string]any `json:"extraConfig,omitempty"`
	CreatedAt     time.Time      `json:"createdAt"`
	UpdatedAt     time.Time      `json:"updatedAt"`
	ApplicationID string         `json:"applicationId"`
	Arn           string         `json:"arn"`
	Name          string         `json:"name"`
	Type          string         `json:"type"`
	ReleaseLabel  string         `json:"releaseLabel"`
	Architecture  string         `json:"architecture,omitempty"`
	State         string         `json:"state"`
}

// JobRun represents an EMR Serverless job run.
type JobRun struct {
	Tags map[string]string `json:"tags,omitempty"`
	// JobDriver is the job driver (sparkSubmit/hive) supplied to StartJobRun.
	// GetJobRun/ListJobRuns mark this as a required response field in the
	// real API; storing and echoing it verbatim (rather than discarding it)
	// avoids silently dropping the job specification the caller submitted.
	JobDriver any `json:"jobDriver,omitempty"`
	// ConfigurationOverrides is the configurationOverrides supplied to
	// StartJobRun, echoed back verbatim.
	ConfigurationOverrides any       `json:"configurationOverrides,omitempty"`
	CreatedAt              time.Time `json:"createdAt"`
	UpdatedAt              time.Time `json:"updatedAt"`
	ApplicationID          string    `json:"applicationId"`
	JobRunID               string    `json:"jobRunId"`
	Arn                    string    `json:"arn"`
	Name                   string    `json:"name"`
	State                  string    `json:"state"`
	ExecutionRoleArn       string    `json:"executionRoleArn"`
	Mode                   string    `json:"mode,omitempty"`
	ReleaseLabel           string    `json:"releaseLabel,omitempty"`
	StateDetails           string    `json:"stateDetails,omitempty"`
}

// JobRunAttemptSummary represents a single attempt of a job run.
type JobRunAttemptSummary struct {
	CreatedAt     time.Time `json:"createdAt"`
	UpdatedAt     time.Time `json:"updatedAt"`
	JobCreatedAt  time.Time `json:"jobCreatedAt"`
	ApplicationID string    `json:"applicationId"`
	Arn           string    `json:"arn"`
	CreatedBy     string    `json:"createdBy"`
	ExecutionRole string    `json:"executionRole"`
	ID            string    `json:"id"`
	ReleaseLabel  string    `json:"releaseLabel"`
	State         string    `json:"state"`
	StateDetails  string    `json:"stateDetails"`
	Name          string    `json:"name"`
	Type          string    `json:"type"`
	Attempt       int32     `json:"attempt"`
}

// CreateApplicationOptions carries optional CreateApplication parameters
// beyond the always-present name/type/releaseLabel/architecture/tags: the
// client idempotency token (matching AWS's CreateApplicationInput.ClientToken,
// a required input field on the real API) and the configuration sub-objects
// this backend stores but does not interpret. Passed as a trailing variadic
// argument so existing call sites that don't need these are unaffected.
type CreateApplicationOptions struct {
	ExtraConfig map[string]any
	ClientToken string
}

// StartJobRunOptions carries optional StartJobRun parameters beyond the
// always-present applicationID/executionRoleArn/name/mode/tags: the client
// idempotency token (matching AWS's StartJobRunInput.ClientToken, a required
// input field on the real API), the job driver, and configuration overrides.
// Passed as a trailing variadic argument so existing call sites that don't
// need these are unaffected.
type StartJobRunOptions struct {
	JobDriver              any
	ConfigurationOverrides any
	ClientToken            string
}

// cloneJSONValue returns a shallow copy of a value produced by decoding JSON
// into `any` (a map[string]any or []any; scalars are returned as-is). Nested
// values are not recursively cloned -- this matches the shallow-copy
// convention already used for Session.ConfigurationOverrides in cloneSession.
func cloneJSONValue(v any) any {
	switch val := v.(type) {
	case map[string]any:
		return maps.Clone(val)
	case []any:
		return slices.Clone(val)
	default:
		return val
	}
}

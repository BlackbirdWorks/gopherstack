package codepipeline

import (
	"fmt"
	"maps"
	"time"

	"github.com/google/uuid"
)

const (
	jobStatusQueued    = "Queued"
	jobStatusSucceeded = "Succeeded"
	jobStatusFailed    = "Failed"

	ownerThirdParty = "ThirdParty"

	maxPercentComplete = 100
)

// JobExecutionDetails mirrors types.ExecutionDetails.
type JobExecutionDetails struct {
	ExternalExecutionID string
	Summary             string
	PercentComplete     int32
}

// JobCurrentRevision mirrors types.CurrentRevision.
type JobCurrentRevision struct {
	Revision         string
	ChangeIdentifier string
	Created          float64
}

// JobSuccess carries the optional members of PutJobSuccessResult and
// PutThirdPartyJobSuccessResult.
type JobSuccess struct {
	ExecutionDetails  *JobExecutionDetails
	CurrentRevision   *JobCurrentRevision
	OutputVariables   map[string]string
	ContinuationToken string
}

// JobFailure carries PutJobFailureResult's FailureDetails.
type JobFailure struct {
	Message             string
	Type                string
	ExternalExecutionID string
}

func isJobWorkerAction(action Action) bool {
	return action.ActionTypeID.Owner == keyOwnerCustom
}

// queueActionJobLocked creates the Queued job a worker polls for to run action.
func (b *InMemoryBackend) queueActionJobLocked(
	region, pipelineName string, ae *ActionExecution, action Action, continuation string,
) {
	b.jobs.Put(&Job{
		region:            region,
		ID:                uuid.NewString(),
		Nonce:             uuid.NewString(),
		Status:            jobStatusQueued,
		ActionTypeID:      action.ActionTypeID,
		PipelineName:      pipelineName,
		ExecutionID:       ae.PipelineExecutionID,
		StageName:         ae.StageName,
		ActionName:        ae.ActionName,
		ActionExecutionID: ae.ActionExecutionID,
		Configuration:     maps.Clone(action.Configuration),
		ContinuationToken: continuation,
	})
}

func (b *InMemoryBackend) findJobActionExecutionLocked(job *Job) *ActionExecution {
	if job.ActionExecutionID == "" {
		return nil
	}

	for _, ae := range b.actionExecutionsStore(job.region)[job.PipelineName] {
		if ae.ActionExecutionID == job.ActionExecutionID {
			return ae
		}
	}

	return nil
}

func validateJobSuccess(res JobSuccess) error {
	if d := res.ExecutionDetails; d != nil && (d.PercentComplete < 0 || d.PercentComplete > maxPercentComplete) {
		return fmt.Errorf(
			"%w: executionDetails.percentComplete must be between 0 and %d",
			ErrValidation,
			maxPercentComplete,
		)
	}

	return nil
}

// completeJobSuccessLocked records a job's success and advances the pipeline
// action it ran. Callers must hold b.mu.Lock.
func (b *InMemoryBackend) completeJobSuccessLocked(job *Job, res JobSuccess) error {
	if err := validateJobSuccess(res); err != nil {
		return err
	}

	ae := b.findJobActionExecutionLocked(job)
	if err := checkJobOpen(job, ae); err != nil {
		return err
	}

	job.Status = jobStatusSucceeded

	if ae == nil {
		return nil
	}

	now := time.Now().UTC()
	ae.LastUpdateTime = now

	if d := res.ExecutionDetails; d != nil {
		ae.ExternalExecutionID = d.ExternalExecutionID
		ae.PercentComplete = d.PercentComplete

		if d.Summary != "" {
			ae.Summary = d.Summary
		}
	}

	if len(res.OutputVariables) > 0 {
		ae.OutputVariables = maps.Clone(res.OutputVariables)
	}

	if rev := res.CurrentRevision; rev != nil {
		b.actionRevisionsStore(job.region)[actionRevisionKey(job.PipelineName, job.StageName, job.ActionName)] =
			&ActionRevisionRecord{
				RevisionID:       rev.Revision,
				RevisionChangeID: rev.ChangeIdentifier,
				Created:          rev.Created,
			}
	}

	p, ok := b.pipelines.Get(regionKey(job.region, job.PipelineName))
	if !ok {
		return nil
	}

	if res.ContinuationToken != "" {
		if action := findAction(findStage(p, job.StageName), job.ActionName); action != nil {
			b.queueActionJobLocked(job.region, job.PipelineName, ae, *action, res.ContinuationToken)
		}

		return nil
	}

	ae.Status = statusSucceeded

	if exec := findExecution(b.executionsStore(job.region)[job.PipelineName], job.ExecutionID); exec != nil {
		b.runPipelineActions(job.region, p, exec)
		exec.LastUpdateTime = now
	}

	return nil
}

// completeJobFailureLocked records a job's failure and fails the pipeline
// action it ran. Callers must hold b.mu.Lock.
func (b *InMemoryBackend) completeJobFailureLocked(job *Job, fail JobFailure) error {
	ae := b.findJobActionExecutionLocked(job)
	if err := checkJobOpen(job, ae); err != nil {
		return err
	}

	job.Status = jobStatusFailed
	job.FailureMessage = fail.Message
	job.FailureType = fail.Type

	if ae == nil {
		return nil
	}

	now := time.Now().UTC()
	ae.Status = statusFailed
	ae.ErrorCode = fail.Type
	ae.ErrorMessage = fail.Message
	ae.ExternalExecutionID = fail.ExternalExecutionID
	ae.LastUpdateTime = now

	if exec := findExecution(b.executionsStore(job.region)[job.PipelineName], job.ExecutionID); exec != nil {
		exec.Status = statusFailed
		exec.LastUpdateTime = now
	}

	return nil
}

func checkJobOpen(job *Job, ae *ActionExecution) error {
	if job.Status == jobStatusSucceeded || job.Status == jobStatusFailed {
		return fmt.Errorf("%w: job %q is already %s", ErrInvalidJobState, job.ID, job.Status)
	}

	if ae != nil && ae.Status != statusInProgress {
		return fmt.Errorf("%w: job %q action execution is %s", ErrInvalidJobState, job.ID, ae.Status)
	}

	return nil
}

// deleteJobsForPipelineLocked removes every job created for pipelineName in region.
func (b *InMemoryBackend) deleteJobsForPipelineLocked(region, pipelineName string) {
	for _, j := range append([]*Job(nil), b.jobsByRegion.Get(region)...) {
		if j.PipelineName == pipelineName {
			b.jobs.Delete(jobKeyFn(j))
		}
	}
}

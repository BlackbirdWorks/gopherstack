package macie2

import (
	"fmt"
	"time"
	"unicode/utf8"
)

const (
	jobStatusComplete = "COMPLETE"
	jobTypeOneTime    = "ONE_TIME"
	jobTypeScheduled  = "SCHEDULED"

	defaultJobRunWindow = 5 * time.Second

	maxJobNameLen        = 500
	maxJobDescriptionLen = 200
	maxJobTags           = 50
)

// SetLifecycleDelay overrides how long a ONE_TIME classification job stays
// RUNNING before it is COMPLETE; 0 keeps the built-in 5s window.
func (b *InMemoryBackend) SetLifecycleDelay(d time.Duration) {
	b.mu.Lock("SetLifecycleDelay")
	defer b.mu.Unlock()

	b.lifecycleDelay = d
}

// settleJobLocked completes a RUNNING one-time job once its run window has
// elapsed; callers hold b.mu for writing.
func (b *InMemoryBackend) settleJobLocked(job *ClassificationJob, now time.Time) {
	if job.JobStatus != jobStatusRunning || job.JobType != jobTypeOneTime {
		return
	}

	window := defaultJobRunWindow
	if b.lifecycleDelay > 0 {
		window = b.lifecycleDelay
	}

	if now.Before(job.CreatedAt.Add(window)) {
		return
	}

	job.JobStatus = jobStatusComplete
	job.UserPausedDetails = nil
}

func (b *InMemoryBackend) jobReferencesExistLocked(allowListIDs, customIDs []string) error {
	for _, id := range allowListIDs {
		if !b.allowLists.Has(id) {
			return fmt.Errorf("%w: allow list %s", ErrAllowListNotFound, id)
		}
	}

	for _, id := range customIDs {
		if !b.customDataIDs.Has(id) {
			return fmt.Errorf("%w: custom data identifier %s", ErrCustomDataIDNotFound, id)
		}
	}

	return nil
}

type jobRequest struct {
	tags               map[string]string
	s3JobDefinition    map[string]any
	scheduleFrequency  map[string]any
	name               string
	description        string
	jobType            string
	selector           string
	customIDs          []string
	managedIDs         []string
	samplingPercentage int32
	hasSamplingPercent bool
}

// validateJobRequest enforces the CreateClassificationJob input constraints
// documented on the SDK's CreateClassificationJobInput.
func validateJobRequest(r jobRequest) error {
	switch {
	case r.name == "" || utf8.RuneCountInString(r.name) > maxJobNameLen:
		return fmt.Errorf("%w: name must be 1 to %d characters", ErrValidation, maxJobNameLen)
	case utf8.RuneCountInString(r.description) > maxJobDescriptionLen:
		return fmt.Errorf("%w: description can contain at most %d characters", ErrValidation, maxJobDescriptionLen)
	case r.hasSamplingPercent && (r.samplingPercentage < 1 || r.samplingPercentage > 100):
		return fmt.Errorf("%w: samplingPercentage must be between 1 and 100", ErrValidation)
	}

	if err := validateJobSchedule(r); err != nil {
		return err
	}

	if err := validateJobSelector(r.selector, r.managedIDs, r.customIDs); err != nil {
		return err
	}

	return validateJobTags(r.tags)
}

func validateJobSchedule(r jobRequest) error {
	switch {
	case r.jobType != jobTypeOneTime && r.jobType != jobTypeScheduled:
		return fmt.Errorf("%w: jobType must be ONE_TIME or SCHEDULED", ErrValidation)
	case len(r.s3JobDefinition) == 0:
		return fmt.Errorf("%w: s3JobDefinition is required", ErrValidation)
	case r.jobType == jobTypeOneTime && len(r.scheduleFrequency) > 0:
		return fmt.Errorf("%w: scheduleFrequency can't be specified for a ONE_TIME job", ErrValidation)
	case r.jobType == jobTypeScheduled && len(r.scheduleFrequency) == 0:
		return fmt.Errorf("%w: scheduleFrequency is required for a SCHEDULED job", ErrValidation)
	}

	return nil
}

func validateJobTags(tags map[string]string) error {
	if len(tags) > maxJobTags {
		return fmt.Errorf("%w: a job can have at most %d tags", ErrValidation, maxJobTags)
	}

	for k, v := range tags {
		if k == "" || len(k) > maxTagKeyLen || len(v) > maxTagValueLen {
			return fmt.Errorf("%w: tag keys must be 1 to %d characters and values at most %d",
				ErrValidation, maxTagKeyLen, maxTagValueLen)
		}
	}

	return nil
}

func validateJobSelector(selector string, managedIDs, customIDs []string) error {
	switch selector {
	case "", valueRecommended, "ALL":
		if len(managedIDs) > 0 {
			return fmt.Errorf("%w: managedDataIdentifierIds can't be specified with the %s selector",
				ErrValidation, selectorOrDefault(selector))
		}
	case valueNone:
		if len(managedIDs) > 0 || len(customIDs) == 0 {
			return fmt.Errorf("%w: the NONE selector requires customDataIdentifierIds and no managedDataIdentifierIds",
				ErrValidation)
		}
	case "INCLUDE", "EXCLUDE":
		if len(managedIDs) == 0 {
			return fmt.Errorf("%w: the %s selector requires managedDataIdentifierIds", ErrValidation, selector)
		}
	default:
		return fmt.Errorf("%w: unknown managedDataIdentifierSelector %q", ErrValidation, selector)
	}

	return nil
}

func selectorOrDefault(s string) string {
	if s == "" {
		return valueRecommended
	}

	return s
}

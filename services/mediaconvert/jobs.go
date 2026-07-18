package mediaconvert

import (
	"fmt"
	"sort"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

// AddJobInternal inserts a job directly into the backend.
func (b *InMemoryBackend) AddJobInternal(j *Job) {
	b.mu.Lock("AddJobInternal")
	defer b.mu.Unlock()

	b.jobs.Put(j)
}

// CreateJob creates a new MediaConvert job.
func (b *InMemoryBackend) CreateJob(
	role, queue, jobTemplate string,
	settings map[string]any,
	tags map[string]string,
	userMetadata map[string]string,
	billingTagsSource string,
) (*Job, error) {
	return b.CreateJobFull(
		role, queue, jobTemplate,
		settings, tags, userMetadata,
		billingTagsSource, "", "", "", 0, nil,
	)
}

// lookupTokenLocked checks the dedup cache and returns a cloned existing job if found.
// Caller must hold the write lock.
func (b *InMemoryBackend) lookupTokenLocked(token string) (*Job, bool) {
	if token == "" {
		return nil, false
	}

	entry, ok := b.tokenIndex.Get(token)
	if !ok || time.Since(entry.createdAt) >= tokenTTL {
		return nil, false
	}

	j, exists := b.jobs.Get(entry.jobID)
	if !exists {
		return nil, false
	}

	return cloneJob(j), true
}

// CreateJobFull creates a new MediaConvert job with all optional fields.
func (b *InMemoryBackend) CreateJobFull(
	role, queue, jobTemplate string,
	settings map[string]any,
	tags map[string]string,
	userMetadata map[string]string,
	billingTagsSource, clientRequestToken, accelerationMode, jobEngineVersionReq string,
	priority int,
	hopDestinations []HopDestination,
) (*Job, error) {
	b.mu.Lock("CreateJobFull")
	defer b.mu.Unlock()

	if role == "" {
		return nil, fmt.Errorf("%w: role is required", ErrValidation)
	}

	// Dedup by ClientRequestToken.
	if j, ok := b.lookupTokenLocked(clientRequestToken); ok {
		return j, nil
	}

	// Resolve queue ARN from queue name or ARN.
	queueArn := ""

	if queue != "" {
		resolved, err := b.resolveQueueLocked(queue)
		if err != nil {
			return nil, err
		}

		queueArn = resolved.Arn
	}

	accelStatus := "NOT_APPLICABLE"
	if accelerationMode == "ENABLED" {
		accelStatus = "PREFERRED"
	}

	var accelSettings *AccelerationSettings
	if accelerationMode != "" {
		accelSettings = &AccelerationSettings{Mode: accelerationMode}
	}

	now := epochSeconds(time.Now())
	id := generateJobID()

	var hopDests []HopDestination
	if len(hopDestinations) > 0 {
		hopDests = make([]HopDestination, len(hopDestinations))
		copy(hopDests, hopDestinations)
	}

	j := &Job{
		Arn:                       arn.Build("mediaconvert", b.region, b.accountID, "jobs/"+id),
		ID:                        id,
		Role:                      role,
		Queue:                     queue,
		QueueArn:                  queueArn,
		JobTemplate:               jobTemplate,
		Status:                    jobStatusSubmitted,
		CurrentPhase:              jobPhaseProbing,
		Settings:                  deepCloneMap(settings),
		Tags:                      nonNilTagsCopy(tags),
		UserMetadata:              nonNilTagsCopy(userMetadata),
		BillingTagsSource:         billingTagsSource,
		AccelerationStatus:        accelStatus,
		AccelerationSettings:      accelSettings,
		SimulateReservedQueue:     "DISABLED",
		StatusUpdateInterval:      "SECONDS_60",
		Timing:                    &JobTiming{SubmitTime: now},
		CreatedAt:                 now,
		Priority:                  priority,
		HopDestinations:           hopDests,
		ClientRequestToken:        clientRequestToken,
		JobEngineVersionRequested: jobEngineVersionReq,
		JobEngineVersionUsed:      jobEngineVersionUsed,
		Messages:                  &JobMessages{},
		Warnings:                  []WarningGroup{},
		ShareStatus:               "NOT_SHARED",
	}
	b.jobs.Put(j)

	// Update queue counter for the new SUBMITTED job.
	b.adjustQueueCounterLocked(queueArn, jobStatusSubmitted, +1)

	// Record token for dedup (cap to prevent unbounded growth).
	if clientRequestToken != "" && b.tokenIndex.Len() < maxTokens {
		b.tokenIndex.Put(&tokenEntry{
			token:     clientRequestToken,
			jobID:     id,
			createdAt: time.Now(),
		})
	}

	if len(tags) > 0 {
		b.storeTagsLocked(j.Arn, tags)
	}

	return cloneJob(j), nil
}

// GetJob returns a job by ID.
func (b *InMemoryBackend) GetJob(id string) (*Job, error) {
	b.mu.RLock("GetJob")
	defer b.mu.RUnlock()

	j, ok := b.jobs.Get(id)
	if !ok {
		return nil, fmt.Errorf("%w: job %s not found", ErrNotFound, id)
	}

	return cloneJob(j), nil
}

// ListJobs returns all jobs sorted by creation time (newest first).
func (b *InMemoryBackend) ListJobs() []*Job {
	return b.ListJobsFiltered("", "", "")
}

// ListJobsFiltered returns jobs filtered by status and/or queue ARN/name,
// sorted by creation time. order="" or "DESCENDING" → newest first; "ASCENDING" → oldest first.
func (b *InMemoryBackend) ListJobsFiltered(status, queue, order string) []*Job {
	b.mu.RLock("ListJobsFiltered")
	defer b.mu.RUnlock()

	list := make([]*Job, 0, b.jobs.Len())

	for _, j := range b.jobs.All() {
		if status != "" && j.Status != status {
			continue
		}

		if queue != "" && j.QueueArn != queue && j.Queue != queue {
			continue
		}

		list = append(list, cloneJob(j))
	}

	if order == "ASCENDING" {
		sort.Slice(list, func(i, j int) bool {
			return list[i].CreatedAt < list[j].CreatedAt
		})
	} else {
		sort.Slice(list, func(i, j int) bool {
			return list[i].CreatedAt > list[j].CreatedAt
		})
	}

	return list
}

// CancelJob cancels a job by ID.
// Only SUBMITTED or PROGRESSING jobs can be canceled.
func (b *InMemoryBackend) CancelJob(id string) error {
	b.mu.Lock("CancelJob")
	defer b.mu.Unlock()

	j, ok := b.jobs.Get(id)
	if !ok {
		return fmt.Errorf("%w: job %s not found", ErrNotFound, id)
	}

	if j.Status != jobStatusSubmitted && j.Status != jobStatusProgressing {
		return fmt.Errorf("%w: job %s cannot be canceled in status %s", ErrValidation, id, j.Status)
	}

	prevStatus := j.Status
	j.Status = jobStatusCanceled

	if j.Timing == nil {
		j.Timing = &JobTiming{}
	}

	j.Timing.FinishTime = epochSeconds(time.Now())

	// Update queue counter: remove from the previous status bucket.
	b.adjustQueueCounterLocked(j.QueueArn, prevStatus, -1)

	return nil
}

// UpdateJob updates a job's priority, queue, and hop destinations.
func (b *InMemoryBackend) UpdateJob(id, queue string, priority *int, hopDestinations []HopDestination) (*Job, error) {
	b.mu.Lock("UpdateJob")
	defer b.mu.Unlock()

	j, ok := b.jobs.Get(id)
	if !ok {
		return nil, fmt.Errorf("%w: job %s not found", ErrNotFound, id)
	}

	if priority != nil {
		if *priority < priorityMin || *priority > priorityMax {
			return nil, fmt.Errorf("%w: priority must be between %d and %d", ErrValidation, priorityMin, priorityMax)
		}

		j.Priority = *priority
	}

	if queue != "" && queue != j.Queue {
		resolved, err := b.resolveQueueLocked(queue)
		if err != nil {
			return nil, err
		}

		// Record the queue transition.
		transition := QueueTransition{
			Timestamp:        epochSeconds(time.Now()),
			SourceQueue:      j.Queue,
			DestinationQueue: queue,
		}
		j.QueueTransitions = append(j.QueueTransitions, transition)

		// Update per-queue counters: move job from old queue to new queue.
		b.adjustQueueCounterLocked(j.QueueArn, j.Status, -1)

		j.Queue = queue
		j.QueueArn = resolved.Arn

		b.adjustQueueCounterLocked(j.QueueArn, j.Status, +1)
	}

	if hopDestinations != nil {
		dests := make([]HopDestination, len(hopDestinations))
		copy(dests, hopDestinations)
		j.HopDestinations = dests
	}

	return cloneJob(j), nil
}

// generateJobID generates a MediaConvert-style job ID.
func generateJobID() string {
	ts := time.Now().UnixMilli()
	suffix := uuid.NewString()[:13]

	return fmt.Sprintf("%d-%s", ts, suffix)
}

// --- Deep clone helpers ---

func cloneJob(j *Job) *Job {
	cp := *j
	cp.Settings = deepCloneMap(j.Settings)
	cp.Tags = nonNilTagsCopy(j.Tags)
	cp.UserMetadata = nonNilTagsCopy(j.UserMetadata)

	if j.Timing != nil {
		t := *j.Timing
		cp.Timing = &t
	}

	if j.AccelerationSettings != nil {
		as := *j.AccelerationSettings
		cp.AccelerationSettings = &as
	}

	if j.Messages != nil {
		m := &JobMessages{}
		if len(j.Messages.Info) > 0 {
			m.Info = append([]string(nil), j.Messages.Info...)
		}

		if len(j.Messages.Warning) > 0 {
			m.Warning = append([]string(nil), j.Messages.Warning...)
		}

		cp.Messages = m
	}

	if len(j.Warnings) > 0 {
		cp.Warnings = append([]WarningGroup(nil), j.Warnings...)
	}

	if len(j.HopDestinations) > 0 {
		cp.HopDestinations = append([]HopDestination(nil), j.HopDestinations...)
	}

	if len(j.QueueTransitions) > 0 {
		cp.QueueTransitions = append([]QueueTransition(nil), j.QueueTransitions...)
	}

	if len(j.OutputGroupDetails) > 0 {
		cp.OutputGroupDetails = cloneOutputGroupDetails(j.OutputGroupDetails)
	}

	if j.LastShareDetails != nil {
		sd := *j.LastShareDetails
		cp.LastShareDetails = &sd
	}

	return &cp
}

func cloneOutputGroupDetails(details []OutputGroupDetail) []OutputGroupDetail {
	cp := make([]OutputGroupDetail, len(details))
	for i, d := range details {
		out := OutputGroupDetail{}

		if len(d.OutputDetails) > 0 {
			out.OutputDetails = make([]OutputDetail, len(d.OutputDetails))
			for j, od := range d.OutputDetails {
				cloned := OutputDetail{DurationInMs: od.DurationInMs}

				if od.VideoDetails != nil {
					vd := *od.VideoDetails
					cloned.VideoDetails = &vd
				}

				out.OutputDetails[j] = cloned
			}
		}

		cp[i] = out
	}

	return cp
}

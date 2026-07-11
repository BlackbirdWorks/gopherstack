package quicksight

import (
	"fmt"
	"time"

	"github.com/google/uuid"
)

const (
	automationJobStatusRunning   = "RUNNING"
	automationJobStatusSucceeded = "SUCCEEDED"
)

// storedAutomationJob is the persisted representation of one run of a
// QuickSight automation, keyed by (AutomationGroupId, AutomationId, JobId).
type storedAutomationJob struct {
	CreatedAt         time.Time `json:"createdAt"`
	StartedAt         time.Time `json:"startedAt"`
	EndedAt           time.Time `json:"endedAt,omitzero"`
	AutomationGroupID string    `json:"automationGroupId"`
	AutomationID      string    `json:"automationId"`
	JobID             string    `json:"jobId"`
	Arn               string    `json:"arn"`
	Status            string    `json:"status"`
	InputPayload      string    `json:"inputPayload,omitempty"`
	OutputPayload     string    `json:"outputPayload,omitempty"`
}

func (j *storedAutomationJob) toAutomationJob() *AutomationJob {
	return &AutomationJob{
		CreatedAt:         j.CreatedAt,
		StartedAt:         j.StartedAt,
		EndedAt:           j.EndedAt,
		AutomationGroupID: j.AutomationGroupID,
		AutomationID:      j.AutomationID,
		JobID:             j.JobID,
		Arn:               j.Arn,
		Status:            j.Status,
		InputPayload:      j.InputPayload,
		OutputPayload:     j.OutputPayload,
	}
}

func automationJobKey(accountID, automationGroupID, automationID, jobID string) string {
	return accountID + "/" + automationGroupID + "/" + automationID + "/" + jobID
}

// ---- Automation jobs ----

// StartAutomationJob starts a new run of automationID (scoped to
// automationGroupID) and returns the freshly assigned job. QuickSight has no
// API to create automation groups/automations themselves (they're authored
// via the console), so any non-empty IDs are accepted.
func (b *InMemoryBackend) StartAutomationJob(
	_, automationGroupID, automationID, inputPayload string,
) (*AutomationJob, error) {
	if automationGroupID == "" || automationID == "" {
		return nil, ErrValidation
	}

	b.mu.Lock("StartAutomationJob")
	defer b.mu.Unlock()

	jobID := uuid.New().String()
	arn := fmt.Sprintf(
		"%s/automations/%s/jobs/%s",
		b.buildARN("automation-group", automationGroupID), automationID, jobID,
	)
	now := time.Now().UTC()
	j := &storedAutomationJob{
		CreatedAt:         now,
		StartedAt:         now,
		AutomationGroupID: automationGroupID,
		AutomationID:      automationID,
		JobID:             jobID,
		Arn:               arn,
		Status:            automationJobStatusRunning,
		InputPayload:      inputPayload,
	}
	b.automationJobs.Put(j)

	return j.toAutomationJob(), nil
}

// settleAutomationJobLocked settles a RUNNING automation job to SUCCEEDED
// deterministically on first poll, mirroring how a real automation job
// finishes shortly after being started.
func (b *InMemoryBackend) settleAutomationJobLocked(j *storedAutomationJob) {
	if j.Status != automationJobStatusRunning {
		return
	}

	j.Status = automationJobStatusSucceeded
	j.EndedAt = time.Now().UTC()
	j.OutputPayload = fmt.Sprintf(`{"jobId":%q,"result":"ok"}`, j.JobID)
}

// DescribeAutomationJob returns the current state of jobID, settling it to
// SUCCEEDED if it was still RUNNING.
func (b *InMemoryBackend) DescribeAutomationJob(
	accountID, automationGroupID, automationID, jobID string,
) (*AutomationJob, error) {
	b.mu.Lock("DescribeAutomationJob")
	defer b.mu.Unlock()

	j, ok := b.automationJobs.Get(automationJobKey(accountID, automationGroupID, automationID, jobID))
	if !ok {
		return nil, ErrAutomationJobNotFound
	}

	b.settleAutomationJobLocked(j)

	return j.toAutomationJob(), nil
}

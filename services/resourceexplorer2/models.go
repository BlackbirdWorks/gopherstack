package resourceexplorer2

import "time"

const (
	indexTypeLocal      = "LOCAL"
	indexTypeAggregator = "AGGREGATOR"

	stateCreating = "CREATING"
	stateActive   = "ACTIVE"
	stateUpdating = "UPDATING"
	stateDeleting = "DELETING"
	stateDeleted  = "DELETED"

	statusSucceeded  = "SUCCEEDED"
	statusFailed     = "FAILED"
	statusInProgress = "IN_PROGRESS"
	statusSkipped    = "SKIPPED"

	taskCreate = "create"
	taskDelete = "delete"
)

// Index is the per-Region index of an account. State is the settled state; Transition is shown until SettleAt.
type Index struct {
	CreatedAt     time.Time         `json:"createdAt"`
	LastUpdatedAt time.Time         `json:"lastUpdatedAt"`
	SettleAt      time.Time         `json:"settleAt"`
	Tags          map[string]string `json:"tags,omitempty"`
	ARN           string            `json:"arn"`
	Region        string            `json:"region"`
	AccountID     string            `json:"accountID"`
	Type          string            `json:"type"`
	State         string            `json:"state"`
	Transition    string            `json:"transition,omitempty"`
}

// View is a saved search scope in one Region.
type View struct {
	LastUpdatedAt      time.Time         `json:"lastUpdatedAt"`
	Tags               map[string]string `json:"tags,omitempty"`
	ARN                string            `json:"arn"`
	Name               string            `json:"name"`
	Region             string            `json:"region"`
	AccountID          string            `json:"accountID"`
	Scope              string            `json:"scope"`
	FilterString       string            `json:"filterString"`
	IncludedProperties []string          `json:"includedProperties,omitempty"`
}

// DefaultView records the default view of an account in a Region; Key is "account|region".
type DefaultView struct {
	Key     string `json:"key"`
	ViewARN string `json:"viewArn"`
}

// AccountState holds account-wide index bookkeeping.
type AccountState struct {
	AggregatorBlockedUntil time.Time `json:"aggregatorBlockedUntil"`
	AccountID              string    `json:"accountID"`
}

// ErrorDetail is a per-Region failure recorded by a setup task.
type ErrorDetail struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

// TaskRegion is the outcome of a setup task in one Region.
type TaskRegion struct {
	IndexErr    *ErrorDetail `json:"indexErr,omitempty"`
	ViewErr     *ErrorDetail `json:"viewErr,omitempty"`
	Region      string       `json:"region"`
	IndexARN    string       `json:"indexArn,omitempty"`
	ViewARN     string       `json:"viewArn,omitempty"`
	IndexStatus string       `json:"indexStatus"`
	ViewStatus  string       `json:"viewStatus"`
}

// SetupTask is a CreateResourceExplorerSetup or DeleteResourceExplorerSetup run.
type SetupTask struct {
	ID        string       `json:"id"`
	AccountID string       `json:"accountID"`
	Kind      string       `json:"kind"`
	Regions   []TaskRegion `json:"regions"`
}

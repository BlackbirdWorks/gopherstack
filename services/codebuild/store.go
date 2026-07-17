package codebuild

import (
	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

const (
	buildStatusSucceeded  = "SUCCEEDED"
	buildStatusInProgress = "IN_PROGRESS"
	buildStatusStopped    = "STOPPED"
	phaseSubmitted        = "SUBMITTED"
	phaseCompleted        = "COMPLETED"
)

// InMemoryBackend is a thread-safe in-memory store for CodeBuild resources.
//
// Every resource map is a *store.Table[T] (see store_setup.go), with former
// ARN reverse-lookup maps and per-parent grouping maps replaced by companion
// *store.Index values. resourcePolicies remains a plain map since its values
// are strings, not *T.
type InMemoryBackend struct {
	projects                   *store.Table[Project]
	projectsByARN              *store.Index[Project]
	builds                     *store.Table[Build]
	buildsByARN                *store.Index[Build]
	buildsByProject            *store.Index[Build]
	fleets                     *store.Table[Fleet]
	fleetsByARN                *store.Index[Fleet]
	reportGroups               *store.Table[ReportGroup]
	reportGroupsByARN          *store.Index[ReportGroup]
	reports                    *store.Table[Report]
	reportsByGroup             *store.Index[Report]
	buildBatches               *store.Table[BuildBatch]
	buildBatchesByARN          *store.Index[BuildBatch]
	buildBatchesByProject      *store.Index[BuildBatch]
	commandExecutions          *store.Table[CommandExecution]
	commandExecutionsBySandbox *store.Index[CommandExecution]
	sandboxes                  *store.Table[Sandbox]
	sandboxesByProject         *store.Index[Sandbox]
	webhooks                   *store.Table[Webhook]
	sourceCredentials          *store.Table[SourceCredentials]
	registry                   *store.Registry
	resourcePolicies           map[string]string // ARN → policy JSON
	mu                         *lockmetrics.RWMutex
	accountID                  string
	region                     string
}

// NewInMemoryBackend creates a new backend for the given account and region.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	b := &InMemoryBackend{
		resourcePolicies: make(map[string]string),
		registry:         store.NewRegistry(),
		accountID:        accountID,
		region:           region,
		mu:               lockmetrics.New("codebuild"),
	}

	registerAllTables(b)

	return b
}

// Region returns the region for this backend instance.
func (b *InMemoryBackend) Region() string { return b.region }

// Reset clears all state in the backend, resetting it to a pristine empty state.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	b.registry.ResetAll()
	b.resourcePolicies = make(map[string]string)
}

func randomID() string {
	return uuid.NewString()[:8]
}

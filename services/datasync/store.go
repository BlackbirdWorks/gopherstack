package datasync

import (
	"fmt"
	"maps"
	"strings"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

// InMemoryBackend implements StorageBackend using pkgs/store tables.
//
// agents, locations, and tasks are flat ARN-keyed resources with globally
// unique identity, so each is registered directly on b.registry (see
// store_setup.go). executions is likewise keyed by its own globally-unique
// TaskExecutionArn (which embeds the owning TaskArn), so it too is a direct
// registration; the former taskArn → executionArn → execution nesting is
// replaced by the executionsByTask secondary index, additively derived from
// TaskExecutionArn via extractTaskArnFromExecution. tags is left as a raw map
// because its values are plain string maps, not *T.
type InMemoryBackend struct {
	mu       *lockmetrics.RWMutex
	registry *store.Registry

	agents    *store.Table[storedAgent]    // keyed by AgentArn
	locations *store.Table[storedLocation] // keyed by LocationArn
	tasks     *store.Table[storedTask]     // keyed by TaskArn

	executions       *store.Table[storedTaskExecution] // keyed by TaskExecutionArn
	executionsByTask *store.Index[storedTaskExecution] // grouped by parent TaskArn

	tags map[string]map[string]string // resourceArn → tags

	accountID string
	region    string
}

// NewInMemoryBackend constructs a new InMemoryBackend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	b := &InMemoryBackend{
		mu:        lockmetrics.New("datasync"),
		registry:  store.NewRegistry(),
		accountID: accountID,
		region:    region,
		tags:      make(map[string]map[string]string),
	}
	registerAllTables(b)

	return b
}

func (b *InMemoryBackend) agentARN(id string) string {
	return arn.Build("datasync", b.region, b.accountID, "agent/"+id)
}

func (b *InMemoryBackend) locationARN(id string) string {
	return arn.Build("datasync", b.region, b.accountID, "location/"+id)
}

func (b *InMemoryBackend) taskARN(id string) string {
	return arn.Build("datasync", b.region, b.accountID, "task/"+id)
}

func (b *InMemoryBackend) executionARN(taskArn, id string) string {
	// Extract task resource portion: task/<task-id>
	parts := strings.SplitN(taskArn, ":task/", arnSplitParts)
	if len(parts) == arnSplitParts {
		return arn.Build("datasync", b.region, b.accountID, fmt.Sprintf("task/%s/execution/%s", parts[1], id))
	}

	return arn.Build("datasync", b.region, b.accountID, "task/unknown/execution/"+id)
}

func newID() string {
	return strings.ReplaceAll(uuid.NewString(), "-", "")[:16]
}

func (b *InMemoryBackend) storeLocation(l *storedLocation) Location {
	b.locations.Put(l)

	if len(l.Tags) > 0 {
		b.tags[l.LocationArn] = make(map[string]string)
		maps.Copy(b.tags[l.LocationArn], l.Tags)
	}

	cp := l.toLocation()

	return cp
}

// AccountID returns the account ID.
func (b *InMemoryBackend) AccountID() string { return b.accountID }

// Region returns the region.
func (b *InMemoryBackend) Region() string { return b.region }

// Reset clears all state.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	b.registry.ResetAll()
	b.tags = make(map[string]map[string]string)
}

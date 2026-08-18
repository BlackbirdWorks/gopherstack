package support

import (
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

// InMemoryBackend is the in-memory store for Support cases.
type InMemoryBackend struct {
	registry                    *store.Registry
	cases                       *store.Table[Case]
	attachmentSets              *store.Table[AttachmentSet]
	attachments                 *store.Table[Attachment]
	checkRefreshStatuses        *store.Table[TrustedAdvisorCheckRefreshStatus]
	checkResults                *store.Table[TrustedAdvisorCheckResult]
	communications              map[string][]Communication
	mu                          *lockmetrics.RWMutex
	attachmentSetCreationTimes  []time.Time
	describeAttachmentCallTimes []time.Time
	nextDisplayID               uint64
}

// NewInMemoryBackend creates a new InMemoryBackend.
func NewInMemoryBackend() *InMemoryBackend {
	b := &InMemoryBackend{
		registry:       store.NewRegistry(),
		communications: make(map[string][]Communication),
		mu:             lockmetrics.New("support"),
	}
	registerAllTables(b)

	return b
}

// Reset clears all backend state.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	b.resetTablesLocked()
	b.communications = make(map[string][]Communication)
	b.nextDisplayID = 0
	b.attachmentSetCreationTimes = nil
	b.describeAttachmentCallTimes = nil
}

// resetTablesLocked resets every store.Table-backed resource field to empty:
// the "clean" tables via one b.registry.ResetAll() call, plus the "dirty"
// attachmentSets table individually since it is not registered on b.registry
// (see store_setup.go). The caller MUST hold b.mu for writing.
func (b *InMemoryBackend) resetTablesLocked() {
	b.registry.ResetAll()
	b.attachmentSets.Reset()
}

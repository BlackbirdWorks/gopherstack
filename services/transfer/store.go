package transfer

import (
	"context"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
	"github.com/blackbirdworks/gopherstack/pkgs/worker"
)

// InMemoryBackend is the in-memory store for Transfer resources.
type InMemoryBackend struct {
	registry             *store.Registry
	servers              *store.Table[Server]
	users                *store.Table[User] // composite key: serverID + "\x00" + userName
	usersByServer        *store.Index[User]
	accesses             *store.Table[Access] // composite key: serverID + "\x00" + externalID
	accessesByServer     *store.Index[Access]
	agreements           *store.Table[Agreement] // composite key: serverID + "\x00" + agreementID
	agreementsByServer   *store.Index[Agreement]
	connectors           *store.Table[Connector]
	profiles             *store.Table[Profile]
	webApps              *store.Table[WebApp]
	webAppCustomizations *store.Table[WebAppCustomization] // NOT persisted -- see store_setup.go
	workflows            *store.Table[Workflow]
	certificates         *store.Table[Certificate]
	hostKeys             *store.Table[HostKey] // composite key: serverID + "\x00" + hostKeyID
	hostKeysByServer     *store.Index[HostKey]
	sshPublicKeys        *store.Table[SSHPublicKey] // composite key: serverID + "\x00" + userName + "\x00" + keyID
	sshKeysByServer      *store.Index[SSHPublicKey]
	sshKeysByServerUser  *store.Index[SSHPublicKey]
	// sshKeyBodies indexes normalized SSH key bodies for O(1) duplicate detection.
	// Left as a plain map (not a store.Table): its values are sets
	// (map[string]struct{}), not *T.
	sshKeyBodies         map[string]map[string]map[string]struct{} // serverID -> userName -> normalizedBody -> {}
	executions           *store.Table[Execution]                   // composite key: workflowID + "\x00" + executionID
	executionsByWorkflow *store.Index[Execution]
	// tagsStore is left as a plain map (not a store.Table): its values are
	// map[string]string, not *T.
	tagsStore       map[string]map[string]string // arn -> tags
	transferRecords *store.Table[FileTransferResult]
	asyncOperations *store.Table[AsyncOperationRecord]
	mu              *lockmetrics.RWMutex
	work            *worker.Group
	accountID       string
	region          string
}

// NewInMemoryBackend creates a new InMemoryBackend.
func NewInMemoryBackend(ctx context.Context, accountID, region string) *InMemoryBackend {
	b := &InMemoryBackend{
		registry:     store.NewRegistry(),
		sshKeyBodies: make(map[string]map[string]map[string]struct{}),
		tagsStore:    make(map[string]map[string]string),
		accountID:    accountID,
		region:       region,
		mu:           lockmetrics.New("transfer"),
		work:         worker.NewGroup(ctx, "transfer"),
	}
	registerAllTables(b)

	return b
}

// Close stops all scheduled server state-transition timers so none outlives the
// backend. It is safe to call multiple times.
func (b *InMemoryBackend) Close() { b.work.Stop() }

// AccountID returns the AWS account ID for this backend.
func (b *InMemoryBackend) AccountID() string { return b.accountID }

// Region returns the AWS region for this backend.
func (b *InMemoryBackend) Region() string { return b.region }

// Reset clears all stored resources, returning the backend to a clean state.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	b.registry.ResetAll()
	b.sshKeyBodies = make(map[string]map[string]map[string]struct{})
	b.tagsStore = make(map[string]map[string]string)
}

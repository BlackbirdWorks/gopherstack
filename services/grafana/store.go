package grafana

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
	"github.com/blackbirdworks/gopherstack/pkgs/worker"
)

// workspaceTransitionDelay is the simulated delay before an async workspace
// status transition (CREATING/UPDATING/UPGRADING/VERSION_UPDATING) reaches
// its terminal state, mirroring services/eks's clusterTransitionDelay.
const workspaceTransitionDelay = 100 * time.Millisecond

// grafanaVersions is the ordered, ascending list of Grafana versions this
// emulator supports for CreateWorkspace/UpdateWorkspaceConfiguration/
// ListVersions. The real AWS-supported set is operational data (which
// versions AWS currently ships), not something encoded in the Go SDK
// module -- this list is a reasonable, documented stand-in; see PARITY.md.
var grafanaVersions = []string{"8.4", "9.4", "10.4"} //nolint:gochecknoglobals // static reference data, not mutated

// InMemoryBackend is the in-memory store for Amazon Managed Grafana.
//
// A single coarse lock guards every collection below: nearly every
// operation in this service reads or mutates workspace-scoped state (a
// workspace's own fields, or one of its child collections keyed by
// workspace ID), so the invariant boundary is the whole backend, not any
// one collection -- see .claude/memories/pkgs-catalog.md's locking rule.
type InMemoryBackend struct {
	permissions                *store.Table[Permission]
	permissionsByWorkspace     *store.Index[Permission]
	apiKeys                    *store.Table[APIKey]
	apiKeysByWorkspace         *store.Index[APIKey]
	serviceAccounts            *store.Table[ServiceAccount]
	serviceAccountsByWorkspace *store.Index[ServiceAccount]
	workspaces                 *store.Table[Workspace]
	tokens                     *store.Table[ServiceAccountToken]
	registry                   *store.Registry
	tokensByServiceAccount     *store.Index[ServiceAccountToken]
	mu                         *lockmetrics.RWMutex
	work                       *worker.Group
	accountID                  string
	region                     string
	nextServiceAccountID       uint64
	nextTokenID                uint64
}

// NewInMemoryBackend creates a new in-memory Amazon Managed Grafana backend.
func NewInMemoryBackend(ctx context.Context, accountID, region string) *InMemoryBackend {
	b := &InMemoryBackend{
		registry:  store.NewRegistry(),
		accountID: accountID,
		region:    region,
		mu:        lockmetrics.New("grafana"),
		work:      worker.NewGroup(ctx, "grafana"),
	}
	registerAllTables(b)

	return b
}

// Region returns the AWS region this backend is configured for.
func (b *InMemoryBackend) Region() string { return b.region }

// AccountID returns the account ID for this backend.
func (b *InMemoryBackend) AccountID() string { return b.accountID }

// Close stops all scheduled state-transition timers so none outlives the
// backend. Safe to call multiple times.
func (b *InMemoryBackend) Close() { b.work.Stop() }

// Reset clears all stored state.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	for _, w := range b.workspaces.All() {
		if w.Tags != nil {
			w.Tags.Close()
		}
	}

	b.registry.ResetAll()
	b.nextServiceAccountID = 0
	b.nextTokenID = 0
}

// WorkspaceARN builds the ARN for a workspace ID. Confirmed via
// terraform-provider-aws's internal/service/grafana/workspace.go
// (workspaceARN: c.RegionalARN(ctx, "grafana", "/workspaces/"+id)) --
// see PARITY.md for the full verification trail. Note the leading "/"
// baked into the resource segment itself, not a separate "/" join.
func (b *InMemoryBackend) WorkspaceARN(id string) string {
	return arn.Build("grafana", b.region, b.accountID, "/workspaces/"+id)
}

// workspaceIDFromARN extracts the workspace ID from a Grafana workspace ARN
// of the form "arn:{partition}:grafana:{region}:{account}:/workspaces/{id}".
// Returns ok=false if arnStr does not match that shape.
func workspaceIDFromARN(arnStr string) (string, bool) {
	const marker = ":/workspaces/"

	idx := strings.LastIndex(arnStr, marker)
	if idx < 0 {
		return "", false
	}

	id := arnStr[idx+len(marker):]
	if id == "" {
		return "", false
	}

	return id, true
}

const randomIDBytes = 5

// randomHexID returns a random 10-character lowercase hex string.
func randomHexID() string {
	buf := make([]byte, randomIDBytes)
	if _, err := rand.Read(buf); err != nil {
		return fmt.Sprintf("%010x", time.Now().UnixNano())
	}

	return hex.EncodeToString(buf)
}

// newWorkspaceID generates a workspace ID in the "g-xxxxxxxxxx" shape real
// Amazon Managed Grafana workspace IDs use. This format is a reasonable
// emulation, not something confirmed byte-for-byte from the SDK (WorkspaceId
// is a bare *string with no pattern trait reproduced in the Go source).
func newWorkspaceID() string {
	return "g-" + randomHexID()
}

// nextServiceAccountIDLocked returns the next sequential service account ID
// as a decimal string, matching real Grafana's own service accounts (small
// integers). Callers must hold b.mu.
func (b *InMemoryBackend) nextServiceAccountIDLocked() string {
	b.nextServiceAccountID++

	return strconv.FormatUint(b.nextServiceAccountID, 10)
}

// nextTokenIDLocked returns the next sequential service account token ID as
// a decimal string. Callers must hold b.mu.
func (b *InMemoryBackend) nextTokenIDLocked() string {
	b.nextTokenID++

	return strconv.FormatUint(b.nextTokenID, 10)
}

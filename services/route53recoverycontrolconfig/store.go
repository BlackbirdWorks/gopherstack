package route53recoverycontrolconfig

import (
	"maps"
	"slices"
	"sort"
	"strings"
	"sync/atomic"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/restroute"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

// InMemoryBackend holds the Route 53 ARC control plane: clusters, control panels, routing controls, safety rules.
type InMemoryBackend struct {
	appConfig        any
	externalEndpoint atomic.Pointer[string]
	registry         *store.Registry
	clusters         *store.Table[Cluster]
	panels           *store.Table[ControlPanel]
	controls         *store.Table[RoutingControl]
	rules            *store.Table[SafetyRule]
	tokens           map[string]string
	mu               *lockmetrics.RWMutex
	accountID        string
	activationDelay  time.Duration
}

// NewInMemoryBackend creates an empty backend for accountID.
func NewInMemoryBackend(accountID string) *InMemoryBackend {
	b := &InMemoryBackend{
		registry:  store.NewRegistry(),
		tokens:    map[string]string{},
		mu:        lockmetrics.New("route53recoverycontrolconfig"),
		accountID: accountID,
	}

	b.clusters = store.Register(b.registry, "clusters", store.New(func(v *Cluster) string { return v.ARN }))
	b.panels = store.Register(b.registry, "panels", store.New(func(v *ControlPanel) string { return v.ARN }))
	b.controls = store.Register(b.registry, "controls", store.New(func(v *RoutingControl) string { return v.ARN }))
	b.rules = store.Register(b.registry, "rules", store.New(func(v *SafetyRule) string { return v.ARN }))

	return b
}

// Reset clears all state.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	b.registry.ResetAll()
	b.tokens = map[string]string{}
}

// SetActivationDelay keeps new resources PENDING (and deleted ones PENDING_DELETION) for d; 0 makes both instant.
func (b *InMemoryBackend) SetActivationDelay(d time.Duration) {
	b.mu.Lock("SetActivationDelay")
	defer b.mu.Unlock()

	b.activationDelay = d
}

func newID() string { return strings.ReplaceAll(uuid.NewString(), "-", "") }

func (b *InMemoryBackend) clusterARN(id string) string {
	return arn.BuildGlobal(arnService, "", b.accountID, "cluster/"+id)
}

func (b *InMemoryBackend) panelARN(id string) string {
	return arn.BuildGlobal(arnService, "", b.accountID, "controlpanel/"+id)
}

func (b *InMemoryBackend) statusOf(created time.Time, deleted *time.Time) string {
	if deleted != nil {
		return statusPendingDeletion
	}

	if b.activationDelay > 0 && time.Since(created) < b.activationDelay {
		return statusPending
	}

	return statusDeployed
}

// alive reports whether a possibly-deleted resource is still visible.
func (b *InMemoryBackend) alive(deleted *time.Time) bool {
	return deleted == nil || time.Since(*deleted) < b.activationDelay
}

// markDeleted removes immediately when no delay is configured, otherwise stamps PENDING_DELETION.
func (b *InMemoryBackend) markDeleted(deleted **time.Time, remove func()) {
	if b.activationDelay <= 0 {
		remove()

		return
	}

	now := time.Now().UTC()
	*deleted = &now
}

func (b *InMemoryBackend) getCluster(arn string) (*Cluster, error) {
	c, ok := b.clusters.Get(arn)
	if !ok || !b.alive(c.DeletedAt) {
		return nil, restroute.NotFound("Cluster %s was not found", arn)
	}

	return c, nil
}

func (b *InMemoryBackend) getPanel(arn string) (*ControlPanel, error) {
	p, ok := b.panels.Get(arn)
	if !ok || !b.alive(p.DeletedAt) {
		return nil, restroute.NotFound("Control panel %s was not found", arn)
	}

	return p, nil
}

// activeCluster is getCluster for new children: a cluster being deleted rejects them.
func (b *InMemoryBackend) activeCluster(arn string) error {
	c, err := b.getCluster(arn)
	if err != nil {
		return err
	}

	if c.DeletedAt != nil {
		return restroute.Conflict("Cluster %s is being deleted", arn)
	}

	return nil
}

// activePanel is getPanel for new children: a control panel being deleted rejects them.
func (b *InMemoryBackend) activePanel(arn string) (*ControlPanel, error) {
	p, err := b.getPanel(arn)
	if err != nil {
		return nil, err
	}

	if p.DeletedAt != nil {
		return nil, restroute.Conflict("Control panel %s is being deleted", arn)
	}

	return p, nil
}

func (b *InMemoryBackend) getControl(arn string) (*RoutingControl, error) {
	c, ok := b.controls.Get(arn)
	if !ok || !b.alive(c.DeletedAt) {
		return nil, restroute.NotFound("Routing control %s was not found", arn)
	}

	return c, nil
}

func (b *InMemoryBackend) getRule(arn string) (*SafetyRule, error) {
	r, ok := b.rules.Get(arn)
	if !ok || !b.alive(r.DeletedAt) {
		return nil, restroute.NotFound("Safety rule %s was not found", arn)
	}

	return r, nil
}

func sortedBy[T any](items []*T, key func(*T) string) []*T {
	out := slices.Clone(items)
	sort.SliceStable(out, func(i, j int) bool { return key(out[i]) < key(out[j]) })

	return out
}

func cloneTags(t map[string]string) map[string]string {
	if len(t) == 0 {
		return nil
	}

	return maps.Clone(t)
}

// idempotent returns the ARN previously created with the same kind and client token.
func (b *InMemoryBackend) idempotent(kind, token string) (string, bool) {
	if token == "" {
		return "", false
	}

	a, ok := b.tokens[kind+"|"+token]

	return a, ok
}

func (b *InMemoryBackend) rememberToken(kind, token, arn string) {
	if token != "" {
		b.tokens[kind+"|"+token] = arn
	}
}

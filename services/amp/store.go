package amp

import (
	"context"
	"slices"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awsmeta"
	"github.com/blackbirdworks/gopherstack/pkgs/idempotency"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

const (
	serviceName   = "aps"
	defaultRegion = "us-east-1"
	defaultDwell  = 500 * time.Millisecond
)

// ClusterResolver reports whether an EKS cluster exists, for scraper source validation.
type ClusterResolver interface {
	ClusterExists(region, name string) bool
}

// InMemoryBackend is the AMP state; one coarse lock guards every table.
type InMemoryBackend struct {
	workspaces    *store.Table[Workspace]
	namespaces    *store.Table[RuleGroupsNamespace]
	scrapers      *store.Table[Scraper]
	detectors     *store.Table[AnomalyDetector]
	series        *store.Table[SeriesRecord]
	seriesByWS    *store.Index[SeriesRecord]
	metadata      *store.Table[MetadataRecord]
	metadataByWS  *store.Index[MetadataRecord]
	registry      *store.Registry
	mu            *lockmetrics.RWMutex
	now           func() time.Time
	clusters      ClusterResolver
	idem          *idempotency.Memo
	publisher     SNSPublisher
	groups        map[string]*groupRuntime
	parsed        map[string]*parsedNamespace
	ams           map[string]*alertManager
	accountID     string
	region        string
	dwellOverride time.Duration
}

// NewInMemoryBackend creates an empty backend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	if region == "" {
		region = defaultRegion
	}

	b := &InMemoryBackend{
		registry:  store.NewRegistry(),
		mu:        lockmetrics.New(serviceName),
		now:       func() time.Time { return time.Now().UTC() },
		idem:      idempotency.New(serviceName),
		accountID: accountID,
		region:    region,
	}

	registerAllTables(b)
	b.resetRuntimeLocked()

	return b
}

// Region returns the default region.
func (b *InMemoryBackend) Region() string { return b.region }

// SetLifecycleDelay overrides the CREATING/UPDATING/DELETING window.
func (b *InMemoryBackend) SetLifecycleDelay(d time.Duration) {
	b.mu.Lock("SetLifecycleDelay")
	defer b.mu.Unlock()

	b.dwellOverride = d
}

// SetClusterResolver wires the EKS lookup used to validate scraper sources.
func (b *InMemoryBackend) SetClusterResolver(r ClusterResolver) {
	b.mu.Lock("SetClusterResolver")
	defer b.mu.Unlock()

	b.clusters = r
}

// Reset clears every table.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	b.registry.ResetAll()
	b.idem.Clear()
	b.resetRuntimeLocked()
}

func (b *InMemoryBackend) dwell() time.Duration {
	if b.dwellOverride > 0 {
		return b.dwellOverride
	}

	return defaultDwell
}

func (b *InMemoryBackend) pend(kind string) Pending {
	return Pending{Kind: kind, Until: b.now().Add(b.dwell())}
}

// state is the externally visible status: the pending kind until it elapses, then ACTIVE.
func (b *InMemoryBackend) state(p Pending) string {
	if b.now().Before(p.Until) {
		return p.Kind
	}

	return statusActive
}

// gone reports a deleting resource whose window has elapsed.
func (b *InMemoryBackend) gone(p Pending) bool {
	return p.Kind == statusDeleting && !b.now().Before(p.Until)
}

// scope identifies the caller's account and region.
type scope struct{ account, region string }

func (b *InMemoryBackend) scopeOf(ctx context.Context) scope {
	sc := scope{account: awsmeta.Account(ctx), region: awsmeta.Region(ctx)}
	if sc.account == "" {
		sc.account = b.accountID
	}

	if sc.region == "" {
		sc.region = b.region
	}

	return sc
}

func (s scope) arn(resource string) string {
	return arn.Build("aps", s.region, s.account, resource)
}

func (s scope) owns(account, region string) bool { return s.account == account && s.region == region }

func nsKey(ws, name string) string     { return ws + "/" + name }
func detectorKey(ws, id string) string { return ws + "/" + id }
func seriesKey(ws, fp string) string   { return ws + "|" + fp }
func workspaceKey(v *Workspace) string { return v.ID }
func namespaceKey(v *RuleGroupsNamespace) string {
	return nsKey(v.WorkspaceID, v.Name)
}
func scraperKey(v *Scraper) string            { return v.ID }
func detectorKeyOf(v *AnomalyDetector) string { return detectorKey(v.WorkspaceID, v.ID) }
func seriesKeyOf(v *SeriesRecord) string      { return v.Key }
func seriesWorkspace(v *SeriesRecord) string  { return v.WorkspaceID }

// purgeLocked drops deleting resources whose window elapsed, with their children and samples.
func (b *InMemoryBackend) purgeLocked() {
	var dead []string

	b.workspaces.Range(func(w *Workspace) bool {
		if b.gone(w.Pending) {
			dead = append(dead, w.ID)
		}

		return true
	})

	for _, id := range dead {
		b.dropWorkspaceLocked(id)
	}

	b.purgeChildrenLocked()
}

func (b *InMemoryBackend) dropWorkspaceLocked(id string) {
	b.workspaces.Delete(id)

	for _, n := range slices.Clone(b.namespaces.All()) {
		if n.WorkspaceID == id {
			b.namespaces.Delete(namespaceKey(n))
		}
	}

	for _, d := range slices.Clone(b.detectors.All()) {
		if d.WorkspaceID == id {
			b.detectors.Delete(detectorKeyOf(d))
		}
	}

	for _, s := range slices.Clone(b.seriesByWS.Get(id)) {
		b.series.Delete(s.Key)
	}

	for _, m := range slices.Clone(b.metadataByWS.Get(id)) {
		b.metadata.Delete(m.Key)
	}
}

func (b *InMemoryBackend) purgeChildrenLocked() {
	for _, n := range b.namespaces.All() {
		if b.gone(n.Doc.Pending) {
			b.namespaces.Delete(namespaceKey(n))
		}
	}

	for _, d := range b.detectors.All() {
		if b.gone(d.Pending) {
			b.detectors.Delete(detectorKeyOf(d))
		}
	}

	for _, s := range b.scrapers.All() {
		if b.gone(s.Pending) {
			b.scrapers.Delete(s.ID)
		} else if s.Logging != nil && b.gone(s.Logging.Pending) {
			s.Logging = nil
		}
	}

	b.workspaces.Range(func(w *Workspace) bool {
		purgeSingletons(b, w)

		return true
	})
}

func purgeSingletons(b *InMemoryBackend, w *Workspace) {
	if w.AlertManager != nil && b.gone(w.AlertManager.Pending) {
		w.AlertManager = nil
	}

	if w.Logging != nil && b.gone(w.Logging.Pending) {
		w.Logging = nil
	}

	if w.QueryLogging != nil && b.gone(w.QueryLogging.Pending) {
		w.QueryLogging = nil
	}

	if w.Policy != nil && b.gone(w.Policy.Pending) {
		w.Policy = nil
	}
}

func hasPrefixFold(s, prefix string) bool {
	return strings.HasPrefix(strings.ToLower(s), strings.ToLower(prefix))
}

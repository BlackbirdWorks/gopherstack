package appstream

import (
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

// InMemoryBackend implements StorageBackend using in-memory maps.
//
// Every resource collection that is a map[string]*T is a *store.Table[T]
// (see store_setup.go), registered once on registry so Reset/Snapshot/
// Restore collapse to one registry call each instead of one hand-written
// block per map (see pkgs/store's package doc, Phase 3.3 of the datalayer
// refactor). stacks, fleets, appBlocks, appBlockBuilders, applications,
// directoryConfigs, images, imageBuilders, exportTasks, and sessions were
// already flat and keyed by their own real identity field, so each registers
// directly with no composite key. entitlements and users were already keyed
// by a composite string (entitlementKey/userKey) built from two real fields
// each already carries (Name+StackName, UserName+AuthenticationType), so
// both also register directly -- the composite key is just their keyFn.
// themes is keyed directly by its own StackName field (one theme per stack).
// imagePermissions previously had no identity field of its own (it was
// always looked up by an external imageName); it gained a real (not hidden --
// this type is purely internal, never marshaled as an AWS wire shape) Name
// field for this purpose, see storedImagePermissions in images.go.
//
// associations, appBlockBuilderAssoc, appFleetAssoc, entitlementApps,
// softwareAssoc, and userStackAssoc are many-to-many association sets
// (map[string]map[string]bool); tags is map[string]map[string]string. None
// of these have a *T value store.Table could key on, so all seven remain
// plain maps, persisted directly by persistence.go (see its doc comment).
// usageReport is a single scalar record (not a map at all), so it also stays
// a plain field.
type InMemoryBackend struct {
	directoryConfigs     *store.Table[storedDirectoryConfig]
	themes               *store.Table[storedTheme]
	fleets               *store.Table[storedFleet]
	associations         map[string]map[string]bool
	tags                 map[string]map[string]string
	appBlocks            *store.Table[storedAppBlock]
	appBlockBuilders     *store.Table[storedAppBlockBuilder]
	appBlockBuilderAssoc map[string]map[string]bool
	applications         *store.Table[storedApplication]
	appFleetAssoc        map[string]map[string]bool
	entitlements         *store.Table[storedEntitlement]
	imagePermissions     *store.Table[storedImagePermissions]
	stacks               *store.Table[storedStack]
	mu                   *lockmetrics.RWMutex
	entitlementApps      map[string]map[string]bool
	imageBuilders        *store.Table[storedImageBuilder]
	softwareAssoc        map[string]map[string]bool
	exportTasks          *store.Table[storedExportImageTask]
	images               *store.Table[storedImage]
	users                *store.Table[storedUser]
	userStackAssoc       map[string]map[string]bool
	sessions             *store.Table[storedSession]
	registry             *store.Registry
	usageReport          *storedUsageReportSubscription
	accountID            string
	region               string
	sessionSeq           int
	exportTaskSeq        int
}

// NewInMemoryBackend constructs a new InMemoryBackend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	b := &InMemoryBackend{
		mu:                   lockmetrics.New("appstream"),
		registry:             store.NewRegistry(),
		associations:         make(map[string]map[string]bool),
		tags:                 make(map[string]map[string]string),
		appBlockBuilderAssoc: make(map[string]map[string]bool),
		appFleetAssoc:        make(map[string]map[string]bool),
		entitlementApps:      make(map[string]map[string]bool),
		softwareAssoc:        make(map[string]map[string]bool),
		userStackAssoc:       make(map[string]map[string]bool),
		accountID:            accountID,
		region:               region,
	}

	registerAllTables(b)

	return b
}

// AccountID returns the account ID.
func (b *InMemoryBackend) AccountID() string { return b.accountID }

// Region returns the region.
func (b *InMemoryBackend) Region() string { return b.region }

// Reset clears all state. Snapshot/Restore (and resetRawMaps, shared with
// Restore's version-mismatch path) live in persistence.go, alongside
// backendSnapshot, since both need the same registry/raw-map inventory this
// method resets.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	b.registry.ResetAll()
	b.resetRawMaps()
}

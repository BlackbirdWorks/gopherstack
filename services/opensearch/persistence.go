package opensearch

import (
	"context"
	"encoding/json"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

type backendSnapshot struct {
	Domains                map[string]*Domain                     `json:"domains"`
	InboundConnections     map[string]*InboundConnection          `json:"inboundConnections"`
	OutboundConnections    map[string]*OutboundConnection         `json:"outboundConnections"`
	DomainDataSources      map[string]map[string]*DataSource      `json:"domainDataSources"`
	DirectQueryDataSources map[string]*DirectQueryDataSource      `json:"directQueryDataSources"`
	PackageAssociations    map[string]map[string]bool             `json:"packageAssociations"`
	VpcAuthorizations      map[string][]AuthorizedPrincipal       `json:"vpcAuthorizations"`
	VpcEndpoints           map[string]*VpcEndpoint                `json:"vpcEndpoints"`
	Applications           map[string]*Application                `json:"applications"`
	Packages               map[string]*Package                    `json:"packages"`
	ScheduledActions       map[string][]*ScheduledAction          `json:"scheduledActions"`
	ReservedInstances      map[string]*ReservedInstance           `json:"reservedInstances"`
	DomainMaintenances     map[string][]*DomainMaintenance        `json:"domainMaintenances"`
	DomainIndexes          map[string]map[string]*DomainIndex     `json:"domainIndexes"`
	UpgradeHistory         map[string][]*UpgradeHistory           `json:"upgradeHistory"`
	AutoTunes              map[string]*AutoTuneConfig             `json:"autoTunes"`
	SlCollections          map[string]*ServerlessCollection       `json:"slCollections"`
	SlAccessPolicies       map[string]*ServerlessAccessPolicy     `json:"slAccessPolicies"`
	SlSecurityConfigs      map[string]*ServerlessSecurityConfig   `json:"slSecurityConfigs"`
	SlEncryptionPolicies   map[string]*ServerlessEncryptionPolicy `json:"slEncryptionPolicies"`
	SlNetworkPolicies      map[string]*ServerlessNetworkPolicy    `json:"slNetworkPolicies"`
	AccountID              string                                 `json:"accountID"`
	Region                 string                                 `json:"region"`
	AppIDCounter           int                                    `json:"appIDCounter"`
	ConnCounter            int                                    `json:"connCounter"`
	VpcEndpointCounter     int                                    `json:"vpcEndpointCounter"`
	PackageCounter         int                                    `json:"packageCounter"`
	MaintenanceCounter     int                                    `json:"maintenanceCounter"`
	ReservedCounter        int                                    `json:"reservedCounter"`
	SlCollCounter          int                                    `json:"slCollCounter"`
	SlSecConfigCounter     int                                    `json:"slSecConfigCounter"`
}

// Snapshot serialises the backend state to JSON.
// It implements persistence.Persistable.
func (b *InMemoryBackend) Snapshot(ctx context.Context) []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	snap := backendSnapshot{
		Domains:                b.domains,
		InboundConnections:     b.inboundConnections,
		OutboundConnections:    b.outboundConnections,
		DomainDataSources:      b.domainDataSources,
		DirectQueryDataSources: b.directQueryDataSources,
		PackageAssociations:    b.packageAssociations,
		VpcAuthorizations:      b.vpcAuthorizations,
		VpcEndpoints:           b.vpcEndpoints,
		Applications:           b.applications,
		Packages:               b.packages,
		ScheduledActions:       b.scheduledActions,
		ReservedInstances:      b.reservedInstances,
		DomainMaintenances:     b.domainMaintenances,
		DomainIndexes:          b.domainIndexes,
		UpgradeHistory:         b.upgradeHistory,
		AutoTunes:              b.autoTunes,
		SlCollections:          b.slCollections,
		SlAccessPolicies:       b.slAccessPolicies,
		SlSecurityConfigs:      b.slSecurityConfigs,
		SlEncryptionPolicies:   b.slEncryptionPolicies,
		SlNetworkPolicies:      b.slNetworkPolicies,
		AppIDCounter:           b.appIDCounter,
		ConnCounter:            b.connCounter,
		VpcEndpointCounter:     b.vpcEndpointCounter,
		PackageCounter:         b.packageCounter,
		MaintenanceCounter:     b.maintenanceCounter,
		ReservedCounter:        b.reservedCounter,
		SlCollCounter:          b.slCollCounter,
		SlSecConfigCounter:     b.slSecConfigCounter,
		AccountID:              b.accountID,
		Region:                 b.region,
	}

	data, err := json.Marshal(snap)
	if err != nil {
		logger.Load(ctx).WarnContext(ctx, "opensearch: failed to snapshot backend", "error", err)

		return nil
	}

	return data
}

// Restore loads backend state from a JSON snapshot.
// It implements persistence.Persistable.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var snap backendSnapshot

	if err := persistence.UnmarshalSnapshot(ctx, "opensearch", data, &snap); err != nil {
		return err
	}

	ensureNonNilMaps(&snap)
	fixNilDomainTags(&snap)

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	// Release tags resources from the domains being replaced.
	for _, d := range b.domains {
		d.Tags.Close()
	}

	b.domains = snap.Domains
	b.inboundConnections = snap.InboundConnections
	b.outboundConnections = snap.OutboundConnections
	b.domainDataSources = snap.DomainDataSources
	b.directQueryDataSources = snap.DirectQueryDataSources
	b.packageAssociations = snap.PackageAssociations
	b.vpcAuthorizations = snap.VpcAuthorizations
	b.vpcEndpoints = snap.VpcEndpoints
	b.applications = snap.Applications
	b.packages = snap.Packages
	b.scheduledActions = snap.ScheduledActions
	b.reservedInstances = snap.ReservedInstances
	b.domainMaintenances = snap.DomainMaintenances
	b.domainIndexes = snap.DomainIndexes
	b.upgradeHistory = snap.UpgradeHistory
	b.autoTunes = snap.AutoTunes
	b.slCollections = snap.SlCollections
	b.slAccessPolicies = snap.SlAccessPolicies
	b.slSecurityConfigs = snap.SlSecurityConfigs
	b.slEncryptionPolicies = snap.SlEncryptionPolicies
	b.slNetworkPolicies = snap.SlNetworkPolicies
	b.appIDCounter = snap.AppIDCounter
	b.connCounter = snap.ConnCounter
	b.vpcEndpointCounter = snap.VpcEndpointCounter
	b.packageCounter = snap.PackageCounter
	b.maintenanceCounter = snap.MaintenanceCounter
	b.reservedCounter = snap.ReservedCounter
	b.slCollCounter = snap.SlCollCounter
	b.slSecConfigCounter = snap.SlSecConfigCounter
	b.accountID = snap.AccountID
	b.region = snap.Region

	// Rebuild the ARN index from restored domains.
	b.arnIndex = rebuildARNIndex(snap.Domains, snap.AccountID, snap.Region)

	return nil
}

// ensureNonNilMaps initialises nil maps in the snapshot to empty maps.
func ensureNonNilMaps(snap *backendSnapshot) {
	ensureNonNilCoreMaps(snap)
	ensureNonNilExtendedMaps(snap)
}

// ensureNonNilCoreMaps initialises nil core maps in the snapshot.
func ensureNonNilCoreMaps(snap *backendSnapshot) {
	if snap.Domains == nil {
		snap.Domains = make(map[string]*Domain)
	}

	if snap.InboundConnections == nil {
		snap.InboundConnections = make(map[string]*InboundConnection)
	}

	if snap.OutboundConnections == nil {
		snap.OutboundConnections = make(map[string]*OutboundConnection)
	}

	if snap.DomainDataSources == nil {
		snap.DomainDataSources = make(map[string]map[string]*DataSource)
	}

	if snap.DirectQueryDataSources == nil {
		snap.DirectQueryDataSources = make(map[string]*DirectQueryDataSource)
	}

	if snap.PackageAssociations == nil {
		snap.PackageAssociations = make(map[string]map[string]bool)
	}

	if snap.VpcAuthorizations == nil {
		snap.VpcAuthorizations = make(map[string][]AuthorizedPrincipal)
	}

	if snap.Applications == nil {
		snap.Applications = make(map[string]*Application)
	}
}

// ensureNonNilExtendedMaps initialises nil extended maps in the snapshot.
func ensureNonNilExtendedMaps(snap *backendSnapshot) {
	if snap.VpcEndpoints == nil {
		snap.VpcEndpoints = make(map[string]*VpcEndpoint)
	}

	if snap.Packages == nil {
		snap.Packages = make(map[string]*Package)
	}

	if snap.ScheduledActions == nil {
		snap.ScheduledActions = make(map[string][]*ScheduledAction)
	}

	if snap.ReservedInstances == nil {
		snap.ReservedInstances = make(map[string]*ReservedInstance)
	}

	if snap.DomainMaintenances == nil {
		snap.DomainMaintenances = make(map[string][]*DomainMaintenance)
	}

	if snap.DomainIndexes == nil {
		snap.DomainIndexes = make(map[string]map[string]*DomainIndex)
	}

	if snap.UpgradeHistory == nil {
		snap.UpgradeHistory = make(map[string][]*UpgradeHistory)
	}

	if snap.AutoTunes == nil {
		snap.AutoTunes = make(map[string]*AutoTuneConfig)
	}

	if snap.SlCollections == nil {
		snap.SlCollections = make(map[string]*ServerlessCollection)
	}

	if snap.SlAccessPolicies == nil {
		snap.SlAccessPolicies = make(map[string]*ServerlessAccessPolicy)
	}

	if snap.SlSecurityConfigs == nil {
		snap.SlSecurityConfigs = make(map[string]*ServerlessSecurityConfig)
	}

	if snap.SlEncryptionPolicies == nil {
		snap.SlEncryptionPolicies = make(map[string]*ServerlessEncryptionPolicy)
	}

	if snap.SlNetworkPolicies == nil {
		snap.SlNetworkPolicies = make(map[string]*ServerlessNetworkPolicy)
	}
}

// fixNilDomainTags ensures that every restored domain has a valid Tags instance.
// When a domain is round-tripped through JSON the tags.Tags.UnmarshalJSON creates
// a new underlying safemap, but if the JSON value was null we still need a usable Tags.
func fixNilDomainTags(snap *backendSnapshot) {
	for name, d := range snap.Domains {
		if d != nil && d.Tags == nil {
			d.Tags = tags.New("opensearch." + name + ".tags")
		}
	}
}

// rebuildARNIndex reconstructs the arnIndex map from a set of restored domains.
func rebuildARNIndex(domains map[string]*Domain, accountID, region string) map[string]string {
	idx := make(map[string]string, len(domains))

	for name := range domains {
		domainARN := arn.Build("es", region, accountID, "domain/"+name)
		idx[domainARN] = name
	}

	return idx
}

// Snapshot implements persistence.Persistable by delegating to the backend.
func (h *Handler) Snapshot(ctx context.Context) []byte {
	return h.Backend.Snapshot(ctx)
}

// Restore implements persistence.Persistable by delegating to the backend.
func (h *Handler) Restore(ctx context.Context, data []byte) error {
	return h.Backend.Restore(ctx, data)
}

package redshift

import (
	"encoding/json"
	"log/slog"
)

type backendSnapshot struct {
	Clusters           map[string]*Cluster                  `json:"clusters"`
	ReservedNodes      map[string]*ReservedNode             `json:"reservedNodes"`
	Partners           map[string]*Partner                  `json:"partners"`
	DataShares         map[string]*DataShare                `json:"dataShares"`
	SecurityGroups     map[string]*ClusterSecurityGroup     `json:"securityGroups"`
	Snapshots          map[string]*Snapshot                 `json:"snapshots"`
	EndpointAuths      map[string]*EndpointAuthorization    `json:"endpointAuths"`
	ActiveResizes      map[string]*ResizeProgress           `json:"activeResizes"`
	ParameterGroups    map[string]*ClusterParameterGroup    `json:"parameterGroups"`
	SubnetGroups       map[string]*ClusterSubnetGroup       `json:"subnetGroups"`
	LoggingStatuses    map[string]*LoggingStatus            `json:"loggingStatuses"`
	EventSubscriptions map[string]*EventSubscription        `json:"eventSubscriptions"`
	Events             map[string]*Event                    `json:"events"`
	HsmClientCerts     map[string]*HsmClientCertificate     `json:"hsmClientCerts"`
	HsmConfigs         map[string]*HsmConfiguration         `json:"hsmConfigs"`
	ScheduledActions   map[string]*ScheduledAction          `json:"scheduledActions"`
	CustomDomains      map[string]*CustomDomainAssociation  `json:"customDomains"`
	EndpointAccesses   map[string]*EndpointAccess           `json:"endpointAccesses"`
	Integrations       map[string]*Integration              `json:"integrations"`
	IdcApplications    map[string]*RedshiftIdcApplication   `json:"idcApplications"`
	AccountID          string                               `json:"accountID"`
	Region             string                               `json:"region"`
}

func (s *backendSnapshot) ensureNonNilMaps() {
	s.ensureCoreMaps()
	s.ensureExtendedMaps()
}

func (s *backendSnapshot) ensureCoreMaps() {
	if s.Clusters == nil {
		s.Clusters = make(map[string]*Cluster)
	}
	if s.ReservedNodes == nil {
		s.ReservedNodes = make(map[string]*ReservedNode)
	}
	if s.Partners == nil {
		s.Partners = make(map[string]*Partner)
	}
	if s.DataShares == nil {
		s.DataShares = make(map[string]*DataShare)
	}
	if s.SecurityGroups == nil {
		s.SecurityGroups = make(map[string]*ClusterSecurityGroup)
	}
	if s.Snapshots == nil {
		s.Snapshots = make(map[string]*Snapshot)
	}
	if s.EndpointAuths == nil {
		s.EndpointAuths = make(map[string]*EndpointAuthorization)
	}
	if s.ActiveResizes == nil {
		s.ActiveResizes = make(map[string]*ResizeProgress)
	}
}

func (s *backendSnapshot) ensureExtendedMaps() {
	if s.ParameterGroups == nil {
		s.ParameterGroups = make(map[string]*ClusterParameterGroup)
	}
	if s.SubnetGroups == nil {
		s.SubnetGroups = make(map[string]*ClusterSubnetGroup)
	}
	if s.LoggingStatuses == nil {
		s.LoggingStatuses = make(map[string]*LoggingStatus)
	}
	if s.EventSubscriptions == nil {
		s.EventSubscriptions = make(map[string]*EventSubscription)
	}
	if s.Events == nil {
		s.Events = make(map[string]*Event)
	}
	if s.HsmClientCerts == nil {
		s.HsmClientCerts = make(map[string]*HsmClientCertificate)
	}
	if s.HsmConfigs == nil {
		s.HsmConfigs = make(map[string]*HsmConfiguration)
	}
	if s.ScheduledActions == nil {
		s.ScheduledActions = make(map[string]*ScheduledAction)
	}

	if s.CustomDomains == nil {
		s.CustomDomains = make(map[string]*CustomDomainAssociation)
	}

	if s.EndpointAccesses == nil {
		s.EndpointAccesses = make(map[string]*EndpointAccess)
	}

	if s.Integrations == nil {
		s.Integrations = make(map[string]*Integration)
	}

	if s.IdcApplications == nil {
		s.IdcApplications = make(map[string]*RedshiftIdcApplication)
	}
}

// Snapshot serialises the backend state to JSON.
// It implements persistence.Persistable.
func (b *InMemoryBackend) Snapshot() []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	snap := backendSnapshot{
		Clusters:           b.clusters,
		ReservedNodes:      b.reservedNodes,
		Partners:           b.partners,
		DataShares:         b.dataShares,
		SecurityGroups:     b.securityGroups,
		Snapshots:          b.snapshots,
		HsmClientCerts:     b.hsmClientCerts,
		HsmConfigs:         b.hsmConfigs,
		ScheduledActions:   b.scheduledActions,
		CustomDomains:      b.customDomains,
		EndpointAccesses:   b.endpointAccesses,
		Integrations:       b.integrations,
		IdcApplications:    b.idcApplications,
		EndpointAuths:      b.endpointAuths,
		ActiveResizes:      b.activeResizes,
		ParameterGroups:    b.parameterGroups,
		SubnetGroups:       b.subnetGroups,
		LoggingStatuses:    b.loggingStatuses,
		EventSubscriptions: b.eventSubscriptions,
		Events:             b.events,
		AccountID:          b.accountID,
		Region:             b.region,
	}

	data, err := json.Marshal(snap)
	if err != nil {
		slog.Default().Warn("redshift: failed to marshal snapshot", "error", err)

		return nil
	}

	return data
}

// Restore loads backend state from a JSON snapshot.
// It implements persistence.Persistable.
func (b *InMemoryBackend) Restore(data []byte) error {
	var snap backendSnapshot

	if err := json.Unmarshal(data, &snap); err != nil {
		return err
	}

	snap.ensureNonNilMaps()

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	b.clusters = snap.Clusters
	b.reservedNodes = snap.ReservedNodes
	b.partners = snap.Partners
	b.dataShares = snap.DataShares
	b.securityGroups = snap.SecurityGroups
	b.snapshots = snap.Snapshots
	b.endpointAuths = snap.EndpointAuths
	b.activeResizes = snap.ActiveResizes
	b.parameterGroups = snap.ParameterGroups
	b.subnetGroups = snap.SubnetGroups
	b.loggingStatuses = snap.LoggingStatuses
	b.eventSubscriptions = snap.EventSubscriptions
	b.events = snap.Events
	b.hsmClientCerts = snap.HsmClientCerts
	b.hsmConfigs = snap.HsmConfigs
	b.scheduledActions = snap.ScheduledActions
	b.customDomains = snap.CustomDomains
	b.endpointAccesses = snap.EndpointAccesses
	b.integrations = snap.Integrations
	b.idcApplications = snap.IdcApplications
	b.accountID = snap.AccountID
	b.region = snap.Region

	return nil
}

// Snapshot implements persistence.Persistable by delegating to the backend.
func (h *Handler) Snapshot() []byte {
	return h.Backend.Snapshot()
}

// Restore implements persistence.Persistable by delegating to the backend.
func (h *Handler) Restore(data []byte) error {
	return h.Backend.Restore(data)
}

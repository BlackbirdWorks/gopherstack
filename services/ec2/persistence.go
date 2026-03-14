package ec2

import (
	"encoding/json"
)

type backendSnapshot struct {
	Instances          map[string]*Instance            `json:"instances"`
	SecurityGroups     map[string]*SecurityGroup       `json:"securityGroups"`
	VPCs               map[string]*VPC                 `json:"vpcs"`
	Subnets            map[string]*Subnet              `json:"subnets"`
	KeyPairs           map[string]*KeyPair             `json:"keyPairs"`
	Volumes            map[string]*Volume              `json:"volumes"`
	Addresses          map[string]*Address             `json:"addresses"`
	InternetGateways   map[string]*InternetGateway     `json:"internetGateways"`
	RouteTables        map[string]*RouteTable          `json:"routeTables"`
	NatGateways        map[string]*NatGateway          `json:"natGateways"`
	NetworkInterfaces  map[string]*NetworkInterface    `json:"networkInterfaces"`
	SpotRequests       map[string]*SpotInstanceRequest `json:"spotRequests"`
	PlacementGroups    map[string]*PlacementGroup      `json:"placementGroups"`
	Tags               map[string]map[string]string    `json:"tags"`
	AccountID          string                          `json:"accountID"`
	Region             string                          `json:"region"`
	NextPrivateIPIndex int                             `json:"nextPrivateIPIndex"`
	NextElasticIPIndex int                             `json:"nextElasticIPIndex"`
}

// Snapshot serialises the backend state to JSON.
// It implements persistence.Persistable.
func (b *InMemoryBackend) Snapshot() []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	snap := backendSnapshot{
		Instances:          b.instances,
		SecurityGroups:     b.securityGroups,
		VPCs:               b.vpcs,
		Subnets:            b.subnets,
		KeyPairs:           b.keyPairs,
		Volumes:            b.volumes,
		Addresses:          b.addresses,
		InternetGateways:   b.internetGateways,
		RouteTables:        b.routeTables,
		NatGateways:        b.natGateways,
		NetworkInterfaces:  b.networkInterfaces,
		SpotRequests:       b.spotRequests,
		PlacementGroups:    b.placementGroups,
		Tags:               b.tags,
		AccountID:          b.AccountID,
		Region:             b.Region,
		NextPrivateIPIndex: b.nextPrivateIPIndex,
		NextElasticIPIndex: b.nextElasticIPIndex,
	}

	data, err := json.Marshal(snap)
	if err != nil {
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

	snap.initMissingMaps()

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	b.instances = snap.Instances
	b.securityGroups = snap.SecurityGroups
	b.vpcs = snap.VPCs
	b.subnets = snap.Subnets
	b.keyPairs = snap.KeyPairs
	b.volumes = snap.Volumes
	b.addresses = snap.Addresses
	b.internetGateways = snap.InternetGateways
	b.routeTables = snap.RouteTables
	b.natGateways = snap.NatGateways
	b.networkInterfaces = snap.NetworkInterfaces
	b.spotRequests = snap.SpotRequests
	b.placementGroups = snap.PlacementGroups
	b.tags = snap.Tags
	b.AccountID = snap.AccountID
	b.Region = snap.Region
	b.nextPrivateIPIndex = snap.NextPrivateIPIndex
	b.nextElasticIPIndex = snap.NextElasticIPIndex

	return nil
}

// initMissingMaps ensures all map fields in the snapshot are non-nil.
// This prevents nil-map panics when the snapshot was created from a backend
// that never populated a particular resource type.
func (s *backendSnapshot) initMissingMaps() {
	if s.Instances == nil {
		s.Instances = make(map[string]*Instance)
	}

	if s.SecurityGroups == nil {
		s.SecurityGroups = make(map[string]*SecurityGroup)
	}

	if s.VPCs == nil {
		s.VPCs = make(map[string]*VPC)
	}

	if s.Subnets == nil {
		s.Subnets = make(map[string]*Subnet)
	}

	if s.KeyPairs == nil {
		s.KeyPairs = make(map[string]*KeyPair)
	}

	if s.Volumes == nil {
		s.Volumes = make(map[string]*Volume)
	}

	if s.Addresses == nil {
		s.Addresses = make(map[string]*Address)
	}

	if s.InternetGateways == nil {
		s.InternetGateways = make(map[string]*InternetGateway)
	}

	if s.RouteTables == nil {
		s.RouteTables = make(map[string]*RouteTable)
	}

	if s.NatGateways == nil {
		s.NatGateways = make(map[string]*NatGateway)
	}

	if s.NetworkInterfaces == nil {
		s.NetworkInterfaces = make(map[string]*NetworkInterface)
	}

	if s.SpotRequests == nil {
		s.SpotRequests = make(map[string]*SpotInstanceRequest)
	}

	if s.PlacementGroups == nil {
		s.PlacementGroups = make(map[string]*PlacementGroup)
	}

	if s.Tags == nil {
		s.Tags = make(map[string]map[string]string)
	}
}

// Snapshot implements persistence.Persistable by delegating to the backend.
// It type-asserts the backend to check for Snapshot support so that alternative
// backend implementations that do not persist state still compile.
func (h *Handler) Snapshot() []byte {
	type snapshotter interface{ Snapshot() []byte }
	if s, ok := h.Backend.(snapshotter); ok {
		return s.Snapshot()
	}

	return nil
}

// Restore implements persistence.Persistable by delegating to the backend.
// It type-asserts the backend to check for Restore support so that alternative
// backend implementations that do not persist state still compile.
func (h *Handler) Restore(data []byte) error {
	type restorer interface{ Restore([]byte) error }
	if r, ok := h.Backend.(restorer); ok {
		return r.Restore(data)
	}

	return nil
}

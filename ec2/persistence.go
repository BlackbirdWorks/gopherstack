package ec2

import (
	"encoding/json"
)

type backendSnapshot struct {
	Instances          map[string]*Instance         `json:"instances"`
	SecurityGroups     map[string]*SecurityGroup    `json:"securityGroups"`
	VPCs               map[string]*VPC              `json:"vpcs"`
	Subnets            map[string]*Subnet           `json:"subnets"`
	KeyPairs           map[string]*KeyPair          `json:"keyPairs"`
	Volumes            map[string]*Volume           `json:"volumes"`
	Addresses          map[string]*Address          `json:"addresses"`
	InternetGateways   map[string]*InternetGateway  `json:"internetGateways"`
	RouteTables        map[string]*RouteTable       `json:"routeTables"`
	NatGateways        map[string]*NatGateway       `json:"natGateways"`
	NetworkInterfaces  map[string]*NetworkInterface `json:"networkInterfaces"`
	AccountID          string                       `json:"accountID"`
	Region             string                       `json:"region"`
	NextPrivateIPIndex int                          `json:"nextPrivateIPIndex"`
	NextElasticIPIndex int                          `json:"nextElasticIPIndex"`
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

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	if snap.Instances == nil {
		snap.Instances = make(map[string]*Instance)
	}

	if snap.SecurityGroups == nil {
		snap.SecurityGroups = make(map[string]*SecurityGroup)
	}

	if snap.VPCs == nil {
		snap.VPCs = make(map[string]*VPC)
	}

	if snap.Subnets == nil {
		snap.Subnets = make(map[string]*Subnet)
	}

	if snap.KeyPairs == nil {
		snap.KeyPairs = make(map[string]*KeyPair)
	}

	if snap.Volumes == nil {
		snap.Volumes = make(map[string]*Volume)
	}

	if snap.Addresses == nil {
		snap.Addresses = make(map[string]*Address)
	}

	if snap.InternetGateways == nil {
		snap.InternetGateways = make(map[string]*InternetGateway)
	}

	if snap.RouteTables == nil {
		snap.RouteTables = make(map[string]*RouteTable)
	}

	if snap.NatGateways == nil {
		snap.NatGateways = make(map[string]*NatGateway)
	}

	if snap.NetworkInterfaces == nil {
		snap.NetworkInterfaces = make(map[string]*NetworkInterface)
	}

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
	b.AccountID = snap.AccountID
	b.Region = snap.Region
	b.nextPrivateIPIndex = snap.NextPrivateIPIndex
	b.nextElasticIPIndex = snap.NextElasticIPIndex

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

package ec2

import (
	"encoding/json"
)

type backendSnapshot struct {
	Instances      map[string]*Instance      `json:"instances"`
	SecurityGroups map[string]*SecurityGroup `json:"securityGroups"`
	VPCs           map[string]*VPC           `json:"vpcs"`
	Subnets        map[string]*Subnet        `json:"subnets"`
	AccountID      string                    `json:"accountID"`
	Region         string                    `json:"region"`
}

// Snapshot serialises the backend state to JSON.
// It implements persistence.Persistable.
func (b *InMemoryBackend) Snapshot() []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	snap := backendSnapshot{
		Instances:      b.instances,
		SecurityGroups: b.securityGroups,
		VPCs:           b.vpcs,
		Subnets:        b.subnets,
		AccountID:      b.AccountID,
		Region:         b.Region,
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

	b.instances = snap.Instances
	b.securityGroups = snap.SecurityGroups
	b.vpcs = snap.VPCs
	b.subnets = snap.Subnets
	b.AccountID = snap.AccountID
	b.Region = snap.Region

	return nil
}

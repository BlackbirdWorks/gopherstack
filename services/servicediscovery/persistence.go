package servicediscovery

import (
	"context"
	"encoding/json"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
)

type backendSnapshot struct {
	Namespaces             map[string]*Namespace        `json:"namespaces"`
	Services               map[string]*Service          `json:"services"`
	Instances              map[string]*Instance         `json:"instances"`
	Operations             map[string]*Operation        `json:"operations"`
	ServiceAttributes      map[string]map[string]string `json:"serviceAttributes"`
	InstanceHealthStatuses map[string]string            `json:"instanceHealthStatuses"`
	AccountID              string                       `json:"accountID"`
	Region                 string                       `json:"region"`
	InstanceRevision       int64                        `json:"instanceRevision"`
	NsCounter              int                          `json:"nsCounter"`
	SvcCounter             int                          `json:"svcCounter"`
	OpCounter              int                          `json:"opCounter"`
}

// Snapshot serialises the backend state to JSON.
func (b *InMemoryBackend) Snapshot(ctx context.Context) []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	snap := backendSnapshot{
		Namespaces:             b.namespaces,
		Services:               b.services,
		Instances:              b.instances,
		Operations:             b.operations,
		ServiceAttributes:      b.serviceAttributes,
		InstanceHealthStatuses: b.instanceHealthStatuses,
		AccountID:              b.accountID,
		Region:                 b.region,
		InstanceRevision:       b.instanceRevision,
		NsCounter:              b.nsCounter,
		SvcCounter:             b.svcCounter,
		OpCounter:              b.opCounter,
	}

	data, err := json.Marshal(snap)
	if err != nil {
		logger.Load(ctx).WarnContext(ctx, "servicediscovery: snapshot marshal failed", "error", err)

		return nil
	}

	return data
}

// Restore loads backend state from a JSON snapshot.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var snap backendSnapshot

	if err := persistence.UnmarshalSnapshot(ctx, "servicediscovery", data, &snap); err != nil {
		return err
	}

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	if snap.Namespaces == nil {
		snap.Namespaces = make(map[string]*Namespace)
	}

	if snap.Services == nil {
		snap.Services = make(map[string]*Service)
	}

	if snap.Instances == nil {
		snap.Instances = make(map[string]*Instance)
	}

	if snap.Operations == nil {
		snap.Operations = make(map[string]*Operation)
	}

	if snap.ServiceAttributes == nil {
		snap.ServiceAttributes = make(map[string]map[string]string)
	}

	if snap.InstanceHealthStatuses == nil {
		snap.InstanceHealthStatuses = make(map[string]string)
	}

	b.namespaces = snap.Namespaces
	b.services = snap.Services
	b.instances = snap.Instances
	b.operations = snap.Operations
	b.serviceAttributes = snap.ServiceAttributes
	b.instanceHealthStatuses = snap.InstanceHealthStatuses
	b.instanceRevision = snap.InstanceRevision
	b.accountID = snap.AccountID
	b.region = snap.Region
	b.nsCounter = snap.NsCounter
	b.svcCounter = snap.SvcCounter
	b.opCounter = snap.OpCounter

	// Rebuild derived indices.
	b.nsARNIndex = make(map[string]string, len(b.namespaces))
	b.nsNameIndex = make(map[string]string, len(b.namespaces))
	b.svcARNIndex = make(map[string]string, len(b.services))
	b.svcByNsAndName = make(map[string]string, len(b.services))
	b.instancesByService = make(map[string]map[string]*Instance)

	for id, ns := range b.namespaces {
		b.nsARNIndex[ns.ARN] = id
		b.nsNameIndex[ns.Name] = id
	}

	for id, svc := range b.services {
		b.svcARNIndex[svc.ARN] = id

		if svc.NamespaceID != "" {
			b.svcByNsAndName[svc.NamespaceID+":"+svc.Name] = id
		}

		b.instancesByService[id] = make(map[string]*Instance)
	}

	for _, inst := range b.instances {
		svcID := inst.ServiceID
		if b.instancesByService[svcID] == nil {
			b.instancesByService[svcID] = make(map[string]*Instance)
		}

		b.instancesByService[svcID][inst.ID] = inst
	}

	return nil
}

// Snapshot implements persistence.Persistable by delegating to the backend.
func (h *Handler) Snapshot(ctx context.Context) []byte {
	return h.Backend.Snapshot(ctx)
}

// Restore implements persistence.Persistable by delegating to the backend.
func (h *Handler) Restore(ctx context.Context, data []byte) error {
	return h.Backend.Restore(ctx, data)
}

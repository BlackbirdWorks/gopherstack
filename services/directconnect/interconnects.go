package directconnect

import (
	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

// CreateInterconnect creates a new interconnect -- an AWS-internal/
// partner-only resource in real life (interconnects are provisioned by
// Direct Connect Partners). See PARITY.md: honest state bookkeeping only,
// no physical cross-connect is simulated.
func (b *InMemoryBackend) CreateInterconnect(req *createInterconnectRequest) (*Interconnect, error) {
	if req.Bandwidth == "" || req.InterconnectName == "" || req.Location == "" {
		return nil, clientError("bandwidth, interconnectName, and location are required")
	}

	if err := validateNewTags(tagWireKeys(req.Tags)); err != nil {
		return nil, err
	}

	b.mu.Lock("CreateInterconnect")
	defer b.mu.Unlock()

	if req.LagID != "" {
		if _, ok := b.lags.Get(req.LagID); !ok {
			return nil, notFoundError(resourceLag, req.LagID)
		}
	}

	id := newInterconnectID()
	t := tags.New("directconnect.interconnect." + id + ".tags")
	t.Merge(tagWireToMap(req.Tags))

	ic := &Interconnect{
		InterconnectID:    id,
		InterconnectName:  req.InterconnectName,
		InterconnectState: InterconnectStateRequested,
		Bandwidth:         req.Bandwidth,
		Location:          req.Location,
		Region:            b.region,
		LagID:             req.LagID,
		ProviderName:      req.ProviderName,
		Tags:              t,
	}
	b.interconnects.Put(ic)

	b.scheduleTransition(
		"interconnect:"+id,
		[]string{InterconnectStatePending, InterconnectStateAvailable},
		&ic.InterconnectState,
	)

	return ic.clone(), nil
}

// DescribeInterconnects returns interconnects, optionally filtered by a
// single InterconnectId.
func (b *InMemoryBackend) DescribeInterconnects(interconnectID string) []*Interconnect {
	b.mu.RLock("DescribeInterconnects")
	defer b.mu.RUnlock()

	if interconnectID != "" {
		if ic, ok := b.interconnects.Get(interconnectID); ok {
			return []*Interconnect{ic.clone()}
		}

		return nil
	}

	all := b.interconnects.Snapshot()
	out := make([]*Interconnect, 0, len(all))

	for _, ic := range all {
		out = append(out, ic.clone())
	}

	return out
}

// DeleteInterconnect transitions an interconnect to "deleting" then
// "deleted".
func (b *InMemoryBackend) DeleteInterconnect(interconnectID string) (string, error) {
	b.mu.Lock("DeleteInterconnect")
	defer b.mu.Unlock()

	ic, ok := b.interconnects.Get(interconnectID)
	if !ok {
		return "", notFoundError(resourceInterconnect, interconnectID)
	}

	ic.InterconnectState = InterconnectStateDeleting
	b.scheduleTransition("interconnect:"+interconnectID, []string{InterconnectStateDeleted}, &ic.InterconnectState)

	return ic.InterconnectState, nil
}

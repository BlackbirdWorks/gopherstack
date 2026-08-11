package bedrock

import (
	"fmt"
	"sort"
	"strconv"
	"time"
)

// CreateFlowVersion creates a numbered snapshot version of a Flow.
func (b *InMemoryBackend) CreateFlowVersion(flowID string) (*FlowVersion, error) {
	b.mu.Lock("CreateFlowVersion")
	defer b.mu.Unlock()

	f, ok := b.flows.Get(flowID)
	if !ok {
		return nil, fmt.Errorf("%w: flow %q not found", ErrNotFound, flowID)
	}

	b.flowVersionCounters[flowID]++
	ver := strconv.Itoa(b.flowVersionCounters[flowID])

	fv := &FlowVersion{
		CreatedAt: time.Now(),
		FlowID:    flowID,
		FlowArn:   f.FlowArn,
		Version:   ver,
		Status:    flowStatusPrepared,
	}

	b.flowVersionsStore(flowID).Put(fv)
	cp := *fv

	return &cp, nil
}

// GetFlowVersion returns a specific Flow version.
func (b *InMemoryBackend) GetFlowVersion(flowID, version string) (*FlowVersion, error) {
	b.mu.RLock("GetFlowVersion")
	defer b.mu.RUnlock()

	versions, versionsOK := b.flowVersions[flowID]
	if !versionsOK {
		return nil, fmt.Errorf("%w: flow %q not found", ErrNotFound, flowID)
	}

	fv, verOK := versions.Get(version)
	if !verOK {
		return nil, fmt.Errorf(
			"%w: flow version %q not found for flow %q",
			ErrNotFound,
			version,
			flowID,
		)
	}

	cp := *fv

	return &cp, nil
}

// ListFlowVersions lists all versions for a Flow.
func (b *InMemoryBackend) ListFlowVersions(
	flowID string,
	maxResults int,
	nextToken string,
) ([]*FlowVersion, string) {
	b.mu.RLock("ListFlowVersions")
	defer b.mu.RUnlock()

	versions := b.flowVersions[flowID]
	list := make([]*FlowVersion, 0)

	if versions != nil {
		for _, fv := range versions.All() {
			cp := *fv
			list = append(list, &cp)
		}
	}

	sort.Slice(list, func(i, j int) bool { return list[i].Version < list[j].Version })

	return paginate(list, maxResults, nextToken)
}

// DeleteFlowVersion deletes a specific Flow version.
func (b *InMemoryBackend) DeleteFlowVersion(flowID, version string) error {
	b.mu.Lock("DeleteFlowVersion")
	defer b.mu.Unlock()

	versions, versionsOK := b.flowVersions[flowID]
	if !versionsOK {
		return fmt.Errorf("%w: flow %q not found", ErrNotFound, flowID)
	}

	if !versions.Has(version) {
		return fmt.Errorf(
			"%w: flow version %q not found for flow %q",
			ErrNotFound,
			version,
			flowID,
		)
	}

	versions.Delete(version)

	return nil
}

package medialive

import (
	"fmt"
	"sort"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

// --- Multiplex operations ---

// CreateMultiplex creates a new Multiplex.
func (b *InMemoryBackend) CreateMultiplex(
	name string,
	availabilityZones []string,
	settings MultiplexSettings,
	tags map[string]string,
) (*Multiplex, error) {
	if name == "" {
		return nil, fmt.Errorf("%w: name required", ErrInvalidParameter)
	}

	zones := make([]string, len(availabilityZones))
	copy(zones, availabilityZones)

	id := newID()
	m := &storedMultiplex{
		ARN:               b.multiplexARN(id),
		ID:                id,
		Name:              name,
		State:             stateIdle,
		AvailabilityZones: zones,
		Settings:          storedMultiplexSettings(settings),
		Tags:              copyTags(tags),
		Programs:          make(map[string]*storedMultiplexProgram),
	}

	b.mu.Lock("CreateMultiplex")
	defer b.mu.Unlock()

	b.multiplexes.Put(m)

	return m.toMultiplex(), nil
}

// DescribeMultiplex returns a Multiplex by ID.
func (b *InMemoryBackend) DescribeMultiplex(multiplexID string) (*Multiplex, error) {
	b.mu.RLock("DescribeMultiplex")
	defer b.mu.RUnlock()

	m, ok := b.multiplexes.Get(multiplexID)
	if !ok {
		return nil, fmt.Errorf("%w: multiplex %s not found", ErrNotFound, multiplexID)
	}

	return m.toMultiplex(), nil
}

// UpdateMultiplex updates a Multiplex's mutable fields.
func (b *InMemoryBackend) UpdateMultiplex(
	multiplexID, name string,
	settings MultiplexSettings,
) (*Multiplex, error) {
	b.mu.Lock("UpdateMultiplex")
	defer b.mu.Unlock()

	m, ok := b.multiplexes.Get(multiplexID)
	if !ok {
		return nil, fmt.Errorf("%w: multiplex %s not found", ErrNotFound, multiplexID)
	}

	if name != "" {
		m.Name = name
	}

	m.Settings = storedMultiplexSettings(settings)

	return m.toMultiplex(), nil
}

// DeleteMultiplex deletes a Multiplex.
func (b *InMemoryBackend) DeleteMultiplex(multiplexID string) (*Multiplex, error) {
	b.mu.Lock("DeleteMultiplex")
	defer b.mu.Unlock()

	m, ok := b.multiplexes.Get(multiplexID)
	if !ok {
		return nil, fmt.Errorf("%w: multiplex %s not found", ErrNotFound, multiplexID)
	}

	if m.State == stateRunning {
		return nil, fmt.Errorf("%w: multiplex must be idle before deleting", ErrConflict)
	}

	m.State = stateDeleted
	b.multiplexes.Delete(multiplexID)

	return m.toMultiplex(), nil
}

// ListMultiplexes returns a paginated list of multiplexes.
func (b *InMemoryBackend) ListMultiplexes(
	maxResults int,
	nextToken string,
) ([]*MultiplexSummary, string, error) {
	b.mu.RLock("ListMultiplexes")
	defer b.mu.RUnlock()

	all := b.multiplexes.All()

	sort.Slice(all, func(i, j int) bool { return all[i].ID < all[j].ID })

	pg := page.New(all, nextToken, maxResults, defaultMaxResults)

	summaries := make([]*MultiplexSummary, 0, len(pg.Data))
	for _, m := range pg.Data {
		summaries = append(summaries, m.toSummary())
	}

	return summaries, pg.Next, nil
}

// StartMultiplex transitions a Multiplex toward RUNNING.
// Stored state advances immediately; response carries STARTING per AWS contract.
func (b *InMemoryBackend) StartMultiplex(multiplexID string) (*Multiplex, error) {
	b.mu.Lock("StartMultiplex")
	defer b.mu.Unlock()

	m, ok := b.multiplexes.Get(multiplexID)
	if !ok {
		return nil, fmt.Errorf("%w: multiplex %s not found", ErrNotFound, multiplexID)
	}

	if m.State != stateIdle {
		return nil, fmt.Errorf("%w: multiplex must be idle to start", ErrConflict)
	}

	m.State = stateRunning

	result := m.toMultiplex()
	result.State = stateStarting

	return result, nil
}

// StopMultiplex transitions a Multiplex toward IDLE.
// Stored state advances immediately; response carries STOPPING per AWS contract.
func (b *InMemoryBackend) StopMultiplex(multiplexID string) (*Multiplex, error) {
	b.mu.Lock("StopMultiplex")
	defer b.mu.Unlock()

	m, ok := b.multiplexes.Get(multiplexID)
	if !ok {
		return nil, fmt.Errorf("%w: multiplex %s not found", ErrNotFound, multiplexID)
	}

	if m.State != stateRunning {
		return nil, fmt.Errorf("%w: multiplex must be running to stop", ErrConflict)
	}

	m.State = stateIdle

	result := m.toMultiplex()
	result.State = stateStopping

	return result, nil
}

// ListMultiplexAlerts returns alerts for a multiplex (always empty in emulation).
func (b *InMemoryBackend) ListMultiplexAlerts(multiplexID string) ([]map[string]any, error) {
	b.mu.RLock("ListMultiplexAlerts")
	defer b.mu.RUnlock()

	if !b.multiplexes.Has(multiplexID) {
		return nil, fmt.Errorf("%w: multiplex %s not found", ErrNotFound, multiplexID)
	}

	return []map[string]any{}, nil
}

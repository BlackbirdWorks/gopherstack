package medialive

import (
	"fmt"
	"sort"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

// --- MultiplexProgram operations ---

// CreateMultiplexProgram creates a program within a Multiplex.
func (b *InMemoryBackend) CreateMultiplexProgram(
	multiplexID string,
	prog MultiplexProgramSettings,
) (*MultiplexProgram, error) {
	if prog.ProgramName == "" {
		return nil, fmt.Errorf("%w: programName required", ErrInvalidParameter)
	}

	b.mu.Lock("CreateMultiplexProgram")
	defer b.mu.Unlock()

	m, ok := b.multiplexes.Get(multiplexID)
	if !ok {
		return nil, fmt.Errorf("%w: multiplex %s not found", ErrNotFound, multiplexID)
	}

	if _, exists := m.Programs[prog.ProgramName]; exists {
		return nil, fmt.Errorf("%w: program %s already exists", ErrConflict, prog.ProgramName)
	}

	p := &storedMultiplexProgram{
		ProgramName: prog.ProgramName,
		Settings: storedMultiplexProgramSettings{
			ProgramNumber:            prog.ProgramNumber,
			PreferredChannelPipeline: prog.PreferredChannelPipeline,
			ServiceDescriptor: storedServiceDescriptor{
				ProviderName: prog.ServiceDescriptor.ProviderName,
				ServiceName:  prog.ServiceDescriptor.ServiceName,
			},
		},
	}

	m.Programs[prog.ProgramName] = p

	return p.toProgram(), nil
}

// DescribeMultiplexProgram returns a program by multiplex ID and program name.
func (b *InMemoryBackend) DescribeMultiplexProgram(
	multiplexID, programName string,
) (*MultiplexProgram, error) {
	b.mu.RLock("DescribeMultiplexProgram")
	defer b.mu.RUnlock()

	m, ok := b.multiplexes.Get(multiplexID)
	if !ok {
		return nil, fmt.Errorf("%w: multiplex %s not found", ErrNotFound, multiplexID)
	}

	p, ok := m.Programs[programName]
	if !ok {
		return nil, fmt.Errorf("%w: program %s not found", ErrNotFound, programName)
	}

	return p.toProgram(), nil
}

// UpdateMultiplexProgram updates a program's settings.
func (b *InMemoryBackend) UpdateMultiplexProgram(
	multiplexID string,
	prog MultiplexProgramSettings,
) (*MultiplexProgram, error) {
	b.mu.Lock("UpdateMultiplexProgram")
	defer b.mu.Unlock()

	m, ok := b.multiplexes.Get(multiplexID)
	if !ok {
		return nil, fmt.Errorf("%w: multiplex %s not found", ErrNotFound, multiplexID)
	}

	p, ok := m.Programs[prog.ProgramName]
	if !ok {
		return nil, fmt.Errorf("%w: program %s not found", ErrNotFound, prog.ProgramName)
	}

	p.Settings = storedMultiplexProgramSettings{
		ProgramNumber:            prog.ProgramNumber,
		PreferredChannelPipeline: prog.PreferredChannelPipeline,
		ServiceDescriptor: storedServiceDescriptor{
			ProviderName: prog.ServiceDescriptor.ProviderName,
			ServiceName:  prog.ServiceDescriptor.ServiceName,
		},
	}

	return p.toProgram(), nil
}

// DeleteMultiplexProgram removes a program from a Multiplex.
func (b *InMemoryBackend) DeleteMultiplexProgram(
	multiplexID, programName string,
) (*MultiplexProgram, error) {
	b.mu.Lock("DeleteMultiplexProgram")
	defer b.mu.Unlock()

	m, ok := b.multiplexes.Get(multiplexID)
	if !ok {
		return nil, fmt.Errorf("%w: multiplex %s not found", ErrNotFound, multiplexID)
	}

	p, ok := m.Programs[programName]
	if !ok {
		return nil, fmt.Errorf("%w: program %s not found", ErrNotFound, programName)
	}

	delete(m.Programs, programName)

	return p.toProgram(), nil
}

// ListMultiplexPrograms returns a paginated list of programs for a Multiplex.
func (b *InMemoryBackend) ListMultiplexPrograms(
	multiplexID string,
	maxResults int,
	nextToken string,
) ([]*MultiplexProgramSummary, string, error) {
	b.mu.RLock("ListMultiplexPrograms")
	defer b.mu.RUnlock()

	m, ok := b.multiplexes.Get(multiplexID)
	if !ok {
		return nil, "", fmt.Errorf("%w: multiplex %s not found", ErrNotFound, multiplexID)
	}

	all := make([]*storedMultiplexProgram, 0, len(m.Programs))
	for _, p := range m.Programs {
		all = append(all, p)
	}

	sort.Slice(all, func(i, j int) bool { return all[i].ProgramName < all[j].ProgramName })

	pg := page.New(all, nextToken, maxResults, defaultMaxResults)

	summaries := make([]*MultiplexProgramSummary, 0, len(pg.Data))
	for _, p := range pg.Data {
		summaries = append(summaries, p.toSummary())
	}

	return summaries, pg.Next, nil
}

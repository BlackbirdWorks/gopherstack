package mediatailor

import (
	"fmt"
	"slices"
	"sort"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

// --- Program operations ---

func programKey(channelName, programName string) string {
	return channelName + "/" + programName
}

// CreateProgram creates a program within a channel.
func (b *InMemoryBackend) CreateProgram(
	channelName, programName, sourceLocationName, vodSourceName, liveSourceName string,
	tags map[string]string,
) (*Program, error) {
	b.mu.Lock("CreateProgram")
	defer b.mu.Unlock()

	if !b.channels.Has(channelName) {
		return nil, fmt.Errorf("%w: channel %s not found", ErrNotFound, channelName)
	}

	progARN := fmt.Sprintf(
		"arn:aws:mediatailor:%s:%s:channel/%s/program/%s",
		b.region, b.accountID, channelName, programName,
	)
	prog := &Program{
		Tags:               copyTags(tags),
		ARN:                progARN,
		ChannelName:        channelName,
		ProgramName:        programName,
		SourceLocationName: sourceLocationName,
		VodSourceName:      vodSourceName,
		LiveSourceName:     liveSourceName,
	}
	b.programs.Put(prog)

	return prog, nil
}

// DescribeProgram returns a program.
func (b *InMemoryBackend) DescribeProgram(channelName, programName string) (*Program, error) {
	b.mu.RLock("DescribeProgram")
	defer b.mu.RUnlock()

	prog, ok := b.programs.Get(programKey(channelName, programName))
	if !ok {
		return nil, fmt.Errorf("%w: program %s not found", ErrNotFound, programName)
	}

	return prog, nil
}

// UpdateProgram updates a program.
func (b *InMemoryBackend) UpdateProgram(channelName, programName string) (*Program, error) {
	b.mu.RLock("UpdateProgram")
	defer b.mu.RUnlock()

	prog, ok := b.programs.Get(programKey(channelName, programName))
	if !ok {
		return nil, fmt.Errorf("%w: program %s not found", ErrNotFound, programName)
	}

	return prog, nil
}

// DeleteProgram deletes a program.
func (b *InMemoryBackend) DeleteProgram(channelName, programName string) error {
	b.mu.Lock("DeleteProgram")
	defer b.mu.Unlock()

	key := programKey(channelName, programName)
	if !b.programs.Has(key) {
		return fmt.Errorf("%w: program %s not found", ErrNotFound, programName)
	}

	b.programs.Delete(key)

	return nil
}

// GetChannelSchedule returns a paginated schedule for a channel.
func (b *InMemoryBackend) GetChannelSchedule(
	channelName string, maxResults int, nextToken string,
) ([]*ProgramScheduleEntry, string, error) {
	b.mu.RLock("GetChannelSchedule")
	defer b.mu.RUnlock()

	if !b.channels.Has(channelName) {
		return nil, "", fmt.Errorf("%w: channel %s not found", ErrNotFound, channelName)
	}

	all := slices.Clone(b.programsByChannel.Get(channelName))

	sort.Slice(all, func(i, j int) bool { return all[i].ProgramName < all[j].ProgramName })

	pg := page.New(all, nextToken, maxResults, defaultMaxResults)

	out := make([]*ProgramScheduleEntry, 0, len(pg.Data))
	for _, prog := range pg.Data {
		out = append(out, &ProgramScheduleEntry{
			ARN:         prog.ARN,
			ChannelName: prog.ChannelName,
			ProgramName: prog.ProgramName,
		})
	}

	return out, pg.Next, nil
}

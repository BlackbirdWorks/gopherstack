package mediatailor

import (
	"fmt"
	"slices"
	"sort"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

const (
	prefetchScheduleTypeSingle    = "SINGLE"
	prefetchScheduleTypeRecurring = "RECURRING"

	listPrefetchScheduleTypeAll = "ALL"
)

// --- PrefetchSchedule operations ---

func prefetchScheduleKey(playbackConfigName, name string) string {
	return playbackConfigName + "/" + name
}

// CreatePrefetchSchedule creates a prefetch schedule.
func (b *InMemoryBackend) CreatePrefetchSchedule(
	playbackConfigName, name, scheduleType, streamID string,
	retrieval *PrefetchRetrieval,
	consumption *PrefetchConsumption,
	recurringConfig map[string]any,
	tags map[string]string,
) (*PrefetchSchedule, error) {
	switch scheduleType {
	case "":
		scheduleType = prefetchScheduleTypeSingle
	case prefetchScheduleTypeSingle, prefetchScheduleTypeRecurring:
	default:
		return nil, fmt.Errorf(
			"%w: ScheduleType must be %s or %s",
			ErrInvalidParameter, prefetchScheduleTypeSingle, prefetchScheduleTypeRecurring,
		)
	}

	b.mu.Lock("CreatePrefetchSchedule")
	defer b.mu.Unlock()

	if !b.playbackConfigurations.Has(playbackConfigName) {
		return nil, fmt.Errorf("%w: playback configuration %s not found", ErrNotFound, playbackConfigName)
	}

	psARN := fmt.Sprintf(
		"arn:aws:mediatailor:%s:%s:prefetchSchedule/%s/%s",
		b.region, b.accountID, playbackConfigName, name,
	)
	ps := &PrefetchSchedule{
		ARN:                            psARN,
		Name:                           name,
		PlaybackConfigurationName:      playbackConfigName,
		ScheduleType:                   scheduleType,
		StreamID:                       streamID,
		Retrieval:                      retrieval,
		Consumption:                    consumption,
		RecurringPrefetchConfiguration: recurringConfig,
		CreationTime:                   time.Now().UTC(),
		Tags:                           copyTags(tags),
	}
	b.prefetchSchedules.Put(ps)
	b.tags[psARN] = copyTags(tags)

	return ps, nil
}

// GetPrefetchSchedule returns a prefetch schedule.
func (b *InMemoryBackend) GetPrefetchSchedule(playbackConfigName, name string) (*PrefetchSchedule, error) {
	b.mu.RLock("GetPrefetchSchedule")
	defer b.mu.RUnlock()

	ps, ok := b.prefetchSchedules.Get(prefetchScheduleKey(playbackConfigName, name))
	if !ok {
		return nil, fmt.Errorf("%w: prefetch schedule %s not found", ErrNotFound, name)
	}

	return ps, nil
}

// DeletePrefetchSchedule deletes a prefetch schedule.
func (b *InMemoryBackend) DeletePrefetchSchedule(playbackConfigName, name string) error {
	b.mu.Lock("DeletePrefetchSchedule")
	defer b.mu.Unlock()

	key := prefetchScheduleKey(playbackConfigName, name)
	if !b.prefetchSchedules.Has(key) {
		return fmt.Errorf("%w: prefetch schedule %s not found", ErrNotFound, name)
	}

	b.prefetchSchedules.Delete(key)

	return nil
}

// ListPrefetchSchedules returns prefetch schedules for a playback
// configuration, optionally filtered by scheduleType ("SINGLE", "RECURRING",
// "ALL", or "" -- all three are treated as no filter, matching
// ListPrefetchScheduleType's ALL member) and by an exact streamID match.
func (b *InMemoryBackend) ListPrefetchSchedules(
	playbackConfigName, scheduleType, streamID string,
	maxResults int,
	nextToken string,
) ([]*PrefetchSchedule, string, error) {
	b.mu.RLock("ListPrefetchSchedules")
	defer b.mu.RUnlock()

	all := slices.Clone(b.prefetchSchedulesByConfig.Get(playbackConfigName))

	filtered := all[:0:0]

	for _, ps := range all {
		if scheduleType != "" && scheduleType != listPrefetchScheduleTypeAll && ps.ScheduleType != scheduleType {
			continue
		}

		if streamID != "" && ps.StreamID != streamID {
			continue
		}

		filtered = append(filtered, ps)
	}

	sort.Slice(filtered, func(i, j int) bool { return filtered[i].Name < filtered[j].Name })

	pg := page.New(filtered, nextToken, maxResults, defaultMaxResults)

	return pg.Data, pg.Next, nil
}

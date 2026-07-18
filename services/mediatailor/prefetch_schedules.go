package mediatailor

import (
	"fmt"
	"slices"
	"sort"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

// --- PrefetchSchedule operations ---

func prefetchScheduleKey(playbackConfigName, name string) string {
	return playbackConfigName + "/" + name
}

// CreatePrefetchSchedule creates a prefetch schedule.
func (b *InMemoryBackend) CreatePrefetchSchedule(
	playbackConfigName, name string,
	retrieval *PrefetchRetrieval,
	consumption *PrefetchConsumption,
) (*PrefetchSchedule, error) {
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
		ARN:                       psARN,
		Name:                      name,
		PlaybackConfigurationName: playbackConfigName,
		Retrieval:                 retrieval,
		Consumption:               consumption,
	}
	b.prefetchSchedules.Put(ps)

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

// ListPrefetchSchedules returns prefetch schedules for a playback configuration.
func (b *InMemoryBackend) ListPrefetchSchedules(
	playbackConfigName string,
	maxResults int,
	nextToken string,
) ([]*PrefetchSchedule, string, error) {
	b.mu.RLock("ListPrefetchSchedules")
	defer b.mu.RUnlock()

	all := slices.Clone(b.prefetchSchedulesByConfig.Get(playbackConfigName))

	sort.Slice(all, func(i, j int) bool { return all[i].Name < all[j].Name })

	pg := page.New(all, nextToken, maxResults, defaultMaxResults)

	return pg.Data, pg.Next, nil
}

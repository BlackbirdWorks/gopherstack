package mediatailor

import (
	"fmt"
	"slices"
	"sort"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

// --- LiveSource operations ---

func liveSourceKey(sourceLocationName, liveSourceName string) string {
	return sourceLocationName + "/" + liveSourceName
}

// CreateLiveSource creates a new live source.
func (b *InMemoryBackend) CreateLiveSource(
	sourceLocationName, liveSourceName string,
	httpPackageConfigurations []HTTPPackageConfiguration,
	tags map[string]string,
) (*LiveSource, error) {
	b.mu.Lock("CreateLiveSource")
	defer b.mu.Unlock()

	if !b.sourceLocations.Has(sourceLocationName) {
		return nil, fmt.Errorf("%w: source location %s not found", ErrNotFound, sourceLocationName)
	}

	key := liveSourceKey(sourceLocationName, liveSourceName)
	if b.liveSources.Has(key) {
		return nil, fmt.Errorf("%w: live source %s already exists", ErrConflict, liveSourceName)
	}

	cfgs := make([]HTTPPackageConfiguration, len(httpPackageConfigurations))
	copy(cfgs, httpPackageConfigurations)

	lsARN := fmt.Sprintf(
		"arn:aws:mediatailor:%s:%s:sourceLocation/%s/liveSource/%s",
		b.region, b.accountID, sourceLocationName, liveSourceName,
	)
	now := time.Now().UTC()
	ls := &LiveSource{
		CreationTime:              now,
		LastModified:              now,
		Tags:                      copyTags(tags),
		ARN:                       lsARN,
		SourceLocationName:        sourceLocationName,
		LiveSourceName:            liveSourceName,
		HTTPPackageConfigurations: cfgs,
	}
	b.liveSources.Put(ls)
	b.tags[lsARN] = copyTags(tags)

	return ls, nil
}

// DescribeLiveSource returns a live source by name.
func (b *InMemoryBackend) DescribeLiveSource(sourceLocationName, liveSourceName string) (*LiveSource, error) {
	b.mu.RLock("DescribeLiveSource")
	defer b.mu.RUnlock()

	key := liveSourceKey(sourceLocationName, liveSourceName)

	ls, ok := b.liveSources.Get(key)
	if !ok {
		return nil, fmt.Errorf("%w: live source %s not found", ErrNotFound, liveSourceName)
	}

	out := *ls
	out.Tags = copyTags(b.tags[ls.ARN])

	return &out, nil
}

// UpdateLiveSource updates a live source.
func (b *InMemoryBackend) UpdateLiveSource(
	sourceLocationName, liveSourceName string,
	httpPackageConfigurations []HTTPPackageConfiguration,
) (*LiveSource, error) {
	b.mu.Lock("UpdateLiveSource")
	defer b.mu.Unlock()

	key := liveSourceKey(sourceLocationName, liveSourceName)

	ls, ok := b.liveSources.Get(key)
	if !ok {
		return nil, fmt.Errorf("%w: live source %s not found", ErrNotFound, liveSourceName)
	}

	cfgs := make([]HTTPPackageConfiguration, len(httpPackageConfigurations))
	copy(cfgs, httpPackageConfigurations)
	ls.HTTPPackageConfigurations = cfgs
	ls.LastModified = time.Now().UTC()

	out := *ls
	out.Tags = copyTags(b.tags[ls.ARN])

	return &out, nil
}

// DeleteLiveSource deletes a live source.
func (b *InMemoryBackend) DeleteLiveSource(sourceLocationName, liveSourceName string) error {
	b.mu.Lock("DeleteLiveSource")
	defer b.mu.Unlock()

	key := liveSourceKey(sourceLocationName, liveSourceName)

	ls, ok := b.liveSources.Get(key)
	if !ok {
		return fmt.Errorf("%w: live source %s not found", ErrNotFound, liveSourceName)
	}

	delete(b.tags, ls.ARN)
	b.liveSources.Delete(key)

	return nil
}

// ListLiveSources returns live sources for a source location.
func (b *InMemoryBackend) ListLiveSources(
	sourceLocationName string, maxResults int, nextToken string,
) ([]*LiveSourceSummary, string, error) {
	b.mu.RLock("ListLiveSources")
	defer b.mu.RUnlock()

	all := slices.Clone(b.liveSourcesByLocation.Get(sourceLocationName))

	sort.Slice(all, func(i, j int) bool { return all[i].LiveSourceName < all[j].LiveSourceName })

	pg := page.New(all, nextToken, maxResults, defaultMaxResults)

	out := make([]*LiveSourceSummary, 0, len(pg.Data))
	for _, ls := range pg.Data {
		out = append(out, &LiveSourceSummary{
			Tags:               copyTags(b.tags[ls.ARN]),
			SourceLocationName: ls.SourceLocationName,
			LiveSourceName:     ls.LiveSourceName,
			ARN:                ls.ARN,
		})
	}

	return out, pg.Next, nil
}

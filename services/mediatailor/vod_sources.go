package mediatailor

import (
	"fmt"
	"maps"
	"slices"
	"sort"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

type storedVodSource struct {
	Tags                      map[string]string          `json:"tags"`
	SourceLocationName        string                     `json:"sourceLocationName"`
	VodSourceName             string                     `json:"vodSourceName"`
	ARN                       string                     `json:"arn"`
	HTTPPackageConfigurations []HTTPPackageConfiguration `json:"httpPackageConfigurations"`
}

func (v *storedVodSource) toVodSource() *VodSource {
	tags := make(map[string]string, len(v.Tags))
	maps.Copy(tags, v.Tags)

	cfgs := make([]HTTPPackageConfiguration, len(v.HTTPPackageConfigurations))
	copy(cfgs, v.HTTPPackageConfigurations)

	return &VodSource{
		Tags:                      tags,
		SourceLocationName:        v.SourceLocationName,
		VodSourceName:             v.VodSourceName,
		ARN:                       v.ARN,
		HTTPPackageConfigurations: cfgs,
	}
}

func (v *storedVodSource) toSummary() *VodSourceSummary {
	tags := make(map[string]string, len(v.Tags))
	maps.Copy(tags, v.Tags)

	return &VodSourceSummary{
		Tags:               tags,
		SourceLocationName: v.SourceLocationName,
		VodSourceName:      v.VodSourceName,
		ARN:                v.ARN,
	}
}

// --- VodSource operations ---

func vodSourceKey(sourceLocationName, vodSourceName string) string {
	return sourceLocationName + "/" + vodSourceName
}

// CreateVodSource creates a new VOD source.
func (b *InMemoryBackend) CreateVodSource(
	sourceLocationName, vodSourceName string,
	httpPackageConfigurations []HTTPPackageConfiguration,
	tags map[string]string,
) (*VodSource, error) {
	if sourceLocationName == "" {
		return nil, fmt.Errorf("%w: SourceLocationName required", ErrInvalidParameter)
	}

	if vodSourceName == "" {
		return nil, fmt.Errorf("%w: VodSourceName required", ErrInvalidParameter)
	}

	b.mu.Lock("CreateVodSource")
	defer b.mu.Unlock()

	if !b.sourceLocations.Has(sourceLocationName) {
		return nil, fmt.Errorf("%w: source location %s not found", ErrNotFound, sourceLocationName)
	}

	key := vodSourceKey(sourceLocationName, vodSourceName)
	if b.vodSources.Has(key) {
		return nil, fmt.Errorf("%w: vod source %s already exists", ErrConflict, vodSourceName)
	}

	cfgs := make([]HTTPPackageConfiguration, len(httpPackageConfigurations))
	copy(cfgs, httpPackageConfigurations)

	vs := &storedVodSource{
		Tags:                      copyTags(tags),
		SourceLocationName:        sourceLocationName,
		VodSourceName:             vodSourceName,
		ARN:                       b.vodSourceARN(sourceLocationName, vodSourceName),
		HTTPPackageConfigurations: cfgs,
	}

	b.vodSources.Put(vs)

	return vs.toVodSource(), nil
}

// DescribeVodSource returns a VOD source by name.
func (b *InMemoryBackend) DescribeVodSource(sourceLocationName, vodSourceName string) (*VodSource, error) {
	b.mu.RLock("DescribeVodSource")
	defer b.mu.RUnlock()

	key := vodSourceKey(sourceLocationName, vodSourceName)

	vs, ok := b.vodSources.Get(key)
	if !ok {
		return nil, fmt.Errorf("%w: vod source %s not found", ErrNotFound, vodSourceName)
	}

	result := vs.toVodSource()
	result.Tags = make(map[string]string)
	maps.Copy(result.Tags, b.tags[vs.ARN])

	return result, nil
}

// UpdateVodSource updates a VOD source's package configurations.
func (b *InMemoryBackend) UpdateVodSource(
	sourceLocationName, vodSourceName string,
	httpPackageConfigurations []HTTPPackageConfiguration,
) (*VodSource, error) {
	b.mu.Lock("UpdateVodSource")
	defer b.mu.Unlock()

	key := vodSourceKey(sourceLocationName, vodSourceName)

	vs, ok := b.vodSources.Get(key)
	if !ok {
		return nil, fmt.Errorf("%w: vod source %s not found", ErrNotFound, vodSourceName)
	}

	cfgs := make([]HTTPPackageConfiguration, len(httpPackageConfigurations))
	copy(cfgs, httpPackageConfigurations)
	vs.HTTPPackageConfigurations = cfgs

	return vs.toVodSource(), nil
}

// DeleteVodSource deletes a VOD source.
func (b *InMemoryBackend) DeleteVodSource(sourceLocationName, vodSourceName string) error {
	b.mu.Lock("DeleteVodSource")
	defer b.mu.Unlock()

	key := vodSourceKey(sourceLocationName, vodSourceName)

	vs, ok := b.vodSources.Get(key)
	if !ok {
		return fmt.Errorf("%w: vod source %s not found", ErrNotFound, vodSourceName)
	}

	delete(b.tags, vs.ARN)
	b.vodSources.Delete(key)

	return nil
}

// ListVodSources returns a paginated list of VOD sources for a source location.
func (b *InMemoryBackend) ListVodSources(
	sourceLocationName string,
	maxResults int,
	nextToken string,
) ([]*VodSourceSummary, string, error) {
	b.mu.RLock("ListVodSources")
	defer b.mu.RUnlock()

	all := slices.Clone(b.vodSourcesByLocation.Get(sourceLocationName))

	sort.Slice(all, func(i, j int) bool { return all[i].VodSourceName < all[j].VodSourceName })

	pg := page.New(all, nextToken, maxResults, defaultMaxResults)

	summaries := make([]*VodSourceSummary, 0, len(pg.Data))
	for _, vs := range pg.Data {
		s := vs.toSummary()
		s.Tags = make(map[string]string)
		maps.Copy(s.Tags, b.tags[vs.ARN])
		summaries = append(summaries, s)
	}

	return summaries, pg.Next, nil
}

package mediatailor

import (
	"fmt"
	"maps"
	"slices"
	"sort"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

const (
	accessTypeS3SigV4                   = "S3_SIGV4"
	accessTypeSecretsManagerAccessToken = "SECRETS_MANAGER_ACCESS_TOKEN"
	accessTypeAutodetectSigV4           = "AUTODETECT_SIGV4"
)

type storedSourceLocation struct {
	CreationTime                        time.Time                            `json:"creationTime"`
	LastModified                        time.Time                            `json:"lastModified"`
	AccessConfiguration                 *AccessConfiguration                 `json:"accessConfiguration,omitempty"`
	DefaultSegmentDeliveryConfiguration *DefaultSegmentDeliveryConfiguration `json:"defaultSegmentDelivery,omitempty"`
	Tags                                map[string]string                    `json:"tags"`
	Name                                string                               `json:"name"`
	ARN                                 string                               `json:"arn"`
	HTTPConfigurationURL                string                               `json:"httpConfigurationUrl"`
	SegmentDeliveryConfigurations       []SegmentDeliveryConfiguration       `json:"segmentDeliveryConfigs,omitempty"`
}

func cloneAccessConfiguration(in *AccessConfiguration) *AccessConfiguration {
	if in == nil {
		return nil
	}

	out := *in

	if in.SecretsManagerAccessTokenConfiguration != nil {
		cfg := *in.SecretsManagerAccessTokenConfiguration
		out.SecretsManagerAccessTokenConfiguration = &cfg
	}

	return &out
}

func cloneDefaultSegmentDeliveryConfiguration(
	in *DefaultSegmentDeliveryConfiguration,
) *DefaultSegmentDeliveryConfiguration {
	if in == nil {
		return nil
	}

	out := *in

	return &out
}

func validateAccessConfiguration(cfg *AccessConfiguration) error {
	if cfg == nil {
		return nil
	}

	switch cfg.AccessType {
	case "", accessTypeS3SigV4, accessTypeSecretsManagerAccessToken, accessTypeAutodetectSigV4:
		return nil
	default:
		return fmt.Errorf(
			"%w: AccessConfiguration.AccessType must be %s, %s, or %s",
			ErrInvalidParameter, accessTypeS3SigV4, accessTypeSecretsManagerAccessToken, accessTypeAutodetectSigV4,
		)
	}
}

func (s *storedSourceLocation) toSourceLocation() *SourceLocation {
	tags := make(map[string]string, len(s.Tags))
	maps.Copy(tags, s.Tags)

	return &SourceLocation{
		CreationTime:        s.CreationTime,
		LastModified:        s.LastModified,
		AccessConfiguration: cloneAccessConfiguration(s.AccessConfiguration),
		DefaultSegmentDeliveryConfiguration: cloneDefaultSegmentDeliveryConfiguration(
			s.DefaultSegmentDeliveryConfiguration,
		),
		Tags:                          tags,
		SegmentDeliveryConfigurations: slices.Clone(s.SegmentDeliveryConfigurations),
		Name:                          s.Name,
		ARN:                           s.ARN,
		HTTPConfigurationURL:          s.HTTPConfigurationURL,
	}
}

func (s *storedSourceLocation) toSummary() *SourceLocationSummary {
	tags := make(map[string]string, len(s.Tags))
	maps.Copy(tags, s.Tags)

	return &SourceLocationSummary{
		CreationTime:        s.CreationTime,
		LastModified:        s.LastModified,
		AccessConfiguration: cloneAccessConfiguration(s.AccessConfiguration),
		DefaultSegmentDeliveryConfiguration: cloneDefaultSegmentDeliveryConfiguration(
			s.DefaultSegmentDeliveryConfiguration,
		),
		Tags:                          tags,
		SegmentDeliveryConfigurations: slices.Clone(s.SegmentDeliveryConfigurations),
		Name:                          s.Name,
		ARN:                           s.ARN,
		HTTPConfigurationURL:          s.HTTPConfigurationURL,
	}
}

// --- SourceLocation operations ---

// CreateSourceLocation creates a new source location.
func (b *InMemoryBackend) CreateSourceLocation(
	name, baseURL string,
	accessConfig *AccessConfiguration,
	defaultSegmentDelivery *DefaultSegmentDeliveryConfiguration,
	segmentDeliveryConfigs []SegmentDeliveryConfiguration,
	tags map[string]string,
) (*SourceLocation, error) {
	if name == "" {
		return nil, fmt.Errorf("%w: SourceLocationName required", ErrInvalidParameter)
	}

	if baseURL == "" {
		return nil, fmt.Errorf("%w: HttpConfiguration.BaseUrl required", ErrInvalidParameter)
	}

	if err := validateAccessConfiguration(accessConfig); err != nil {
		return nil, err
	}

	b.mu.Lock("CreateSourceLocation")
	defer b.mu.Unlock()

	if b.sourceLocations.Has(name) {
		return nil, fmt.Errorf("%w: source location %s already exists", ErrConflict, name)
	}

	now := time.Now().UTC()
	slARN := b.sourceLocationARN(name)
	sl := &storedSourceLocation{
		CreationTime:                        now,
		LastModified:                        now,
		AccessConfiguration:                 cloneAccessConfiguration(accessConfig),
		DefaultSegmentDeliveryConfiguration: cloneDefaultSegmentDeliveryConfiguration(defaultSegmentDelivery),
		Tags:                                copyTags(tags),
		SegmentDeliveryConfigurations:       slices.Clone(segmentDeliveryConfigs),
		Name:                                name,
		ARN:                                 slARN,
		HTTPConfigurationURL:                baseURL,
	}

	b.sourceLocations.Put(sl)
	b.tags[slARN] = copyTags(tags)

	return sl.toSourceLocation(), nil
}

// DescribeSourceLocation returns a source location by name.
func (b *InMemoryBackend) DescribeSourceLocation(name string) (*SourceLocation, error) {
	b.mu.RLock("DescribeSourceLocation")
	defer b.mu.RUnlock()

	sl, ok := b.sourceLocations.Get(name)
	if !ok {
		return nil, fmt.Errorf("%w: source location %s not found", ErrNotFound, name)
	}

	result := sl.toSourceLocation()
	result.Tags = make(map[string]string)
	maps.Copy(result.Tags, b.tags[sl.ARN])

	return result, nil
}

// UpdateSourceLocation updates a source location's base URL and optional
// access/segment-delivery configuration.
func (b *InMemoryBackend) UpdateSourceLocation(
	name, baseURL string,
	accessConfig *AccessConfiguration,
	defaultSegmentDelivery *DefaultSegmentDeliveryConfiguration,
	segmentDeliveryConfigs []SegmentDeliveryConfiguration,
) (*SourceLocation, error) {
	if err := validateAccessConfiguration(accessConfig); err != nil {
		return nil, err
	}

	b.mu.Lock("UpdateSourceLocation")
	defer b.mu.Unlock()

	sl, ok := b.sourceLocations.Get(name)
	if !ok {
		return nil, fmt.Errorf("%w: source location %s not found", ErrNotFound, name)
	}

	if baseURL != "" {
		sl.HTTPConfigurationURL = baseURL
	}

	sl.AccessConfiguration = cloneAccessConfiguration(accessConfig)
	sl.DefaultSegmentDeliveryConfiguration = cloneDefaultSegmentDeliveryConfiguration(defaultSegmentDelivery)
	sl.SegmentDeliveryConfigurations = slices.Clone(segmentDeliveryConfigs)
	sl.LastModified = time.Now().UTC()

	return sl.toSourceLocation(), nil
}

// DeleteSourceLocation deletes a source location.
// Returns ConflictException if any vod or live sources are still attached.
func (b *InMemoryBackend) DeleteSourceLocation(name string) error {
	b.mu.Lock("DeleteSourceLocation")
	defer b.mu.Unlock()

	sl, ok := b.sourceLocations.Get(name)
	if !ok {
		return fmt.Errorf("%w: source location %s not found", ErrNotFound, name)
	}

	if attached := b.vodSourcesByLocation.Get(name); len(attached) > 0 {
		return fmt.Errorf(
			"%w: source location %s has attached vod source %s",
			ErrConflict, name, attached[0].VodSourceName,
		)
	}

	if attached := b.liveSourcesByLocation.Get(name); len(attached) > 0 {
		return fmt.Errorf(
			"%w: source location %s has attached live source %s",
			ErrConflict, name, attached[0].LiveSourceName,
		)
	}

	delete(b.tags, sl.ARN)
	b.sourceLocations.Delete(name)

	return nil
}

// ListSourceLocations returns a paginated list of source locations.
func (b *InMemoryBackend) ListSourceLocations(
	maxResults int,
	nextToken string,
) ([]*SourceLocationSummary, string, error) {
	b.mu.RLock("ListSourceLocations")
	defer b.mu.RUnlock()

	all := b.sourceLocations.All()

	sort.Slice(all, func(i, j int) bool { return all[i].Name < all[j].Name })

	pg := page.New(all, nextToken, maxResults, defaultMaxResults)

	summaries := make([]*SourceLocationSummary, 0, len(pg.Data))
	for _, sl := range pg.Data {
		s := sl.toSummary()
		s.Tags = make(map[string]string)
		maps.Copy(s.Tags, b.tags[sl.ARN])
		summaries = append(summaries, s)
	}

	return summaries, pg.Next, nil
}

package mediatailor

import (
	"fmt"
	"maps"
	"slices"
	"sort"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/page"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

const (
	defaultMaxResults = 20

	channelStateRunning = "RUNNING"
	channelStateStopped = "STOPPED"

	playbackModeLinear = "LINEAR"
	playbackModeLoop   = "LOOP"

	resourceTypePlaybackConfiguration = "playbackConfiguration"
	resourceTypeChannel               = "channel"
	resourceTypeSourceLocation        = "sourceLocation"
	resourceTypeVodSource             = "vodSource"
)

// ErrNotFound is returned when a resource does not exist.
var ErrNotFound = awserr.New("NotFoundException", awserr.ErrNotFound)

// ErrConflict is returned for state conflict operations.
var ErrConflict = awserr.New("ConflictException", awserr.ErrAlreadyExists)

// ErrInvalidParameter is returned for invalid input.
var ErrInvalidParameter = awserr.New("BadRequestException", awserr.ErrInvalidParameter)

type storedPlaybackConfiguration struct {
	Tags                        map[string]string `json:"tags"`
	Name                        string            `json:"name"`
	AdDecisionServerURL         string            `json:"adDecisionServerUrl"`
	VideoContentSourceURL       string            `json:"videoContentSourceUrl"`
	PlaybackConfigurationARN    string            `json:"playbackConfigurationArn"`
	PlaybackEndpointPrefix      string            `json:"playbackEndpointPrefix"`
	SessionInitializationPrefix string            `json:"sessionInitializationPrefix"`
	HlsManifestEndpointPrefix   string            `json:"hlsManifestEndpointPrefix"`
}

func (s *storedPlaybackConfiguration) toPlaybackConfiguration() *PlaybackConfiguration {
	tags := make(map[string]string, len(s.Tags))
	maps.Copy(tags, s.Tags)

	return &PlaybackConfiguration{
		Tags:                        tags,
		Name:                        s.Name,
		AdDecisionServerURL:         s.AdDecisionServerURL,
		VideoContentSourceURL:       s.VideoContentSourceURL,
		PlaybackConfigurationARN:    s.PlaybackConfigurationARN,
		PlaybackEndpointPrefix:      s.PlaybackEndpointPrefix,
		SessionInitializationPrefix: s.SessionInitializationPrefix,
		HlsManifestEndpointPrefix:   s.HlsManifestEndpointPrefix,
	}
}

func (s *storedPlaybackConfiguration) toSummary() *PlaybackConfigurationSummary {
	tags := make(map[string]string, len(s.Tags))
	maps.Copy(tags, s.Tags)

	return &PlaybackConfigurationSummary{
		Tags:                     tags,
		Name:                     s.Name,
		AdDecisionServerURL:      s.AdDecisionServerURL,
		VideoContentSourceURL:    s.VideoContentSourceURL,
		PlaybackConfigurationARN: s.PlaybackConfigurationARN,
	}
}

type storedChannel struct {
	FillerSlate  *SlateSource      `json:"fillerSlate,omitempty"`
	CreationTime time.Time         `json:"creationTime"`
	LastModified time.Time         `json:"lastModified"`
	Tags         map[string]string `json:"tags"`
	Name         string            `json:"name"`
	ARN          string            `json:"arn"`
	PlaybackMode string            `json:"playbackMode"`
	ChannelState string            `json:"channelState"`
	Tier         string            `json:"tier"`
	Outputs      []OutputItem      `json:"outputs"`
}

func (c *storedChannel) toChannel() *Channel {
	tags := make(map[string]string, len(c.Tags))
	maps.Copy(tags, c.Tags)

	outputs := make([]OutputItem, len(c.Outputs))
	copy(outputs, c.Outputs)

	ch := &Channel{
		CreationTime: c.CreationTime,
		LastModified: c.LastModified,
		Tags:         tags,
		Name:         c.Name,
		ARN:          c.ARN,
		PlaybackMode: c.PlaybackMode,
		ChannelState: c.ChannelState,
		Tier:         c.Tier,
		Outputs:      outputs,
	}

	if c.FillerSlate != nil {
		ch.FillerSlate = &SlateSource{
			SourceLocationName: c.FillerSlate.SourceLocationName,
			VodSourceName:      c.FillerSlate.VodSourceName,
		}
	}

	return ch
}

func (c *storedChannel) toSummary() *ChannelSummary {
	tags := make(map[string]string, len(c.Tags))
	maps.Copy(tags, c.Tags)

	s := &ChannelSummary{
		CreationTime: c.CreationTime,
		LastModified: c.LastModified,
		Tags:         tags,
		Name:         c.Name,
		ARN:          c.ARN,
		PlaybackMode: c.PlaybackMode,
		ChannelState: c.ChannelState,
		Tier:         c.Tier,
	}

	if c.FillerSlate != nil {
		s.FillerSlate = &SlateSource{
			SourceLocationName: c.FillerSlate.SourceLocationName,
			VodSourceName:      c.FillerSlate.VodSourceName,
		}
	}

	return s
}

type storedSourceLocation struct {
	Tags                 map[string]string `json:"tags"`
	Name                 string            `json:"name"`
	ARN                  string            `json:"arn"`
	HTTPConfigurationURL string            `json:"httpConfigurationUrl"`
}

func (s *storedSourceLocation) toSourceLocation() *SourceLocation {
	tags := make(map[string]string, len(s.Tags))
	maps.Copy(tags, s.Tags)

	return &SourceLocation{
		Tags:                 tags,
		Name:                 s.Name,
		ARN:                  s.ARN,
		HTTPConfigurationURL: s.HTTPConfigurationURL,
	}
}

func (s *storedSourceLocation) toSummary() *SourceLocationSummary {
	tags := make(map[string]string, len(s.Tags))
	maps.Copy(tags, s.Tags)

	return &SourceLocationSummary{
		Tags:                 tags,
		Name:                 s.Name,
		ARN:                  s.ARN,
		HTTPConfigurationURL: s.HTTPConfigurationURL,
	}
}

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

// InMemoryBackend is an in-memory implementation of StorageBackend.
type InMemoryBackend struct {
	mu       *lockmetrics.RWMutex
	registry *store.Registry

	playbackConfigurations *store.Table[storedPlaybackConfiguration]
	channels               *store.Table[storedChannel]
	channelPolicies        map[string]string
	sourceLocations        *store.Table[storedSourceLocation]

	vodSources           *store.Table[storedVodSource]
	vodSourcesByLocation *store.Index[storedVodSource]

	liveSources           *store.Table[LiveSource]
	liveSourcesByLocation *store.Index[LiveSource]

	prefetchSchedules         *store.Table[PrefetchSchedule]
	prefetchSchedulesByConfig *store.Index[PrefetchSchedule]

	programs          *store.Table[Program]
	programsByChannel *store.Index[Program]

	functions *store.Table[Function]

	tags map[string]map[string]string

	accountID string
	region    string
}

// NewInMemoryBackend creates a new InMemoryBackend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	b := &InMemoryBackend{
		mu:              lockmetrics.New("mediatailor"),
		registry:        store.NewRegistry(),
		channelPolicies: make(map[string]string),
		tags:            make(map[string]map[string]string),
		accountID:       accountID,
		region:          region,
	}
	registerAllTables(b)

	return b
}

// AccountID returns the configured account ID.
func (b *InMemoryBackend) AccountID() string { return b.accountID }

// Region returns the configured region.
func (b *InMemoryBackend) Region() string { return b.region }

// Reset clears all stored data.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	b.registry.ResetAll()
	b.channelPolicies = make(map[string]string)
	b.tags = make(map[string]map[string]string)
}

func (b *InMemoryBackend) playbackConfigARN(name string) string {
	return arn.Build("mediatailor", b.region, b.accountID, resourceTypePlaybackConfiguration+"/"+name)
}

func (b *InMemoryBackend) channelARN(name string) string {
	return arn.Build("mediatailor", b.region, b.accountID, resourceTypeChannel+"/"+name)
}

func (b *InMemoryBackend) sourceLocationARN(name string) string {
	return arn.Build("mediatailor", b.region, b.accountID, resourceTypeSourceLocation+"/"+name)
}

func (b *InMemoryBackend) vodSourceARN(sourceLocationName, vodSourceName string) string {
	return arn.Build(
		"mediatailor",
		b.region,
		b.accountID,
		resourceTypeSourceLocation+"/"+sourceLocationName+"/"+resourceTypeVodSource+"/"+vodSourceName,
	)
}

// --- PlaybackConfiguration operations ---

// PutPlaybackConfiguration creates or updates a playback configuration.
func (b *InMemoryBackend) PutPlaybackConfiguration(
	name, adDecisionServerURL, videoContentSourceURL string,
	tags map[string]string,
) (*PlaybackConfiguration, error) {
	if name == "" {
		return nil, fmt.Errorf("%w: Name required", ErrInvalidParameter)
	}

	if adDecisionServerURL == "" {
		return nil, fmt.Errorf("%w: AdDecisionServerUrl required", ErrInvalidParameter)
	}

	if videoContentSourceURL == "" {
		return nil, fmt.Errorf("%w: VideoContentSourceUrl required", ErrInvalidParameter)
	}

	cfgARN := b.playbackConfigARN(name)
	cfg := &storedPlaybackConfiguration{
		Tags:                     copyTags(tags),
		Name:                     name,
		AdDecisionServerURL:      adDecisionServerURL,
		VideoContentSourceURL:    videoContentSourceURL,
		PlaybackConfigurationARN: cfgARN,
		PlaybackEndpointPrefix: fmt.Sprintf(
			"https://%s.mediatailor.%s.amazonaws.com/v1/master/%s/%s",
			b.accountID,
			b.region,
			b.accountID,
			name,
		),
		SessionInitializationPrefix: fmt.Sprintf(
			"https://%s.mediatailor.%s.amazonaws.com/v1/session/%s/%s",
			b.accountID,
			b.region,
			b.accountID,
			name,
		),
		HlsManifestEndpointPrefix: fmt.Sprintf(
			"https://%s.mediatailor.%s.amazonaws.com/v1/master/%s/%s/",
			b.accountID,
			b.region,
			b.accountID,
			name,
		),
	}

	b.mu.Lock("PutPlaybackConfiguration")
	defer b.mu.Unlock()

	b.playbackConfigurations.Put(cfg)
	b.tags[cfgARN] = copyTags(tags)

	return cfg.toPlaybackConfiguration(), nil
}

// GetPlaybackConfiguration returns a playback configuration by name.
func (b *InMemoryBackend) GetPlaybackConfiguration(name string) (*PlaybackConfiguration, error) {
	b.mu.RLock("GetPlaybackConfiguration")
	defer b.mu.RUnlock()

	cfg, ok := b.playbackConfigurations.Get(name)
	if !ok {
		return nil, fmt.Errorf("%w: playback configuration %s not found", ErrNotFound, name)
	}

	result := cfg.toPlaybackConfiguration()
	result.Tags = make(map[string]string)
	maps.Copy(result.Tags, b.tags[cfg.PlaybackConfigurationARN])

	return result, nil
}

// DeletePlaybackConfiguration deletes a playback configuration.
// Idempotent: returns nil if the configuration does not exist.
func (b *InMemoryBackend) DeletePlaybackConfiguration(name string) error {
	b.mu.Lock("DeletePlaybackConfiguration")
	defer b.mu.Unlock()

	cfg, ok := b.playbackConfigurations.Get(name)
	if !ok {
		return nil
	}

	delete(b.tags, cfg.PlaybackConfigurationARN)
	b.playbackConfigurations.Delete(name)

	return nil
}

// ListPlaybackConfigurations returns a paginated list of playback configurations.
func (b *InMemoryBackend) ListPlaybackConfigurations(
	maxResults int,
	nextToken string,
) ([]*PlaybackConfigurationSummary, string, error) {
	b.mu.RLock("ListPlaybackConfigurations")
	defer b.mu.RUnlock()

	all := b.playbackConfigurations.All()

	sort.Slice(all, func(i, j int) bool { return all[i].Name < all[j].Name })

	pg := page.New(all, nextToken, maxResults, defaultMaxResults)

	summaries := make([]*PlaybackConfigurationSummary, 0, len(pg.Data))
	for _, cfg := range pg.Data {
		s := cfg.toSummary()
		s.Tags = make(map[string]string)
		maps.Copy(s.Tags, b.tags[cfg.PlaybackConfigurationARN])
		summaries = append(summaries, s)
	}

	return summaries, pg.Next, nil
}

// --- Channel operations ---

// CreateChannel creates a new channel.
func (b *InMemoryBackend) CreateChannel(
	name, playbackMode string,
	outputs []OutputItem,
	fillerSlate *SlateSource,
	tags map[string]string,
) (*Channel, error) {
	if name == "" {
		return nil, fmt.Errorf("%w: ChannelName required", ErrInvalidParameter)
	}

	switch playbackMode {
	case "", playbackModeLoop:
		playbackMode = playbackModeLoop
	case playbackModeLinear:
	default:
		return nil, fmt.Errorf(
			"%w: PlaybackMode must be %s or %s",
			ErrInvalidParameter, playbackModeLinear, playbackModeLoop,
		)
	}

	b.mu.Lock("CreateChannel")
	defer b.mu.Unlock()

	if b.channels.Has(name) {
		return nil, fmt.Errorf("%w: channel %s already exists", ErrConflict, name)
	}

	out := make([]OutputItem, len(outputs))
	copy(out, outputs)

	now := time.Now().UTC()
	ch := &storedChannel{
		Tags:         copyTags(tags),
		Name:         name,
		ARN:          b.channelARN(name),
		PlaybackMode: playbackMode,
		ChannelState: channelStateStopped,
		Tier:         "BASIC",
		CreationTime: now,
		LastModified: now,
		Outputs:      out,
	}

	if fillerSlate != nil {
		ch.FillerSlate = &SlateSource{
			SourceLocationName: fillerSlate.SourceLocationName,
			VodSourceName:      fillerSlate.VodSourceName,
		}
	}

	b.channels.Put(ch)

	return ch.toChannel(), nil
}

// DescribeChannel returns a channel by name.
func (b *InMemoryBackend) DescribeChannel(name string) (*Channel, error) {
	b.mu.RLock("DescribeChannel")
	defer b.mu.RUnlock()

	ch, ok := b.channels.Get(name)
	if !ok {
		return nil, fmt.Errorf("%w: channel %s not found", ErrNotFound, name)
	}

	result := ch.toChannel()
	result.Tags = make(map[string]string)
	maps.Copy(result.Tags, b.tags[ch.ARN])

	return result, nil
}

// UpdateChannel updates a channel's outputs.
func (b *InMemoryBackend) UpdateChannel(name string, outputs []OutputItem) (*Channel, error) {
	b.mu.Lock("UpdateChannel")
	defer b.mu.Unlock()

	ch, ok := b.channels.Get(name)
	if !ok {
		return nil, fmt.Errorf("%w: channel %s not found", ErrNotFound, name)
	}

	out := make([]OutputItem, len(outputs))
	copy(out, outputs)
	ch.Outputs = out

	return ch.toChannel(), nil
}

// DeleteChannel deletes a channel.
func (b *InMemoryBackend) DeleteChannel(name string) error {
	b.mu.Lock("DeleteChannel")
	defer b.mu.Unlock()

	ch, ok := b.channels.Get(name)
	if !ok {
		return fmt.Errorf("%w: channel %s not found", ErrNotFound, name)
	}

	if ch.ChannelState == channelStateRunning {
		return fmt.Errorf("%w: channel must be stopped before deleting", ErrConflict)
	}

	delete(b.tags, ch.ARN)
	b.channels.Delete(name)

	return nil
}

// ListChannels returns a paginated list of channels.
func (b *InMemoryBackend) ListChannels(maxResults int, nextToken string) ([]*ChannelSummary, string, error) {
	b.mu.RLock("ListChannels")
	defer b.mu.RUnlock()

	all := b.channels.All()

	sort.Slice(all, func(i, j int) bool { return all[i].Name < all[j].Name })

	pg := page.New(all, nextToken, maxResults, defaultMaxResults)

	summaries := make([]*ChannelSummary, 0, len(pg.Data))
	for _, ch := range pg.Data {
		s := ch.toSummary()
		s.Tags = make(map[string]string)
		maps.Copy(s.Tags, b.tags[ch.ARN])
		summaries = append(summaries, s)
	}

	return summaries, pg.Next, nil
}

// StartChannel transitions a channel to RUNNING.
// Idempotent: no error if already running.
func (b *InMemoryBackend) StartChannel(name string) error {
	b.mu.Lock("StartChannel")
	defer b.mu.Unlock()

	ch, ok := b.channels.Get(name)
	if !ok {
		return fmt.Errorf("%w: channel %s not found", ErrNotFound, name)
	}

	ch.ChannelState = channelStateRunning

	return nil
}

// StopChannel transitions a channel to STOPPED.
// Idempotent: no error if already stopped.
func (b *InMemoryBackend) StopChannel(name string) error {
	b.mu.Lock("StopChannel")
	defer b.mu.Unlock()

	ch, ok := b.channels.Get(name)
	if !ok {
		return fmt.Errorf("%w: channel %s not found", ErrNotFound, name)
	}

	ch.ChannelState = channelStateStopped

	return nil
}

// ConfigureLogsForChannel sets log types on a channel.
func (b *InMemoryBackend) ConfigureLogsForChannel(channelName string, logTypes []string) (string, []string, error) {
	b.mu.Lock("ConfigureLogsForChannel")
	defer b.mu.Unlock()

	if !b.channels.Has(channelName) {
		return "", nil, fmt.Errorf("%w: channel %s not found", ErrNotFound, channelName)
	}

	result := make([]string, len(logTypes))
	copy(result, logTypes)

	return channelName, result, nil
}

// ConfigureLogsForPlaybackConfiguration sets the percent enabled for playback config logs.
func (b *InMemoryBackend) ConfigureLogsForPlaybackConfiguration(
	playbackConfigName string,
	percentEnabled int,
) (string, int, error) {
	b.mu.Lock("ConfigureLogsForPlaybackConfiguration")
	defer b.mu.Unlock()

	if !b.playbackConfigurations.Has(playbackConfigName) {
		return "", 0, fmt.Errorf("%w: playback configuration %s not found", ErrNotFound, playbackConfigName)
	}

	return playbackConfigName, percentEnabled, nil
}

// --- SourceLocation operations ---

// CreateSourceLocation creates a new source location.
func (b *InMemoryBackend) CreateSourceLocation(
	name, baseURL string,
	tags map[string]string,
) (*SourceLocation, error) {
	if name == "" {
		return nil, fmt.Errorf("%w: SourceLocationName required", ErrInvalidParameter)
	}

	if baseURL == "" {
		return nil, fmt.Errorf("%w: HttpConfiguration.BaseUrl required", ErrInvalidParameter)
	}

	b.mu.Lock("CreateSourceLocation")
	defer b.mu.Unlock()

	if b.sourceLocations.Has(name) {
		return nil, fmt.Errorf("%w: source location %s already exists", ErrConflict, name)
	}

	sl := &storedSourceLocation{
		Tags:                 copyTags(tags),
		Name:                 name,
		ARN:                  b.sourceLocationARN(name),
		HTTPConfigurationURL: baseURL,
	}

	b.sourceLocations.Put(sl)

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

// UpdateSourceLocation updates a source location's base URL.
func (b *InMemoryBackend) UpdateSourceLocation(name, baseURL string) (*SourceLocation, error) {
	b.mu.Lock("UpdateSourceLocation")
	defer b.mu.Unlock()

	sl, ok := b.sourceLocations.Get(name)
	if !ok {
		return nil, fmt.Errorf("%w: source location %s not found", ErrNotFound, name)
	}

	if baseURL != "" {
		sl.HTTPConfigurationURL = baseURL
	}

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

// --- Tag operations ---

// ListTagsForResource returns all tags for a resource.
func (b *InMemoryBackend) ListTagsForResource(resourceARN string) (map[string]string, error) {
	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()

	existing := b.tags[resourceARN]
	result := make(map[string]string, len(existing))
	maps.Copy(result, existing)

	return result, nil
}

// TagResource adds tags to a resource.
func (b *InMemoryBackend) TagResource(resourceARN string, tags map[string]string) error {
	b.mu.Lock("TagResource")
	defer b.mu.Unlock()

	if b.tags[resourceARN] == nil {
		b.tags[resourceARN] = make(map[string]string)
	}

	maps.Copy(b.tags[resourceARN], tags)

	return nil
}

// UntagResource removes tag keys from a resource.
// Idempotent: returns nil if the resource has no tags.
func (b *InMemoryBackend) UntagResource(resourceARN string, tagKeys []string) error {
	b.mu.Lock("UntagResource")
	defer b.mu.Unlock()

	existing := b.tags[resourceARN]
	if existing == nil {
		return nil
	}

	for _, k := range tagKeys {
		delete(existing, k)
	}

	return nil
}

func copyTags(tags map[string]string) map[string]string {
	if len(tags) == 0 {
		return make(map[string]string)
	}

	result := make(map[string]string, len(tags))
	maps.Copy(result, tags)

	return result
}

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
	ls := &LiveSource{
		Tags:                      copyTags(tags),
		ARN:                       lsARN,
		SourceLocationName:        sourceLocationName,
		LiveSourceName:            liveSourceName,
		HTTPPackageConfigurations: cfgs,
	}
	b.liveSources.Put(ls)

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

	return ls, nil
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

	return ls, nil
}

// DeleteLiveSource deletes a live source.
func (b *InMemoryBackend) DeleteLiveSource(sourceLocationName, liveSourceName string) error {
	b.mu.Lock("DeleteLiveSource")
	defer b.mu.Unlock()

	key := liveSourceKey(sourceLocationName, liveSourceName)
	if !b.liveSources.Has(key) {
		return fmt.Errorf("%w: live source %s not found", ErrNotFound, liveSourceName)
	}

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
			Tags:               copyTags(ls.Tags),
			SourceLocationName: ls.SourceLocationName,
			LiveSourceName:     ls.LiveSourceName,
			ARN:                ls.ARN,
		})
	}

	return out, pg.Next, nil
}

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

// GetChannelSchedule returns the schedule for a channel.
func (b *InMemoryBackend) GetChannelSchedule(
	channelName string, _ int, _ string,
) ([]*ProgramScheduleEntry, string, error) {
	b.mu.RLock("GetChannelSchedule")
	defer b.mu.RUnlock()

	if !b.channels.Has(channelName) {
		return nil, "", fmt.Errorf("%w: channel %s not found", ErrNotFound, channelName)
	}

	var out []*ProgramScheduleEntry
	for _, prog := range b.programsByChannel.Get(channelName) {
		out = append(out, &ProgramScheduleEntry{
			ARN:         prog.ARN,
			ChannelName: prog.ChannelName,
			ProgramName: prog.ProgramName,
		})
	}

	return out, "", nil
}

// --- ChannelPolicy operations ---

// PutChannelPolicy sets a policy on a channel.
func (b *InMemoryBackend) PutChannelPolicy(channelName, policy string) error {
	b.mu.Lock("PutChannelPolicy")
	defer b.mu.Unlock()

	if !b.channels.Has(channelName) {
		return fmt.Errorf("%w: channel %s not found", ErrNotFound, channelName)
	}

	b.channelPolicies[channelName] = policy

	return nil
}

// GetChannelPolicy returns a channel policy.
func (b *InMemoryBackend) GetChannelPolicy(channelName string) (string, error) {
	b.mu.RLock("GetChannelPolicy")
	defer b.mu.RUnlock()

	if !b.channels.Has(channelName) {
		return "", fmt.Errorf("%w: channel %s not found", ErrNotFound, channelName)
	}

	policy, ok := b.channelPolicies[channelName]
	if !ok {
		return "", fmt.Errorf("%w: no policy for channel %s", ErrNotFound, channelName)
	}

	return policy, nil
}

// DeleteChannelPolicy deletes a channel policy.
func (b *InMemoryBackend) DeleteChannelPolicy(channelName string) error {
	b.mu.Lock("DeleteChannelPolicy")
	defer b.mu.Unlock()

	if !b.channels.Has(channelName) {
		return fmt.Errorf("%w: channel %s not found", ErrNotFound, channelName)
	}

	delete(b.channelPolicies, channelName)

	return nil
}

// --- Function operations ---

// PutFunction creates or updates a function.
func (b *InMemoryBackend) PutFunction(
	functionID, functionType, description string,
	tags map[string]string,
) (*Function, error) {
	if functionType == "" {
		return nil, fmt.Errorf("%w: FunctionType is required", ErrInvalidParameter)
	}

	b.mu.Lock("PutFunction")
	defer b.mu.Unlock()

	arnStr := arn.Build("mediatailor", b.region, b.accountID, fmt.Sprintf("function/%s", functionID))
	fn := &Function{
		Tags:         copyTags(tags),
		FunctionID:   functionID,
		FunctionType: functionType,
		ARN:          arnStr,
		Description:  description,
	}
	b.functions.Put(fn)

	return fn, nil
}

// GetFunction returns a function by ID.
func (b *InMemoryBackend) GetFunction(functionID string) (*Function, error) {
	b.mu.RLock("GetFunction")
	defer b.mu.RUnlock()

	fn, ok := b.functions.Get(functionID)
	if !ok {
		return nil, fmt.Errorf("%w: function %s not found", ErrNotFound, functionID)
	}

	return fn, nil
}

// DeleteFunction deletes a function.
func (b *InMemoryBackend) DeleteFunction(functionID string) error {
	b.mu.Lock("DeleteFunction")
	defer b.mu.Unlock()

	if !b.functions.Has(functionID) {
		return fmt.Errorf("%w: function %s not found", ErrNotFound, functionID)
	}

	b.functions.Delete(functionID)

	return nil
}

// ListFunctions returns all functions.
func (b *InMemoryBackend) ListFunctions(_ int, _ string) ([]*FunctionSummary, string, error) {
	b.mu.RLock("ListFunctions")
	defer b.mu.RUnlock()

	all := b.functions.All()

	out := make([]*FunctionSummary, 0, len(all))
	for _, fn := range all {
		out = append(out, &FunctionSummary{
			FunctionID:   fn.FunctionID,
			FunctionType: fn.FunctionType,
			ARN:          fn.ARN,
			Tags:         copyTags(fn.Tags),
		})
	}

	return out, "", nil
}

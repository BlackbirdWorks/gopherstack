package mediatailor

import (
	"fmt"
	"maps"
	"sort"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

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

// --- PlaybackConfiguration operations ---

// PutPlaybackConfiguration creates or updates a playback configuration.
func (b *InMemoryBackend) PutPlaybackConfiguration(
	name, adDecisionServerURL, videoContentSourceURL string,
	tags map[string]string,
) (*PlaybackConfiguration, error) {
	// Name is the only required member of PutPlaybackConfigurationInput —
	// confirmed against aws-sdk-go-v2's validators.go and botocore's
	// service-2.json ("required": ["Name"]). AdDecisionServerUrl and
	// VideoContentSourceUrl are both optional on the wire; rejecting a
	// request that omits them is a false BadRequestException a real SDK
	// client would never trigger.
	if name == "" {
		return nil, fmt.Errorf("%w: Name required", ErrInvalidParameter)
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

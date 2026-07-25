package cloudfront

import (
	"fmt"
	"maps"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

func (b *InMemoryBackend) streamingDistributionARN(id string) string {
	return arn.Build(
		"cloudfront",
		"",
		b.accountID,
		fmt.Sprintf("streaming-distribution/%s", id),
	)
}

// copyStreamingDistribution returns a deep copy of a StreamingDistribution.
// Must be called with the lock held.
func (b *InMemoryBackend) copyStreamingDistribution(sd *StreamingDistribution) *StreamingDistribution {
	cp := *sd
	cp.Config.Aliases = append([]string(nil), sd.Config.Aliases...)
	cp.Config.TrustedSigners.Items = append([]string(nil), sd.Config.TrustedSigners.Items...)
	cp.RawConfig = append([]byte(nil), sd.RawConfig...)

	if sd.Tags != nil {
		cp.Tags = make(map[string]string, len(sd.Tags))
		maps.Copy(cp.Tags, sd.Tags)
	}

	return &cp
}

// CreateStreamingDistribution creates a new RTMP streaming distribution.
//
// Reusing a non-empty CallerReference always fails with
// StreamingDistributionAlreadyExists -- like CreateDistribution, the real
// CreateStreamingDistribution API docs state this "regardless of the content
// of the StreamingDistributionConfig object" (not content-comparison
// idempotent like OAI/PublicKey/KeyGroup/FLE-profile).
func (b *InMemoryBackend) CreateStreamingDistribution(
	cfg StreamingDistributionConfig,
	rawConfig []byte,
) (*StreamingDistribution, error) {
	b.mu.Lock("CreateStreamingDistribution")
	defer b.mu.Unlock()

	if cfg.CallerReference != "" {
		if _, ok := b.streamingDistributionCallerRefs[cfg.CallerReference]; ok {
			return nil, fmt.Errorf(
				"%w: CallerReference %q is associated with another streaming distribution",
				ErrStreamingDistributionAlreadyExists, cfg.CallerReference,
			)
		}
	}

	id := generateID()
	sd := &StreamingDistribution{
		ID:               id,
		ARN:              b.streamingDistributionARN(id),
		DomainName:       strings.ToLower(id) + ".cloudfront.net",
		Status:           statusDeployed,
		ETag:             uuid.NewString(),
		LastModifiedTime: time.Now().UTC().Format(time.RFC3339),
		Config:           cfg,
		RawConfig:        rawConfig,
		Tags:             make(map[string]string),
	}
	b.streamingDistributions.Put(sd)
	b.streamingDistributionARNs[sd.ARN] = id

	if cfg.CallerReference != "" {
		b.streamingDistributionCallerRefs[cfg.CallerReference] = id
	}

	return b.copyStreamingDistribution(sd), nil
}

// GetStreamingDistribution returns a streaming distribution by ID.
func (b *InMemoryBackend) GetStreamingDistribution(id string) (*StreamingDistribution, error) {
	b.mu.RLock("GetStreamingDistribution")
	defer b.mu.RUnlock()

	sd, ok := b.streamingDistributions.Get(id)
	if !ok {
		return nil, fmt.Errorf("%w: streaming distribution %s not found", ErrStreamingDistributionNotFound, id)
	}

	return b.copyStreamingDistribution(sd), nil
}

// ListStreamingDistributions returns all streaming distributions sorted by ID.
func (b *InMemoryBackend) ListStreamingDistributions() []*StreamingDistribution {
	b.mu.RLock("ListStreamingDistributions")
	defer b.mu.RUnlock()

	out := make([]*StreamingDistribution, 0, b.streamingDistributions.Len())
	for _, sd := range b.streamingDistributions.All() {
		out = append(out, b.copyStreamingDistribution(sd))
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })

	return out
}

// UpdateStreamingDistribution updates an existing streaming distribution's config.
// CallerReference is immutable and is preserved from the existing config.
func (b *InMemoryBackend) UpdateStreamingDistribution(
	id string,
	cfg StreamingDistributionConfig,
	rawConfig []byte,
) (*StreamingDistribution, error) {
	b.mu.Lock("UpdateStreamingDistribution")
	defer b.mu.Unlock()

	sd, ok := b.streamingDistributions.Get(id)
	if !ok {
		return nil, fmt.Errorf("%w: streaming distribution %s not found", ErrStreamingDistributionNotFound, id)
	}

	cfg.CallerReference = sd.Config.CallerReference
	sd.Config = cfg
	sd.RawConfig = rawConfig
	sd.ETag = uuid.NewString()
	sd.LastModifiedTime = time.Now().UTC().Format(time.RFC3339)

	return b.copyStreamingDistribution(sd), nil
}

// DeleteStreamingDistribution deletes a streaming distribution by ID.
// The streaming distribution must be disabled first, mirroring AWS's requirement.
func (b *InMemoryBackend) DeleteStreamingDistribution(id string) error {
	b.mu.Lock("DeleteStreamingDistribution")
	defer b.mu.Unlock()

	sd, ok := b.streamingDistributions.Get(id)
	if !ok {
		return fmt.Errorf("%w: streaming distribution %s not found", ErrStreamingDistributionNotFound, id)
	}

	if sd.Config.Enabled {
		return fmt.Errorf(
			"%w: streaming distribution %s must be disabled before it can be deleted",
			ErrStreamingDistributionNotDisabled, id,
		)
	}

	delete(b.streamingDistributionARNs, sd.ARN)
	delete(b.streamingDistributionCallerRefs, sd.Config.CallerReference)
	b.streamingDistributions.Delete(id)

	return nil
}

// ---------------------------------------------------------------------------
// MonitoringSubscription (per-distribution)
// ---------------------------------------------------------------------------

package mq

import "fmt"

// DescribeConfigurationRevision returns a specific revision of a configuration.
func (b *InMemoryBackend) DescribeConfigurationRevision(
	configID string,
	revision int32,
) (*ConfigurationRevision, string, error) {
	b.mu.RLock("DescribeConfigurationRevision")
	defer b.mu.RUnlock()

	cfg, ok := b.configurations.Get(configID)
	if !ok {
		return nil, "", fmt.Errorf("%w: configuration %s not found", ErrNotFound, configID)
	}

	for _, rev := range cfg.Revisions {
		if rev.Revision == revision {
			cp := rev
			data := cfg.Data[revision]

			return &cp, data, nil
		}
	}

	return nil, "", fmt.Errorf("%w: revision %d not found for configuration %s", ErrNotFound, revision, configID)
}

// ListConfigurationRevisions returns all revisions for a configuration.
func (b *InMemoryBackend) ListConfigurationRevisions(configID string) ([]ConfigurationRevision, error) {
	b.mu.RLock("ListConfigurationRevisions")
	defer b.mu.RUnlock()

	cfg, ok := b.configurations.Get(configID)
	if !ok {
		return nil, fmt.Errorf("%w: configuration %s not found", ErrNotFound, configID)
	}

	revisions := make([]ConfigurationRevision, len(cfg.Revisions))
	copy(revisions, cfg.Revisions)

	return revisions, nil
}

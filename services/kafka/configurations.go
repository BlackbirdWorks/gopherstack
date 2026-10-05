package kafka

import (
	"context"
	"fmt"
	"slices"
	"time"
)

// revisionOf snapshots c's current state as its latest revision (number 1 until updated).
func revisionOf(c *Configuration) *ConfigurationRevision {
	number := int64(1)
	creationTime := c.CreationTime

	if c.LatestRevision != nil {
		number = c.LatestRevision.Revision
		creationTime = c.LatestRevision.CreationTime
	}

	return &ConfigurationRevision{
		ConfigurationArn: c.Arn,
		Revision:         number,
		Description:      c.Description,
		ServerProperties: c.ServerProperties,
		CreationTime:     creationTime,
	}
}

// CreateConfiguration creates a new MSK configuration.
func (b *InMemoryBackend) CreateConfiguration(
	ctx context.Context,
	name, description string,
	kafkaVersions []string,
	serverProperties string,
) (*Configuration, error) {
	if name == "" {
		return nil, fmt.Errorf("name is required: %w", ErrValidation)
	}

	region := getRegion(ctx, b.region)

	b.mu.Lock("CreateConfiguration")
	defer b.mu.Unlock()

	for _, c := range b.configurationsByRegion.Get(region) {
		if c.Name == name {
			return nil, ErrAlreadyExists
		}
	}

	configArn := b.configurationARN(region, name)
	kvs := make([]string, len(kafkaVersions))
	copy(kvs, kafkaVersions)
	config := &Configuration{
		Arn:              configArn,
		Name:             name,
		Description:      description,
		KafkaVersions:    kvs,
		ServerProperties: serverProperties,
		CreationTime:     time.Now().UTC().Format(time.RFC3339),
		State:            ClusterStateActive,
		Tags:             make(map[string]string),
	}
	config.LatestRevision = revisionOf(config)
	b.configurations.Put(config)

	return cloneConfiguration(config), nil
}

// DescribeConfiguration retrieves a configuration by ARN.
func (b *InMemoryBackend) DescribeConfiguration(_ context.Context, configArn string) (*Configuration, error) {
	b.mu.RLock("DescribeConfiguration")
	defer b.mu.RUnlock()

	c, ok := b.configurations.Get(configArn)
	if !ok {
		return nil, ErrNotFound
	}

	return cloneConfiguration(c), nil
}

// ListConfigurations returns all MSK configurations in the request's region sorted by name.
func (b *InMemoryBackend) ListConfigurations(ctx context.Context) []*Configuration {
	region := getRegion(ctx, b.region)

	b.mu.RLock("ListConfigurations")
	defer b.mu.RUnlock()

	configurations := b.configurationsByRegion.Get(region)
	out := make([]*Configuration, 0, len(configurations))
	for _, c := range configurations {
		out = append(out, cloneConfiguration(c))
	}

	slices.SortFunc(out, func(a, b *Configuration) int {
		if a.Name < b.Name {
			return -1
		}
		if a.Name > b.Name {
			return 1
		}

		return 0
	})

	return out
}

// DeleteConfiguration deletes a configuration by ARN.
func (b *InMemoryBackend) DeleteConfiguration(_ context.Context, configArn string) error {
	b.mu.Lock("DeleteConfiguration")
	defer b.mu.Unlock()

	if !b.configurations.Delete(configArn) {
		return ErrNotFound
	}

	return nil
}

// DescribeConfigurationRevision retrieves a configuration revision.
func (b *InMemoryBackend) DescribeConfigurationRevision(
	_ context.Context,
	configArn string,
	revision int64,
) (*ConfigurationRevision, error) {
	b.mu.RLock("DescribeConfigurationRevision")
	defer b.mu.RUnlock()

	c, ok := b.configurations.Get(configArn)
	if !ok {
		return nil, ErrNotFound
	}

	for _, r := range c.PriorRevisions {
		if r.Revision == revision {
			cp := *r

			return &cp, nil
		}
	}

	if latest := revisionOf(c); latest.Revision == revision {
		return latest, nil
	}

	return nil, ErrNotFound
}

// UpdateConfiguration updates a configuration's server properties and description.
func (b *InMemoryBackend) UpdateConfiguration(
	_ context.Context,
	configArn, description, serverProperties string,
) (*Configuration, error) {
	b.mu.Lock("UpdateConfiguration")
	defer b.mu.Unlock()

	c, ok := b.configurations.Get(configArn)
	if !ok {
		return nil, ErrNotFound
	}

	prev := revisionOf(c)
	c.PriorRevisions = append(c.PriorRevisions, prev)

	if description != "" {
		c.Description = description
	}

	if serverProperties != "" {
		c.ServerProperties = serverProperties
	}

	c.LatestRevision = revisionOf(c)
	c.LatestRevision.Revision = prev.Revision + 1
	c.LatestRevision.CreationTime = time.Now().UTC().Format(time.RFC3339)

	return cloneConfiguration(c), nil
}

// ListConfigurationRevisions lists revisions for a configuration.
// Revisions are returned oldest first, ending with the latest.
func (b *InMemoryBackend) ListConfigurationRevisions(
	_ context.Context,
	configArn string,
) ([]*ConfigurationRevision, error) {
	b.mu.RLock("ListConfigurationRevisions")
	defer b.mu.RUnlock()

	c, ok := b.configurations.Get(configArn)
	if !ok {
		return nil, ErrNotFound
	}

	revs := make([]*ConfigurationRevision, 0, len(c.PriorRevisions)+1)
	for _, r := range c.PriorRevisions {
		cp := *r
		revs = append(revs, &cp)
	}

	return append(revs, revisionOf(c)), nil
}

func (b *InMemoryBackend) AddConfigurationInternal(name string) *Configuration {
	b.mu.Lock("AddConfigurationInternal")
	defer b.mu.Unlock()

	configArn := b.configurationARN(b.region, name)
	config := &Configuration{
		Arn:           configArn,
		Name:          name,
		KafkaVersions: []string{"2.8.0"},
		CreationTime:  time.Now().UTC().Format(time.RFC3339),
		State:         ClusterStateActive,
		Tags:          make(map[string]string),
	}
	config.LatestRevision = revisionOf(config)
	b.configurations.Put(config)

	return cloneConfiguration(config)
}

// AddReplicatorInternal creates a replicator directly for testing purposes.

// cloneConfiguration creates a deep copy of a Configuration.
func cloneConfiguration(c *Configuration) *Configuration {
	kvs := make([]string, len(c.KafkaVersions))
	copy(kvs, c.KafkaVersions)

	var latestRevision *ConfigurationRevision
	if c.LatestRevision != nil {
		rev := *c.LatestRevision
		latestRevision = &rev
	}

	return &Configuration{
		Arn:              c.Arn,
		Name:             c.Name,
		Description:      c.Description,
		ServerProperties: c.ServerProperties,
		CreationTime:     c.CreationTime,
		State:            c.State,
		KafkaVersions:    kvs,
		LatestRevision:   latestRevision,
		Tags:             nonNilTagsCopy(c.Tags),
	}
}

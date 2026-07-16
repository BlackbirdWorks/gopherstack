package kafka

import (
	"context"
	"fmt"
	"slices"
)

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
		Tags:             make(map[string]string),
	}
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
// In this stub, revision 1 always refers to the current configuration state.
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

	return &ConfigurationRevision{
		ConfigurationArn: c.Arn,
		Revision:         revision,
		Description:      c.Description,
		ServerProperties: c.ServerProperties,
	}, nil
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

	if description != "" {
		c.Description = description
	}

	if serverProperties != "" {
		c.ServerProperties = serverProperties
	}

	return cloneConfiguration(c), nil
}

// ListConfigurationRevisions lists revisions for a configuration.
// In this stub, every configuration has a single revision (revision 1).
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

	return []*ConfigurationRevision{
		{
			ConfigurationArn: c.Arn,
			Revision:         1,
			Description:      c.Description,
			ServerProperties: c.ServerProperties,
		},
	}, nil
}

func (b *InMemoryBackend) AddConfigurationInternal(name string) *Configuration {
	b.mu.Lock("AddConfigurationInternal")
	defer b.mu.Unlock()

	configArn := b.configurationARN(b.region, name)
	config := &Configuration{
		Arn:           configArn,
		Name:          name,
		KafkaVersions: []string{"2.8.0"},
		Tags:          make(map[string]string),
	}
	b.configurations.Put(config)

	return cloneConfiguration(config)
}

// AddReplicatorInternal creates a replicator directly for testing purposes.

// cloneConfiguration creates a deep copy of a Configuration.
func cloneConfiguration(c *Configuration) *Configuration {
	kvs := make([]string, len(c.KafkaVersions))
	copy(kvs, c.KafkaVersions)

	return &Configuration{
		Arn:              c.Arn,
		Name:             c.Name,
		Description:      c.Description,
		ServerProperties: c.ServerProperties,
		KafkaVersions:    kvs,
		Tags:             nonNilTagsCopy(c.Tags),
	}
}

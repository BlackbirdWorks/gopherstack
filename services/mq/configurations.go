package mq

import (
	"encoding/base64"
	"fmt"
	"maps"
	"sort"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

const (
	// maxConfigurationRevisions is the maximum number of revisions retained per configuration.
	// AWS MQ supports up to 50 revisions; older ones are pruned when this limit is exceeded.
	maxConfigurationRevisions = 50

	// maxConfigurationDataBytes caps the size of a single revision's data payload.
	// AWS MQ rejects configuration data larger than 250 KiB; we enforce the same limit
	// to avoid unbounded memory growth from a malicious or buggy client.
	maxConfigurationDataBytes = 256 * 1024

	// minConfigurationNameLen and maxConfigurationNameLen bound a configuration
	// name per CreateConfigurationRequest.Name in the MQ API reference.
	minConfigurationNameLen = 1
	maxConfigurationNameLen = 150
)

// validateConfigurationName enforces AWS MQ configuration-name constraints:
// 1-150 characters, alphanumeric plus dashes, periods, underscores, and
// tildes (matching the ActiveMQ/RabbitMQ configuration name charset
// documented for CreateConfigurationRequest.Name).
func validateConfigurationName(name string) error {
	if len(name) < minConfigurationNameLen || len(name) > maxConfigurationNameLen {
		return fmt.Errorf(
			"%w: configuration name must be %d-%d characters (got %d)",
			ErrValidation, minConfigurationNameLen, maxConfigurationNameLen, len(name),
		)
	}

	for _, c := range name {
		if !isAlphanumeric(c) && c != '-' && c != '.' && c != '_' && c != '~' {
			return fmt.Errorf(
				"%w: configuration name must contain only alphanumeric characters, "+
					"dashes, periods, underscores, and tildes, got %q",
				ErrValidation, c,
			)
		}
	}

	return nil
}

// CreateConfiguration creates a new Amazon MQ configuration.
func (b *InMemoryBackend) CreateConfiguration(
	name, description, engineType, engineVersion string,
	tags map[string]string,
) (*Configuration, error) {
	if err := validateConfigurationName(name); err != nil {
		return nil, err
	}

	if err := validateTagsMap(tags); err != nil {
		return nil, err
	}

	b.mu.Lock("CreateConfiguration")
	defer b.mu.Unlock()

	if engineType != EngineTypeActiveMQ && engineType != EngineTypeRabbitMQ {
		return nil, fmt.Errorf("%w: engineType must be ACTIVEMQ or RABBITMQ, got %q", ErrValidation, engineType)
	}

	var duplicate bool

	b.configurations.Range(func(c *Configuration) bool {
		if c.Name == name {
			duplicate = true

			return false
		}

		return true
	})

	if duplicate {
		return nil, fmt.Errorf("%w: configuration %s already exists", ErrAlreadyExists, name)
	}

	if engineVersion == "" {
		if engineType == EngineTypeRabbitMQ {
			engineVersion = "3.11.20"
		} else {
			engineVersion = "5.15.14"
		}
	}

	id := "c-" + uuid.NewString()[:8]
	configArn := arn.Build("mq", b.region, b.accountID, "configuration:"+id)
	now := time.Now().UTC().Format(time.RFC3339)

	rev := ConfigurationRevision{
		Created:     now,
		Description: description,
		Revision:    1,
	}

	tagsCopy := make(map[string]string)
	maps.Copy(tagsCopy, tags)

	cfg := &Configuration{
		Arn:            configArn,
		ID:             id,
		Name:           name,
		Description:    description,
		EngineType:     engineType,
		EngineVersion:  engineVersion,
		LatestRevision: &rev,
		Created:        now,
		Tags:           tagsCopy,
		Revisions:      []ConfigurationRevision{rev},
		Data:           map[int32]string{1: defaultConfigurationData(engineType)},
	}

	b.configurations.Put(cfg)
	b.tags[configArn] = tagsCopy

	return b.copyConfiguration(cfg), nil
}

// defaultConfigurationData returns a base64-encoded default broker configuration
// appropriate for the given engine type, matching what AWS MQ seeds for revision 1.
func defaultConfigurationData(engineType string) string {
	var raw string

	switch engineType {
	case EngineTypeRabbitMQ:
		raw = "# Default RabbitMQ configuration\n"
	default:
		raw = "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\n" +
			"<broker xmlns=\"http://activemq.apache.org/schema/core\">\n" +
			"</broker>\n"
	}

	return base64.StdEncoding.EncodeToString([]byte(raw))
}

// DescribeConfiguration returns a configuration by ID.
func (b *InMemoryBackend) DescribeConfiguration(configID string) (*Configuration, error) {
	b.mu.RLock("DescribeConfiguration")
	defer b.mu.RUnlock()

	cfg, ok := b.configurations.Get(configID)
	if !ok {
		return nil, fmt.Errorf("%w: configuration %s not found", ErrNotFound, configID)
	}

	return b.copyConfiguration(cfg), nil
}

// ListConfigurations returns all configurations sorted by name.
func (b *InMemoryBackend) ListConfigurations() []*Configuration {
	b.mu.RLock("ListConfigurations")
	defer b.mu.RUnlock()

	all := b.configurations.All()
	list := make([]*Configuration, 0, len(all))

	for _, c := range all {
		list = append(list, b.copyConfiguration(c))
	}

	sort.Slice(list, func(i, j int) bool { return list[i].Name < list[j].Name })

	return list
}

// UpdateConfiguration updates a configuration (creates a new revision).
func (b *InMemoryBackend) UpdateConfiguration(configID, description, data string) (*Configuration, error) {
	if len(data) > maxConfigurationDataBytes {
		return nil, fmt.Errorf(
			"%w: configuration data exceeds %d bytes (got %d)",
			ErrValidation, maxConfigurationDataBytes, len(data),
		)
	}

	b.mu.Lock("UpdateConfiguration")
	defer b.mu.Unlock()

	cfg, ok := b.configurations.Get(configID)
	if !ok {
		return nil, fmt.Errorf("%w: configuration %s not found", ErrNotFound, configID)
	}

	nextRev := cfg.LatestRevision.Revision + 1
	now := time.Now().UTC().Format(time.RFC3339)

	rev := ConfigurationRevision{
		Created:     now,
		Description: description,
		Revision:    nextRev,
	}

	if description != "" {
		cfg.Description = description
	}

	cfg.LatestRevision = &rev
	cfg.Revisions = append(cfg.Revisions, rev)
	cfg.Data[nextRev] = data

	// Prune oldest revision when the cap is exceeded.
	if len(cfg.Revisions) > maxConfigurationRevisions {
		oldest := cfg.Revisions[0]
		cfg.Revisions = cfg.Revisions[1:]
		delete(cfg.Data, oldest.Revision)
	}

	return b.copyConfiguration(cfg), nil
}

// copyConfiguration returns a copy of a configuration.
func (b *InMemoryBackend) copyConfiguration(c *Configuration) *Configuration {
	cp := *c

	cp.Tags = make(map[string]string, len(c.Tags))
	maps.Copy(cp.Tags, c.Tags)

	if c.LatestRevision != nil {
		rev := *c.LatestRevision
		cp.LatestRevision = &rev
	}

	cp.Revisions = append([]ConfigurationRevision{}, c.Revisions...)

	cp.Data = make(map[int32]string, len(c.Data))
	maps.Copy(cp.Data, c.Data)

	return &cp
}

// DeleteConfiguration removes a configuration by ID.
func (b *InMemoryBackend) DeleteConfiguration(configID string) error {
	b.mu.Lock("DeleteConfiguration")
	defer b.mu.Unlock()

	cfg, ok := b.configurations.Get(configID)
	if !ok {
		return fmt.Errorf("%w: configuration %s not found", ErrNotFound, configID)
	}

	b.configurations.Delete(configID)
	delete(b.tags, cfg.Arn)

	return nil
}

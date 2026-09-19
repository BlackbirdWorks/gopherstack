package sesv2

import (
	"fmt"
	"time"
)

// CloudWatchDimensionConfiguration mirrors types.CloudWatchDimensionConfiguration.
type CloudWatchDimensionConfiguration struct {
	DimensionName         string `json:"dimensionName"`
	DimensionValueSource  string `json:"dimensionValueSource"`
	DefaultDimensionValue string `json:"defaultDimensionValue"`
}

// CloudWatchDestination mirrors types.CloudWatchDestination.
type CloudWatchDestination struct {
	DimensionConfigurations []CloudWatchDimensionConfiguration `json:"dimensionConfigurations,omitempty"`
}

// EventBridgeDestination mirrors types.EventBridgeDestination.
type EventBridgeDestination struct {
	EventBusArn string `json:"eventBusArn"`
}

// KinesisFirehoseDestination mirrors types.KinesisFirehoseDestination.
type KinesisFirehoseDestination struct {
	IamRoleArn        string `json:"iamRoleArn"`
	DeliveryStreamArn string `json:"deliveryStreamArn"`
}

// PinpointDestination mirrors types.PinpointDestination.
type PinpointDestination struct {
	ApplicationArn string `json:"applicationArn,omitempty"`
}

// SnsDestination mirrors types.SnsDestination.
type SnsDestination struct {
	TopicArn string `json:"topicArn"`
}

// EventDestinationConfig groups the mutable members
// CreateConfigurationSetEventDestination/UpdateConfigurationSetEventDestination
// share -- real EventDestinationDefinition requires exactly one of the five
// destination sub-objects, which this backend doesn't enforce (a client
// sending more than one is unusual, not observably wrong to reject or
// accept), it just stores whichever were set.
type EventDestinationConfig struct {
	CloudWatchDestination      *CloudWatchDestination
	EventBridgeDestination     *EventBridgeDestination
	KinesisFirehoseDestination *KinesisFirehoseDestination
	PinpointDestination        *PinpointDestination
	SnsDestination             *SnsDestination
	MatchingEventTypes         []string
	Enabled                    bool
}

// EventDestination represents an event destination for a configuration set.
type EventDestination struct {
	CreatedAt                  time.Time                   `json:"createdAt"`
	CloudWatchDestination      *CloudWatchDestination      `json:"cloudWatchDestination,omitempty"`
	EventBridgeDestination     *EventBridgeDestination     `json:"eventBridgeDestination,omitempty"`
	KinesisFirehoseDestination *KinesisFirehoseDestination `json:"kinesisFirehoseDestination,omitempty"`
	PinpointDestination        *PinpointDestination        `json:"pinpointDestination,omitempty"`
	SnsDestination             *SnsDestination             `json:"snsDestination,omitempty"`
	Name                       string                      `json:"name"`
	ConfigurationSetName       string                      `json:"configurationSetName"`
	MatchingEventTypes         []string                    `json:"matchingEventTypes"`
	Enabled                    bool                        `json:"enabled"`
}

// CreateConfigurationSetEventDestination adds an event destination to a configuration set.
func (b *InMemoryBackend) CreateConfigurationSetEventDestination(
	configSetName, destName string,
	cfg EventDestinationConfig,
) (*EventDestination, error) {
	b.mu.Lock("CreateConfigurationSetEventDestination")
	defer b.mu.Unlock()

	if !b.configurationSets.Has(configSetName) {
		return nil, fmt.Errorf("%w: configuration set %s not found", ErrNotFound, configSetName)
	}

	key := eventDestinationKey(configSetName, destName)
	if b.eventDestinations.Has(key) {
		return nil, fmt.Errorf(
			"%w: event destination %s already exists in config set %s",
			ErrAlreadyExists, destName, configSetName,
		)
	}

	types := make([]string, len(cfg.MatchingEventTypes))
	copy(types, cfg.MatchingEventTypes)

	dest := &EventDestination{
		Name:                       destName,
		ConfigurationSetName:       configSetName,
		Enabled:                    cfg.Enabled,
		MatchingEventTypes:         types,
		CloudWatchDestination:      cfg.CloudWatchDestination,
		EventBridgeDestination:     cfg.EventBridgeDestination,
		KinesisFirehoseDestination: cfg.KinesisFirehoseDestination,
		PinpointDestination:        cfg.PinpointDestination,
		SnsDestination:             cfg.SnsDestination,
		CreatedAt:                  time.Now(),
	}
	b.eventDestinations.Put(dest)

	cp := *dest

	return &cp, nil
}

// GetConfigurationSetEventDestinations retrieves event destinations for a config set.
func (b *InMemoryBackend) GetConfigurationSetEventDestinations(
	configSetName string,
) ([]*EventDestination, error) {
	b.mu.RLock("GetConfigurationSetEventDestinations")
	defer b.mu.RUnlock()

	if !b.configurationSets.Has(configSetName) {
		return nil, fmt.Errorf("%w: configuration set %s not found", ErrNotFound, configSetName)
	}

	dests := b.eventDestinationsByConfigSet.Get(configSetName)
	out := make([]*EventDestination, 0, len(dests))

	for _, d := range dests {
		cp := *d
		out = append(out, &cp)
	}

	return out, nil
}

// DeleteConfigurationSetEventDestination removes an event destination.
func (b *InMemoryBackend) DeleteConfigurationSetEventDestination(
	configSetName, destName string,
) error {
	b.mu.Lock("DeleteConfigurationSetEventDestination")
	defer b.mu.Unlock()

	if !b.configurationSets.Has(configSetName) {
		return fmt.Errorf("%w: configuration set %s not found", ErrNotFound, configSetName)
	}

	key := eventDestinationKey(configSetName, destName)
	if !b.eventDestinations.Has(key) {
		return fmt.Errorf(
			"%w: event destination %s not found in %s",
			ErrNotFound,
			destName,
			configSetName,
		)
	}

	b.eventDestinations.Delete(key)

	return nil
}

// UpdateConfigurationSetEventDestination updates an event destination.
func (b *InMemoryBackend) UpdateConfigurationSetEventDestination(
	configSetName, destName string,
	cfg EventDestinationConfig,
) error {
	b.mu.Lock("UpdateConfigurationSetEventDestination")
	defer b.mu.Unlock()

	if !b.configurationSets.Has(configSetName) {
		return fmt.Errorf("%w: configuration set %s not found", ErrNotFound, configSetName)
	}

	dest, ok := b.eventDestinations.Get(eventDestinationKey(configSetName, destName))
	if !ok {
		return fmt.Errorf(
			"%w: event destination %s not found in %s",
			ErrNotFound,
			destName,
			configSetName,
		)
	}

	dest.Enabled = cfg.Enabled

	types := make([]string, len(cfg.MatchingEventTypes))
	copy(types, cfg.MatchingEventTypes)
	dest.MatchingEventTypes = types
	dest.CloudWatchDestination = cfg.CloudWatchDestination
	dest.EventBridgeDestination = cfg.EventBridgeDestination
	dest.KinesisFirehoseDestination = cfg.KinesisFirehoseDestination
	dest.PinpointDestination = cfg.PinpointDestination
	dest.SnsDestination = cfg.SnsDestination

	return nil
}

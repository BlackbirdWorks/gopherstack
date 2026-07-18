package ses

import (
	"fmt"
	"slices"
	"sort"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

// CreateConfigurationSet registers a new configuration set. Returns ErrConfigSetExists if it already exists.
func (b *InMemoryBackend) CreateConfigurationSet(name string) error {
	if strings.TrimSpace(name) == "" {
		return fmt.Errorf("%w: ConfigurationSetName is required", ErrInvalidParameter)
	}

	b.mu.Lock("CreateConfigurationSet")
	defer b.mu.Unlock()

	if b.configSets.Has(name) {
		return fmt.Errorf("%w: configuration set %s already exists", ErrConfigSetExists, name)
	}

	b.configSets.Put(&ConfigurationSet{Name: name, SendingEnabled: true})

	return nil
}

// DeleteConfigurationSet removes a configuration set.
// Returns ErrConfigSetNotFound if the set does not exist, matching real AWS SES behavior.
func (b *InMemoryBackend) DeleteConfigurationSet(name string) error {
	b.mu.Lock("DeleteConfigurationSet")
	defer b.mu.Unlock()

	if !b.configSets.Has(name) {
		return fmt.Errorf("%w: %s", ErrConfigSetNotFound, name)
	}

	b.configSets.Delete(name)

	// Cascade-delete every event destination of this configuration set.
	// slices.Clone the index results first: deleting from b.eventDestinations
	// mutates eventDestinationsByConfigSet's backing slice in place, which
	// would otherwise corrupt this very loop.
	for _, d := range slices.Clone(b.eventDestinationsByConfigSet.Get(name)) {
		b.eventDestinations.Delete(eventDestinationKey(name, d.Name))
	}

	b.trackingOptions.Delete(name)

	return nil
}

// ListConfigurationSets returns configuration set names sorted alphabetically.
func (b *InMemoryBackend) ListConfigurationSets(nextToken string, maxItems int) page.Page[string] {
	b.mu.RLock("ListConfigurationSets")
	defer b.mu.RUnlock()

	snap := b.configSets.Snapshot()
	names := make([]string, len(snap))

	for i, cs := range snap {
		names[i] = cs.Name
	}

	return page.New(names, nextToken, maxItems, sesDefaultMaxItems)
}

// CreateConfigurationSetTrackingOptions sets the tracking options for a configuration set.
func (b *InMemoryBackend) CreateConfigurationSetTrackingOptions(configSetName, customRedirectDomain string) error {
	if strings.TrimSpace(configSetName) == "" {
		return fmt.Errorf("%w: ConfigurationSetName is required", ErrInvalidParameter)
	}

	b.mu.Lock("CreateConfigurationSetTrackingOptions")
	defer b.mu.Unlock()

	if !b.configSets.Has(configSetName) {
		return fmt.Errorf("%w: %s", ErrConfigSetNotFound, configSetName)
	}

	if b.trackingOptions.Has(configSetName) {
		return fmt.Errorf(
			"%w: tracking options already exist for configuration set %s",
			ErrTrackingOptionsExists,
			configSetName,
		)
	}

	b.trackingOptions.Put(&TrackingOptions{ConfigSetName: configSetName, CustomRedirectDomain: customRedirectDomain})

	return nil
}

// DeleteConfigurationSetTrackingOptions removes the tracking options from a configuration set.
func (b *InMemoryBackend) DeleteConfigurationSetTrackingOptions(configSetName string) error {
	if strings.TrimSpace(configSetName) == "" {
		return fmt.Errorf("%w: ConfigurationSetName is required", ErrInvalidParameter)
	}

	b.mu.Lock("DeleteConfigurationSetTrackingOptions")
	defer b.mu.Unlock()

	if !b.configSets.Has(configSetName) {
		return fmt.Errorf("%w: %s", ErrConfigSetNotFound, configSetName)
	}

	if !b.trackingOptions.Has(configSetName) {
		return fmt.Errorf(
			"%w: tracking options do not exist for configuration set %s",
			ErrTrackingOptionsNotFound,
			configSetName,
		)
	}

	b.trackingOptions.Delete(configSetName)

	return nil
}

// DescribeConfigurationSet returns the named configuration set metadata plus event destinations and tracking options.
func (b *InMemoryBackend) DescribeConfigurationSet(name string) (ConfigurationSetDescription, error) {
	if strings.TrimSpace(name) == "" {
		return ConfigurationSetDescription{}, fmt.Errorf("%w: ConfigurationSetName is required", ErrInvalidParameter)
	}

	b.mu.RLock("DescribeConfigurationSet")
	defer b.mu.RUnlock()

	cs, exists := b.configSets.Get(name)
	if !exists {
		return ConfigurationSetDescription{}, fmt.Errorf("%w: %s", ErrConfigSetNotFound, name)
	}

	desc := ConfigurationSetDescription{
		Name:                     name,
		SendingEnabled:           cs.SendingEnabled,
		ReputationMetricsEnabled: cs.ReputationMetrics,
	}

	if cs.TLSPolicy != "" {
		desc.DeliveryOptions = &DeliveryOptions{TLSPolicy: cs.TLSPolicy}
	}

	if dests := b.eventDestinationsByConfigSet.Get(name); len(dests) > 0 {
		for _, d := range dests {
			dc := *d
			desc.EventDestinations = append(desc.EventDestinations, dc)
		}

		sort.Slice(desc.EventDestinations, func(i, j int) bool {
			return desc.EventDestinations[i].Name < desc.EventDestinations[j].Name
		})
	}

	if to, ok := b.trackingOptions.Get(name); ok {
		tc := *to
		desc.TrackingOptions = &tc
	}

	return desc, nil
}

// PutConfigurationSetDeliveryOptions persists the TLS policy for a configuration set.
func (b *InMemoryBackend) PutConfigurationSetDeliveryOptions(configSetName, tlsPolicy string) error {
	if strings.TrimSpace(configSetName) == "" {
		return fmt.Errorf("%w: ConfigurationSetName is required", ErrInvalidParameter)
	}

	b.mu.Lock("PutConfigurationSetDeliveryOptions")
	defer b.mu.Unlock()

	cs, exists := b.configSets.Get(configSetName)
	if !exists {
		return fmt.Errorf("%w: %s", ErrConfigSetNotFound, configSetName)
	}

	cs.TLSPolicy = tlsPolicy

	return nil
}

// UpdateConfigurationSetReputationMetricsEnabled persists the reputation metrics flag.
func (b *InMemoryBackend) UpdateConfigurationSetReputationMetricsEnabled(configSetName string, enabled bool) error {
	if strings.TrimSpace(configSetName) == "" {
		return fmt.Errorf("%w: ConfigurationSetName is required", ErrInvalidParameter)
	}

	b.mu.Lock("UpdateConfigurationSetReputationMetricsEnabled")
	defer b.mu.Unlock()

	cs, exists := b.configSets.Get(configSetName)
	if !exists {
		return fmt.Errorf("%w: %s", ErrConfigSetNotFound, configSetName)
	}

	cs.ReputationMetrics = enabled

	return nil
}

// UpdateConfigurationSetSendingEnabled persists the sending-enabled flag.
func (b *InMemoryBackend) UpdateConfigurationSetSendingEnabled(configSetName string, enabled bool) error {
	if strings.TrimSpace(configSetName) == "" {
		return fmt.Errorf("%w: ConfigurationSetName is required", ErrInvalidParameter)
	}

	b.mu.Lock("UpdateConfigurationSetSendingEnabled")
	defer b.mu.Unlock()

	cs, exists := b.configSets.Get(configSetName)
	if !exists {
		return fmt.Errorf("%w: %s", ErrConfigSetNotFound, configSetName)
	}

	cs.SendingEnabled = enabled

	return nil
}

// UpdateConfigurationSetTrackingOptions updates the tracking options for a configuration set.
func (b *InMemoryBackend) UpdateConfigurationSetTrackingOptions(configSetName, customRedirectDomain string) error {
	if strings.TrimSpace(configSetName) == "" {
		return fmt.Errorf("%w: ConfigurationSetName is required", ErrInvalidParameter)
	}

	b.mu.Lock("UpdateConfigurationSetTrackingOptions")
	defer b.mu.Unlock()

	if !b.configSets.Has(configSetName) {
		return fmt.Errorf("%w: %s", ErrConfigSetNotFound, configSetName)
	}

	if !b.trackingOptions.Has(configSetName) {
		return fmt.Errorf(
			"%w: tracking options do not exist for configuration set %s",
			ErrTrackingOptionsNotFound,
			configSetName,
		)
	}

	b.trackingOptions.Put(&TrackingOptions{ConfigSetName: configSetName, CustomRedirectDomain: customRedirectDomain})

	return nil
}

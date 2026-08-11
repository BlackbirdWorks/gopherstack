package ses

import (
	"fmt"
	"strings"
)

// isValidEventType reports whether eventType is one of the AWS SES EventType
// enum values (aws-sdk-go-v2/service/ses/types/enums.go).
func isValidEventType(eventType string) bool {
	switch eventType {
	case "send", "reject", "bounce", "complaint", "delivery", "open", "click", "renderingFailure":
		return true
	default:
		return false
	}
}

// validateMatchingEventTypes checks that eventTypes is non-empty (a required
// EventDestination member) and every entry is a known EventType enum value.
func validateMatchingEventTypes(eventTypes []string) error {
	if len(eventTypes) == 0 {
		return fmt.Errorf("%w: EventDestination.MatchingEventTypes is required", ErrInvalidParameter)
	}

	for _, et := range eventTypes {
		if !isValidEventType(et) {
			return fmt.Errorf("%w: invalid MatchingEventTypes value %q", ErrInvalidParameter, et)
		}
	}

	return nil
}

// CreateConfigurationSetEventDestination adds an event destination to a configuration set.
func (b *InMemoryBackend) CreateConfigurationSetEventDestination(configSetName string, dest EventDestination) error {
	if strings.TrimSpace(configSetName) == "" {
		return fmt.Errorf("%w: ConfigurationSetName is required", ErrInvalidParameter)
	}

	if strings.TrimSpace(dest.Name) == "" {
		return fmt.Errorf("%w: EventDestination.Name is required", ErrInvalidParameter)
	}

	if err := validateMatchingEventTypes(dest.MatchingEventTypes); err != nil {
		return err
	}

	b.mu.Lock("CreateConfigurationSetEventDestination")
	defer b.mu.Unlock()

	if !b.configSets.Has(configSetName) {
		return fmt.Errorf("%w: %s", ErrConfigSetNotFound, configSetName)
	}

	key := eventDestinationKey(configSetName, dest.Name)
	if b.eventDestinations.Has(key) {
		return fmt.Errorf(
			"%w: event destination %s already exists in configuration set %s",
			ErrEventDestinationExists,
			dest.Name,
			configSetName,
		)
	}

	d := dest
	d.ConfigSetName = configSetName
	b.eventDestinations.Put(&d)

	return nil
}

// DeleteConfigurationSetEventDestination removes an event destination from a configuration set.
func (b *InMemoryBackend) DeleteConfigurationSetEventDestination(configSetName, destName string) error {
	if strings.TrimSpace(configSetName) == "" {
		return fmt.Errorf("%w: ConfigurationSetName is required", ErrInvalidParameter)
	}

	b.mu.Lock("DeleteConfigurationSetEventDestination")
	defer b.mu.Unlock()

	if !b.configSets.Has(configSetName) {
		return fmt.Errorf("%w: %s", ErrConfigSetNotFound, configSetName)
	}

	key := eventDestinationKey(configSetName, destName)
	if !b.eventDestinations.Has(key) {
		return fmt.Errorf("%w: %s", ErrEventDestinationNotFound, destName)
	}

	b.eventDestinations.Delete(key)

	return nil
}

// UpdateConfigurationSetEventDestination updates an existing event destination on a configuration set.
func (b *InMemoryBackend) UpdateConfigurationSetEventDestination(configSetName string, dest EventDestination) error {
	if strings.TrimSpace(configSetName) == "" {
		return fmt.Errorf("%w: ConfigurationSetName is required", ErrInvalidParameter)
	}

	if strings.TrimSpace(dest.Name) == "" {
		return fmt.Errorf("%w: EventDestination.Name is required", ErrInvalidParameter)
	}

	if err := validateMatchingEventTypes(dest.MatchingEventTypes); err != nil {
		return err
	}

	b.mu.Lock("UpdateConfigurationSetEventDestination")
	defer b.mu.Unlock()

	if !b.configSets.Has(configSetName) {
		return fmt.Errorf("%w: %s", ErrConfigSetNotFound, configSetName)
	}

	key := eventDestinationKey(configSetName, dest.Name)
	if !b.eventDestinations.Has(key) {
		return fmt.Errorf("%w: %s", ErrEventDestinationNotFound, dest.Name)
	}

	d := dest
	d.ConfigSetName = configSetName
	b.eventDestinations.Put(&d)

	return nil
}

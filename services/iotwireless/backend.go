package iotwireless

import (
	"errors"
	"fmt"
	"maps"
	"sync"
	"time"

	"github.com/google/uuid"
)

// Sentinel errors for IoT Wireless backend operations.
var (
	// ErrDeviceNotFound is returned when a wireless device does not exist.
	ErrDeviceNotFound = errors.New("ResourceNotFoundException: Wireless device not found")
	// ErrGatewayNotFound is returned when a wireless gateway does not exist.
	ErrGatewayNotFound = errors.New("ResourceNotFoundException: Wireless gateway not found")
	// ErrServiceProfileNotFound is returned when a service profile does not exist.
	ErrServiceProfileNotFound = errors.New("ResourceNotFoundException: Service profile not found")
	// ErrDestinationNotFound is returned when a destination does not exist.
	ErrDestinationNotFound = errors.New("ResourceNotFoundException: Destination not found")
)

// StorageBackend is the interface for the IoT Wireless backend.
type StorageBackend interface {
	CreateWirelessDevice(
		accountID, region, name, devType, destinationName, description string,
		tags map[string]string,
	) (*WirelessDevice, error)
	GetWirelessDevice(accountID, region, id string) (*WirelessDevice, error)
	ListWirelessDevices(accountID, region string) []*WirelessDevice
	DeleteWirelessDevice(accountID, region, id string) error

	CreateWirelessGateway(accountID, region, name, description string, tags map[string]string) (*WirelessGateway, error)
	GetWirelessGateway(accountID, region, id string) (*WirelessGateway, error)
	ListWirelessGateways(accountID, region string) []*WirelessGateway
	DeleteWirelessGateway(accountID, region, id string) error

	CreateServiceProfile(accountID, region, name string, tags map[string]string) (*ServiceProfile, error)
	GetServiceProfile(accountID, region, id string) (*ServiceProfile, error)
	ListServiceProfiles(accountID, region string) []*ServiceProfile
	DeleteServiceProfile(accountID, region, id string) error

	CreateDestination(
		accountID, region, name, expression, expressionType, roleArn, description string,
		tags map[string]string,
	) (*Destination, error)
	GetDestination(accountID, region, name string) (*Destination, error)
	ListDestinations(accountID, region string) []*Destination
	DeleteDestination(accountID, region, name string) error

	TagResource(arn string, tags map[string]string) error
	UntagResource(arn string, tagKeys []string) error
	ListTagsForResource(arn string) (map[string]string, error)
}

// resourceKey uniquely identifies a resource within an account and region.
type resourceKey struct {
	AccountID string
	Region    string
	ID        string
}

// InMemoryBackend is the in-memory backend for IoT Wireless.
type InMemoryBackend struct {
	devices         map[resourceKey]*WirelessDevice
	gateways        map[resourceKey]*WirelessGateway
	serviceProfiles map[resourceKey]*ServiceProfile
	destinations    map[resourceKey]*Destination
	resourceTags    map[string]map[string]string
	mu              sync.RWMutex
}

// NewInMemoryBackend creates a new in-memory IoT Wireless backend.
func NewInMemoryBackend() *InMemoryBackend {
	return &InMemoryBackend{
		devices:         make(map[resourceKey]*WirelessDevice),
		gateways:        make(map[resourceKey]*WirelessGateway),
		serviceProfiles: make(map[resourceKey]*ServiceProfile),
		destinations:    make(map[resourceKey]*Destination),
		resourceTags:    make(map[string]map[string]string),
	}
}

func wirelessDeviceARN(region, accountID, id string) string {
	return fmt.Sprintf("arn:aws:iotwireless:%s:%s:WirelessDevice/%s", region, accountID, id)
}

func wirelessGatewayARN(region, accountID, id string) string {
	return fmt.Sprintf("arn:aws:iotwireless:%s:%s:WirelessGateway/%s", region, accountID, id)
}

func serviceProfileARN(region, accountID, id string) string {
	return fmt.Sprintf("arn:aws:iotwireless:%s:%s:ServiceProfile/%s", region, accountID, id)
}

func destinationARN(region, accountID, name string) string {
	return fmt.Sprintf("arn:aws:iotwireless:%s:%s:Destination/%s", region, accountID, name)
}

// CreateWirelessDevice creates a new wireless device.
func (b *InMemoryBackend) CreateWirelessDevice(
	accountID, region, name, devType, destinationName, description string,
	tags map[string]string,
) (*WirelessDevice, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	id := uuid.NewString()
	arn := wirelessDeviceARN(region, accountID, id)

	tagsCopy := make(map[string]string, len(tags))
	maps.Copy(tagsCopy, tags)

	d := &WirelessDevice{
		ID:              id,
		ARN:             arn,
		Name:            name,
		Type:            devType,
		DestinationName: destinationName,
		Description:     description,
		Tags:            tagsCopy,
		CreatedAt:       time.Now(),
	}

	key := resourceKey{AccountID: accountID, Region: region, ID: id}
	b.devices[key] = d

	b.resourceTags[arn] = make(map[string]string, len(tags))
	maps.Copy(b.resourceTags[arn], tags)

	return d, nil
}

// GetWirelessDevice returns a wireless device by ID.
func (b *InMemoryBackend) GetWirelessDevice(accountID, region, id string) (*WirelessDevice, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	key := resourceKey{AccountID: accountID, Region: region, ID: id}

	d, ok := b.devices[key]
	if !ok {
		return nil, ErrDeviceNotFound
	}

	return d, nil
}

// ListWirelessDevices returns all wireless devices for the given account and region.
func (b *InMemoryBackend) ListWirelessDevices(accountID, region string) []*WirelessDevice {
	b.mu.RLock()
	defer b.mu.RUnlock()

	var result []*WirelessDevice

	for k, d := range b.devices {
		if k.AccountID == accountID && k.Region == region {
			result = append(result, d)
		}
	}

	return result
}

// DeleteWirelessDevice deletes a wireless device.
func (b *InMemoryBackend) DeleteWirelessDevice(accountID, region, id string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	key := resourceKey{AccountID: accountID, Region: region, ID: id}

	d, ok := b.devices[key]
	if !ok {
		return ErrDeviceNotFound
	}

	delete(b.resourceTags, d.ARN)
	delete(b.devices, key)

	return nil
}

// CreateWirelessGateway creates a new wireless gateway.
func (b *InMemoryBackend) CreateWirelessGateway(
	accountID, region, name, description string,
	tags map[string]string,
) (*WirelessGateway, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	id := uuid.NewString()
	arn := wirelessGatewayARN(region, accountID, id)

	tagsCopy := make(map[string]string, len(tags))
	maps.Copy(tagsCopy, tags)

	gw := &WirelessGateway{
		ID:          id,
		ARN:         arn,
		Name:        name,
		Description: description,
		Tags:        tagsCopy,
		CreatedAt:   time.Now(),
	}

	key := resourceKey{AccountID: accountID, Region: region, ID: id}
	b.gateways[key] = gw

	b.resourceTags[arn] = make(map[string]string, len(tags))
	maps.Copy(b.resourceTags[arn], tags)

	return gw, nil
}

// GetWirelessGateway returns a wireless gateway by ID.
func (b *InMemoryBackend) GetWirelessGateway(accountID, region, id string) (*WirelessGateway, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	key := resourceKey{AccountID: accountID, Region: region, ID: id}

	gw, ok := b.gateways[key]
	if !ok {
		return nil, ErrGatewayNotFound
	}

	return gw, nil
}

// ListWirelessGateways returns all wireless gateways for the given account and region.
func (b *InMemoryBackend) ListWirelessGateways(accountID, region string) []*WirelessGateway {
	b.mu.RLock()
	defer b.mu.RUnlock()

	var result []*WirelessGateway

	for k, gw := range b.gateways {
		if k.AccountID == accountID && k.Region == region {
			result = append(result, gw)
		}
	}

	return result
}

// DeleteWirelessGateway deletes a wireless gateway.
func (b *InMemoryBackend) DeleteWirelessGateway(accountID, region, id string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	key := resourceKey{AccountID: accountID, Region: region, ID: id}

	gw, ok := b.gateways[key]
	if !ok {
		return ErrGatewayNotFound
	}

	delete(b.resourceTags, gw.ARN)
	delete(b.gateways, key)

	return nil
}

// CreateServiceProfile creates a new service profile.
func (b *InMemoryBackend) CreateServiceProfile(
	accountID, region, name string,
	tags map[string]string,
) (*ServiceProfile, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	id := uuid.NewString()
	arn := serviceProfileARN(region, accountID, id)

	tagsCopy := make(map[string]string, len(tags))
	maps.Copy(tagsCopy, tags)

	sp := &ServiceProfile{
		ID:        id,
		ARN:       arn,
		Name:      name,
		Tags:      tagsCopy,
		CreatedAt: time.Now(),
	}

	key := resourceKey{AccountID: accountID, Region: region, ID: id}
	b.serviceProfiles[key] = sp

	b.resourceTags[arn] = make(map[string]string, len(tags))
	maps.Copy(b.resourceTags[arn], tags)

	return sp, nil
}

// GetServiceProfile returns a service profile by ID.
func (b *InMemoryBackend) GetServiceProfile(accountID, region, id string) (*ServiceProfile, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	key := resourceKey{AccountID: accountID, Region: region, ID: id}

	sp, ok := b.serviceProfiles[key]
	if !ok {
		return nil, ErrServiceProfileNotFound
	}

	return sp, nil
}

// ListServiceProfiles returns all service profiles for the given account and region.
func (b *InMemoryBackend) ListServiceProfiles(accountID, region string) []*ServiceProfile {
	b.mu.RLock()
	defer b.mu.RUnlock()

	var result []*ServiceProfile

	for k, sp := range b.serviceProfiles {
		if k.AccountID == accountID && k.Region == region {
			result = append(result, sp)
		}
	}

	return result
}

// DeleteServiceProfile deletes a service profile.
func (b *InMemoryBackend) DeleteServiceProfile(accountID, region, id string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	key := resourceKey{AccountID: accountID, Region: region, ID: id}

	sp, ok := b.serviceProfiles[key]
	if !ok {
		return ErrServiceProfileNotFound
	}

	delete(b.resourceTags, sp.ARN)
	delete(b.serviceProfiles, key)

	return nil
}

// CreateDestination creates a new destination.
func (b *InMemoryBackend) CreateDestination(
	accountID, region, name, expression, expressionType, roleArn, description string,
	tags map[string]string,
) (*Destination, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	arn := destinationARN(region, accountID, name)

	tagsCopy := make(map[string]string, len(tags))
	maps.Copy(tagsCopy, tags)

	dest := &Destination{
		Name:           name,
		ARN:            arn,
		Expression:     expression,
		ExpressionType: expressionType,
		RoleArn:        roleArn,
		Description:    description,
		Tags:           tagsCopy,
		CreatedAt:      time.Now(),
	}

	key := resourceKey{AccountID: accountID, Region: region, ID: name}
	b.destinations[key] = dest

	b.resourceTags[arn] = make(map[string]string, len(tags))
	maps.Copy(b.resourceTags[arn], tags)

	return dest, nil
}

// GetDestination returns a destination by name.
func (b *InMemoryBackend) GetDestination(accountID, region, name string) (*Destination, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	key := resourceKey{AccountID: accountID, Region: region, ID: name}

	dest, ok := b.destinations[key]
	if !ok {
		return nil, ErrDestinationNotFound
	}

	return dest, nil
}

// ListDestinations returns all destinations for the given account and region.
func (b *InMemoryBackend) ListDestinations(accountID, region string) []*Destination {
	b.mu.RLock()
	defer b.mu.RUnlock()

	var result []*Destination

	for k, dest := range b.destinations {
		if k.AccountID == accountID && k.Region == region {
			result = append(result, dest)
		}
	}

	return result
}

// DeleteDestination deletes a destination by name.
func (b *InMemoryBackend) DeleteDestination(accountID, region, name string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	key := resourceKey{AccountID: accountID, Region: region, ID: name}

	dest, ok := b.destinations[key]
	if !ok {
		return ErrDestinationNotFound
	}

	delete(b.resourceTags, dest.ARN)
	delete(b.destinations, key)

	return nil
}

// TagResource adds or updates tags on a resource identified by ARN.
func (b *InMemoryBackend) TagResource(arn string, tags map[string]string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, ok := b.resourceTags[arn]; !ok {
		b.resourceTags[arn] = make(map[string]string)
	}

	maps.Copy(b.resourceTags[arn], tags)

	return nil
}

// UntagResource removes tags from a resource identified by ARN.
func (b *InMemoryBackend) UntagResource(arn string, tagKeys []string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, ok := b.resourceTags[arn]; !ok {
		return nil
	}

	for _, k := range tagKeys {
		delete(b.resourceTags[arn], k)
	}

	return nil
}

// ListTagsForResource returns all tags for a resource identified by ARN.
func (b *InMemoryBackend) ListTagsForResource(arn string) (map[string]string, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	tags, ok := b.resourceTags[arn]
	if !ok {
		return map[string]string{}, nil
	}

	result := make(map[string]string, len(tags))
	maps.Copy(result, tags)

	return result, nil
}

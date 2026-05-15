package iotwireless

import (
	"cmp"
	"errors"
	"fmt"
	"maps"
	"slices"
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
	// ErrDeviceProfileNotFound is returned when a device profile does not exist.
	ErrDeviceProfileNotFound = errors.New("ResourceNotFoundException: Device profile not found")
	// ErrFuotaTaskNotFound is returned when a FUOTA task does not exist.
	ErrFuotaTaskNotFound = errors.New("ResourceNotFoundException: FUOTA task not found")
	// ErrMulticastGroupNotFound is returned when a multicast group does not exist.
	ErrMulticastGroupNotFound = errors.New("ResourceNotFoundException: Multicast group not found")
	// ErrNetworkAnalyzerConfigNotFound is returned when a network analyzer configuration does not exist.
	ErrNetworkAnalyzerConfigNotFound = errors.New("ResourceNotFoundException: Network analyzer configuration not found")
	// ErrValidation is returned when a request contains invalid parameters.
	ErrValidation = errors.New("ValidationException: invalid request parameters")
)

// StorageBackend is the interface for the IoT Wireless backend.
type StorageBackend interface {
	Reset()

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

	CreateDeviceProfile(accountID, region, name string, tags map[string]string) (*DeviceProfile, error)
	GetDeviceProfile(accountID, region, id string) (*DeviceProfile, error)
	ListDeviceProfiles(accountID, region string) []*DeviceProfile
	DeleteDeviceProfile(accountID, region, id string) error

	CreateFuotaTask(
		accountID, region, name, description, firmwareUpdateImage, firmwareUpdateRole string,
		tags map[string]string,
	) (*FuotaTask, error)
	GetFuotaTask(accountID, region, id string) (*FuotaTask, error)
	ListFuotaTasks(accountID, region string) []*FuotaTask
	DeleteFuotaTask(accountID, region, id string) error
	UpdateFuotaTask(accountID, region, id, name, description string) error

	UpdateWirelessGateway(accountID, region, id, name, description string) error

	UpdateDestination(accountID, region, name, expression, expressionType, roleArn, description string) error

	CreateMulticastGroup(accountID, region, name, description string, tags map[string]string) (*MulticastGroup, error)
	GetMulticastGroup(accountID, region, id string) (*MulticastGroup, error)
	ListMulticastGroups(accountID, region string) []*MulticastGroup
	DeleteMulticastGroup(accountID, region, id string) error
	UpdateMulticastGroup(accountID, region, id, name, description string) error

	CreateNetworkAnalyzerConfig(
		accountID, region, name, description string,
		wirelessDevices, wirelessGateways []string,
		tags map[string]string,
	) (*NetworkAnalyzerConfig, error)
	GetNetworkAnalyzerConfig(accountID, region, name string) (*NetworkAnalyzerConfig, error)
	ListNetworkAnalyzerConfigs(accountID, region string) []*NetworkAnalyzerConfig
	DeleteNetworkAnalyzerConfig(accountID, region, name string) error
	UpdateNetworkAnalyzerConfig(
		accountID, region, name, description string,
		wirelessDevices, wirelessGateways []string,
	) error

	AssociateAwsAccountWithPartnerAccount(
		accountID, region, partnerAccountID string,
		tags map[string]string,
	) (string, error)
	AssociateMulticastGroupWithFuotaTask(fuotaTaskID, multicastGroupID string) error
	AssociateWirelessDeviceWithFuotaTask(fuotaTaskID, wirelessDeviceID string) error
	AssociateWirelessDeviceWithMulticastGroup(multicastGroupID, wirelessDeviceID string) error
	AssociateWirelessDeviceWithThing(accountID, region, wirelessDeviceID, thingArn string) error
	AssociateWirelessGatewayWithCertificate(accountID, region, gatewayID, iotCertificateID string) (string, error)
	AssociateWirelessGatewayWithThing(accountID, region, gatewayID, thingArn string) error
	CancelMulticastGroupSession(multicastGroupID string) error

	TagResource(arn string, tags map[string]string) error
	UntagResource(arn string, tagKeys []string) error
	ListTagsForResource(arn string) (map[string]string, error)
}

// Compile-time assertion that InMemoryBackend satisfies StorageBackend.
var _ StorageBackend = (*InMemoryBackend)(nil)

// resourceKey uniquely identifies a resource within an account and region.
type resourceKey struct {
	AccountID string
	Region    string
	ID        string
}

// InMemoryBackend is the in-memory backend for IoT Wireless.
type InMemoryBackend struct {
	devices                map[resourceKey]*WirelessDevice
	gateways               map[resourceKey]*WirelessGateway
	serviceProfiles        map[resourceKey]*ServiceProfile
	destinations           map[resourceKey]*Destination
	deviceProfiles         map[resourceKey]*DeviceProfile
	fuotaTasks             map[resourceKey]*FuotaTask
	multicastGroups        map[resourceKey]*MulticastGroup
	networkAnalyzerConfigs map[resourceKey]*NetworkAnalyzerConfig
	resourceTags           map[string]map[string]string
	partnerAccounts        map[string]string // partnerAccountID -> arn
	fuotaTaskMulticast     map[string]string // fuotaTaskID -> multicastGroupID
	fuotaTaskDevices       map[string]string // fuotaTaskID -> wirelessDeviceID
	multicastGroupDevices  map[string]string // multicastGroupID -> wirelessDeviceID
	multicastGroupSessions map[string]bool   // multicastGroupIDs with active sessions
	wirelessDeviceThings   map[string]string // wirelessDeviceID -> thingArn
	wirelessGatewayCerts   map[string]string // gatewayID -> iotCertificateID
	wirelessGatewayThings  map[string]string // gatewayID -> thingArn
	mu                     sync.RWMutex
}

// NewInMemoryBackend creates a new in-memory IoT Wireless backend.
func NewInMemoryBackend() *InMemoryBackend {
	return &InMemoryBackend{
		devices:                make(map[resourceKey]*WirelessDevice),
		gateways:               make(map[resourceKey]*WirelessGateway),
		serviceProfiles:        make(map[resourceKey]*ServiceProfile),
		destinations:           make(map[resourceKey]*Destination),
		deviceProfiles:         make(map[resourceKey]*DeviceProfile),
		fuotaTasks:             make(map[resourceKey]*FuotaTask),
		multicastGroups:        make(map[resourceKey]*MulticastGroup),
		networkAnalyzerConfigs: make(map[resourceKey]*NetworkAnalyzerConfig),
		resourceTags:           make(map[string]map[string]string),
		partnerAccounts:        make(map[string]string),
		fuotaTaskMulticast:     make(map[string]string),
		fuotaTaskDevices:       make(map[string]string),
		multicastGroupDevices:  make(map[string]string),
		multicastGroupSessions: make(map[string]bool),
		wirelessDeviceThings:   make(map[string]string),
		wirelessGatewayCerts:   make(map[string]string),
		wirelessGatewayThings:  make(map[string]string),
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

func deviceProfileARN(region, accountID, id string) string {
	return fmt.Sprintf("arn:aws:iotwireless:%s:%s:DeviceProfile/%s", region, accountID, id)
}

func fuotaTaskARN(region, accountID, id string) string {
	return fmt.Sprintf("arn:aws:iotwireless:%s:%s:FuotaTask/%s", region, accountID, id)
}

func partnerAccountARN(accountID, region, partnerAccountID string) string {
	return fmt.Sprintf("arn:aws:iotwireless:%s:%s:PartnerAccount/%s", region, accountID, partnerAccountID)
}

func iotCertificateARN(accountID, region, certID string) string {
	return fmt.Sprintf("arn:aws:iot:%s:%s:cert/%s", region, accountID, certID)
}

// Reset clears all in-memory state, returning the backend to a pristine condition.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.devices = make(map[resourceKey]*WirelessDevice)
	b.gateways = make(map[resourceKey]*WirelessGateway)
	b.serviceProfiles = make(map[resourceKey]*ServiceProfile)
	b.destinations = make(map[resourceKey]*Destination)
	b.deviceProfiles = make(map[resourceKey]*DeviceProfile)
	b.fuotaTasks = make(map[resourceKey]*FuotaTask)
	b.multicastGroups = make(map[resourceKey]*MulticastGroup)
	b.networkAnalyzerConfigs = make(map[resourceKey]*NetworkAnalyzerConfig)
	b.resourceTags = make(map[string]map[string]string)
	b.partnerAccounts = make(map[string]string)
	b.fuotaTaskMulticast = make(map[string]string)
	b.fuotaTaskDevices = make(map[string]string)
	b.multicastGroupDevices = make(map[string]string)
	b.multicastGroupSessions = make(map[string]bool)
	b.wirelessDeviceThings = make(map[string]string)
	b.wirelessGatewayCerts = make(map[string]string)
	b.wirelessGatewayThings = make(map[string]string)
}

// copyWirelessDevice returns a shallow copy of d with an independent Tags map.
func copyWirelessDevice(d *WirelessDevice) *WirelessDevice {
	cp := *d
	cp.Tags = make(map[string]string, len(d.Tags))
	maps.Copy(cp.Tags, d.Tags)

	return &cp
}

// copyWirelessGateway returns a shallow copy of gw with an independent Tags map.
func copyWirelessGateway(gw *WirelessGateway) *WirelessGateway {
	cp := *gw
	cp.Tags = make(map[string]string, len(gw.Tags))
	maps.Copy(cp.Tags, gw.Tags)

	return &cp
}

// copyServiceProfile returns a shallow copy of sp with an independent Tags map.
func copyServiceProfile(sp *ServiceProfile) *ServiceProfile {
	cp := *sp
	cp.Tags = make(map[string]string, len(sp.Tags))
	maps.Copy(cp.Tags, sp.Tags)

	return &cp
}

// copyDestination returns a shallow copy of dest with an independent Tags map.
func copyDestination(dest *Destination) *Destination {
	cp := *dest
	cp.Tags = make(map[string]string, len(dest.Tags))
	maps.Copy(cp.Tags, dest.Tags)

	return &cp
}

// copyDeviceProfile returns a shallow copy of dp with an independent Tags map.
func copyDeviceProfile(dp *DeviceProfile) *DeviceProfile {
	cp := *dp
	cp.Tags = make(map[string]string, len(dp.Tags))
	maps.Copy(cp.Tags, dp.Tags)

	return &cp
}

// copyFuotaTask returns a shallow copy of ft with an independent Tags map.
func copyFuotaTask(ft *FuotaTask) *FuotaTask {
	cp := *ft
	cp.Tags = make(map[string]string, len(ft.Tags))
	maps.Copy(cp.Tags, ft.Tags)

	return &cp
}

// newTagsCopy returns a copy of the provided tag map.
// An empty non-nil map is returned for nil input.
func newTagsCopy(tags map[string]string) map[string]string {
	cp := make(map[string]string, len(tags))
	maps.Copy(cp, tags)

	return cp
}

// storeResourceTagsLocked initialises the resource tag entry for the given ARN.
// Must be called with b.mu held for writing.
func (b *InMemoryBackend) storeResourceTagsLocked(arn string, tags map[string]string) {
	b.resourceTags[arn] = newTagsCopy(tags)
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

	d := &WirelessDevice{
		ID:              id,
		ARN:             arn,
		Name:            name,
		Type:            devType,
		DestinationName: destinationName,
		Description:     description,
		Tags:            newTagsCopy(tags),
		CreatedAt:       time.Now(),
	}

	key := resourceKey{AccountID: accountID, Region: region, ID: id}
	b.devices[key] = d
	b.storeResourceTagsLocked(arn, tags)

	return copyWirelessDevice(d), nil
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

	return copyWirelessDevice(d), nil
}

// ListWirelessDevices returns all wireless devices for the given account and region,
// sorted by name for deterministic output.
func (b *InMemoryBackend) ListWirelessDevices(accountID, region string) []*WirelessDevice {
	b.mu.RLock()
	defer b.mu.RUnlock()

	result := make([]*WirelessDevice, 0, len(b.devices))

	for k, d := range b.devices {
		if k.AccountID == accountID && k.Region == region {
			result = append(result, copyWirelessDevice(d))
		}
	}

	slices.SortFunc(result, func(a, b *WirelessDevice) int {
		return cmp.Compare(a.Name, b.Name)
	})

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

	gw := &WirelessGateway{
		ID:          id,
		ARN:         arn,
		Name:        name,
		Description: description,
		Tags:        newTagsCopy(tags),
		CreatedAt:   time.Now(),
	}

	key := resourceKey{AccountID: accountID, Region: region, ID: id}
	b.gateways[key] = gw
	b.storeResourceTagsLocked(arn, tags)

	return copyWirelessGateway(gw), nil
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

	return copyWirelessGateway(gw), nil
}

// ListWirelessGateways returns all wireless gateways for the given account and region,
// sorted by name for deterministic output.
func (b *InMemoryBackend) ListWirelessGateways(accountID, region string) []*WirelessGateway {
	b.mu.RLock()
	defer b.mu.RUnlock()

	result := make([]*WirelessGateway, 0, len(b.gateways))

	for k, gw := range b.gateways {
		if k.AccountID == accountID && k.Region == region {
			result = append(result, copyWirelessGateway(gw))
		}
	}

	slices.SortFunc(result, func(a, b *WirelessGateway) int {
		return cmp.Compare(a.Name, b.Name)
	})

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

	sp := &ServiceProfile{
		ID:        id,
		ARN:       arn,
		Name:      name,
		Tags:      newTagsCopy(tags),
		CreatedAt: time.Now(),
	}

	key := resourceKey{AccountID: accountID, Region: region, ID: id}
	b.serviceProfiles[key] = sp
	b.storeResourceTagsLocked(arn, tags)

	return copyServiceProfile(sp), nil
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

	return copyServiceProfile(sp), nil
}

// ListServiceProfiles returns all service profiles for the given account and region,
// sorted by name for deterministic output.
func (b *InMemoryBackend) ListServiceProfiles(accountID, region string) []*ServiceProfile {
	b.mu.RLock()
	defer b.mu.RUnlock()

	result := make([]*ServiceProfile, 0, len(b.serviceProfiles))

	for k, sp := range b.serviceProfiles {
		if k.AccountID == accountID && k.Region == region {
			result = append(result, copyServiceProfile(sp))
		}
	}

	slices.SortFunc(result, func(a, b *ServiceProfile) int {
		return cmp.Compare(a.Name, b.Name)
	})

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

	dest := &Destination{
		Name:           name,
		ARN:            arn,
		Expression:     expression,
		ExpressionType: expressionType,
		RoleArn:        roleArn,
		Description:    description,
		Tags:           newTagsCopy(tags),
		CreatedAt:      time.Now(),
	}

	key := resourceKey{AccountID: accountID, Region: region, ID: name}
	b.destinations[key] = dest
	b.storeResourceTagsLocked(arn, tags)

	return copyDestination(dest), nil
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

	return copyDestination(dest), nil
}

// ListDestinations returns all destinations for the given account and region.
// ListDestinations returns all destinations for the given account and region,
// sorted by name for deterministic output.
func (b *InMemoryBackend) ListDestinations(accountID, region string) []*Destination {
	b.mu.RLock()
	defer b.mu.RUnlock()

	result := make([]*Destination, 0, len(b.destinations))

	for k, dest := range b.destinations {
		if k.AccountID == accountID && k.Region == region {
			result = append(result, copyDestination(dest))
		}
	}

	slices.SortFunc(result, func(a, b *Destination) int {
		return cmp.Compare(a.Name, b.Name)
	})

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
// If all tags are removed the empty map entry is cleaned up to prevent memory leaks.
func (b *InMemoryBackend) UntagResource(arn string, tagKeys []string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, ok := b.resourceTags[arn]; !ok {
		return nil
	}

	for _, k := range tagKeys {
		delete(b.resourceTags[arn], k)
	}

	if len(b.resourceTags[arn]) == 0 {
		delete(b.resourceTags, arn)
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

// CreateDeviceProfile creates a new device profile.
func (b *InMemoryBackend) CreateDeviceProfile(
	accountID, region, name string,
	tags map[string]string,
) (*DeviceProfile, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	id := uuid.NewString()
	arn := deviceProfileARN(region, accountID, id)

	dp := &DeviceProfile{
		ID:        id,
		ARN:       arn,
		Name:      name,
		Tags:      newTagsCopy(tags),
		CreatedAt: time.Now(),
	}

	key := resourceKey{AccountID: accountID, Region: region, ID: id}
	b.deviceProfiles[key] = dp
	b.storeResourceTagsLocked(arn, tags)

	return copyDeviceProfile(dp), nil
}

// GetDeviceProfile returns a device profile by ID.
func (b *InMemoryBackend) GetDeviceProfile(accountID, region, id string) (*DeviceProfile, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	key := resourceKey{AccountID: accountID, Region: region, ID: id}

	dp, ok := b.deviceProfiles[key]
	if !ok {
		return nil, ErrDeviceProfileNotFound
	}

	return copyDeviceProfile(dp), nil
}

// ListDeviceProfiles returns all device profiles for the given account and region,
// sorted by name for deterministic output.
func (b *InMemoryBackend) ListDeviceProfiles(accountID, region string) []*DeviceProfile {
	b.mu.RLock()
	defer b.mu.RUnlock()

	result := make([]*DeviceProfile, 0, len(b.deviceProfiles))

	for k, dp := range b.deviceProfiles {
		if k.AccountID == accountID && k.Region == region {
			result = append(result, copyDeviceProfile(dp))
		}
	}

	slices.SortFunc(result, func(a, b *DeviceProfile) int {
		return cmp.Compare(a.Name, b.Name)
	})

	return result
}

// DeleteDeviceProfile deletes a device profile by ID.
func (b *InMemoryBackend) DeleteDeviceProfile(accountID, region, id string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	key := resourceKey{AccountID: accountID, Region: region, ID: id}

	dp, ok := b.deviceProfiles[key]
	if !ok {
		return ErrDeviceProfileNotFound
	}

	delete(b.resourceTags, dp.ARN)
	delete(b.deviceProfiles, key)

	return nil
}

// CreateFuotaTask creates a new FUOTA task.
func (b *InMemoryBackend) CreateFuotaTask(
	accountID, region, name, description, firmwareUpdateImage, firmwareUpdateRole string,
	tags map[string]string,
) (*FuotaTask, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	id := uuid.NewString()
	arn := fuotaTaskARN(region, accountID, id)

	ft := &FuotaTask{
		ID:                  id,
		ARN:                 arn,
		Name:                name,
		Description:         description,
		FirmwareUpdateImage: firmwareUpdateImage,
		FirmwareUpdateRole:  firmwareUpdateRole,
		Tags:                newTagsCopy(tags),
		CreatedAt:           time.Now(),
	}

	key := resourceKey{AccountID: accountID, Region: region, ID: id}
	b.fuotaTasks[key] = ft
	b.storeResourceTagsLocked(arn, tags)

	return copyFuotaTask(ft), nil
}

// GetFuotaTask returns a FUOTA task by ID.
func (b *InMemoryBackend) GetFuotaTask(accountID, region, id string) (*FuotaTask, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	key := resourceKey{AccountID: accountID, Region: region, ID: id}

	ft, ok := b.fuotaTasks[key]
	if !ok {
		return nil, ErrFuotaTaskNotFound
	}

	return copyFuotaTask(ft), nil
}

// ListFuotaTasks returns all FUOTA tasks for the given account and region,
// sorted by name for deterministic output.
func (b *InMemoryBackend) ListFuotaTasks(accountID, region string) []*FuotaTask {
	b.mu.RLock()
	defer b.mu.RUnlock()

	result := make([]*FuotaTask, 0, len(b.fuotaTasks))

	for k, ft := range b.fuotaTasks {
		if k.AccountID == accountID && k.Region == region {
			result = append(result, copyFuotaTask(ft))
		}
	}

	slices.SortFunc(result, func(a, b *FuotaTask) int {
		return cmp.Compare(a.Name, b.Name)
	})

	return result
}

// DeleteFuotaTask deletes a FUOTA task by ID.
func (b *InMemoryBackend) DeleteFuotaTask(accountID, region, id string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	key := resourceKey{AccountID: accountID, Region: region, ID: id}

	ft, ok := b.fuotaTasks[key]
	if !ok {
		return ErrFuotaTaskNotFound
	}

	delete(b.resourceTags, ft.ARN)
	delete(b.fuotaTasks, key)

	return nil
}

// UpdateFuotaTask updates mutable fields on an existing FUOTA task.
func (b *InMemoryBackend) UpdateFuotaTask(accountID, region, id, name, description string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	key := resourceKey{AccountID: accountID, Region: region, ID: id}

	ft, ok := b.fuotaTasks[key]
	if !ok {
		return ErrFuotaTaskNotFound
	}

	if name != "" {
		ft.Name = name
	}

	ft.Description = description

	return nil
}

// UpdateWirelessGateway updates mutable fields on an existing wireless gateway.
func (b *InMemoryBackend) UpdateWirelessGateway(accountID, region, id, name, description string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	key := resourceKey{AccountID: accountID, Region: region, ID: id}

	gw, ok := b.gateways[key]
	if !ok {
		return ErrGatewayNotFound
	}

	if name != "" {
		gw.Name = name
	}

	gw.Description = description

	return nil
}

// UpdateDestination updates mutable fields on an existing destination.
func (b *InMemoryBackend) UpdateDestination(
	accountID, region, name, expression, expressionType, roleArn, description string,
) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	key := resourceKey{AccountID: accountID, Region: region, ID: name}

	dest, ok := b.destinations[key]
	if !ok {
		return ErrDestinationNotFound
	}

	if expression != "" {
		dest.Expression = expression
	}

	if expressionType != "" {
		dest.ExpressionType = expressionType
	}

	if roleArn != "" {
		dest.RoleArn = roleArn
	}

	dest.Description = description

	return nil
}

func multicastGroupARN(region, accountID, id string) string {
	return fmt.Sprintf("arn:aws:iotwireless:%s:%s:MulticastGroup/%s", region, accountID, id)
}

func networkAnalyzerConfigARN(region, accountID, name string) string {
	return fmt.Sprintf("arn:aws:iotwireless:%s:%s:NetworkAnalyzerConfiguration/%s", region, accountID, name)
}

func copyMulticastGroup(mg *MulticastGroup) *MulticastGroup {
	cp := *mg
	cp.Tags = make(map[string]string, len(mg.Tags))
	maps.Copy(cp.Tags, mg.Tags)

	return &cp
}

func copyNetworkAnalyzerConfig(nc *NetworkAnalyzerConfig) *NetworkAnalyzerConfig {
	cp := *nc
	cp.Tags = make(map[string]string, len(nc.Tags))
	maps.Copy(cp.Tags, nc.Tags)

	if nc.WirelessDevices != nil {
		cp.WirelessDevices = make([]string, len(nc.WirelessDevices))
		copy(cp.WirelessDevices, nc.WirelessDevices)
	}

	if nc.WirelessGateways != nil {
		cp.WirelessGateways = make([]string, len(nc.WirelessGateways))
		copy(cp.WirelessGateways, nc.WirelessGateways)
	}

	return &cp
}

// CreateMulticastGroup creates a new multicast group.
func (b *InMemoryBackend) CreateMulticastGroup(
	accountID, region, name, description string,
	tags map[string]string,
) (*MulticastGroup, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	id := uuid.NewString()
	arn := multicastGroupARN(region, accountID, id)

	mg := &MulticastGroup{
		ID:          id,
		ARN:         arn,
		Name:        name,
		Description: description,
		Status:      "Pending",
		Tags:        newTagsCopy(tags),
		CreatedAt:   time.Now(),
	}

	key := resourceKey{AccountID: accountID, Region: region, ID: id}
	b.multicastGroups[key] = mg
	b.storeResourceTagsLocked(arn, tags)

	return copyMulticastGroup(mg), nil
}

// GetMulticastGroup returns a multicast group by ID.
func (b *InMemoryBackend) GetMulticastGroup(accountID, region, id string) (*MulticastGroup, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	key := resourceKey{AccountID: accountID, Region: region, ID: id}

	mg, ok := b.multicastGroups[key]
	if !ok {
		return nil, ErrMulticastGroupNotFound
	}

	return copyMulticastGroup(mg), nil
}

// ListMulticastGroups returns all multicast groups for the given account and region,
// sorted by name for deterministic output.
func (b *InMemoryBackend) ListMulticastGroups(accountID, region string) []*MulticastGroup {
	b.mu.RLock()
	defer b.mu.RUnlock()

	result := make([]*MulticastGroup, 0, len(b.multicastGroups))

	for k, mg := range b.multicastGroups {
		if k.AccountID == accountID && k.Region == region {
			result = append(result, copyMulticastGroup(mg))
		}
	}

	slices.SortFunc(result, func(a, b *MulticastGroup) int {
		return cmp.Compare(a.Name, b.Name)
	})

	return result
}

// DeleteMulticastGroup deletes a multicast group by ID.
func (b *InMemoryBackend) DeleteMulticastGroup(accountID, region, id string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	key := resourceKey{AccountID: accountID, Region: region, ID: id}

	mg, ok := b.multicastGroups[key]
	if !ok {
		return ErrMulticastGroupNotFound
	}

	delete(b.resourceTags, mg.ARN)
	delete(b.multicastGroups, key)

	return nil
}

// UpdateMulticastGroup updates mutable fields on an existing multicast group.
func (b *InMemoryBackend) UpdateMulticastGroup(accountID, region, id, name, description string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	key := resourceKey{AccountID: accountID, Region: region, ID: id}

	mg, ok := b.multicastGroups[key]
	if !ok {
		return ErrMulticastGroupNotFound
	}

	if name != "" {
		mg.Name = name
	}

	mg.Description = description

	return nil
}

// CreateNetworkAnalyzerConfig creates a new network analyzer configuration.
func (b *InMemoryBackend) CreateNetworkAnalyzerConfig(
	accountID, region, name, description string,
	wirelessDevices, wirelessGateways []string,
	tags map[string]string,
) (*NetworkAnalyzerConfig, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	arn := networkAnalyzerConfigARN(region, accountID, name)

	nc := &NetworkAnalyzerConfig{
		Name:             name,
		ARN:              arn,
		Description:      description,
		WirelessDevices:  append([]string(nil), wirelessDevices...),
		WirelessGateways: append([]string(nil), wirelessGateways...),
		Tags:             newTagsCopy(tags),
	}

	key := resourceKey{AccountID: accountID, Region: region, ID: name}
	b.networkAnalyzerConfigs[key] = nc
	b.storeResourceTagsLocked(arn, tags)

	return copyNetworkAnalyzerConfig(nc), nil
}

// GetNetworkAnalyzerConfig returns a network analyzer configuration by name.
func (b *InMemoryBackend) GetNetworkAnalyzerConfig(accountID, region, name string) (*NetworkAnalyzerConfig, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	key := resourceKey{AccountID: accountID, Region: region, ID: name}

	nc, ok := b.networkAnalyzerConfigs[key]
	if !ok {
		return nil, ErrNetworkAnalyzerConfigNotFound
	}

	return copyNetworkAnalyzerConfig(nc), nil
}

// ListNetworkAnalyzerConfigs returns all network analyzer configurations for the given account and region,
// sorted by name for deterministic output.
func (b *InMemoryBackend) ListNetworkAnalyzerConfigs(accountID, region string) []*NetworkAnalyzerConfig {
	b.mu.RLock()
	defer b.mu.RUnlock()

	result := make([]*NetworkAnalyzerConfig, 0, len(b.networkAnalyzerConfigs))

	for k, nc := range b.networkAnalyzerConfigs {
		if k.AccountID == accountID && k.Region == region {
			result = append(result, copyNetworkAnalyzerConfig(nc))
		}
	}

	slices.SortFunc(result, func(a, b *NetworkAnalyzerConfig) int {
		return cmp.Compare(a.Name, b.Name)
	})

	return result
}

// DeleteNetworkAnalyzerConfig deletes a network analyzer configuration by name.
func (b *InMemoryBackend) DeleteNetworkAnalyzerConfig(accountID, region, name string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	key := resourceKey{AccountID: accountID, Region: region, ID: name}

	nc, ok := b.networkAnalyzerConfigs[key]
	if !ok {
		return ErrNetworkAnalyzerConfigNotFound
	}

	delete(b.resourceTags, nc.ARN)
	delete(b.networkAnalyzerConfigs, key)

	return nil
}

// UpdateNetworkAnalyzerConfig updates mutable fields on an existing network analyzer configuration.
func (b *InMemoryBackend) UpdateNetworkAnalyzerConfig(
	accountID, region, name, description string,
	wirelessDevices, wirelessGateways []string,
) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	key := resourceKey{AccountID: accountID, Region: region, ID: name}

	nc, ok := b.networkAnalyzerConfigs[key]
	if !ok {
		return ErrNetworkAnalyzerConfigNotFound
	}

	nc.Description = description

	if wirelessDevices != nil {
		nc.WirelessDevices = append([]string(nil), wirelessDevices...)
	}

	if wirelessGateways != nil {
		nc.WirelessGateways = append([]string(nil), wirelessGateways...)
	}

	return nil
}

// AssociateAwsAccountWithPartnerAccount stores a partner account association and returns its ARN.
func (b *InMemoryBackend) AssociateAwsAccountWithPartnerAccount(
	accountID, region, partnerAccountID string,
	tags map[string]string,
) (string, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	arn := partnerAccountARN(accountID, region, partnerAccountID)
	b.partnerAccounts[partnerAccountID] = arn
	b.storeResourceTagsLocked(arn, tags)

	return arn, nil
}

// AssociateMulticastGroupWithFuotaTask records the association of a multicast group with a FUOTA task.
func (b *InMemoryBackend) AssociateMulticastGroupWithFuotaTask(fuotaTaskID, multicastGroupID string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.fuotaTaskMulticast[fuotaTaskID] = multicastGroupID

	return nil
}

// AssociateWirelessDeviceWithFuotaTask records the association of a wireless device with a FUOTA task.
func (b *InMemoryBackend) AssociateWirelessDeviceWithFuotaTask(fuotaTaskID, wirelessDeviceID string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.fuotaTaskDevices[fuotaTaskID] = wirelessDeviceID

	return nil
}

// AssociateWirelessDeviceWithMulticastGroup records the association of a wireless device with a multicast group.
func (b *InMemoryBackend) AssociateWirelessDeviceWithMulticastGroup(multicastGroupID, wirelessDeviceID string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.multicastGroupDevices[multicastGroupID] = wirelessDeviceID

	return nil
}

// AssociateWirelessDeviceWithThing associates a wireless device with an IoT Thing.
// Returns ErrDeviceNotFound when the device does not exist.
func (b *InMemoryBackend) AssociateWirelessDeviceWithThing(
	accountID, region, wirelessDeviceID, thingArn string,
) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	key := resourceKey{AccountID: accountID, Region: region, ID: wirelessDeviceID}
	if _, ok := b.devices[key]; !ok {
		return ErrDeviceNotFound
	}

	b.wirelessDeviceThings[wirelessDeviceID] = thingArn

	return nil
}

// AssociateWirelessGatewayWithCertificate associates a wireless gateway with an IoT certificate
// and returns the certificate ARN. Returns ErrGatewayNotFound when the gateway does not exist.
func (b *InMemoryBackend) AssociateWirelessGatewayWithCertificate(
	accountID, region, gatewayID, iotCertificateID string,
) (string, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	key := resourceKey{AccountID: accountID, Region: region, ID: gatewayID}
	if _, ok := b.gateways[key]; !ok {
		return "", ErrGatewayNotFound
	}

	b.wirelessGatewayCerts[gatewayID] = iotCertificateID
	certARN := iotCertificateARN(accountID, region, iotCertificateID)

	return certARN, nil
}

// AssociateWirelessGatewayWithThing associates a wireless gateway with an IoT Thing.
// Returns ErrGatewayNotFound when the gateway does not exist.
func (b *InMemoryBackend) AssociateWirelessGatewayWithThing(
	accountID, region, gatewayID, thingArn string,
) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	key := resourceKey{AccountID: accountID, Region: region, ID: gatewayID}
	if _, ok := b.gateways[key]; !ok {
		return ErrGatewayNotFound
	}

	b.wirelessGatewayThings[gatewayID] = thingArn

	return nil
}

// CancelMulticastGroupSession marks the multicast group session as cancelled.
// If no session is active, the call is a no-op (idempotent).
func (b *InMemoryBackend) CancelMulticastGroupSession(multicastGroupID string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	delete(b.multicastGroupSessions, multicastGroupID)

	return nil
}

// --- Seed helpers for testing ---

// AddWirelessDeviceInternal inserts a WirelessDevice directly into the backend, bypassing ID generation.
// Intended for test setup only.
func (b *InMemoryBackend) AddWirelessDeviceInternal(accountID, region string, d *WirelessDevice) {
	b.mu.Lock()
	defer b.mu.Unlock()

	cp := copyWirelessDevice(d)
	key := resourceKey{AccountID: accountID, Region: region, ID: d.ID}
	b.devices[key] = cp
	b.storeResourceTagsLocked(d.ARN, d.Tags)
}

// AddWirelessGatewayInternal inserts a WirelessGateway directly into the backend, bypassing ID generation.
// Intended for test setup only.
func (b *InMemoryBackend) AddWirelessGatewayInternal(accountID, region string, gw *WirelessGateway) {
	b.mu.Lock()
	defer b.mu.Unlock()

	cp := copyWirelessGateway(gw)
	key := resourceKey{AccountID: accountID, Region: region, ID: gw.ID}
	b.gateways[key] = cp
	b.storeResourceTagsLocked(gw.ARN, gw.Tags)
}

// AddServiceProfileInternal inserts a ServiceProfile directly into the backend, bypassing ID generation.
// Intended for test setup only.
func (b *InMemoryBackend) AddServiceProfileInternal(accountID, region string, sp *ServiceProfile) {
	b.mu.Lock()
	defer b.mu.Unlock()

	cp := copyServiceProfile(sp)
	key := resourceKey{AccountID: accountID, Region: region, ID: sp.ID}
	b.serviceProfiles[key] = cp
	b.storeResourceTagsLocked(sp.ARN, sp.Tags)
}

// AddDestinationInternal inserts a Destination directly into the backend, bypassing ID generation.
// Intended for test setup only.
func (b *InMemoryBackend) AddDestinationInternal(accountID, region string, dest *Destination) {
	b.mu.Lock()
	defer b.mu.Unlock()

	cp := copyDestination(dest)
	key := resourceKey{AccountID: accountID, Region: region, ID: dest.Name}
	b.destinations[key] = cp
	b.storeResourceTagsLocked(dest.ARN, dest.Tags)
}

// AddDeviceProfileInternal inserts a DeviceProfile directly into the backend, bypassing ID generation.
// Intended for test setup only.
func (b *InMemoryBackend) AddDeviceProfileInternal(accountID, region string, dp *DeviceProfile) {
	b.mu.Lock()
	defer b.mu.Unlock()

	cp := copyDeviceProfile(dp)
	key := resourceKey{AccountID: accountID, Region: region, ID: dp.ID}
	b.deviceProfiles[key] = cp
	b.storeResourceTagsLocked(dp.ARN, dp.Tags)
}

// AddFuotaTaskInternal inserts a FuotaTask directly into the backend, bypassing ID generation.
// Intended for test setup only.
func (b *InMemoryBackend) AddFuotaTaskInternal(accountID, region string, ft *FuotaTask) {
	b.mu.Lock()
	defer b.mu.Unlock()

	cp := copyFuotaTask(ft)
	key := resourceKey{AccountID: accountID, Region: region, ID: ft.ID}
	b.fuotaTasks[key] = cp
	b.storeResourceTagsLocked(ft.ARN, ft.Tags)
}

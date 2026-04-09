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
	// ErrDeviceProfileNotFound is returned when a device profile does not exist.
	ErrDeviceProfileNotFound = errors.New("ResourceNotFoundException: Device profile not found")
	// ErrFuotaTaskNotFound is returned when a FUOTA task does not exist.
	ErrFuotaTaskNotFound = errors.New("ResourceNotFoundException: FUOTA task not found")
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

	CreateDeviceProfile(accountID, region, name string, tags map[string]string) (*DeviceProfile, error)

	CreateFuotaTask(
		accountID, region, name, description, firmwareUpdateImage, firmwareUpdateRole string,
		tags map[string]string,
	) (*FuotaTask, error)

	AssociateAwsAccountWithPartnerAccount(accountID, partnerAccountID string, tags map[string]string) (string, error)
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

func partnerAccountARN(accountID, partnerAccountID string) string {
	return fmt.Sprintf("arn:aws:iotwireless:us-east-1:%s:PartnerAccount/%s", accountID, partnerAccountID)
}

func iotCertificateARN(accountID, certID string) string {
	return fmt.Sprintf("arn:aws:iot:us-east-1:%s:cert/%s", accountID, certID)
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

// ListWirelessDevices returns all wireless devices for the given account and region.
func (b *InMemoryBackend) ListWirelessDevices(accountID, region string) []*WirelessDevice {
	b.mu.RLock()
	defer b.mu.RUnlock()

	var result []*WirelessDevice

	for k, d := range b.devices {
		if k.AccountID == accountID && k.Region == region {
			result = append(result, copyWirelessDevice(d))
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

// ListWirelessGateways returns all wireless gateways for the given account and region.
func (b *InMemoryBackend) ListWirelessGateways(accountID, region string) []*WirelessGateway {
	b.mu.RLock()
	defer b.mu.RUnlock()

	var result []*WirelessGateway

	for k, gw := range b.gateways {
		if k.AccountID == accountID && k.Region == region {
			result = append(result, copyWirelessGateway(gw))
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

// ListServiceProfiles returns all service profiles for the given account and region.
func (b *InMemoryBackend) ListServiceProfiles(accountID, region string) []*ServiceProfile {
	b.mu.RLock()
	defer b.mu.RUnlock()

	var result []*ServiceProfile

	for k, sp := range b.serviceProfiles {
		if k.AccountID == accountID && k.Region == region {
			result = append(result, copyServiceProfile(sp))
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
func (b *InMemoryBackend) ListDestinations(accountID, region string) []*Destination {
	b.mu.RLock()
	defer b.mu.RUnlock()

	var result []*Destination

	for k, dest := range b.destinations {
		if k.AccountID == accountID && k.Region == region {
			result = append(result, copyDestination(dest))
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

// AssociateAwsAccountWithPartnerAccount stores a partner account association and returns its ARN.
func (b *InMemoryBackend) AssociateAwsAccountWithPartnerAccount(
	accountID, partnerAccountID string,
	tags map[string]string,
) (string, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	arn := partnerAccountARN(accountID, partnerAccountID)
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
	certARN := iotCertificateARN(accountID, iotCertificateID)

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

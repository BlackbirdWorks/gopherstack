package servicediscovery

import (
	"encoding/base64"
	"fmt"
	"maps"
	"math/rand/v2"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

const (
	typeNamespace = "NAMESPACE"
	typeService   = "SERVICE"
	typeInstance  = "INSTANCE"
)

var (
	// ErrNamespaceNotFound is returned when a namespace does not exist.
	ErrNamespaceNotFound = awserr.New("NamespaceNotFound", awserr.ErrNotFound)
	// ErrServiceNotFound is returned when a service does not exist.
	ErrServiceNotFound = awserr.New("ServiceNotFound", awserr.ErrNotFound)
	// ErrInstanceNotFound is returned when an instance does not exist.
	ErrInstanceNotFound = awserr.New("InstanceNotFound", awserr.ErrNotFound)
	// ErrOperationNotFound is returned when an operation does not exist.
	ErrOperationNotFound = awserr.New("OperationNotFound", awserr.ErrNotFound)
	// ErrNamespaceAlreadyExists is returned when a namespace with the same name already exists.
	ErrNamespaceAlreadyExists = awserr.New("NamespaceAlreadyExists", awserr.ErrAlreadyExists)
	// ErrServiceAttributesNotFound is returned when no attributes exist for a service.
	ErrServiceAttributesNotFound = awserr.New("ServiceAttributesNotFound", awserr.ErrNotFound)
	// ErrInvalidInput is returned when an input value is invalid.
	ErrInvalidInput = awserr.New("InvalidInput", awserr.ErrInvalidParameter)
	// ErrResourceNotFound is returned when a tagged resource ARN is not found.
	ErrResourceNotFound = awserr.New("ResourceNotFoundException", awserr.ErrNotFound)
	// ErrResourceInUse is returned when a delete is attempted on a non-empty namespace or service.
	ErrResourceInUse = awserr.New("ResourceInUse", awserr.ErrConflict)
	// ErrCustomHealthNotFound is returned when UpdateInstanceCustomHealthStatus is called on a
	// service that has no HealthCheckCustomConfig.
	ErrCustomHealthNotFound = awserr.New("CustomHealthNotFound", awserr.ErrNotFound)
)

const (
	namespaceTypeHTTP       = "HTTP"
	namespaceTypeDNSPrivate = "DNS_PRIVATE"
	namespaceTypeDNSPublic  = "DNS_PUBLIC"

	operationStatusSuccess = "SUCCESS"

	operationTypeCreateNamespace    = "CREATE_NAMESPACE"
	operationTypeDeleteNamespace    = "DELETE_NAMESPACE"
	operationTypeUpdateNamespace    = "UPDATE_NAMESPACE"
	operationTypeUpdateService      = "UPDATE_SERVICE"
	operationTypeRegisterInstance   = "REGISTER_INSTANCE"
	operationTypeDeregisterInstance = "DEREGISTER_INSTANCE"

	instanceHealthStatusHealthy   = "HEALTHY"
	instanceHealthStatusUnhealthy = "UNHEALTHY"

	serviceTypeHTTP    = "HTTP"
	serviceTypeDNS     = "DNS"
	serviceTypeDNSHTTP = "DNS_HTTP"

	defaultSOATTL int64 = 15

	maxResultsDefault = 100
	maxResultsCap     = 100
)

// DNSRecord represents a single DNS record configuration in a Cloud Map service.
type DNSRecord struct {
	Type string `json:"type"`
	TTL  int64  `json:"ttl"`
}

// DNSConfig holds the DNS configuration for a Cloud Map service.
type DNSConfig struct {
	NamespaceID   string      `json:"namespaceID,omitempty"`
	RoutingPolicy string      `json:"routingPolicy,omitempty"`
	DNSRecords    []DNSRecord `json:"dnsRecords,omitempty"`
}

// HealthCheckConfig holds the configuration for an AWS-managed HTTP/TCP health check.
type HealthCheckConfig struct {
	Type             string `json:"type"`
	ResourcePath     string `json:"resourcePath,omitempty"`
	FailureThreshold int    `json:"failureThreshold,omitempty"`
}

// HealthCheckCustomConfig holds the configuration for a custom health check.
type HealthCheckCustomConfig struct {
	FailureThreshold int `json:"failureThreshold,omitempty"`
}

// SOA holds the Start of Authority TTL for a DNS namespace.
type SOA struct {
	TTL int64 `json:"ttl"`
}

// DNSProperties holds the DNS-specific properties of a namespace.
type DNSProperties struct {
	SOA          *SOA   `json:"soa,omitempty"`
	HostedZoneID string `json:"hostedZoneId,omitempty"`
}

// HTTPProperties holds the HTTP-specific properties of a namespace.
type HTTPProperties struct {
	HTTPName string `json:"httpName,omitempty"`
}

// NamespaceProperties holds the type-specific properties of a namespace.
type NamespaceProperties struct {
	DNSProperties  *DNSProperties  `json:"dnsProperties,omitempty"`
	HTTPProperties *HTTPProperties `json:"httpProperties,omitempty"`
}

// Namespace represents an AWS Cloud Map namespace.
type Namespace struct {
	CreatedAt    time.Time            `json:"createdAt"`
	Tags         map[string]string    `json:"tags,omitempty"`
	Properties   *NamespaceProperties `json:"properties,omitempty"`
	ID           string               `json:"id"`
	ARN          string               `json:"arn"`
	Name         string               `json:"name"`
	Type         string               `json:"type"`
	Description  string               `json:"description,omitempty"`
	VPC          string               `json:"vpc,omitempty"`
	ServiceCount int                  `json:"serviceCount,omitempty"`
}

// Service represents an AWS Cloud Map service.
type Service struct {
	CreatedAt               time.Time                `json:"createdAt"`
	Tags                    map[string]string        `json:"tags,omitempty"`
	DNSConfig               *DNSConfig               `json:"dnsConfig,omitempty"`
	HealthCheckConfig       *HealthCheckConfig       `json:"healthCheckConfig,omitempty"`
	HealthCheckCustomConfig *HealthCheckCustomConfig `json:"healthCheckCustomConfig,omitempty"`
	ID                      string                   `json:"id"`
	ARN                     string                   `json:"arn"`
	Name                    string                   `json:"name"`
	NamespaceID             string                   `json:"namespaceID"`
	Description             string                   `json:"description,omitempty"`
	Type                    string                   `json:"type,omitempty"`
	InstanceCount           int                      `json:"instanceCount,omitempty"`
}

// Instance represents a registered instance in a Cloud Map service.
type Instance struct {
	Attributes map[string]string `json:"attributes,omitempty"`
	ID         string            `json:"id"`
	ServiceID  string            `json:"serviceID"`
}

// DiscoveredInstance is the richer per-instance response for DiscoverInstances.
type DiscoveredInstance struct {
	Attributes    map[string]string
	InstanceID    string
	NamespaceName string
	ServiceName   string
	HealthStatus  string
}

// Operation represents an async Cloud Map operation (e.g., create/delete namespace).
type Operation struct {
	CreateDate   time.Time         `json:"createDate"`
	UpdateDate   time.Time         `json:"updateDate"`
	Targets      map[string]string `json:"targets,omitempty"`
	ID           string            `json:"id"`
	Type         string            `json:"type"`
	Status       string            `json:"status"`
	ErrorCode    string            `json:"errorCode,omitempty"`
	ErrorMessage string            `json:"errorMessage,omitempty"`
}

// ListNamespacesFilter contains optional filter parameters for ListNamespaces.
type ListNamespacesFilter struct {
	Type string
	Name string
}

// ListServicesFilter contains optional filter parameters for ListServices.
type ListServicesFilter struct {
	NamespaceID string
}

// ListOperationsFilter contains optional filter parameters for ListOperations.
type ListOperationsFilter struct {
	Status string
	Type   string
}

// InMemoryBackend is the in-memory Cloud Map backend.
type InMemoryBackend struct {
	namespaces             map[string]*Namespace
	services               map[string]*Service
	instances              map[string]*Instance
	operations             map[string]*Operation
	serviceAttributes      map[string]map[string]string
	instanceHealthStatuses map[string]string
	instancesByService     map[string]map[string]*Instance
	svcByNsAndName         map[string]string
	nsARNIndex             map[string]string
	svcARNIndex            map[string]string
	nsNameIndex            map[string]string
	mu                     *lockmetrics.RWMutex
	accountID              string
	region                 string
	instanceRevision       int64
	nsCounter              int
	svcCounter             int
	opCounter              int
	deterministicIDs       bool
}

// NewInMemoryBackend creates a new in-memory Cloud Map backend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	return &InMemoryBackend{
		namespaces:             make(map[string]*Namespace),
		services:               make(map[string]*Service),
		instances:              make(map[string]*Instance),
		operations:             make(map[string]*Operation),
		serviceAttributes:      make(map[string]map[string]string),
		instanceHealthStatuses: make(map[string]string),
		instancesByService:     make(map[string]map[string]*Instance),
		svcByNsAndName:         make(map[string]string),
		nsARNIndex:             make(map[string]string),
		svcARNIndex:            make(map[string]string),
		nsNameIndex:            make(map[string]string),
		mu:                     lockmetrics.New("servicediscovery"),
		accountID:              accountID,
		region:                 region,
	}
}

// Region returns the AWS region this backend is configured for.
func (b *InMemoryBackend) Region() string { return b.region }

// AccountID returns the AWS account ID this backend is configured for.
func (b *InMemoryBackend) AccountID() string { return b.accountID }

func (b *InMemoryBackend) namespaceARN(id string) string {
	return arn.Build("servicediscovery", b.region, b.accountID, fmt.Sprintf("namespace/%s", id))
}

func (b *InMemoryBackend) serviceARN(id string) string {
	return arn.Build("servicediscovery", b.region, b.accountID, fmt.Sprintf("service/%s", id))
}

const (
	idChars              = "abcdefghijklmnopqrstuvwxyz0123456789"
	idAlnumLen           = 26
	idOperationSuffixLen = 8
)

func (b *InMemoryBackend) nextNsID() string {
	if b.deterministicIDs {
		b.nsCounter++

		return fmt.Sprintf("ns-%026d", b.nsCounter)
	}

	return "ns-" + randAlnum(idAlnumLen)
}

func (b *InMemoryBackend) nextSvcID() string {
	if b.deterministicIDs {
		b.svcCounter++

		return fmt.Sprintf("srv-%025d", b.svcCounter)
	}

	return "srv-" + randAlnum(idAlnumLen)
}

func (b *InMemoryBackend) nextOpID() string {
	b.opCounter++

	return fmt.Sprintf("op-%08d", b.opCounter)
}

func randAlnum(n int) string {
	buf := make([]byte, n)
	for i := range buf {
		buf[i] = idChars[rand.IntN(len(idChars))] //nolint:gosec // non-cryptographic, math/rand/v2 sufficient
	}

	return string(buf)
}

// syntheticHostedZoneID generates a synthetic Route53 hosted zone ID for DNS namespaces.
func syntheticHostedZoneID() string {
	return "Z" + strings.ToUpper(randAlnum(idOperationSuffixLen))
}

// createNamespace is the internal helper used by all three create-namespace operations.
func (b *InMemoryBackend) createNamespace(
	name, nsType, description, vpc string,
	soaTTL int64,
	tags map[string]string,
) (string, error) {
	b.mu.Lock("createNamespace")
	defer b.mu.Unlock()

	if _, exists := b.nsNameIndex[name]; exists {
		return "", fmt.Errorf("%w: namespace %s already exists", ErrNamespaceAlreadyExists, name)
	}

	id := b.nextNsID()

	var props *NamespaceProperties
	switch nsType {
	case namespaceTypeDNSPrivate, namespaceTypeDNSPublic:
		ttl := soaTTL
		if ttl == 0 {
			ttl = defaultSOATTL
		}

		props = &NamespaceProperties{
			DNSProperties: &DNSProperties{
				HostedZoneID: syntheticHostedZoneID(),
				SOA:          &SOA{TTL: ttl},
			},
		}
	case namespaceTypeHTTP:
		props = &NamespaceProperties{
			HTTPProperties: &HTTPProperties{HTTPName: name},
		}
	}

	now := time.Now()
	ns := &Namespace{
		ID:          id,
		ARN:         b.namespaceARN(id),
		Name:        name,
		Type:        nsType,
		Description: description,
		VPC:         vpc,
		Properties:  props,
		Tags:        copyTags(tags),
		CreatedAt:   now,
	}
	b.namespaces[id] = ns
	b.nsARNIndex[ns.ARN] = id
	b.nsNameIndex[name] = id

	opID := b.nextOpID()
	b.operations[opID] = &Operation{
		ID:         opID,
		Type:       operationTypeCreateNamespace,
		Status:     operationStatusSuccess,
		Targets:    map[string]string{typeNamespace: id},
		CreateDate: now,
		UpdateDate: now,
	}

	return opID, nil
}

// CreateHTTPNamespace creates an HTTP namespace.
func (b *InMemoryBackend) CreateHTTPNamespace(name, description string, tags map[string]string) (string, error) {
	return b.createNamespace(name, namespaceTypeHTTP, description, "", 0, tags)
}

// CreatePrivateDNSNamespace creates a private DNS namespace.
// soaTTL defaults to 15 when zero.
func (b *InMemoryBackend) CreatePrivateDNSNamespace(
	name, description, vpc string,
	soaTTL int64,
	tags map[string]string,
) (string, error) {
	return b.createNamespace(name, namespaceTypeDNSPrivate, description, vpc, soaTTL, tags)
}

// CreatePublicDNSNamespace creates a public DNS namespace.
// soaTTL defaults to 15 when zero.
func (b *InMemoryBackend) CreatePublicDNSNamespace(
	name, description string,
	soaTTL int64,
	tags map[string]string,
) (string, error) {
	return b.createNamespace(name, namespaceTypeDNSPublic, description, "", soaTTL, tags)
}

// DeleteNamespace deletes a namespace by ID.
// Returns ResourceInUse if the namespace still has services.
func (b *InMemoryBackend) DeleteNamespace(id string) (string, error) {
	b.mu.Lock("DeleteNamespace")
	defer b.mu.Unlock()

	ns, ok := b.namespaces[id]
	if !ok {
		return "", fmt.Errorf("%w: namespace %s not found", ErrNamespaceNotFound, id)
	}

	for _, svc := range b.services {
		if svc.NamespaceID == id {
			return "", fmt.Errorf("%w: namespace %s has services; delete them first", ErrResourceInUse, id)
		}
	}

	delete(b.nsARNIndex, ns.ARN)
	delete(b.nsNameIndex, ns.Name)
	delete(b.namespaces, id)

	now := time.Now()
	opID := b.nextOpID()
	b.operations[opID] = &Operation{
		ID:         opID,
		Type:       operationTypeDeleteNamespace,
		Status:     operationStatusSuccess,
		Targets:    map[string]string{typeNamespace: id},
		CreateDate: now,
		UpdateDate: now,
	}

	return opID, nil
}

// GetNamespace returns a namespace by ID.
func (b *InMemoryBackend) GetNamespace(id string) (*Namespace, error) {
	b.mu.RLock("GetNamespace")
	defer b.mu.RUnlock()

	ns, ok := b.namespaces[id]
	if !ok {
		return nil, fmt.Errorf("%w: namespace %s not found", ErrNamespaceNotFound, id)
	}

	cp := copyNamespace(ns)
	cp.ServiceCount = b.countServicesInNamespace(id)

	return cp, nil
}

// countServicesInNamespace counts services belonging to a namespace. Caller must hold at least a read lock.
func (b *InMemoryBackend) countServicesInNamespace(namespaceID string) int {
	count := 0
	for _, svc := range b.services {
		if svc.NamespaceID == namespaceID {
			count++
		}
	}

	return count
}

// ListNamespaces returns all namespaces sorted by name, optionally filtered.
func (b *InMemoryBackend) ListNamespaces(filter ListNamespacesFilter) []Namespace {
	b.mu.RLock("ListNamespaces")
	defer b.mu.RUnlock()

	result := make([]Namespace, 0, len(b.namespaces))

	for _, ns := range b.namespaces {
		if filter.Type != "" && ns.Type != filter.Type {
			continue
		}

		if filter.Name != "" && ns.Name != filter.Name {
			continue
		}

		cp := copyNamespace(ns)
		cp.ServiceCount = b.countServicesInNamespace(ns.ID)
		result = append(result, *cp)
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].Name < result[j].Name
	})

	return result
}

// CreateService creates a new Cloud Map service.
func (b *InMemoryBackend) CreateService(
	name, namespaceID, description, svcType string,
	dnsConfig *DNSConfig,
	hcc *HealthCheckConfig,
	hccc *HealthCheckCustomConfig,
	tags map[string]string,
) (*Service, error) {
	b.mu.Lock("CreateService")
	defer b.mu.Unlock()

	if namespaceID != "" {
		if _, ok := b.namespaces[namespaceID]; !ok {
			return nil, fmt.Errorf("%w: namespace %s not found", ErrNamespaceNotFound, namespaceID)
		}
	}

	// Derive service type when not explicitly set.
	resolvedType := svcType
	if resolvedType == "" && namespaceID != "" {
		if ns, ok := b.namespaces[namespaceID]; ok {
			switch ns.Type {
			case namespaceTypeHTTP:
				resolvedType = serviceTypeHTTP
			case namespaceTypeDNSPrivate, namespaceTypeDNSPublic:
				if dnsConfig != nil {
					resolvedType = serviceTypeDNSHTTP
				} else {
					resolvedType = serviceTypeDNS
				}
			}
		}
	}

	id := b.nextSvcID()

	svc := &Service{
		ID:                      id,
		ARN:                     b.serviceARN(id),
		Name:                    name,
		NamespaceID:             namespaceID,
		Description:             description,
		Type:                    resolvedType,
		DNSConfig:               copyDNSConfig(dnsConfig),
		HealthCheckConfig:       copyHealthCheckConfig(hcc),
		HealthCheckCustomConfig: copyHealthCheckCustomConfig(hccc),
		Tags:                    copyTags(tags),
		CreatedAt:               time.Now(),
	}

	b.services[id] = svc
	b.svcARNIndex[svc.ARN] = id
	b.instancesByService[id] = make(map[string]*Instance)

	if namespaceID != "" {
		b.svcByNsAndName[namespaceID+":"+name] = id
	}

	return copyService(svc), nil
}

// DeleteService deletes a service by ID.
// Returns ResourceInUse if instances are still registered.
func (b *InMemoryBackend) DeleteService(id string) error {
	b.mu.Lock("DeleteService")
	defer b.mu.Unlock()

	svc, ok := b.services[id]
	if !ok {
		return fmt.Errorf("%w: service %s not found", ErrServiceNotFound, id)
	}

	if insts, hasInsts := b.instancesByService[id]; hasInsts && len(insts) > 0 {
		return fmt.Errorf("%w: service %s has registered instances; deregister them first", ErrResourceInUse, id)
	}

	delete(b.svcARNIndex, svc.ARN)
	delete(b.services, id)
	delete(b.serviceAttributes, id)
	delete(b.instancesByService, id)

	if svc.NamespaceID != "" {
		delete(b.svcByNsAndName, svc.NamespaceID+":"+svc.Name)
	}

	return nil
}

// GetService returns a service by ID.
func (b *InMemoryBackend) GetService(id string) (*Service, error) {
	b.mu.RLock("GetService")
	defer b.mu.RUnlock()

	svc, ok := b.services[id]
	if !ok {
		return nil, fmt.Errorf("%w: service %s not found", ErrServiceNotFound, id)
	}

	cp := copyService(svc)
	cp.InstanceCount = len(b.instancesByService[id])

	return cp, nil
}

// ListServices returns all services, optionally filtered.
func (b *InMemoryBackend) ListServices(filter ListServicesFilter) []Service {
	b.mu.RLock("ListServices")
	defer b.mu.RUnlock()

	result := make([]Service, 0, len(b.services))

	for id, svc := range b.services {
		if filter.NamespaceID != "" && svc.NamespaceID != filter.NamespaceID {
			continue
		}

		cp := copyService(svc)
		cp.InstanceCount = len(b.instancesByService[id])
		result = append(result, *cp)
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].Name < result[j].Name
	})

	return result
}

// RegisterInstance registers an instance to a service.
func (b *InMemoryBackend) RegisterInstance(serviceID, instanceID string, attrs map[string]string) (string, error) {
	b.mu.Lock("RegisterInstance")
	defer b.mu.Unlock()

	if _, ok := b.services[serviceID]; !ok {
		return "", fmt.Errorf("%w: service %s not found", ErrServiceNotFound, serviceID)
	}

	key := instanceKey(serviceID, instanceID)

	inst := &Instance{
		ID:         instanceID,
		ServiceID:  serviceID,
		Attributes: copyAttrs(attrs),
	}
	b.instances[key] = inst

	if b.instancesByService[serviceID] == nil {
		b.instancesByService[serviceID] = make(map[string]*Instance)
	}

	b.instancesByService[serviceID][instanceID] = inst
	b.instanceRevision++

	now := time.Now()
	opID := b.nextOpID()
	b.operations[opID] = &Operation{
		ID:         opID,
		Type:       operationTypeRegisterInstance,
		Status:     operationStatusSuccess,
		Targets:    map[string]string{typeInstance: instanceID, typeService: serviceID},
		CreateDate: now,
		UpdateDate: now,
	}

	return opID, nil
}

// DeregisterInstance deregisters an instance from a service.
func (b *InMemoryBackend) DeregisterInstance(serviceID, instanceID string) (string, error) {
	b.mu.Lock("DeregisterInstance")
	defer b.mu.Unlock()

	key := instanceKey(serviceID, instanceID)
	if _, ok := b.instances[key]; !ok {
		return "", fmt.Errorf("%w: instance %s in service %s not found", ErrInstanceNotFound, instanceID, serviceID)
	}

	delete(b.instances, key)
	delete(b.instanceHealthStatuses, key)

	if insts, ok := b.instancesByService[serviceID]; ok {
		delete(insts, instanceID)
	}

	b.instanceRevision++

	now := time.Now()
	opID := b.nextOpID()
	b.operations[opID] = &Operation{
		ID:         opID,
		Type:       operationTypeDeregisterInstance,
		Status:     operationStatusSuccess,
		Targets:    map[string]string{typeInstance: instanceID, typeService: serviceID},
		CreateDate: now,
		UpdateDate: now,
	}

	return opID, nil
}

// GetInstance returns a registered instance.
func (b *InMemoryBackend) GetInstance(serviceID, instanceID string) (*Instance, error) {
	b.mu.RLock("GetInstance")
	defer b.mu.RUnlock()

	key := instanceKey(serviceID, instanceID)
	inst, ok := b.instances[key]

	if !ok {
		return nil, fmt.Errorf("%w: instance %s in service %s not found", ErrInstanceNotFound, instanceID, serviceID)
	}

	cp := *inst
	cp.Attributes = copyAttrs(inst.Attributes)

	return &cp, nil
}

// ListInstances returns all instances registered to a service.
func (b *InMemoryBackend) ListInstances(serviceID string) ([]Instance, error) {
	b.mu.RLock("ListInstances")
	defer b.mu.RUnlock()

	if _, ok := b.services[serviceID]; !ok {
		return nil, fmt.Errorf("%w: service %s not found", ErrServiceNotFound, serviceID)
	}

	insts := b.instancesByService[serviceID]
	result := make([]Instance, 0, len(insts))

	for _, inst := range insts {
		cp := *inst
		cp.Attributes = copyAttrs(inst.Attributes)
		result = append(result, cp)
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].ID < result[j].ID
	})

	return result, nil
}

// DiscoverInstances returns discovered instances with full per-instance metadata.
// Also returns the per-service revision counter.
func (b *InMemoryBackend) DiscoverInstances(
	namespaceName, serviceName, healthStatus string,
	queryParams map[string]string,
) ([]DiscoveredInstance, int64, error) {
	b.mu.RLock("DiscoverInstances")
	defer b.mu.RUnlock()

	nsID, ok := b.nsNameIndex[namespaceName]
	if !ok {
		return []DiscoveredInstance{}, 0, nil
	}

	svcID, ok := b.svcByNsAndName[nsID+":"+serviceName]
	if !ok {
		return []DiscoveredInstance{}, 0, nil
	}

	revision := b.instanceRevision
	insts := b.instancesByService[svcID]
	result := make([]DiscoveredInstance, 0, len(insts))

	for instID, inst := range insts {
		key := instanceKey(svcID, instID)

		if !b.instanceMatchesHealth(key, healthStatus) {
			continue
		}

		if !instanceMatchesQueryParams(inst, queryParams) {
			continue
		}

		hs := b.instanceHealthStatuses[key]
		if hs == "" {
			hs = instanceHealthStatusHealthy
		}

		result = append(result, DiscoveredInstance{
			InstanceID:    inst.ID,
			NamespaceName: namespaceName,
			ServiceName:   serviceName,
			HealthStatus:  hs,
			Attributes:    copyAttrs(inst.Attributes),
		})
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].InstanceID < result[j].InstanceID
	})

	return result, revision, nil
}

// instanceMatchesHealth returns true when the instance matches the health filter.
// An empty or "ALL" health status always matches.
func (b *InMemoryBackend) instanceMatchesHealth(key, healthStatus string) bool {
	if healthStatus == "" || healthStatus == "ALL" {
		return true
	}

	stored := b.instanceHealthStatuses[key]
	if stored == "" {
		stored = instanceHealthStatusHealthy
	}

	return stored == healthStatus
}

// instanceMatchesQueryParams returns true when the instance attributes satisfy
// every key-value pair in queryParams.
func instanceMatchesQueryParams(inst *Instance, queryParams map[string]string) bool {
	for k, v := range queryParams {
		if inst.Attributes[k] != v {
			return false
		}
	}

	return true
}

// GetInstancesHealthStatus returns the health status for instances in a service.
// If instanceIDs is non-empty, only those instances are included.
// Instances without a recorded status default to HEALTHY.
func (b *InMemoryBackend) GetInstancesHealthStatus(serviceID string, instanceIDs []string) (map[string]string, error) {
	b.mu.RLock("GetInstancesHealthStatus")
	defer b.mu.RUnlock()

	if _, ok := b.services[serviceID]; !ok {
		return nil, fmt.Errorf("%w: service %s not found", ErrServiceNotFound, serviceID)
	}

	filter := make(map[string]struct{}, len(instanceIDs))
	for _, id := range instanceIDs {
		filter[id] = struct{}{}
	}

	statuses := make(map[string]string)
	insts := b.instancesByService[serviceID]

	for instID := range insts {
		if len(filter) > 0 {
			if _, ok := filter[instID]; !ok {
				continue
			}
		}

		key := instanceKey(serviceID, instID)
		status := b.instanceHealthStatuses[key]
		if status == "" {
			status = instanceHealthStatusHealthy
		}

		statuses[instID] = status
	}

	return statuses, nil
}

// GetOperation returns an operation by ID.
func (b *InMemoryBackend) GetOperation(id string) (*Operation, error) {
	b.mu.RLock("GetOperation")
	defer b.mu.RUnlock()

	op, ok := b.operations[id]
	if !ok {
		return nil, fmt.Errorf("%w: operation %s not found", ErrOperationNotFound, id)
	}

	cp := copyOperation(op)

	return &cp, nil
}

// ListOperations returns all operations sorted by ID, optionally filtered.
func (b *InMemoryBackend) ListOperations(filter ListOperationsFilter) []Operation {
	b.mu.RLock("ListOperations")
	defer b.mu.RUnlock()

	result := make([]Operation, 0, len(b.operations))

	for _, op := range b.operations {
		if filter.Status != "" && op.Status != filter.Status {
			continue
		}

		if filter.Type != "" && op.Type != filter.Type {
			continue
		}

		result = append(result, copyOperation(op))
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].ID < result[j].ID
	})

	return result
}

// ListTagsForResource returns tags for a resource ARN (namespace or service).
func (b *InMemoryBackend) ListTagsForResource(arn string) (map[string]string, error) {
	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()

	if nsID, ok := b.nsARNIndex[arn]; ok {
		return copyTags(b.namespaces[nsID].Tags), nil
	}

	if svcID, ok := b.svcARNIndex[arn]; ok {
		return copyTags(b.services[svcID].Tags), nil
	}

	return nil, fmt.Errorf("%w: resource %s not found", ErrResourceNotFound, arn)
}

// TagResource adds tags to a resource (namespace or service).
func (b *InMemoryBackend) TagResource(arn string, tags map[string]string) error {
	b.mu.Lock("TagResource")
	defer b.mu.Unlock()

	if nsID, ok := b.nsARNIndex[arn]; ok {
		ns := b.namespaces[nsID]
		if ns.Tags == nil {
			ns.Tags = make(map[string]string)
		}

		maps.Copy(ns.Tags, tags)

		return nil
	}

	if svcID, ok := b.svcARNIndex[arn]; ok {
		svc := b.services[svcID]
		if svc.Tags == nil {
			svc.Tags = make(map[string]string)
		}

		maps.Copy(svc.Tags, tags)

		return nil
	}

	return fmt.Errorf("%w: resource %s not found", ErrResourceNotFound, arn)
}

// UntagResource removes tags from a resource (namespace or service).
func (b *InMemoryBackend) UntagResource(arn string, tagKeys []string) error {
	b.mu.Lock("UntagResource")
	defer b.mu.Unlock()

	if nsID, ok := b.nsARNIndex[arn]; ok {
		for _, k := range tagKeys {
			delete(b.namespaces[nsID].Tags, k)
		}

		return nil
	}

	if svcID, ok := b.svcARNIndex[arn]; ok {
		for _, k := range tagKeys {
			delete(b.services[svcID].Tags, k)
		}

		return nil
	}

	return fmt.Errorf("%w: resource %s not found", ErrResourceNotFound, arn)
}

// UpdateHTTPNamespace updates the description of an HTTP namespace.
func (b *InMemoryBackend) UpdateHTTPNamespace(id, description string) (string, error) {
	return b.updateNamespace(id, namespaceTypeHTTP, description)
}

// UpdatePrivateDNSNamespace updates the description of a private DNS namespace.
func (b *InMemoryBackend) UpdatePrivateDNSNamespace(id, description string) (string, error) {
	return b.updateNamespace(id, namespaceTypeDNSPrivate, description)
}

// UpdatePublicDNSNamespace updates the description of a public DNS namespace.
func (b *InMemoryBackend) UpdatePublicDNSNamespace(id, description string) (string, error) {
	return b.updateNamespace(id, namespaceTypeDNSPublic, description)
}

// updateNamespace is the internal helper for namespace update operations.
func (b *InMemoryBackend) updateNamespace(id, nsType, description string) (string, error) {
	b.mu.Lock("updateNamespace")
	defer b.mu.Unlock()

	ns, ok := b.namespaces[id]
	if !ok {
		return "", fmt.Errorf("%w: namespace %s not found", ErrNamespaceNotFound, id)
	}

	if ns.Type != nsType {
		return "", fmt.Errorf("%w: namespace %s is not of type %s", ErrInvalidInput, id, nsType)
	}

	ns.Description = description

	now := time.Now()
	opID := b.nextOpID()
	b.operations[opID] = &Operation{
		ID:         opID,
		Type:       operationTypeUpdateNamespace,
		Status:     operationStatusSuccess,
		Targets:    map[string]string{typeNamespace: id},
		CreateDate: now,
		UpdateDate: now,
	}

	return opID, nil
}

// UpdateService updates the description and optionally DNSConfig/HealthCheckConfig of a service.
// Returns the operation ID, matching real AWS UpdateService behavior.
func (b *InMemoryBackend) UpdateService(
	id, description string,
	dnsConfig *DNSConfig,
	hcc *HealthCheckConfig,
) (string, error) {
	b.mu.Lock("UpdateService")
	defer b.mu.Unlock()

	svc, ok := b.services[id]
	if !ok {
		return "", fmt.Errorf("%w: service %s not found", ErrServiceNotFound, id)
	}

	svc.Description = description

	if dnsConfig != nil && len(dnsConfig.DNSRecords) > 0 {
		if svc.DNSConfig == nil {
			svc.DNSConfig = &DNSConfig{}
		}

		for i, newRec := range dnsConfig.DNSRecords {
			if i < len(svc.DNSConfig.DNSRecords) {
				svc.DNSConfig.DNSRecords[i].TTL = newRec.TTL
			}
		}
	}

	if hcc != nil {
		svc.HealthCheckConfig = copyHealthCheckConfig(hcc)
	}

	now := time.Now()
	opID := b.nextOpID()
	b.operations[opID] = &Operation{
		ID:         opID,
		Type:       operationTypeUpdateService,
		Status:     operationStatusSuccess,
		Targets:    map[string]string{typeService: id},
		CreateDate: now,
		UpdateDate: now,
	}

	return opID, nil
}

// GetServiceAttributes returns the custom attributes for a service.
func (b *InMemoryBackend) GetServiceAttributes(serviceID string) (string, map[string]string, error) {
	b.mu.RLock("GetServiceAttributes")
	defer b.mu.RUnlock()

	svc, ok := b.services[serviceID]
	if !ok {
		return "", nil, fmt.Errorf("%w: service %s not found", ErrServiceNotFound, serviceID)
	}

	attrs, ok := b.serviceAttributes[serviceID]
	if !ok {
		return "", nil, fmt.Errorf("%w: no attributes found for service %s", ErrServiceAttributesNotFound, serviceID)
	}

	return svc.ARN, copyAttrs(attrs), nil
}

// UpdateServiceAttributes sets or merges custom attributes for a service identified by ARN.
func (b *InMemoryBackend) UpdateServiceAttributes(serviceARN string, attributes map[string]string) error {
	b.mu.Lock("UpdateServiceAttributes")
	defer b.mu.Unlock()

	svcID, ok := b.svcARNIndex[serviceARN]
	if !ok {
		return fmt.Errorf("%w: service with ARN %s not found", ErrServiceNotFound, serviceARN)
	}

	existing := b.serviceAttributes[svcID]
	if existing == nil {
		existing = make(map[string]string)
	}

	maps.Copy(existing, attributes)

	b.serviceAttributes[svcID] = existing

	return nil
}

// DeleteServiceAttributes removes all custom attributes for a service.
func (b *InMemoryBackend) DeleteServiceAttributes(serviceID string) error {
	b.mu.Lock("DeleteServiceAttributes")
	defer b.mu.Unlock()

	if _, ok := b.services[serviceID]; !ok {
		return fmt.Errorf("%w: service %s not found", ErrServiceNotFound, serviceID)
	}

	delete(b.serviceAttributes, serviceID)

	return nil
}

// UpdateInstanceCustomHealthStatus sets a custom health status for an instance.
// Returns CustomHealthNotFound if the service has no HealthCheckCustomConfig.
func (b *InMemoryBackend) UpdateInstanceCustomHealthStatus(serviceID, instanceID, status string) error {
	b.mu.Lock("UpdateInstanceCustomHealthStatus")
	defer b.mu.Unlock()

	if status != instanceHealthStatusHealthy && status != instanceHealthStatusUnhealthy {
		return fmt.Errorf(
			"%w: status must be %s or %s",
			ErrInvalidInput,
			instanceHealthStatusHealthy,
			instanceHealthStatusUnhealthy,
		)
	}

	if _, ok := b.services[serviceID]; !ok {
		return fmt.Errorf("%w: service %s not found", ErrServiceNotFound, serviceID)
	}

	key := instanceKey(serviceID, instanceID)
	if _, ok := b.instances[key]; !ok {
		return fmt.Errorf("%w: instance %s in service %s not found", ErrInstanceNotFound, instanceID, serviceID)
	}

	b.instanceHealthStatuses[key] = status

	return nil
}

// DiscoverInstancesRevision returns the current revision for the specified service.
// Revision is per-service, incremented on each RegisterInstance/DeregisterInstance.
func (b *InMemoryBackend) DiscoverInstancesRevision(namespaceName, serviceName string) (int64, error) {
	b.mu.RLock("DiscoverInstancesRevision")
	defer b.mu.RUnlock()

	nsID, ok := b.nsNameIndex[namespaceName]
	if !ok {
		return 0, fmt.Errorf("%w: namespace %s not found", ErrNamespaceNotFound, namespaceName)
	}

	if _, hasSvc := b.svcByNsAndName[nsID+":"+serviceName]; !hasSvc {
		return 0, fmt.Errorf("%w: service %s not found in namespace %s", ErrServiceNotFound, serviceName, namespaceName)
	}

	return b.instanceRevision, nil
}

// instanceKey creates a unique key for storing instances.
func instanceKey(serviceID, instanceID string) string {
	return serviceID + "/" + instanceID
}

// copyTags returns a shallow copy of a tag map, or nil when input is nil/empty.
func copyTags(tags map[string]string) map[string]string {
	if len(tags) == 0 {
		return nil
	}

	cp := make(map[string]string, len(tags))
	maps.Copy(cp, tags)

	return cp
}

// copyAttrs returns a shallow copy of an attributes map, or nil when input is nil/empty.
func copyAttrs(attrs map[string]string) map[string]string {
	return copyTags(attrs)
}

func copyDNSConfig(dc *DNSConfig) *DNSConfig {
	if dc == nil {
		return nil
	}

	cp := *dc

	if len(dc.DNSRecords) > 0 {
		cp.DNSRecords = make([]DNSRecord, len(dc.DNSRecords))
		copy(cp.DNSRecords, dc.DNSRecords)
	}

	return &cp
}

func copyHealthCheckConfig(hcc *HealthCheckConfig) *HealthCheckConfig {
	if hcc == nil {
		return nil
	}

	cp := *hcc

	return &cp
}

func copyHealthCheckCustomConfig(hccc *HealthCheckCustomConfig) *HealthCheckCustomConfig {
	if hccc == nil {
		return nil
	}

	cp := *hccc

	return &cp
}

func copyNamespace(ns *Namespace) *Namespace {
	cp := *ns
	cp.Tags = copyTags(ns.Tags)

	if ns.Properties != nil {
		props := *ns.Properties

		if ns.Properties.DNSProperties != nil {
			dp := *ns.Properties.DNSProperties

			if ns.Properties.DNSProperties.SOA != nil {
				soa := *ns.Properties.DNSProperties.SOA
				dp.SOA = &soa
			}

			props.DNSProperties = &dp
		}

		if ns.Properties.HTTPProperties != nil {
			hp := *ns.Properties.HTTPProperties
			props.HTTPProperties = &hp
		}

		cp.Properties = &props
	}

	return &cp
}

func copyService(svc *Service) *Service {
	cp := *svc
	cp.Tags = copyTags(svc.Tags)
	cp.DNSConfig = copyDNSConfig(svc.DNSConfig)
	cp.HealthCheckConfig = copyHealthCheckConfig(svc.HealthCheckConfig)
	cp.HealthCheckCustomConfig = copyHealthCheckCustomConfig(svc.HealthCheckCustomConfig)

	return &cp
}

func copyOperation(op *Operation) Operation {
	cp := *op

	if len(op.Targets) > 0 {
		cp.Targets = make(map[string]string, len(op.Targets))
		maps.Copy(cp.Targets, op.Targets)
	}

	return cp
}

// encodeCursor encodes an integer offset as an opaque NextToken.
func encodeCursor(offset int) string {
	return base64.StdEncoding.EncodeToString([]byte(strconv.Itoa(offset)))
}

// decodeCursor decodes an opaque NextToken to an integer offset.
func decodeCursor(token string) int {
	if token == "" {
		return 0
	}

	b, err := base64.StdEncoding.DecodeString(token)
	if err != nil {
		return 0
	}

	var offset int

	_, _ = fmt.Sscanf(string(b), "%d", &offset)

	return offset
}

func applyPaginationNamespaces(items []Namespace, nextToken string, maxResults int) ([]Namespace, string) {
	if maxResults <= 0 || maxResults > maxResultsCap {
		maxResults = maxResultsDefault
	}

	offset := decodeCursor(nextToken)
	if offset >= len(items) {
		return nil, ""
	}

	end := offset + maxResults

	var newToken string

	if end < len(items) {
		newToken = encodeCursor(end)
	} else {
		end = len(items)
	}

	return items[offset:end], newToken
}

func applyPaginationServices(items []Service, nextToken string, maxResults int) ([]Service, string) {
	if maxResults <= 0 || maxResults > maxResultsCap {
		maxResults = maxResultsDefault
	}

	offset := decodeCursor(nextToken)
	if offset >= len(items) {
		return nil, ""
	}

	end := offset + maxResults

	var newToken string

	if end < len(items) {
		newToken = encodeCursor(end)
	} else {
		end = len(items)
	}

	return items[offset:end], newToken
}

func applyPaginationInstances(items []Instance, nextToken string, maxResults int) ([]Instance, string) {
	if maxResults <= 0 || maxResults > maxResultsCap {
		maxResults = maxResultsDefault
	}

	offset := decodeCursor(nextToken)
	if offset >= len(items) {
		return nil, ""
	}

	end := offset + maxResults

	var newToken string

	if end < len(items) {
		newToken = encodeCursor(end)
	} else {
		end = len(items)
	}

	return items[offset:end], newToken
}

func applyPaginationOperations(items []Operation, nextToken string, maxResults int) ([]Operation, string) {
	if maxResults <= 0 || maxResults > maxResultsCap {
		maxResults = maxResultsDefault
	}

	offset := decodeCursor(nextToken)
	if offset >= len(items) {
		return nil, ""
	}

	end := offset + maxResults

	var newToken string

	if end < len(items) {
		newToken = encodeCursor(end)
	} else {
		end = len(items)
	}

	return items[offset:end], newToken
}

// Reset clears all backend state, resetting to an empty store.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	b.namespaces = make(map[string]*Namespace)
	b.services = make(map[string]*Service)
	b.instances = make(map[string]*Instance)
	b.operations = make(map[string]*Operation)
	b.serviceAttributes = make(map[string]map[string]string)
	b.instanceHealthStatuses = make(map[string]string)
	b.instancesByService = make(map[string]map[string]*Instance)
	b.svcByNsAndName = make(map[string]string)
	b.nsARNIndex = make(map[string]string)
	b.svcARNIndex = make(map[string]string)
	b.nsNameIndex = make(map[string]string)
	b.instanceRevision = 0
	b.nsCounter = 0
	b.svcCounter = 0
	b.opCounter = 0
}

func applyPaginationHealthStatuses(ids []string, nextToken string, maxResults int) ([]string, string) {
	if maxResults <= 0 || maxResults > maxResultsCap {
		maxResults = maxResultsDefault
	}

	offset := decodeCursor(nextToken)
	if offset >= len(ids) {
		return nil, ""
	}

	end := offset + maxResults

	var newToken string

	if end < len(ids) {
		newToken = encodeCursor(end)
	} else {
		end = len(ids)
	}

	return ids[offset:end], newToken
}

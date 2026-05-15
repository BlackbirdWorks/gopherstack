package servicediscovery

import (
	"fmt"
	"maps"
	"sort"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

const (
	typeNamespace = "NAMESPACE"
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
)

// Namespace represents an AWS Cloud Map namespace.
type Namespace struct {
	CreatedAt   time.Time         `json:"createdAt"`
	Tags        map[string]string `json:"tags,omitempty"`
	ID          string            `json:"id"`
	ARN         string            `json:"arn"`
	Name        string            `json:"name"`
	Type        string            `json:"type"`
	Description string            `json:"description,omitempty"`
}

// Service represents an AWS Cloud Map service.
type Service struct {
	CreatedAt   time.Time         `json:"createdAt"`
	Tags        map[string]string `json:"tags,omitempty"`
	ID          string            `json:"id"`
	ARN         string            `json:"arn"`
	Name        string            `json:"name"`
	NamespaceID string            `json:"namespaceID"`
	Description string            `json:"description,omitempty"`
}

// Instance represents a registered instance in a Cloud Map service.
type Instance struct {
	Attributes map[string]string `json:"attributes,omitempty"`
	ID         string            `json:"id"`
	ServiceID  string            `json:"serviceID"`
}

// Operation represents an async Cloud Map operation (e.g., create/delete namespace).
type Operation struct {
	ID         string `json:"id"`
	Type       string `json:"type"`
	Status     string `json:"status"`
	TargetID   string `json:"targetID"`
	TargetType string `json:"targetType"`
}

// InMemoryBackend is the in-memory Cloud Map backend.
type InMemoryBackend struct {
	namespaces             map[string]*Namespace
	services               map[string]*Service
	instances              map[string]*Instance
	operations             map[string]*Operation
	serviceAttributes      map[string]map[string]string // service ID → attributes
	instanceHealthStatuses map[string]string            // instanceKey → health status
	nsARNIndex             map[string]string            // ARN → namespace ID
	svcARNIndex            map[string]string            // ARN → service ID
	nsNameIndex            map[string]string            // name → namespace ID
	mu                     *lockmetrics.RWMutex
	accountID              string
	region                 string
	nsCounter              int
	svcCounter             int
	instCounter            int
	opCounter              int
	revisionCounter        int64
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
	return fmt.Sprintf("arn:aws:servicediscovery:%s:%s:namespace/%s", b.region, b.accountID, id)
}

func (b *InMemoryBackend) serviceARN(id string) string {
	return fmt.Sprintf("arn:aws:servicediscovery:%s:%s:service/%s", b.region, b.accountID, id)
}

// createNamespace is the internal helper used by all three create-namespace operations.
func (b *InMemoryBackend) createNamespace(name, nsType, description string, tags map[string]string) (string, error) {
	b.mu.Lock("createNamespace")
	defer b.mu.Unlock()

	if _, exists := b.nsNameIndex[name]; exists {
		return "", fmt.Errorf("%w: namespace %s already exists", ErrNamespaceAlreadyExists, name)
	}

	b.nsCounter++
	id := fmt.Sprintf("ns-%08d", b.nsCounter)

	ns := &Namespace{
		ID:          id,
		ARN:         b.namespaceARN(id),
		Name:        name,
		Type:        nsType,
		Description: description,
		Tags:        copyTags(tags),
		CreatedAt:   time.Now(),
	}
	b.namespaces[id] = ns
	b.nsARNIndex[ns.ARN] = id
	b.nsNameIndex[name] = id

	b.opCounter++
	opID := fmt.Sprintf("op-%08d", b.opCounter)

	b.operations[opID] = &Operation{
		ID:         opID,
		Type:       operationTypeCreateNamespace,
		Status:     operationStatusSuccess,
		TargetID:   id,
		TargetType: typeNamespace,
	}

	return opID, nil
}

// CreateHTTPNamespace creates an HTTP namespace.
func (b *InMemoryBackend) CreateHTTPNamespace(name, description string, tags map[string]string) (string, error) {
	return b.createNamespace(name, namespaceTypeHTTP, description, tags)
}

// CreatePrivateDNSNamespace creates a private DNS namespace.
func (b *InMemoryBackend) CreatePrivateDNSNamespace(name, description string, tags map[string]string) (string, error) {
	return b.createNamespace(name, namespaceTypeDNSPrivate, description, tags)
}

// CreatePublicDNSNamespace creates a public DNS namespace.
func (b *InMemoryBackend) CreatePublicDNSNamespace(name, description string, tags map[string]string) (string, error) {
	return b.createNamespace(name, namespaceTypeDNSPublic, description, tags)
}

// DeleteNamespace deletes a namespace by ID.
func (b *InMemoryBackend) DeleteNamespace(id string) (string, error) {
	b.mu.Lock("DeleteNamespace")
	defer b.mu.Unlock()

	ns, ok := b.namespaces[id]
	if !ok {
		return "", fmt.Errorf("%w: namespace %s not found", ErrNamespaceNotFound, id)
	}

	delete(b.nsARNIndex, ns.ARN)
	delete(b.nsNameIndex, ns.Name)
	delete(b.namespaces, id)

	// Cascade delete services and their instances.
	for svcID, svc := range b.services {
		if svc.NamespaceID == id {
			delete(b.svcARNIndex, svc.ARN)
			delete(b.services, svcID)
			delete(b.serviceAttributes, svcID)

			prefix := svcID + "/"
			for instKey := range b.instances {
				if strings.HasPrefix(instKey, prefix) {
					delete(b.instances, instKey)
					delete(b.instanceHealthStatuses, instKey)
				}
			}
		}
	}

	b.opCounter++
	opID := fmt.Sprintf("op-%08d", b.opCounter)

	b.operations[opID] = &Operation{
		ID:         opID,
		Type:       operationTypeDeleteNamespace,
		Status:     operationStatusSuccess,
		TargetID:   id,
		TargetType: typeNamespace,
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

	result := *ns
	result.Tags = copyTags(ns.Tags)

	return &result, nil
}

// ListNamespaces returns all namespaces sorted by name.
func (b *InMemoryBackend) ListNamespaces() []Namespace {
	b.mu.RLock("ListNamespaces")
	defer b.mu.RUnlock()

	result := make([]Namespace, 0, len(b.namespaces))

	for _, ns := range b.namespaces {
		cp := *ns
		cp.Tags = copyTags(ns.Tags)
		result = append(result, cp)
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].Name < result[j].Name
	})

	return result
}

// CreateService creates a new Cloud Map service.
func (b *InMemoryBackend) CreateService(
	name, namespaceID, description string,
	tags map[string]string,
) (*Service, error) {
	b.mu.Lock("CreateService")
	defer b.mu.Unlock()

	if namespaceID != "" {
		if _, ok := b.namespaces[namespaceID]; !ok {
			return nil, fmt.Errorf("%w: namespace %s not found", ErrNamespaceNotFound, namespaceID)
		}
	}

	b.svcCounter++
	id := fmt.Sprintf("svc-%08d", b.svcCounter)

	svc := &Service{
		ID:          id,
		ARN:         b.serviceARN(id),
		Name:        name,
		NamespaceID: namespaceID,
		Description: description,
		Tags:        copyTags(tags),
		CreatedAt:   time.Now(),
	}

	b.services[id] = svc
	b.svcARNIndex[svc.ARN] = id

	cp := *svc
	cp.Tags = copyTags(svc.Tags)

	return &cp, nil
}

// DeleteService deletes a service by ID.
func (b *InMemoryBackend) DeleteService(id string) error {
	b.mu.Lock("DeleteService")
	defer b.mu.Unlock()

	svc, ok := b.services[id]
	if !ok {
		return fmt.Errorf("%w: service %s not found", ErrServiceNotFound, id)
	}

	delete(b.svcARNIndex, svc.ARN)
	delete(b.services, id)
	delete(b.serviceAttributes, id)

	// Cascade delete all instances for this service.
	prefix := id + "/"
	for instKey := range b.instances {
		if strings.HasPrefix(instKey, prefix) {
			delete(b.instances, instKey)
			delete(b.instanceHealthStatuses, instKey)
		}
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

	cp := *svc
	cp.Tags = copyTags(svc.Tags)

	return &cp, nil
}

// ListServices returns all services, optionally filtered by namespace ID.
func (b *InMemoryBackend) ListServices(namespaceID string) []Service {
	b.mu.RLock("ListServices")
	defer b.mu.RUnlock()

	result := make([]Service, 0, len(b.services))

	for _, svc := range b.services {
		if namespaceID != "" && svc.NamespaceID != namespaceID {
			continue
		}

		cp := *svc
		cp.Tags = copyTags(svc.Tags)
		result = append(result, cp)
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

	b.instCounter++
	key := instanceKey(serviceID, instanceID)

	b.instances[key] = &Instance{
		ID:         instanceID,
		ServiceID:  serviceID,
		Attributes: copyAttrs(attrs),
	}

	b.revisionCounter++

	b.opCounter++
	opID := fmt.Sprintf("op-%08d", b.opCounter)

	b.operations[opID] = &Operation{
		ID:         opID,
		Type:       operationTypeRegisterInstance,
		Status:     operationStatusSuccess,
		TargetID:   instanceID,
		TargetType: "INSTANCE",
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

	b.revisionCounter++

	b.opCounter++
	opID := fmt.Sprintf("op-%08d", b.opCounter)

	b.operations[opID] = &Operation{
		ID:         opID,
		Type:       operationTypeDeregisterInstance,
		Status:     operationStatusSuccess,
		TargetID:   instanceID,
		TargetType: "INSTANCE",
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

	result := make([]Instance, 0, len(b.instances))
	prefix := serviceID + "/"

	for key, inst := range b.instances {
		if strings.HasPrefix(key, prefix) {
			cp := *inst
			cp.Attributes = copyAttrs(inst.Attributes)
			result = append(result, cp)
		}
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].ID < result[j].ID
	})

	return result, nil
}

// DiscoverInstances returns instances matching filters.
func (b *InMemoryBackend) DiscoverInstances(
	namespaceName, serviceName, healthStatus string,
	queryParams map[string]string,
) ([]Instance, error) {
	b.mu.RLock("DiscoverInstances")
	defer b.mu.RUnlock()

	nsID, ok := b.nsNameIndex[namespaceName]
	if !ok {
		return []Instance{}, nil
	}

	svcID := b.findServiceID(nsID, serviceName)
	if svcID == "" {
		return []Instance{}, nil
	}

	result := b.collectInstances(svcID, healthStatus, queryParams)

	sort.Slice(result, func(i, j int) bool {
		return result[i].ID < result[j].ID
	})

	return result, nil
}

// findServiceID returns the service ID matching the given namespace and service name.
// Caller must hold at least a read lock.
func (b *InMemoryBackend) findServiceID(nsID, serviceName string) string {
	for _, svc := range b.services {
		if svc.NamespaceID == nsID && svc.Name == serviceName {
			return svc.ID
		}
	}

	return ""
}

// collectInstances collects and filters instances for a given service.
// Caller must hold at least a read lock.
func (b *InMemoryBackend) collectInstances(svcID, healthStatus string, queryParams map[string]string) []Instance {
	result := make([]Instance, 0, len(b.instances))
	prefix := svcID + "/"

	for key, inst := range b.instances {
		if !strings.HasPrefix(key, prefix) {
			continue
		}

		if !b.instanceMatchesHealth(key, healthStatus) {
			continue
		}

		if !instanceMatchesQueryParams(inst, queryParams) {
			continue
		}

		cp := *inst
		cp.Attributes = copyAttrs(inst.Attributes)
		result = append(result, cp)
	}

	return result
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
// If instanceIDs is non-empty, only those instances are included in the result.
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
	prefix := serviceID + "/"

	for key, inst := range b.instances {
		if !strings.HasPrefix(key, prefix) {
			continue
		}

		if len(filter) > 0 {
			if _, ok := filter[inst.ID]; !ok {
				continue
			}
		}

		status := b.instanceHealthStatuses[key]
		if status == "" {
			status = instanceHealthStatusHealthy
		}

		statuses[inst.ID] = status
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

	cp := *op

	return &cp, nil
}

// ListOperations returns all operations sorted by ID.
func (b *InMemoryBackend) ListOperations() []Operation {
	b.mu.RLock("ListOperations")
	defer b.mu.RUnlock()

	result := make([]Operation, 0, len(b.operations))

	for _, op := range b.operations {
		result = append(result, *op)
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

	b.opCounter++
	opID := fmt.Sprintf("op-%08d", b.opCounter)

	b.operations[opID] = &Operation{
		ID:         opID,
		Type:       operationTypeUpdateNamespace,
		Status:     operationStatusSuccess,
		TargetID:   id,
		TargetType: typeNamespace,
	}

	return opID, nil
}

// UpdateService updates the description of a service.
func (b *InMemoryBackend) UpdateService(id, description string) (*Service, error) {
	b.mu.Lock("UpdateService")
	defer b.mu.Unlock()

	svc, ok := b.services[id]
	if !ok {
		return nil, fmt.Errorf("%w: service %s not found", ErrServiceNotFound, id)
	}

	svc.Description = description

	b.opCounter++
	opID := fmt.Sprintf("op-%08d", b.opCounter)

	b.operations[opID] = &Operation{
		ID:         opID,
		Type:       operationTypeUpdateService,
		Status:     operationStatusSuccess,
		TargetID:   id,
		TargetType: "SERVICE",
	}

	cp := *svc
	cp.Tags = copyTags(svc.Tags)

	return &cp, nil
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

	key := instanceKey(serviceID, instanceID)
	if _, ok := b.instances[key]; !ok {
		return fmt.Errorf("%w: instance %s in service %s not found", ErrInstanceNotFound, instanceID, serviceID)
	}

	b.instanceHealthStatuses[key] = status

	return nil
}

// DiscoverInstancesRevision returns the current revision number for instance discovery
// in the given namespace/service combination, incremented each time instances change.
func (b *InMemoryBackend) DiscoverInstancesRevision(namespaceName, serviceName string) (int64, error) {
	b.mu.RLock("DiscoverInstancesRevision")
	defer b.mu.RUnlock()

	nsID, ok := b.nsNameIndex[namespaceName]
	if !ok {
		return 0, fmt.Errorf("%w: namespace %s not found", ErrNamespaceNotFound, namespaceName)
	}

	var found bool

	for _, svc := range b.services {
		if svc.NamespaceID == nsID && svc.Name == serviceName {
			found = true

			break
		}
	}

	if !found {
		return 0, fmt.Errorf("%w: service %s not found in namespace %s", ErrServiceNotFound, serviceName, namespaceName)
	}

	return b.revisionCounter, nil
}

// instanceKey creates a unique key for storing instances.
func instanceKey(serviceID, instanceID string) string {
	return serviceID + "/" + instanceID
}

// copyTags returns a shallow copy of a tag map.
func copyTags(tags map[string]string) map[string]string {
	if tags == nil {
		return make(map[string]string)
	}

	cp := make(map[string]string, len(tags))

	maps.Copy(cp, tags)

	return cp
}

// copyAttrs returns a shallow copy of an attributes map.
func copyAttrs(attrs map[string]string) map[string]string {
	return copyTags(attrs)
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
	b.nsARNIndex = make(map[string]string)
	b.svcARNIndex = make(map[string]string)
	b.nsNameIndex = make(map[string]string)
	b.nsCounter = 0
	b.svcCounter = 0
	b.instCounter = 0
	b.opCounter = 0
	b.revisionCounter = 0
}

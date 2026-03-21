package servicediscovery

import (
	"fmt"
	"maps"
	"sort"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
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
)

const (
	namespaceTypeHTTP       = "HTTP"
	namespaceTypeDNSPrivate = "DNS_PRIVATE"
	namespaceTypeDNSPublic  = "DNS_PUBLIC"

	operationStatusSuccess = "SUCCESS"

	operationTypeCreateNamespace = "CREATE_NAMESPACE"
	operationTypeDeleteNamespace = "DELETE_NAMESPACE"
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
	namespaces  map[string]*Namespace
	services    map[string]*Service
	instances   map[string]*Instance
	operations  map[string]*Operation
	nsARNIndex  map[string]string // ARN → namespace ID
	svcARNIndex map[string]string // ARN → service ID
	nsNameIndex map[string]string // name → namespace ID
	mu          *lockmetrics.RWMutex
	accountID   string
	region      string
	nsCounter   int
	svcCounter  int
	instCounter int
	opCounter   int
}

// NewInMemoryBackend creates a new in-memory Cloud Map backend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	return &InMemoryBackend{
		namespaces:  make(map[string]*Namespace),
		services:    make(map[string]*Service),
		instances:   make(map[string]*Instance),
		operations:  make(map[string]*Operation),
		nsARNIndex:  make(map[string]string),
		svcARNIndex: make(map[string]string),
		nsNameIndex: make(map[string]string),
		mu:          lockmetrics.New("servicediscovery"),
		accountID:   accountID,
		region:      region,
	}
}

// Region returns the AWS region this backend is configured for.
func (b *InMemoryBackend) Region() string { return b.region }

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
		TargetType: "NAMESPACE",
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

			for instKey := range b.instances {
				if len(instKey) > len(svcID) && instKey[:len(svcID)] == svcID {
					delete(b.instances, instKey)
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
		TargetType: "NAMESPACE",
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

	// Cascade delete all instances for this service.
	for instKey := range b.instances {
		if len(instKey) > len(id) && instKey[:len(id)] == id {
			delete(b.instances, instKey)
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
func (b *InMemoryBackend) RegisterInstance(serviceID, instanceID string, attrs map[string]string) error {
	b.mu.Lock("RegisterInstance")
	defer b.mu.Unlock()

	if _, ok := b.services[serviceID]; !ok {
		return fmt.Errorf("%w: service %s not found", ErrServiceNotFound, serviceID)
	}

	b.instCounter++
	key := instanceKey(serviceID, instanceID)

	b.instances[key] = &Instance{
		ID:         instanceID,
		ServiceID:  serviceID,
		Attributes: copyAttrs(attrs),
	}

	return nil
}

// DeregisterInstance deregisters an instance from a service.
func (b *InMemoryBackend) DeregisterInstance(serviceID, instanceID string) error {
	b.mu.Lock("DeregisterInstance")
	defer b.mu.Unlock()

	key := instanceKey(serviceID, instanceID)
	if _, ok := b.instances[key]; !ok {
		return fmt.Errorf("%w: instance %s in service %s not found", ErrInstanceNotFound, instanceID, serviceID)
	}

	delete(b.instances, key)

	return nil
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

	result := make([]Instance, 0)

	for key, inst := range b.instances {
		if len(key) > len(serviceID) && key[:len(serviceID)] == serviceID {
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
func (b *InMemoryBackend) DiscoverInstances(namespaceName, serviceName string) ([]Instance, error) {
	b.mu.RLock("DiscoverInstances")
	defer b.mu.RUnlock()

	nsID, ok := b.nsNameIndex[namespaceName]
	if !ok {
		return []Instance{}, nil
	}

	var svcID string

	for _, svc := range b.services {
		if svc.NamespaceID == nsID && svc.Name == serviceName {
			svcID = svc.ID

			break
		}
	}

	if svcID == "" {
		return []Instance{}, nil
	}

	result := make([]Instance, 0)

	for key, inst := range b.instances {
		if len(key) > len(svcID) && key[:len(svcID)] == svcID {
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

	return nil, fmt.Errorf("%w: resource %s not found", ErrNamespaceNotFound, arn)
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

	return fmt.Errorf("%w: resource %s not found", ErrNamespaceNotFound, arn)
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

	return fmt.Errorf("%w: resource %s not found", ErrNamespaceNotFound, arn)
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
	b.nsARNIndex = make(map[string]string)
	b.svcARNIndex = make(map[string]string)
	b.nsNameIndex = make(map[string]string)
	b.nsCounter = 0
	b.svcCounter = 0
	b.instCounter = 0
	b.opCounter = 0
}

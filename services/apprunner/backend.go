package apprunner

import (
	"encoding/json"
	"fmt"
	"maps"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

const (
	invalidRequestType   = "InvalidRequestException"
	resourceNotFoundType = "InvalidParameterException"
	conflictType         = "ServiceQuotaExceededException"

	statusRunning = "RUNNING"
	statusPaused  = "PAUSED"
	statusDeleted = "DELETED"

	opTypeCreate = "CREATE_SERVICE"
	opTypePause  = "PAUSE_SERVICE"
	opTypeResume = "RESUME_SERVICE"
	opTypeDelete = "DELETE_SERVICE"
	opTypeDeploy = "START_DEPLOYMENT"
	opTypeUpdate = "UPDATE_SERVICE"

	opStatusSucceeded = "SUCCEEDED"

	defaultMaxResults = 20
	defaultCPU        = "1 vCPU"
	defaultMemory     = "2 GB"
)

var (
	// ErrNotFound is returned when a resource does not exist.
	ErrNotFound = awserr.New(resourceNotFoundType, awserr.ErrNotFound)
	// ErrAlreadyExists is returned when a service name is already in use.
	ErrAlreadyExists = awserr.New(conflictType, awserr.ErrAlreadyExists)
	// ErrInvalidParameter is returned for invalid input.
	ErrInvalidParameter = awserr.New(invalidRequestType, awserr.ErrInvalidParameter)
)

// storedService holds a service with all fields.
// CreatedAt is first so its non-pointer prefix (wall, ext) reduces GC pointer bytes.
type storedService struct {
	CreatedAt   time.Time          `json:"createdAt"`
	UpdatedAt   time.Time          `json:"updatedAt"`
	Tags        map[string]string  `json:"tags"`
	ServiceArn  string             `json:"serviceArn"`
	ServiceID   string             `json:"serviceId"`
	ServiceName string             `json:"serviceName"`
	ServiceURL  string             `json:"serviceUrl"`
	Status      string             `json:"status"`
	CPU         string             `json:"cpu"`
	Memory      string             `json:"memory"`
	ImageURI    string             `json:"imageUri"`
	Operations  []*storedOperation `json:"operations"`
}

func (s *storedService) toService() Service {
	return Service{
		ServiceArn:  s.ServiceArn,
		ServiceID:   s.ServiceID,
		ServiceName: s.ServiceName,
		ServiceURL:  s.ServiceURL,
		Status:      s.Status,
		CPU:         s.CPU,
		Memory:      s.Memory,
		ImageURI:    s.ImageURI,
		CreatedAt:   s.CreatedAt,
		UpdatedAt:   s.UpdatedAt,
	}
}

func (s *storedService) toSummary() ServiceSummary {
	return ServiceSummary{
		ServiceArn:  s.ServiceArn,
		ServiceID:   s.ServiceID,
		ServiceName: s.ServiceName,
		ServiceURL:  s.ServiceURL,
		Status:      s.Status,
		CreatedAt:   s.CreatedAt,
	}
}

// storedOperation holds an operation record.
// StartedAt is first so its non-pointer prefix (wall, ext) reduces GC pointer bytes.
type storedOperation struct {
	StartedAt time.Time `json:"startedAt"`
	EndedAt   time.Time `json:"endedAt"`
	ID        string    `json:"id"`
	Type      string    `json:"type"`
	Status    string    `json:"status"`
	TargetArn string    `json:"targetArn"`
}

func (o *storedOperation) toSummary() OperationSummary {
	return OperationSummary{
		ID:        o.ID,
		Type:      o.Type,
		Status:    o.Status,
		TargetArn: o.TargetArn,
		StartedAt: o.StartedAt,
		EndedAt:   o.EndedAt,
	}
}

// snapshot holds serializable backend state.
type snapshot struct {
	Services map[string]*storedService    `json:"services"`
	Tags     map[string]map[string]string `json:"tags"`
}

// InMemoryBackend implements StorageBackend using in-memory maps.
type InMemoryBackend struct {
	mu        *lockmetrics.RWMutex
	services  map[string]*storedService    // serviceArn → service
	byName    map[string]string            // serviceName → serviceArn
	tags      map[string]map[string]string // resourceArn → tags
	accountID string
	region    string
}

// NewInMemoryBackend constructs a new InMemoryBackend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	return &InMemoryBackend{
		mu:        lockmetrics.New("apprunner"),
		accountID: accountID,
		region:    region,
		services:  make(map[string]*storedService),
		byName:    make(map[string]string),
		tags:      make(map[string]map[string]string),
	}
}

func (b *InMemoryBackend) serviceARN(id string) string {
	return arn.Build("apprunner", b.region, b.accountID, "service/"+id)
}

func newID() string {
	return strings.ReplaceAll(uuid.NewString(), "-", "")[:16]
}

func newOpID() string {
	return uuid.NewString()
}

func buildServiceURL(name, region string) string {
	return fmt.Sprintf("%s.%s.awsapprunner.com", name, region)
}

func (b *InMemoryBackend) addOperation(svc *storedService, opType string) {
	now := time.Now().UTC()
	op := &storedOperation{
		ID:        newOpID(),
		Type:      opType,
		Status:    opStatusSucceeded,
		TargetArn: svc.ServiceArn,
		StartedAt: now,
		EndedAt:   now,
	}
	svc.Operations = append(svc.Operations, op)
}

// CreateService creates a new App Runner service.
func (b *InMemoryBackend) CreateService(
	name, cpu, memory, imageURI string,
	tags map[string]string,
) (*Service, error) {
	b.mu.Lock("CreateService")
	defer b.mu.Unlock()

	if _, exists := b.byName[name]; exists {
		return nil, fmt.Errorf("service %s already exists: %w", name, ErrAlreadyExists)
	}

	id := newID()
	svcArn := b.serviceARN(id)
	now := time.Now().UTC()

	if cpu == "" {
		cpu = defaultCPU
	}

	if memory == "" {
		memory = defaultMemory
	}

	svcTags := make(map[string]string)
	maps.Copy(svcTags, tags)

	svc := &storedService{
		ServiceArn:  svcArn,
		ServiceID:   id,
		ServiceName: name,
		ServiceURL:  buildServiceURL(id, b.region),
		Status:      statusRunning,
		CPU:         cpu,
		Memory:      memory,
		ImageURI:    imageURI,
		CreatedAt:   now,
		UpdatedAt:   now,
		Tags:        svcTags,
	}
	b.addOperation(svc, opTypeCreate)
	b.services[svcArn] = svc
	b.byName[name] = svcArn

	if len(svcTags) > 0 {
		b.tags[svcArn] = make(map[string]string)
		maps.Copy(b.tags[svcArn], svcTags)
	}

	cp := svc.toService()

	return &cp, nil
}

// DescribeService returns full service details.
func (b *InMemoryBackend) DescribeService(serviceArn string) (*Service, error) {
	b.mu.RLock("DescribeService")
	defer b.mu.RUnlock()

	svc, ok := b.services[serviceArn]
	if !ok {
		return nil, fmt.Errorf("service %s not found: %w", serviceArn, ErrNotFound)
	}

	cp := svc.toService()

	return &cp, nil
}

// UpdateService updates a service's configuration.
func (b *InMemoryBackend) UpdateService(serviceArn, cpu, memory, imageURI string) (*Service, error) {
	b.mu.Lock("UpdateService")
	defer b.mu.Unlock()

	svc, ok := b.services[serviceArn]
	if !ok {
		return nil, fmt.Errorf("service %s not found: %w", serviceArn, ErrNotFound)
	}

	if cpu != "" {
		svc.CPU = cpu
	}

	if memory != "" {
		svc.Memory = memory
	}

	if imageURI != "" {
		svc.ImageURI = imageURI
	}

	svc.UpdatedAt = time.Now().UTC()
	b.addOperation(svc, opTypeUpdate)

	cp := svc.toService()

	return &cp, nil
}

// DeleteService marks a service as deleted and removes it from active lookup.
func (b *InMemoryBackend) DeleteService(serviceArn string) (*Service, error) {
	b.mu.Lock("DeleteService")
	defer b.mu.Unlock()

	svc, ok := b.services[serviceArn]
	if !ok {
		return nil, fmt.Errorf("service %s not found: %w", serviceArn, ErrNotFound)
	}

	svc.Status = statusDeleted
	svc.UpdatedAt = time.Now().UTC()
	b.addOperation(svc, opTypeDelete)

	cp := svc.toService()

	delete(b.byName, svc.ServiceName)
	delete(b.services, serviceArn)
	delete(b.tags, serviceArn)

	return &cp, nil
}

// ListServices returns services sorted by ARN with pagination.
func (b *InMemoryBackend) ListServices(maxResults int32, nextToken string) ([]*ServiceSummary, string, error) {
	b.mu.RLock("ListServices")
	defer b.mu.RUnlock()

	arns := make([]string, 0, len(b.services))
	for a := range b.services {
		arns = append(arns, a)
	}
	sort.Strings(arns)

	all := make([]*ServiceSummary, 0, len(arns))
	for _, a := range arns {
		s := b.services[a].toSummary()
		all = append(all, &s)
	}

	limit := int(maxResults)
	pg := page.New(all, nextToken, limit, defaultMaxResults)

	return pg.Data, pg.Next, nil
}

// PauseService pauses a running service.
func (b *InMemoryBackend) PauseService(serviceArn string) (*Service, error) {
	b.mu.Lock("PauseService")
	defer b.mu.Unlock()

	svc, ok := b.services[serviceArn]
	if !ok {
		return nil, fmt.Errorf("service %s not found: %w", serviceArn, ErrNotFound)
	}

	svc.Status = statusPaused
	svc.UpdatedAt = time.Now().UTC()
	b.addOperation(svc, opTypePause)

	cp := svc.toService()

	return &cp, nil
}

// ResumeService resumes a paused service.
func (b *InMemoryBackend) ResumeService(serviceArn string) (*Service, error) {
	b.mu.Lock("ResumeService")
	defer b.mu.Unlock()

	svc, ok := b.services[serviceArn]
	if !ok {
		return nil, fmt.Errorf("service %s not found: %w", serviceArn, ErrNotFound)
	}

	svc.Status = statusRunning
	svc.UpdatedAt = time.Now().UTC()
	b.addOperation(svc, opTypeResume)

	cp := svc.toService()

	return &cp, nil
}

// StartDeployment triggers a deployment for a service.
func (b *InMemoryBackend) StartDeployment(serviceArn string) (string, error) {
	b.mu.Lock("StartDeployment")
	defer b.mu.Unlock()

	svc, ok := b.services[serviceArn]
	if !ok {
		return "", fmt.Errorf("service %s not found: %w", serviceArn, ErrNotFound)
	}

	b.addOperation(svc, opTypeDeploy)
	opID := svc.Operations[len(svc.Operations)-1].ID

	return opID, nil
}

// ListOperations returns operations for a service with pagination.
func (b *InMemoryBackend) ListOperations(
	serviceArn string,
	maxResults int32,
	nextToken string,
) ([]*OperationSummary, string, error) {
	b.mu.RLock("ListOperations")
	defer b.mu.RUnlock()

	svc, ok := b.services[serviceArn]
	if !ok {
		return nil, "", fmt.Errorf("service %s not found: %w", serviceArn, ErrNotFound)
	}

	all := make([]*OperationSummary, 0, len(svc.Operations))
	for _, op := range svc.Operations {
		s := op.toSummary()
		all = append(all, &s)
	}

	limit := int(maxResults)
	pg := page.New(all, nextToken, limit, defaultMaxResults)

	return pg.Data, pg.Next, nil
}

// TagResource adds or updates tags on a resource.
func (b *InMemoryBackend) TagResource(resourceArn string, tags map[string]string) error {
	b.mu.Lock("TagResource")
	defer b.mu.Unlock()

	if _, ok := b.services[resourceArn]; !ok {
		return fmt.Errorf("resource %s not found: %w", resourceArn, ErrNotFound)
	}

	if b.tags[resourceArn] == nil {
		b.tags[resourceArn] = make(map[string]string)
	}
	maps.Copy(b.tags[resourceArn], tags)

	return nil
}

// UntagResource removes tags from a resource.
func (b *InMemoryBackend) UntagResource(resourceArn string, keys []string) error {
	b.mu.Lock("UntagResource")
	defer b.mu.Unlock()

	if _, ok := b.services[resourceArn]; !ok {
		return fmt.Errorf("resource %s not found: %w", resourceArn, ErrNotFound)
	}

	for _, k := range keys {
		delete(b.tags[resourceArn], k)
	}

	return nil
}

// ListTagsForResource returns all tags for a resource.
func (b *InMemoryBackend) ListTagsForResource(resourceArn string) (map[string]string, error) {
	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()

	if _, ok := b.services[resourceArn]; !ok {
		return nil, fmt.Errorf("resource %s not found: %w", resourceArn, ErrNotFound)
	}

	result := make(map[string]string)
	maps.Copy(result, b.tags[resourceArn])

	return result, nil
}

// AccountID returns the account ID.
func (b *InMemoryBackend) AccountID() string { return b.accountID }

// Region returns the region.
func (b *InMemoryBackend) Region() string { return b.region }

// Reset clears all state.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	b.services = make(map[string]*storedService)
	b.byName = make(map[string]string)
	b.tags = make(map[string]map[string]string)
}

// Snapshot serializes the backend state to JSON.
func (b *InMemoryBackend) Snapshot() []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	data, _ := json.Marshal(snapshot{
		Services: b.services,
		Tags:     b.tags,
	})

	return data
}

// Restore deserializes backend state from a snapshot.
func (b *InMemoryBackend) Restore(data []byte) error {
	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	var snap snapshot
	if err := json.Unmarshal(data, &snap); err != nil {
		return err
	}

	if snap.Services != nil {
		b.services = snap.Services
		b.byName = make(map[string]string, len(snap.Services))
		for a, svc := range snap.Services {
			b.byName[svc.ServiceName] = a
		}
	} else {
		b.services = make(map[string]*storedService)
		b.byName = make(map[string]string)
	}

	if snap.Tags != nil {
		b.tags = snap.Tags
	} else {
		b.tags = make(map[string]map[string]string)
	}

	return nil
}

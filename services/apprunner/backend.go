package apprunner

import (
	"context"
	"fmt"
	"maps"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/collections"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/page"
	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
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

	// maxOperationsPerService bounds the per-service operation history so a
	// long-lived service that is repeatedly updated/paused/resumed cannot grow
	// svc.Operations without limit. ListOperations returns the most recent ones.
	maxOperationsPerService = 200

	defaultMaxResults = 20
	defaultCPU        = "1 vCPU"
	defaultMemory     = "2 GB"

	asgStatusActive   = "ACTIVE"
	asgStatusInactive = "INACTIVE"

	connStatusAvailable = "AVAILABLE"
	connStatusDeleted   = "DELETED"

	obsStatusActive   = "ACTIVE"
	obsStatusInactive = "INACTIVE"

	vpcConnStatusActive   = "ACTIVE"
	vpcConnStatusInactive = "INACTIVE"

	vicStatusAvailable = "AVAILABLE"
	vicStatusDeleted   = "DELETED"

	customDomainStatusActive = "ACTIVE"

	defaultMaxConcurrency int32 = 100
	defaultMaxSize        int32 = 25
	defaultMinSize        int32 = 1
)

var (
	// ErrNotFound is returned when a resource does not exist.
	ErrNotFound = awserr.New(resourceNotFoundType, awserr.ErrNotFound)
	// ErrAlreadyExists is returned when a service name is already in use.
	ErrAlreadyExists = awserr.New(conflictType, awserr.ErrAlreadyExists)
	// ErrInvalidParameter is returned for invalid input.
	ErrInvalidParameter = awserr.New(invalidRequestType, awserr.ErrInvalidParameter)
	// ErrInvalidState is returned when a service is in an invalid state for the operation.
	ErrInvalidState = awserr.New("InvalidStateException", awserr.ErrConflict)
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

// storedAutoScalingConfiguration holds an ASG config with all fields.
// CreatedAt is first to reduce GC pointer bytes.
type storedAutoScalingConfiguration struct {
	CreatedAt                        time.Time `json:"createdAt"`
	DeletedAt                        time.Time `json:"deletedAt"`
	AutoScalingConfigurationArn      string    `json:"arn"`
	AutoScalingConfigurationName     string    `json:"name"`
	Status                           string    `json:"status"`
	AutoScalingConfigurationRevision int32     `json:"revision"`
	MaxConcurrency                   int32     `json:"maxConcurrency"`
	MaxSize                          int32     `json:"maxSize"`
	MinSize                          int32     `json:"minSize"`
	IsDefault                        bool      `json:"isDefault"`
	HasAssociatedService             bool      `json:"hasAssociatedService"`
}

func (a *storedAutoScalingConfiguration) toASG() AutoScalingConfiguration {
	return AutoScalingConfiguration{
		AutoScalingConfigurationArn:      a.AutoScalingConfigurationArn,
		AutoScalingConfigurationName:     a.AutoScalingConfigurationName,
		AutoScalingConfigurationRevision: a.AutoScalingConfigurationRevision,
		Status:                           a.Status,
		MaxConcurrency:                   a.MaxConcurrency,
		MaxSize:                          a.MaxSize,
		MinSize:                          a.MinSize,
		IsDefault:                        a.IsDefault,
		HasAssociatedService:             a.HasAssociatedService,
		CreatedAt:                        a.CreatedAt,
		DeletedAt:                        a.DeletedAt,
	}
}

func (a *storedAutoScalingConfiguration) toSummary() AutoScalingConfigurationSummary {
	return AutoScalingConfigurationSummary{
		AutoScalingConfigurationArn:      a.AutoScalingConfigurationArn,
		AutoScalingConfigurationName:     a.AutoScalingConfigurationName,
		AutoScalingConfigurationRevision: a.AutoScalingConfigurationRevision,
		Status:                           a.Status,
		IsDefault:                        a.IsDefault,
		CreatedAt:                        a.CreatedAt,
	}
}

// storedConnection holds a connection resource.
// CreatedAt is first to reduce GC pointer bytes.
type storedConnection struct {
	CreatedAt      time.Time `json:"createdAt"`
	ConnectionArn  string    `json:"arn"`
	ConnectionName string    `json:"name"`
	ProviderType   string    `json:"providerType"`
	Status         string    `json:"status"`
}

func (c *storedConnection) toConnection() Connection {
	return Connection{
		ConnectionArn:  c.ConnectionArn,
		ConnectionName: c.ConnectionName,
		ProviderType:   c.ProviderType,
		Status:         c.Status,
		CreatedAt:      c.CreatedAt,
	}
}

func (c *storedConnection) toSummary() ConnectionSummary {
	return ConnectionSummary{
		ConnectionArn:  c.ConnectionArn,
		ConnectionName: c.ConnectionName,
		ProviderType:   c.ProviderType,
		Status:         c.Status,
		CreatedAt:      c.CreatedAt,
	}
}

// storedObservabilityConfiguration holds an observability config.
// CreatedAt is first to reduce GC pointer bytes.
type storedObservabilityConfiguration struct {
	CreatedAt                          time.Time `json:"createdAt"`
	DeletedAt                          time.Time `json:"deletedAt"`
	ObservabilityConfigurationArn      string    `json:"arn"`
	ObservabilityConfigurationName     string    `json:"name"`
	TracingVendor                      string    `json:"tracingVendor"`
	Status                             string    `json:"status"`
	ObservabilityConfigurationRevision int32     `json:"revision"`
	Latest                             bool      `json:"latest"`
}

func (o *storedObservabilityConfiguration) toObs() ObservabilityConfiguration {
	return ObservabilityConfiguration{
		ObservabilityConfigurationArn:      o.ObservabilityConfigurationArn,
		ObservabilityConfigurationName:     o.ObservabilityConfigurationName,
		ObservabilityConfigurationRevision: o.ObservabilityConfigurationRevision,
		TracingVendor:                      o.TracingVendor,
		Status:                             o.Status,
		Latest:                             o.Latest,
		CreatedAt:                          o.CreatedAt,
		DeletedAt:                          o.DeletedAt,
	}
}

func (o *storedObservabilityConfiguration) toSummary() ObservabilityConfigurationSummary {
	return ObservabilityConfigurationSummary{
		ObservabilityConfigurationArn:      o.ObservabilityConfigurationArn,
		ObservabilityConfigurationName:     o.ObservabilityConfigurationName,
		ObservabilityConfigurationRevision: o.ObservabilityConfigurationRevision,
		Status:                             o.Status,
		Latest:                             o.Latest,
		CreatedAt:                          o.CreatedAt,
	}
}

// storedVpcConnector holds a VPC connector resource.
// CreatedAt is first to reduce GC pointer bytes.
type storedVpcConnector struct {
	CreatedAt            time.Time `json:"createdAt"`
	DeletedAt            time.Time `json:"deletedAt"`
	VpcConnectorArn      string    `json:"arn"`
	VpcConnectorName     string    `json:"name"`
	Status               string    `json:"status"`
	SecurityGroups       []string  `json:"securityGroups"`
	Subnets              []string  `json:"subnets"`
	VpcConnectorRevision int32     `json:"revision"`
}

func (v *storedVpcConnector) toVpcConnector() VpcConnector {
	sg := make([]string, len(v.SecurityGroups))
	copy(sg, v.SecurityGroups)
	sn := make([]string, len(v.Subnets))
	copy(sn, v.Subnets)

	return VpcConnector{
		VpcConnectorArn:      v.VpcConnectorArn,
		VpcConnectorName:     v.VpcConnectorName,
		VpcConnectorRevision: v.VpcConnectorRevision,
		Status:               v.Status,
		Subnets:              sn,
		SecurityGroups:       sg,
		CreatedAt:            v.CreatedAt,
		DeletedAt:            v.DeletedAt,
	}
}

// storedVpcIngressConnection holds a VPC ingress connection resource.
// CreatedAt is first to reduce GC pointer bytes.
type storedVpcIngressConnection struct {
	CreatedAt                time.Time `json:"createdAt"`
	DeletedAt                time.Time `json:"deletedAt"`
	VpcIngressConnectionArn  string    `json:"arn"`
	VpcIngressConnectionName string    `json:"name"`
	ServiceArn               string    `json:"serviceArn"`
	AccountID                string    `json:"accountId"`
	DomainName               string    `json:"domainName"`
	VpcID                    string    `json:"vpcId"`
	VpcEndpointID            string    `json:"vpcEndpointId"`
	Status                   string    `json:"status"`
}

func (v *storedVpcIngressConnection) toVIC() VpcIngressConnection {
	return VpcIngressConnection{
		VpcIngressConnectionArn:  v.VpcIngressConnectionArn,
		VpcIngressConnectionName: v.VpcIngressConnectionName,
		ServiceArn:               v.ServiceArn,
		AccountID:                v.AccountID,
		DomainName:               v.DomainName,
		VpcID:                    v.VpcID,
		VpcEndpointID:            v.VpcEndpointID,
		Status:                   v.Status,
		CreatedAt:                v.CreatedAt,
		DeletedAt:                v.DeletedAt,
	}
}

func (v *storedVpcIngressConnection) toSummary() VpcIngressConnectionSummary {
	return VpcIngressConnectionSummary{
		VpcIngressConnectionArn: v.VpcIngressConnectionArn,
		ServiceArn:              v.ServiceArn,
	}
}

// storedCustomDomain holds a custom domain association.
type storedCustomDomain struct {
	DomainName         string `json:"domainName"`
	Status             string `json:"status"`
	EnableWWWSubdomain bool   `json:"enableWwwSubdomain"`
}

func (d *storedCustomDomain) toCustomDomain() CustomDomain {
	return CustomDomain{
		DomainName:         d.DomainName,
		Status:             d.Status,
		EnableWWWSubdomain: d.EnableWWWSubdomain,
	}
}

// snapshot holds serializable backend state.
type snapshot struct {
	Services                  map[string]*storedService                    `json:"services"`
	AutoScalingConfigurations map[string]*storedAutoScalingConfiguration   `json:"autoScalingConfigurations"`
	Connections               map[string]*storedConnection                 `json:"connections"`
	ObservabilityConfigs      map[string]*storedObservabilityConfiguration `json:"observabilityConfigs"`
	VpcConnectors             map[string]*storedVpcConnector               `json:"vpcConnectors"`
	VpcIngressConnections     map[string]*storedVpcIngressConnection       `json:"vpcIngressConnections"`
	CustomDomains             map[string][]*storedCustomDomain             `json:"customDomains"`
	Tags                      map[string]map[string]string                 `json:"tags"`
}

// InMemoryBackend implements StorageBackend using in-memory maps.
type InMemoryBackend struct {
	mu                    *lockmetrics.RWMutex
	services              map[string]*storedService                    // serviceArn → service
	byName                map[string]string                            // serviceName → serviceArn
	autoScalingConfigs    map[string]*storedAutoScalingConfiguration   // arn → asg config
	asgByName             map[string][]*storedAutoScalingConfiguration // name → revisions (sorted ascending)
	connections           map[string]*storedConnection                 // arn → connection
	connByName            map[string]string                            // name → arn
	observabilityConfigs  map[string]*storedObservabilityConfiguration // arn → obs config
	obsByName             map[string][]*storedObservabilityConfiguration
	vpcConnectors         map[string]*storedVpcConnector         // arn → vpc connector
	vpcIngressConnections map[string]*storedVpcIngressConnection // arn → vic
	vicByName             map[string]string                      // name → arn
	customDomains         map[string][]*storedCustomDomain       // serviceArn → domains
	tags                  map[string]map[string]string           // resourceArn → tags
	accountID             string
	region                string
}

// NewInMemoryBackend constructs a new InMemoryBackend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	return &InMemoryBackend{
		mu:                    lockmetrics.New("apprunner"),
		accountID:             accountID,
		region:                region,
		services:              make(map[string]*storedService),
		byName:                make(map[string]string),
		autoScalingConfigs:    make(map[string]*storedAutoScalingConfiguration),
		asgByName:             make(map[string][]*storedAutoScalingConfiguration),
		connections:           make(map[string]*storedConnection),
		connByName:            make(map[string]string),
		observabilityConfigs:  make(map[string]*storedObservabilityConfiguration),
		obsByName:             make(map[string][]*storedObservabilityConfiguration),
		vpcConnectors:         make(map[string]*storedVpcConnector),
		vpcIngressConnections: make(map[string]*storedVpcIngressConnection),
		vicByName:             make(map[string]string),
		customDomains:         make(map[string][]*storedCustomDomain),
		tags:                  make(map[string]map[string]string),
	}
}

func (b *InMemoryBackend) serviceARN(id string) string {
	return arn.Build("apprunner", b.region, b.accountID, "service/"+id)
}

func (b *InMemoryBackend) asgARN(name string, revision int32, id string) string {
	return arn.Build(
		"apprunner", b.region, b.accountID,
		fmt.Sprintf("autoscalingconfiguration/%s/%d/%s", name, revision, id),
	)
}

func (b *InMemoryBackend) connectionARN(name, id string) string {
	return arn.Build("apprunner", b.region, b.accountID, "connection/"+name+"/"+id)
}

func (b *InMemoryBackend) obsARN(name string, revision int32, id string) string {
	return arn.Build(
		"apprunner", b.region, b.accountID,
		fmt.Sprintf("observabilityconfiguration/%s/%d/%s", name, revision, id),
	)
}

func (b *InMemoryBackend) vpcConnectorARN(name string, revision int32, id string) string {
	return arn.Build(
		"apprunner", b.region, b.accountID,
		fmt.Sprintf("vpcconnector/%s/%d/%s", name, revision, id),
	)
}

func (b *InMemoryBackend) vicARN(name, id string) string {
	return arn.Build("apprunner", b.region, b.accountID, "vpcingressconnection/"+name+"/"+id)
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

	// Retain only the most recent maxOperationsPerService entries. Copy into a
	// fresh slice so the dropped prefix is released for GC rather than pinned by
	// the original backing array.
	if len(svc.Operations) > maxOperationsPerService {
		trimmed := make([]*storedOperation, maxOperationsPerService)
		copy(trimmed, svc.Operations[len(svc.Operations)-maxOperationsPerService:])
		svc.Operations = trimmed
	}
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

	if svc.Status != statusRunning {
		return nil, fmt.Errorf(
			"service %s cannot be updated in status %s: %w",
			serviceArn, svc.Status, ErrInvalidState,
		)
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

	arns := collections.SortedKeys(b.services)

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

	if svc.Status != statusRunning {
		return nil, fmt.Errorf(
			"service %s cannot be paused in status %s: %w",
			serviceArn, svc.Status, ErrInvalidState,
		)
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

	if svc.Status != statusPaused {
		return nil, fmt.Errorf(
			"service %s cannot be resumed in status %s: %w",
			serviceArn, svc.Status, ErrInvalidState,
		)
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

	if svc.Status != statusRunning {
		return "", fmt.Errorf(
			"service %s cannot start deployment in status %s: %w",
			serviceArn, svc.Status, ErrInvalidState,
		)
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
func (b *InMemoryBackend) resourceExists(resourceArn string) bool {
	if _, ok := b.services[resourceArn]; ok {
		return true
	}
	if _, ok := b.autoScalingConfigs[resourceArn]; ok {
		return true
	}
	if _, ok := b.connections[resourceArn]; ok {
		return true
	}
	if _, ok := b.observabilityConfigs[resourceArn]; ok {
		return true
	}
	if _, ok := b.vpcConnectors[resourceArn]; ok {
		return true
	}
	if _, ok := b.vpcIngressConnections[resourceArn]; ok {
		return true
	}

	return false
}

func (b *InMemoryBackend) TagResource(resourceArn string, tags map[string]string) error {
	b.mu.Lock("TagResource")
	defer b.mu.Unlock()

	if !b.resourceExists(resourceArn) {
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

	if !b.resourceExists(resourceArn) {
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

	if !b.resourceExists(resourceArn) {
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
	b.autoScalingConfigs = make(map[string]*storedAutoScalingConfiguration)
	b.asgByName = make(map[string][]*storedAutoScalingConfiguration)
	b.connections = make(map[string]*storedConnection)
	b.connByName = make(map[string]string)
	b.observabilityConfigs = make(map[string]*storedObservabilityConfiguration)
	b.obsByName = make(map[string][]*storedObservabilityConfiguration)
	b.vpcConnectors = make(map[string]*storedVpcConnector)
	b.vpcIngressConnections = make(map[string]*storedVpcIngressConnection)
	b.vicByName = make(map[string]string)
	b.customDomains = make(map[string][]*storedCustomDomain)
	b.tags = make(map[string]map[string]string)
}

// Snapshot serializes the backend state to JSON.
func (b *InMemoryBackend) Snapshot(ctx context.Context) []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	return persistence.MarshalSnapshot(ctx, "apprunner", snapshot{
		Services:                  b.services,
		AutoScalingConfigurations: b.autoScalingConfigs,
		Connections:               b.connections,
		ObservabilityConfigs:      b.observabilityConfigs,
		VpcConnectors:             b.vpcConnectors,
		VpcIngressConnections:     b.vpcIngressConnections,
		CustomDomains:             b.customDomains,
		Tags:                      b.tags,
	})
}

// Restore deserializes backend state from a snapshot.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error { //nolint:gocognit // existing issue.
	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	var snap snapshot
	if err := persistence.UnmarshalSnapshot(ctx, "apprunner", data, &snap); err != nil {
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

	if snap.AutoScalingConfigurations != nil {
		b.autoScalingConfigs = snap.AutoScalingConfigurations
		b.asgByName = make(map[string][]*storedAutoScalingConfiguration)
		for _, cfg := range snap.AutoScalingConfigurations {
			b.asgByName[cfg.AutoScalingConfigurationName] = append(
				b.asgByName[cfg.AutoScalingConfigurationName], cfg,
			)
		}
	} else {
		b.autoScalingConfigs = make(map[string]*storedAutoScalingConfiguration)
		b.asgByName = make(map[string][]*storedAutoScalingConfiguration)
	}

	if snap.Connections != nil {
		b.connections = snap.Connections
		b.connByName = make(map[string]string, len(snap.Connections))
		for a, conn := range snap.Connections {
			b.connByName[conn.ConnectionName] = a
		}
	} else {
		b.connections = make(map[string]*storedConnection)
		b.connByName = make(map[string]string)
	}

	if snap.ObservabilityConfigs != nil {
		b.observabilityConfigs = snap.ObservabilityConfigs
		b.obsByName = make(map[string][]*storedObservabilityConfiguration)
		for _, cfg := range snap.ObservabilityConfigs {
			b.obsByName[cfg.ObservabilityConfigurationName] = append(
				b.obsByName[cfg.ObservabilityConfigurationName], cfg,
			)
		}
	} else {
		b.observabilityConfigs = make(map[string]*storedObservabilityConfiguration)
		b.obsByName = make(map[string][]*storedObservabilityConfiguration)
	}

	if snap.VpcConnectors != nil {
		b.vpcConnectors = snap.VpcConnectors
	} else {
		b.vpcConnectors = make(map[string]*storedVpcConnector)
	}

	if snap.VpcIngressConnections != nil {
		b.vpcIngressConnections = snap.VpcIngressConnections
		b.vicByName = make(map[string]string, len(snap.VpcIngressConnections))
		for a, vic := range snap.VpcIngressConnections {
			b.vicByName[vic.VpcIngressConnectionName] = a
		}
	} else {
		b.vpcIngressConnections = make(map[string]*storedVpcIngressConnection)
		b.vicByName = make(map[string]string)
	}

	if snap.CustomDomains != nil {
		b.customDomains = snap.CustomDomains
	} else {
		b.customDomains = make(map[string][]*storedCustomDomain)
	}

	if snap.Tags != nil {
		b.tags = snap.Tags
	} else {
		b.tags = make(map[string]map[string]string)
	}

	return nil
}

// --- AutoScalingConfiguration operations ---

// CreateAutoScalingConfiguration creates a new ASG config revision.
func (b *InMemoryBackend) CreateAutoScalingConfiguration(
	name string,
	maxConcurrency, maxSize, minSize int32,
	tags map[string]string,
) (*AutoScalingConfiguration, error) {
	b.mu.Lock("CreateAutoScalingConfiguration")
	defer b.mu.Unlock()

	revisions := b.asgByName[name]
	revision := int32(len(revisions) + 1) //nolint:gosec // revision count is always small
	id := newID()
	asgArn := b.asgARN(name, revision, id)
	now := time.Now().UTC()

	if maxConcurrency == 0 {
		maxConcurrency = defaultMaxConcurrency
	}

	if maxSize == 0 {
		maxSize = defaultMaxSize
	}

	if minSize == 0 {
		minSize = defaultMinSize
	}

	cfg := &storedAutoScalingConfiguration{
		AutoScalingConfigurationArn:      asgArn,
		AutoScalingConfigurationName:     name,
		AutoScalingConfigurationRevision: revision,
		Status:                           asgStatusActive,
		MaxConcurrency:                   maxConcurrency,
		MaxSize:                          maxSize,
		MinSize:                          minSize,
		IsDefault:                        false,
		HasAssociatedService:             false,
		CreatedAt:                        now,
	}

	b.autoScalingConfigs[asgArn] = cfg
	b.asgByName[name] = append(revisions, cfg)

	if len(tags) > 0 {
		b.tags[asgArn] = make(map[string]string)
		maps.Copy(b.tags[asgArn], tags)
	}

	cp := cfg.toASG()

	return &cp, nil
}

// DescribeAutoScalingConfiguration returns an ASG config by ARN.
func (b *InMemoryBackend) DescribeAutoScalingConfiguration(asgArn string) (*AutoScalingConfiguration, error) {
	b.mu.RLock("DescribeAutoScalingConfiguration")
	defer b.mu.RUnlock()

	cfg, ok := b.autoScalingConfigs[asgArn]
	if !ok {
		return nil, fmt.Errorf("auto scaling configuration %s not found: %w", asgArn, ErrNotFound)
	}

	cp := cfg.toASG()

	return &cp, nil
}

// DeleteAutoScalingConfiguration deletes an ASG config.
func (b *InMemoryBackend) DeleteAutoScalingConfiguration(asgArn string) (*AutoScalingConfiguration, error) {
	b.mu.Lock("DeleteAutoScalingConfiguration")
	defer b.mu.Unlock()

	cfg, ok := b.autoScalingConfigs[asgArn]
	if !ok {
		return nil, fmt.Errorf("auto scaling configuration %s not found: %w", asgArn, ErrNotFound)
	}

	cfg.Status = asgStatusInactive
	cfg.DeletedAt = time.Now().UTC()
	cp := cfg.toASG()

	delete(b.autoScalingConfigs, asgArn)
	delete(b.tags, asgArn)

	revisions := b.asgByName[cfg.AutoScalingConfigurationName]
	remaining := revisions[:0]
	for _, r := range revisions {
		if r.AutoScalingConfigurationArn != asgArn {
			remaining = append(remaining, r)
		}
	}

	if len(remaining) == 0 {
		delete(b.asgByName, cfg.AutoScalingConfigurationName)
	} else {
		b.asgByName[cfg.AutoScalingConfigurationName] = remaining
	}

	return &cp, nil
}

// ListAutoScalingConfigurations returns ASG configs with optional name filter.
func (b *InMemoryBackend) ListAutoScalingConfigurations( //nolint:dupl // existing issue.
	nameFilter string,
	latestOnly bool,
	maxResults int32,
	nextToken string,
) ([]*AutoScalingConfigurationSummary, string, error) {
	b.mu.RLock("ListAutoScalingConfigurations")
	defer b.mu.RUnlock()

	arns := collections.SortedKeys(b.autoScalingConfigs)

	all := make([]*AutoScalingConfigurationSummary, 0, len(arns))
	seen := make(map[string]struct{})

	for _, a := range arns {
		cfg := b.autoScalingConfigs[a]
		if nameFilter != "" && cfg.AutoScalingConfigurationName != nameFilter {
			continue
		}

		if latestOnly {
			revs := b.asgByName[cfg.AutoScalingConfigurationName]
			if len(revs) == 0 {
				continue
			}
			latest := revs[len(revs)-1]
			if _, already := seen[cfg.AutoScalingConfigurationName]; already {
				continue
			}
			seen[cfg.AutoScalingConfigurationName] = struct{}{}
			s := latest.toSummary()
			all = append(all, &s)
		} else {
			s := cfg.toSummary()
			all = append(all, &s)
		}
	}

	limit := int(maxResults)
	pg := page.New(all, nextToken, limit, defaultMaxResults)

	return pg.Data, pg.Next, nil
}

// UpdateDefaultAutoScalingConfiguration sets the default ASG config.
func (b *InMemoryBackend) UpdateDefaultAutoScalingConfiguration(asgArn string) (*AutoScalingConfiguration, error) {
	b.mu.Lock("UpdateDefaultAutoScalingConfiguration")
	defer b.mu.Unlock()

	cfg, ok := b.autoScalingConfigs[asgArn]
	if !ok {
		return nil, fmt.Errorf("auto scaling configuration %s not found: %w", asgArn, ErrNotFound)
	}

	for _, c := range b.autoScalingConfigs {
		c.IsDefault = false
	}

	cfg.IsDefault = true
	cp := cfg.toASG()

	return &cp, nil
}

// ListServicesForAutoScalingConfiguration returns service ARNs using the ASG config.
func (b *InMemoryBackend) ListServicesForAutoScalingConfiguration(
	asgArn string,
	maxResults int32,
	nextToken string,
) ([]string, string, error) {
	b.mu.RLock("ListServicesForAutoScalingConfiguration")
	defer b.mu.RUnlock()

	if _, ok := b.autoScalingConfigs[asgArn]; !ok {
		return nil, "", fmt.Errorf("auto scaling configuration %s not found: %w", asgArn, ErrNotFound)
	}

	arns := make([]string, 0)
	limit := int(maxResults)
	pg := page.New(arns, nextToken, limit, defaultMaxResults)

	return pg.Data, pg.Next, nil
}

// --- Connection operations ---

// CreateConnection creates a new App Runner connection.
func (b *InMemoryBackend) CreateConnection(name, providerType string, tags map[string]string) (*Connection, error) {
	b.mu.Lock("CreateConnection")
	defer b.mu.Unlock()

	if _, exists := b.connByName[name]; exists {
		return nil, fmt.Errorf("connection %s already exists: %w", name, ErrAlreadyExists)
	}

	id := newID()
	connArn := b.connectionARN(name, id)
	now := time.Now().UTC()

	conn := &storedConnection{
		ConnectionArn:  connArn,
		ConnectionName: name,
		ProviderType:   providerType,
		Status:         connStatusAvailable,
		CreatedAt:      now,
	}

	b.connections[connArn] = conn
	b.connByName[name] = connArn

	if len(tags) > 0 {
		b.tags[connArn] = make(map[string]string)
		maps.Copy(b.tags[connArn], tags)
	}

	cp := conn.toConnection()

	return &cp, nil
}

// DeleteConnection deletes a connection.
func (b *InMemoryBackend) DeleteConnection(connArn string) (*Connection, error) {
	b.mu.Lock("DeleteConnection")
	defer b.mu.Unlock()

	conn, ok := b.connections[connArn]
	if !ok {
		return nil, fmt.Errorf("connection %s not found: %w", connArn, ErrNotFound)
	}

	conn.Status = connStatusDeleted
	cp := conn.toConnection()

	delete(b.connections, connArn)
	delete(b.connByName, conn.ConnectionName)
	delete(b.tags, connArn)

	return &cp, nil
}

// ListConnections returns connections with optional name filter.
func (b *InMemoryBackend) ListConnections(
	nameFilter string,
	maxResults int32,
	nextToken string,
) ([]*ConnectionSummary, string, error) {
	b.mu.RLock("ListConnections")
	defer b.mu.RUnlock()

	arns := collections.SortedKeys(b.connections)

	all := make([]*ConnectionSummary, 0, len(arns))
	for _, a := range arns {
		conn := b.connections[a]
		if nameFilter != "" && conn.ConnectionName != nameFilter {
			continue
		}
		s := conn.toSummary()
		all = append(all, &s)
	}

	limit := int(maxResults)
	pg := page.New(all, nextToken, limit, defaultMaxResults)

	return pg.Data, pg.Next, nil
}

// --- ObservabilityConfiguration operations ---

// CreateObservabilityConfiguration creates a new observability config revision.
func (b *InMemoryBackend) CreateObservabilityConfiguration(
	name, tracingVendor string,
	tags map[string]string,
) (*ObservabilityConfiguration, error) {
	b.mu.Lock("CreateObservabilityConfiguration")
	defer b.mu.Unlock()

	revisions := b.obsByName[name]
	revision := int32(len(revisions) + 1) //nolint:gosec // revision count is always small
	id := newID()
	obsArn := b.obsARN(name, revision, id)
	now := time.Now().UTC()

	if len(revisions) > 0 {
		revisions[len(revisions)-1].Latest = false
	}

	cfg := &storedObservabilityConfiguration{
		ObservabilityConfigurationArn:      obsArn,
		ObservabilityConfigurationName:     name,
		ObservabilityConfigurationRevision: revision,
		TracingVendor:                      tracingVendor,
		Status:                             obsStatusActive,
		Latest:                             true,
		CreatedAt:                          now,
	}

	b.observabilityConfigs[obsArn] = cfg
	b.obsByName[name] = append(revisions, cfg)

	if len(tags) > 0 {
		b.tags[obsArn] = make(map[string]string)
		maps.Copy(b.tags[obsArn], tags)
	}

	cp := cfg.toObs()

	return &cp, nil
}

// DescribeObservabilityConfiguration returns an observability config by ARN.
func (b *InMemoryBackend) DescribeObservabilityConfiguration(obsArn string) (*ObservabilityConfiguration, error) {
	b.mu.RLock("DescribeObservabilityConfiguration")
	defer b.mu.RUnlock()

	cfg, ok := b.observabilityConfigs[obsArn]
	if !ok {
		return nil, fmt.Errorf("observability configuration %s not found: %w", obsArn, ErrNotFound)
	}

	cp := cfg.toObs()

	return &cp, nil
}

// DeleteObservabilityConfiguration deletes an observability config.
func (b *InMemoryBackend) DeleteObservabilityConfiguration(obsArn string) (*ObservabilityConfiguration, error) {
	b.mu.Lock("DeleteObservabilityConfiguration")
	defer b.mu.Unlock()

	cfg, ok := b.observabilityConfigs[obsArn]
	if !ok {
		return nil, fmt.Errorf("observability configuration %s not found: %w", obsArn, ErrNotFound)
	}

	cfg.Status = obsStatusInactive
	cfg.DeletedAt = time.Now().UTC()
	cp := cfg.toObs()

	delete(b.observabilityConfigs, obsArn)
	delete(b.tags, obsArn)

	revisions := b.obsByName[cfg.ObservabilityConfigurationName]
	remaining := revisions[:0]
	for _, r := range revisions {
		if r.ObservabilityConfigurationArn != obsArn {
			remaining = append(remaining, r)
		}
	}

	if len(remaining) == 0 {
		delete(b.obsByName, cfg.ObservabilityConfigurationName)
	} else {
		remaining[len(remaining)-1].Latest = true
		b.obsByName[cfg.ObservabilityConfigurationName] = remaining
	}

	return &cp, nil
}

// ListObservabilityConfigurations returns observability configs with optional name filter.
func (b *InMemoryBackend) ListObservabilityConfigurations( //nolint:dupl // existing issue.
	nameFilter string,
	latestOnly bool,
	maxResults int32,
	nextToken string,
) ([]*ObservabilityConfigurationSummary, string, error) {
	b.mu.RLock("ListObservabilityConfigurations")
	defer b.mu.RUnlock()

	arns := collections.SortedKeys(b.observabilityConfigs)

	all := make([]*ObservabilityConfigurationSummary, 0, len(arns))
	seen := make(map[string]struct{})

	for _, a := range arns {
		cfg := b.observabilityConfigs[a]
		if nameFilter != "" && cfg.ObservabilityConfigurationName != nameFilter {
			continue
		}

		if latestOnly {
			revs := b.obsByName[cfg.ObservabilityConfigurationName]
			if len(revs) == 0 {
				continue
			}
			latest := revs[len(revs)-1]
			if _, already := seen[cfg.ObservabilityConfigurationName]; already {
				continue
			}
			seen[cfg.ObservabilityConfigurationName] = struct{}{}
			s := latest.toSummary()
			all = append(all, &s)
		} else {
			s := cfg.toSummary()
			all = append(all, &s)
		}
	}

	limit := int(maxResults)
	pg := page.New(all, nextToken, limit, defaultMaxResults)

	return pg.Data, pg.Next, nil
}

// --- VpcConnector operations ---

// CreateVpcConnector creates a new VPC connector.
func (b *InMemoryBackend) CreateVpcConnector(
	name string,
	subnets, securityGroups []string,
	tags map[string]string,
) (*VpcConnector, error) {
	b.mu.Lock("CreateVpcConnector")
	defer b.mu.Unlock()

	existing := 0
	for _, vc := range b.vpcConnectors {
		if vc.VpcConnectorName == name {
			existing++
		}
	}

	revision := int32(existing + 1)
	id := newID()
	vcArn := b.vpcConnectorARN(name, revision, id)
	now := time.Now().UTC()

	sg := make([]string, len(securityGroups))
	copy(sg, securityGroups)
	sn := make([]string, len(subnets))
	copy(sn, subnets)

	vc := &storedVpcConnector{
		VpcConnectorArn:      vcArn,
		VpcConnectorName:     name,
		VpcConnectorRevision: revision,
		Status:               vpcConnStatusActive,
		Subnets:              sn,
		SecurityGroups:       sg,
		CreatedAt:            now,
	}

	b.vpcConnectors[vcArn] = vc

	if len(tags) > 0 {
		b.tags[vcArn] = make(map[string]string)
		maps.Copy(b.tags[vcArn], tags)
	}

	cp := vc.toVpcConnector()

	return &cp, nil
}

// DescribeVpcConnector returns a VPC connector by ARN.
func (b *InMemoryBackend) DescribeVpcConnector(vcArn string) (*VpcConnector, error) {
	b.mu.RLock("DescribeVpcConnector")
	defer b.mu.RUnlock()

	vc, ok := b.vpcConnectors[vcArn]
	if !ok {
		return nil, fmt.Errorf("vpc connector %s not found: %w", vcArn, ErrNotFound)
	}

	cp := vc.toVpcConnector()

	return &cp, nil
}

// DeleteVpcConnector deletes a VPC connector.
func (b *InMemoryBackend) DeleteVpcConnector(vcArn string) (*VpcConnector, error) {
	b.mu.Lock("DeleteVpcConnector")
	defer b.mu.Unlock()

	vc, ok := b.vpcConnectors[vcArn]
	if !ok {
		return nil, fmt.Errorf("vpc connector %s not found: %w", vcArn, ErrNotFound)
	}

	vc.Status = vpcConnStatusInactive
	vc.DeletedAt = time.Now().UTC()
	cp := vc.toVpcConnector()

	delete(b.vpcConnectors, vcArn)
	delete(b.tags, vcArn)

	return &cp, nil
}

// ListVpcConnectors returns VPC connectors with pagination.
func (b *InMemoryBackend) ListVpcConnectors(maxResults int32, nextToken string) ([]*VpcConnector, string, error) {
	b.mu.RLock("ListVpcConnectors")
	defer b.mu.RUnlock()

	arns := collections.SortedKeys(b.vpcConnectors)

	all := make([]*VpcConnector, 0, len(arns))
	for _, a := range arns {
		cp := b.vpcConnectors[a].toVpcConnector()
		all = append(all, &cp)
	}

	limit := int(maxResults)
	pg := page.New(all, nextToken, limit, defaultMaxResults)

	return pg.Data, pg.Next, nil
}

// --- VpcIngressConnection operations ---

// CreateVpcIngressConnection creates a new VPC ingress connection.
func (b *InMemoryBackend) CreateVpcIngressConnection(
	name, serviceArn, vpcID, vpcEndpointID string,
	tags map[string]string,
) (*VpcIngressConnection, error) {
	b.mu.Lock("CreateVpcIngressConnection")
	defer b.mu.Unlock()

	if _, exists := b.vicByName[name]; exists {
		return nil, fmt.Errorf("vpc ingress connection %s already exists: %w", name, ErrAlreadyExists)
	}

	id := newID()
	vicArn := b.vicARN(name, id)
	now := time.Now().UTC()

	domainName := fmt.Sprintf("%s.%s.awsapprunner.com", id, b.region)

	vic := &storedVpcIngressConnection{
		VpcIngressConnectionArn:  vicArn,
		VpcIngressConnectionName: name,
		ServiceArn:               serviceArn,
		AccountID:                b.accountID,
		DomainName:               domainName,
		VpcID:                    vpcID,
		VpcEndpointID:            vpcEndpointID,
		Status:                   vicStatusAvailable,
		CreatedAt:                now,
	}

	b.vpcIngressConnections[vicArn] = vic
	b.vicByName[name] = vicArn

	if len(tags) > 0 {
		b.tags[vicArn] = make(map[string]string)
		maps.Copy(b.tags[vicArn], tags)
	}

	cp := vic.toVIC()

	return &cp, nil
}

// DescribeVpcIngressConnection returns a VPC ingress connection by ARN.
func (b *InMemoryBackend) DescribeVpcIngressConnection(vicArn string) (*VpcIngressConnection, error) {
	b.mu.RLock("DescribeVpcIngressConnection")
	defer b.mu.RUnlock()

	vic, ok := b.vpcIngressConnections[vicArn]
	if !ok {
		return nil, fmt.Errorf("vpc ingress connection %s not found: %w", vicArn, ErrNotFound)
	}

	cp := vic.toVIC()

	return &cp, nil
}

// DeleteVpcIngressConnection deletes a VPC ingress connection.
func (b *InMemoryBackend) DeleteVpcIngressConnection(vicArn string) (*VpcIngressConnection, error) {
	b.mu.Lock("DeleteVpcIngressConnection")
	defer b.mu.Unlock()

	vic, ok := b.vpcIngressConnections[vicArn]
	if !ok {
		return nil, fmt.Errorf("vpc ingress connection %s not found: %w", vicArn, ErrNotFound)
	}

	vic.Status = vicStatusDeleted
	vic.DeletedAt = time.Now().UTC()
	cp := vic.toVIC()

	delete(b.vpcIngressConnections, vicArn)
	delete(b.vicByName, vic.VpcIngressConnectionName)
	delete(b.tags, vicArn)

	return &cp, nil
}

// ListVpcIngressConnections returns VPC ingress connections with optional filters.
func (b *InMemoryBackend) ListVpcIngressConnections(
	serviceArnFilter, connectionArnFilter string,
	maxResults int32,
	nextToken string,
) ([]*VpcIngressConnectionSummary, string, error) {
	b.mu.RLock("ListVpcIngressConnections")
	defer b.mu.RUnlock()

	arns := collections.SortedKeys(b.vpcIngressConnections)

	all := make([]*VpcIngressConnectionSummary, 0, len(arns))
	for _, a := range arns {
		vic := b.vpcIngressConnections[a]
		if serviceArnFilter != "" && vic.ServiceArn != serviceArnFilter {
			continue
		}
		if connectionArnFilter != "" && vic.VpcIngressConnectionArn != connectionArnFilter {
			continue
		}
		s := vic.toSummary()
		all = append(all, &s)
	}

	limit := int(maxResults)
	pg := page.New(all, nextToken, limit, defaultMaxResults)

	return pg.Data, pg.Next, nil
}

// UpdateVpcIngressConnection updates a VPC ingress connection's VPC config.
func (b *InMemoryBackend) UpdateVpcIngressConnection(
	vicArn, vpcID, vpcEndpointID string,
) (*VpcIngressConnection, error) {
	b.mu.Lock("UpdateVpcIngressConnection")
	defer b.mu.Unlock()

	vic, ok := b.vpcIngressConnections[vicArn]
	if !ok {
		return nil, fmt.Errorf("vpc ingress connection %s not found: %w", vicArn, ErrNotFound)
	}

	if vpcID != "" {
		vic.VpcID = vpcID
	}

	if vpcEndpointID != "" {
		vic.VpcEndpointID = vpcEndpointID
	}

	cp := vic.toVIC()

	return &cp, nil
}

// --- CustomDomain operations ---

// AssociateCustomDomain associates a custom domain with a service.
func (b *InMemoryBackend) AssociateCustomDomain(
	serviceArn, domainName string,
	enableWWW bool,
) (*CustomDomain, error) {
	b.mu.Lock("AssociateCustomDomain")
	defer b.mu.Unlock()

	if _, ok := b.services[serviceArn]; !ok {
		return nil, fmt.Errorf("service %s not found: %w", serviceArn, ErrNotFound)
	}

	for _, d := range b.customDomains[serviceArn] {
		if d.DomainName == domainName {
			return nil, fmt.Errorf("domain %s already associated: %w", domainName, ErrAlreadyExists)
		}
	}

	cd := &storedCustomDomain{
		DomainName:         domainName,
		Status:             customDomainStatusActive,
		EnableWWWSubdomain: enableWWW,
	}
	b.customDomains[serviceArn] = append(b.customDomains[serviceArn], cd)

	cp := cd.toCustomDomain()

	return &cp, nil
}

// DisassociateCustomDomain removes a custom domain from a service.
func (b *InMemoryBackend) DisassociateCustomDomain(serviceArn, domainName string) (*CustomDomain, error) {
	b.mu.Lock("DisassociateCustomDomain")
	defer b.mu.Unlock()

	if _, ok := b.services[serviceArn]; !ok {
		return nil, fmt.Errorf("service %s not found: %w", serviceArn, ErrNotFound)
	}

	domains := b.customDomains[serviceArn]
	for i, d := range domains {
		if d.DomainName == domainName {
			cp := d.toCustomDomain()
			b.customDomains[serviceArn] = append(domains[:i], domains[i+1:]...)

			return &cp, nil
		}
	}

	return nil, fmt.Errorf("domain %s not found on service %s: %w", domainName, serviceArn, ErrNotFound)
}

// DescribeCustomDomains returns custom domains for a service with pagination.
func (b *InMemoryBackend) DescribeCustomDomains(
	serviceArn string,
	maxResults int32,
	nextToken string,
) ([]*CustomDomain, string, string, error) {
	b.mu.RLock("DescribeCustomDomains")
	defer b.mu.RUnlock()

	svc, ok := b.services[serviceArn]
	if !ok {
		return nil, "", "", fmt.Errorf("service %s not found: %w", serviceArn, ErrNotFound)
	}

	all := make([]*CustomDomain, 0, len(b.customDomains[serviceArn]))
	for _, d := range b.customDomains[serviceArn] {
		cp := d.toCustomDomain()
		all = append(all, &cp)
	}

	limit := int(maxResults)
	pg := page.New(all, nextToken, limit, defaultMaxResults)

	dnsTarget := svc.ServiceURL

	return pg.Data, pg.Next, dnsTarget, nil
}

package ecs

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
)

var (
	// ErrCapacityProviderNotFound is returned when a capacity provider does not exist.
	ErrCapacityProviderNotFound = awserr.New(
		"CapacityProviderNotFoundException",
		awserr.ErrNotFound,
	)
	// ErrCapacityProviderAlreadyExists is returned when a capacity provider already exists.
	ErrCapacityProviderAlreadyExists = awserr.New(
		"CapacityProviderAlreadyExistsException",
		awserr.ErrAlreadyExists,
	)
	// ErrExpressGatewayServiceNotFound is returned when an express gateway service does not exist.
	ErrExpressGatewayServiceNotFound = awserr.New(
		"ExpressGatewayServiceNotFoundException",
		awserr.ErrNotFound,
	)
	// ErrExpressGatewayServiceAlreadyExists is returned when an express gateway service already exists.
	ErrExpressGatewayServiceAlreadyExists = awserr.New(
		"ExpressGatewayServiceAlreadyExistsException", awserr.ErrAlreadyExists,
	)
	// ErrAccountSettingNotFound is returned when an account setting does not exist.
	ErrAccountSettingNotFound = awserr.New("AccountSettingNotFoundException", awserr.ErrNotFound)
	// ErrServiceDeploymentNotFound is returned when a service deployment does not exist.
	ErrServiceDeploymentNotFound = awserr.New(
		"ServiceDeploymentNotFoundException",
		awserr.ErrNotFound,
	)
)

// Tag is a key/value metadata pair.
type Tag struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

// ManagedScaling configures managed scaling for an ASG-backed capacity provider.
type ManagedScaling struct {
	Status                    string `json:"status,omitempty"`
	TargetCapacityPercent     int    `json:"targetCapacityPercent,omitempty"`
	MinimumScalingStepSize    int    `json:"minimumScalingStepSize,omitempty"`
	MaximumScalingStepSize    int    `json:"maximumScalingStepSize,omitempty"`
	InstanceWarmupPeriod      int    `json:"instanceWarmupPeriod,omitempty"`
	TargetCapacityUtilization int    `json:"targetCapacityUtilization,omitempty"`
}

// AutoScalingGroupProvider configures an ASG-backed capacity provider.
type AutoScalingGroupProvider struct {
	AutoScalingGroupArn          string          `json:"autoScalingGroupArn"`
	ManagedScaling               *ManagedScaling `json:"managedScaling,omitempty"`
	ManagedTerminationProtection string          `json:"managedTerminationProtection,omitempty"`
	ManagedDraining              string          `json:"managedDraining,omitempty"`
}

// CapacityProvider represents an ECS capacity provider.
type CapacityProvider struct {
	CreatedAt                time.Time                 `json:"createdAt"`
	CapacityProviderArn      string                    `json:"capacityProviderArn"`
	Name                     string                    `json:"name"`
	Status                   string                    `json:"status"`
	UpdateStatus             string                    `json:"updateStatus,omitempty"`
	UpdateStatusReason       string                    `json:"updateStatusReason,omitempty"`
	AutoScalingGroupProvider *AutoScalingGroupProvider `json:"autoScalingGroupProvider,omitempty"`
	Tags                     []Tag                     `json:"tags,omitempty"`
}

// AccountSetting represents an ECS account setting for a principal.
type AccountSetting struct {
	Name         string `json:"name"`
	Value        string `json:"value"`
	PrincipalArn string `json:"principalArn,omitempty"`
}

// Attribute represents a custom ECS resource attribute.
type Attribute struct {
	Name       string `json:"name"`
	Value      string `json:"value,omitempty"`
	TargetID   string `json:"targetId,omitempty"`
	TargetType string `json:"targetType,omitempty"`
}

// ServiceDeployment represents an ECS service deployment.
type ServiceDeployment struct {
	CreatedAt            *time.Time `json:"createdAt,omitempty"`
	UpdatedAt            *time.Time `json:"updatedAt,omitempty"`
	ServiceDeploymentArn string     `json:"serviceDeploymentArn"`
	ClusterArn           string     `json:"clusterArn"`
	ServiceArn           string     `json:"serviceArn"`
	Status               string     `json:"status"`
	StatusReason         string     `json:"statusReason,omitempty"`
}

// serviceDeploymentArnFor derives the ARN of the service deployment record for
// a Deployment, following the
// arn:aws:ecs:region:account:service-deployment/cluster/service/deployment-id
// scheme (mirroring serviceRevisionArnFor in backend_parity2.go). deploymentID
// already carries its "ecs-svc/" prefix (see newPrimaryDeployment/
// newActiveDeployment), matching the shape of real ECS deployment IDs.
func serviceDeploymentArnFor(svc *Service, deploymentID string) string {
	return strings.Replace(svc.ServiceArn, ":service/", ":service-deployment/", 1) + "/" + deploymentID
}

// serviceDeploymentStatusFor maps a Deployment's RolloutState to the
// corresponding ServiceDeploymentStatus value. IN_PROGRESS is the default for
// any rollout state this backend doesn't model as a distinct terminal state.
func serviceDeploymentStatusFor(rolloutState string) string {
	switch rolloutState {
	case deploymentRolloutStateCompleted:
		return "SUCCESSFUL"
	case deploymentRolloutStateFailed:
		return statusStopped
	default:
		return "IN_PROGRESS"
	}
}

// recordServiceDeploymentLocked upserts the ServiceDeployment record tracking
// a single Deployment. Must be called with the write lock held.
func (b *InMemoryBackend) recordServiceDeploymentLocked(svc *Service, dep *Deployment) {
	depArn := serviceDeploymentArnFor(svc, dep.ID)

	createdAt := time.Now()
	if dep.CreatedAt != nil {
		createdAt = time.Unix(int64(*dep.CreatedAt), 0)
	}

	updatedAt := createdAt
	if dep.UpdatedAt != nil {
		updatedAt = time.Unix(int64(*dep.UpdatedAt), 0)
	}

	b.serviceDeployments.Put(&ServiceDeployment{
		ServiceDeploymentArn: depArn,
		ClusterArn:           svc.ClusterArn,
		ServiceArn:           svc.ServiceArn,
		Status:               serviceDeploymentStatusFor(dep.RolloutState),
		StatusReason:         dep.RolloutStateReason,
		CreatedAt:            &createdAt,
		UpdatedAt:            &updatedAt,
	})
}

// deleteServiceDeploymentsForServiceLocked removes every ServiceDeployment
// record belonging to a service (keyed by ServiceDeploymentArn, which embeds
// the deployment ID — not by ServiceArn), so a deleted/purged service doesn't
// leave stale entries behind. Must be called with the write lock held.
func (b *InMemoryBackend) deleteServiceDeploymentsForServiceLocked(serviceArn string) {
	for _, sd := range b.serviceDeployments.All() {
		if sd.ServiceArn == serviceArn {
			b.serviceDeployments.Delete(sd.ServiceDeploymentArn)
		}
	}
}

// syncServiceDeploymentsLocked upserts a ServiceDeployment record for every
// entry currently on svc.Deployments. CreateService, UpdateService, and the
// deployment-circuit-breaker rollback path (deployment.go) all mutate
// svc.Deployments directly and must call this afterward so
// DescribeServiceDeployments/ListServiceDeployments/StopServiceDeployment stay
// in sync — without it, those three routed ops only ever see data seeded by
// the AddServiceDeploymentInternal test helper, never anything a real
// deployment created (see parity-principles.md rule 4: a "real-looking" op
// filtering a never-populated map is a disguised stub). Must be called with
// the write lock held.
func (b *InMemoryBackend) syncServiceDeploymentsLocked(svc *Service) {
	for i := range svc.Deployments {
		b.recordServiceDeploymentLocked(svc, &svc.Deployments[i])
	}
}

// ExpressGatewayService represents an ECS express gateway service.
type ExpressGatewayService struct {
	CreatedAt             time.Time `json:"createdAt"`
	ServiceArn            string    `json:"serviceArn"`
	ServiceName           string    `json:"serviceName"`
	Cluster               string    `json:"cluster"`
	Status                string    `json:"status"`
	ExecutionRoleArn      string    `json:"executionRoleArn"`
	InfrastructureRoleArn string    `json:"infrastructureRoleArn"`
	Tags                  []Tag     `json:"tags,omitempty"`
}

// Failure represents a resource-level failure returned in batch operations.
type Failure struct {
	Arn    string `json:"arn,omitempty"`
	Detail string `json:"detail,omitempty"`
	Reason string `json:"reason,omitempty"`
}

// CreateCapacityProviderInput holds input for CreateCapacityProvider.
type CreateCapacityProviderInput struct {
	Name                     string
	AutoScalingGroupProvider *AutoScalingGroupProvider
	Tags                     []Tag
}

// CreateExpressGatewayServiceInput holds input for CreateExpressGatewayService.
type CreateExpressGatewayServiceInput struct {
	ExecutionRoleArn      string
	InfrastructureRoleArn string
	Cluster               string
	ServiceName           string
	Tags                  []Tag
}

// copyTags returns a deep copy of the given tag slice.
func copyTags(tags []Tag) []Tag {
	if tags == nil {
		return nil
	}

	out := make([]Tag, len(tags))
	copy(out, tags)

	return out
}

// AddAccountSettingInternal adds an account setting directly (seed helper for
// tests). key is unused now that the store.Table derives its own key from
// setting's fields (accountSettingsKeyFn); retained in the signature so
// existing call sites (which always pass a key consistent with
// accountSettingKey(setting.Name, setting.PrincipalArn)) do not need updating.
func (b *InMemoryBackend) AddAccountSettingInternal(_ string, setting *AccountSetting) {
	b.mu.Lock("AddAccountSettingInternal")
	defer b.mu.Unlock()

	b.accountSettings.Put(setting)
}

// AddAttributeInternal adds an attribute directly (seed helper for tests).
func (b *InMemoryBackend) AddAttributeInternal(cluster string, attr *Attribute) {
	b.mu.Lock("AddAttributeInternal")
	defer b.mu.Unlock()

	if b.attributes[cluster] == nil {
		b.attributes[cluster] = make(map[string]*Attribute)
	}

	key := attributeKey(attr.Name, attr.TargetID)
	b.attributes[cluster][key] = attr
}

// AddServiceDeploymentInternal adds a service deployment directly (seed helper for tests).
func (b *InMemoryBackend) AddServiceDeploymentInternal(sd *ServiceDeployment) {
	b.mu.Lock("AddServiceDeploymentInternal")
	defer b.mu.Unlock()

	c := *sd
	b.serviceDeployments.Put(&c)
}

// AddCapacityProviderInternal adds a capacity provider directly (seed helper for tests).
func (b *InMemoryBackend) AddCapacityProviderInternal(cp *CapacityProvider) {
	b.mu.Lock("AddCapacityProviderInternal")
	defer b.mu.Unlock()

	c := *cp
	c.Tags = copyTags(cp.Tags)
	b.capacityProviders.Put(&c)
}

// accountSettingKey builds the map key for an account setting.
func accountSettingKey(name, principalArn string) string {
	return principalArn + ":" + name
}

// attributeKey builds the map key for an attribute.
func attributeKey(name, targetID string) string {
	return name + ":" + targetID
}

// ---- CapacityProvider operations ----

// CreateCapacityProvider creates a new capacity provider.
func (b *InMemoryBackend) CreateCapacityProvider(
	input CreateCapacityProviderInput,
) (*CapacityProvider, error) {
	if input.Name == "" {
		return nil, fmt.Errorf("%w: name is required", ErrInvalidParameter)
	}

	b.mu.Lock("CreateCapacityProvider")
	defer b.mu.Unlock()

	if b.capacityProviders.Has(input.Name) {
		return nil, fmt.Errorf("%w: %s", ErrCapacityProviderAlreadyExists, input.Name)
	}

	cp := &CapacityProvider{
		CreatedAt: time.Now(),
		CapacityProviderArn: fmt.Sprintf(
			"arn:aws:ecs:%s:%s:capacity-provider/%s", b.region, b.accountID, input.Name,
		),
		Name:                     input.Name,
		Status:                   statusActive,
		AutoScalingGroupProvider: input.AutoScalingGroupProvider,
		Tags:                     copyTags(input.Tags),
	}

	b.capacityProviders.Put(cp)

	out := *cp
	out.Tags = copyTags(cp.Tags)

	return &out, nil
}

// DeleteCapacityProvider deletes a capacity provider by name or ARN.
func (b *InMemoryBackend) DeleteCapacityProvider(nameOrArn string) (*CapacityProvider, error) {
	b.mu.Lock("DeleteCapacityProvider")
	defer b.mu.Unlock()

	key, cp := b.findCapacityProviderLocked(nameOrArn)
	if cp == nil {
		return nil, fmt.Errorf("%w: %s", ErrCapacityProviderNotFound, nameOrArn)
	}

	b.capacityProviders.Delete(key)

	out := *cp

	return &out, nil
}

// DescribeCapacityProviders returns capacity providers, optionally filtered by name/ARN.
func (b *InMemoryBackend) DescribeCapacityProviders(
	nameOrArns []string,
) ([]CapacityProvider, error) {
	b.mu.RLock("DescribeCapacityProviders")
	defer b.mu.RUnlock()

	if len(nameOrArns) == 0 {
		all := b.capacityProviders.All()
		out := make([]CapacityProvider, 0, len(all))
		for _, cp := range all {
			c := *cp
			c.Tags = copyTags(cp.Tags)
			out = append(out, c)
		}

		return out, nil
	}

	out := make([]CapacityProvider, 0, len(nameOrArns))

	for _, ref := range nameOrArns {
		_, cp := b.findCapacityProviderLocked(ref)
		if cp == nil {
			// Fall back to built-in FARGATE / FARGATE_SPOT providers.
			builtin := builtinCapacityProvider(ref)
			if builtin == nil {
				return nil, fmt.Errorf("%w: %s", ErrCapacityProviderNotFound, ref)
			}

			out = append(out, *builtin)

			continue
		}

		c := *cp
		c.Tags = copyTags(cp.Tags)
		out = append(out, c)
	}

	return out, nil
}

// findCapacityProviderLocked returns the map key and pointer for a capacity provider by name or ARN.
// Must be called with at least an RLock held.
func (b *InMemoryBackend) findCapacityProviderLocked(nameOrArn string) (string, *CapacityProvider) {
	if cp, ok := b.capacityProviders.Get(nameOrArn); ok {
		return nameOrArn, cp
	}

	for _, cp := range b.capacityProviders.All() {
		if cp.CapacityProviderArn == nameOrArn {
			return cp.Name, cp
		}
	}

	return "", nil
}

// ---- AccountSetting operations ----

// DeleteAccountSetting deletes an account setting for a principal.
func (b *InMemoryBackend) DeleteAccountSetting(name, principalArn string) (*AccountSetting, error) {
	if name == "" {
		return nil, fmt.Errorf("%w: name is required", ErrInvalidParameter)
	}

	key := accountSettingKey(name, principalArn)

	b.mu.Lock("DeleteAccountSetting")
	defer b.mu.Unlock()

	setting, ok := b.accountSettings.Get(key)
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrAccountSettingNotFound, name)
	}

	b.accountSettings.Delete(key)

	out := *setting

	return &out, nil
}

// ---- Attribute operations ----

// DeleteAttributes deletes custom attributes from ECS resources.
func (b *InMemoryBackend) DeleteAttributes(cluster string, attrs []Attribute) ([]Attribute, error) {
	clusterName := clusterKey(b.resolveCluster(cluster))

	b.mu.Lock("DeleteAttributes")
	defer b.mu.Unlock()

	deleted := make([]Attribute, 0, len(attrs))

	for _, attr := range attrs {
		key := attributeKey(attr.Name, attr.TargetID)

		clusterAttrs := b.attributes[clusterName]
		if clusterAttrs == nil {
			continue
		}

		if _, ok := clusterAttrs[key]; ok {
			delete(clusterAttrs, key)
			deleted = append(deleted, attr)
		}
	}

	return deleted, nil
}

// ---- DeleteTaskDefinitions operation ----

// DeleteTaskDefinitions deletes task definitions that are INACTIVE.
// Definitions that are not INACTIVE are reported as failures.
func (b *InMemoryBackend) DeleteTaskDefinitions(
	taskDefinitionArns []string,
) ([]TaskDefinition, []Failure, error) {
	if len(taskDefinitionArns) == 0 {
		return nil, nil, fmt.Errorf("%w: taskDefinitions is required", ErrInvalidParameter)
	}

	b.mu.Lock("DeleteTaskDefinitions")
	defer b.mu.Unlock()

	deleted := make([]TaskDefinition, 0, len(taskDefinitionArns))
	failures := make([]Failure, 0, len(taskDefinitionArns))

	for _, arnRef := range taskDefinitionArns {
		td, err := b.findTaskDefinitionLocked(arnRef)
		if err != nil {
			failures = append(failures, Failure{
				Arn:    arnRef,
				Reason: statusMissing,
				Detail: err.Error(),
			})

			continue
		}

		if td.Status != statusInactive {
			failures = append(failures, Failure{
				Arn:    td.TaskDefinitionArn,
				Reason: "INVALID",
				Detail: fmt.Sprintf("task definition %s is not in INACTIVE state", arnRef),
			})

			continue
		}

		// Remove the revision from the family slice.
		revs := b.taskDefinitions[td.Family]

		for i, r := range revs {
			if r.TaskDefinitionArn == td.TaskDefinitionArn {
				b.taskDefinitions[td.Family] = append(revs[:i], revs[i+1:]...)
				b.taskDefByArn.Delete(td.TaskDefinitionArn)

				break
			}
		}

		deleted = append(deleted, *td)
	}

	return deleted, failures, nil
}

// ---- ServiceDeployment operations ----

// DescribeServiceDeployments returns service deployments by ARN.
func (b *InMemoryBackend) DescribeServiceDeployments(
	serviceDeploymentArns []string,
) ([]ServiceDeployment, []Failure, error) {
	b.mu.RLock("DescribeServiceDeployments")
	defer b.mu.RUnlock()

	deployments := make([]ServiceDeployment, 0, len(serviceDeploymentArns))
	failures := make([]Failure, 0, len(serviceDeploymentArns))

	for _, arn := range serviceDeploymentArns {
		sd, ok := b.serviceDeployments.Get(arn)
		if !ok {
			failures = append(failures, Failure{
				Arn:    arn,
				Reason: statusMissing,
				Detail: fmt.Sprintf("service deployment %s not found", arn),
			})

			continue
		}

		deployments = append(deployments, *sd)
	}

	return deployments, failures, nil
}

// ---- ExpressGatewayService operations ----

// CreateExpressGatewayService creates a new express gateway service.
func (b *InMemoryBackend) CreateExpressGatewayService(
	input CreateExpressGatewayServiceInput,
) (*ExpressGatewayService, error) {
	if input.ExecutionRoleArn == "" {
		return nil, fmt.Errorf("%w: executionRoleArn is required", ErrInvalidParameter)
	}

	if input.InfrastructureRoleArn == "" {
		return nil, fmt.Errorf("%w: infrastructureRoleArn is required", ErrInvalidParameter)
	}

	clusterName := clusterKey(b.resolveCluster(input.Cluster))

	b.mu.Lock("CreateExpressGatewayService")
	defer b.mu.Unlock()

	serviceName := input.ServiceName
	if serviceName == "" {
		serviceName = "express-" + strconv.FormatInt(time.Now().UnixNano(), 10)
	}

	serviceArn := fmt.Sprintf(
		"arn:aws:ecs:%s:%s:service/%s/%s", b.region, b.accountID, clusterName, serviceName,
	)

	if b.expressGatewayServices.Has(serviceArn) {
		return nil, fmt.Errorf("%w: %s", ErrExpressGatewayServiceAlreadyExists, serviceName)
	}

	svc := &ExpressGatewayService{
		CreatedAt:             time.Now(),
		ServiceArn:            serviceArn,
		ServiceName:           serviceName,
		Cluster:               clusterName,
		Status:                statusActive,
		ExecutionRoleArn:      input.ExecutionRoleArn,
		InfrastructureRoleArn: input.InfrastructureRoleArn,
		Tags:                  copyTags(input.Tags),
	}

	b.expressGatewayServices.Put(svc)

	out := *svc
	out.Tags = copyTags(svc.Tags)

	return &out, nil
}

// DeleteExpressGatewayService deletes an express gateway service by ARN.
func (b *InMemoryBackend) DeleteExpressGatewayService(
	serviceArn string,
) (*ExpressGatewayService, error) {
	if serviceArn == "" {
		return nil, fmt.Errorf("%w: serviceArn is required", ErrInvalidParameter)
	}

	b.mu.Lock("DeleteExpressGatewayService")
	defer b.mu.Unlock()

	svc, ok := b.expressGatewayServices.Get(serviceArn)
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrExpressGatewayServiceNotFound, serviceArn)
	}

	b.expressGatewayServices.Delete(serviceArn)

	out := *svc

	return &out, nil
}

// DescribeExpressGatewayService returns an express gateway service by ARN.
func (b *InMemoryBackend) DescribeExpressGatewayService(
	serviceArn string,
) (*ExpressGatewayService, error) {
	if serviceArn == "" {
		return nil, fmt.Errorf("%w: serviceArn is required", ErrInvalidParameter)
	}

	b.mu.RLock("DescribeExpressGatewayService")
	defer b.mu.RUnlock()

	svc, ok := b.expressGatewayServices.Get(serviceArn)
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrExpressGatewayServiceNotFound, serviceArn)
	}

	out := *svc
	out.Tags = copyTags(svc.Tags)

	return &out, nil
}

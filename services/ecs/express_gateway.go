package ecs

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
)

// Defaults applied to an Express service revision's compute configuration
// when the caller omits them, matching the documented defaults on
// CreateExpressGatewayServiceInput in the real SDK (CPU/Memory/HealthCheckPath).
const (
	expressGatewayDefaultCPU             = "256"
	expressGatewayDefaultMemory          = "512"
	expressGatewayDefaultHealthCheckPath = "/ping"
)

// expressGatewayRevisionArnFor derives the ARN of an Express service revision
// from the owning service's ARN, mirroring serviceRevisionArnFor's
// arn:aws:ecs:region:account:service-revision/cluster/service/id scheme used
// for ordinary ECS services (see serviceRevisionArnFor in services.go).
func expressGatewayRevisionArnFor(svc *ExpressGatewayService) string {
	return strings.Replace(svc.ServiceArn, ":service/", ":service-revision/", 1) +
		"/" + uuid.NewString()
}

// validateExpressGatewayConfigInput enforces the real API's mutual-exclusivity
// rule: "If you provide a task definition ARN, you cannot also specify
// primaryContainer, executionRoleArn, taskRoleArn, cpu, or memory" (see the
// TaskDefinitionArn doc comment on CreateExpressGatewayServiceInput /
// UpdateExpressGatewayServiceInput in the real SDK).
func validateExpressGatewayConfigInput(
	taskDefinitionArn string,
	primaryContainer *ExpressGatewayContainer,
	executionRoleArn, taskRoleArn, cpu, memory string,
) error {
	if taskDefinitionArn == "" {
		return nil
	}

	if primaryContainer != nil || executionRoleArn != "" || taskRoleArn != "" ||
		cpu != "" || memory != "" {
		return fmt.Errorf(
			"%w: taskDefinitionArn cannot be specified together with primaryContainer, "+
				"executionRoleArn, taskRoleArn, cpu, or memory",
			ErrInvalidParameter,
		)
	}

	return nil
}

// buildExpressGatewayServiceConfiguration snapshots a service-revision
// configuration from Create/UpdateExpressGatewayServiceInput fields, applying
// the documented AWS defaults for CPU/Memory/HealthCheckPath when the
// task-definition-managed path is not in use.
func buildExpressGatewayServiceConfiguration(
	svc *ExpressGatewayService,
	cpu, memory, healthCheckPath, executionRoleArn, taskRoleArn, taskDefinitionArn string,
	networkConfiguration *ExpressGatewayServiceNetworkConfiguration,
	primaryContainer *ExpressGatewayContainer,
	scalingTarget *ExpressGatewayScalingTarget,
) ExpressGatewayServiceConfiguration {
	cfg := ExpressGatewayServiceConfiguration{
		CreatedAt:            time.Now(),
		ServiceRevisionArn:   expressGatewayRevisionArnFor(svc),
		ExecutionRoleArn:     executionRoleArn,
		TaskRoleArn:          taskRoleArn,
		TaskDefinitionArn:    taskDefinitionArn,
		NetworkConfiguration: networkConfiguration,
		PrimaryContainer:     primaryContainer,
		ScalingTarget:        scalingTarget,
	}

	if taskDefinitionArn == "" {
		cfg.CPU = cpu
		if cfg.CPU == "" {
			cfg.CPU = expressGatewayDefaultCPU
		}

		cfg.Memory = memory
		if cfg.Memory == "" {
			cfg.Memory = expressGatewayDefaultMemory
		}

		cfg.HealthCheckPath = healthCheckPath
		if cfg.HealthCheckPath == "" {
			cfg.HealthCheckPath = expressGatewayDefaultHealthCheckPath
		}
	}

	return cfg
}

// UpdateExpressGatewayService updates an express gateway service, replacing
// its active configuration with a new service revision built from the
// supplied fields (real AWS creates a new revision and rolls out to it; this
// emulator does not model rollout progress, so the new revision becomes
// current immediately).
func (b *InMemoryBackend) UpdateExpressGatewayService(
	input UpdateExpressGatewayServiceInput,
) (*ExpressGatewayService, error) {
	if input.ServiceArn == "" {
		return nil, fmt.Errorf("%w: serviceArn is required", ErrInvalidParameter)
	}

	if err := validateExpressGatewayConfigInput(
		input.TaskDefinitionArn, input.PrimaryContainer,
		input.ExecutionRoleArn, input.TaskRoleArn, input.CPU, input.Memory,
	); err != nil {
		return nil, err
	}

	b.mu.Lock("UpdateExpressGatewayService")
	defer b.mu.Unlock()

	svc, ok := b.expressGatewayServices.Get(input.ServiceArn)
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrServiceNotFound, input.ServiceArn)
	}

	if input.InfrastructureRoleArn != "" {
		svc.InfrastructureRoleArn = input.InfrastructureRoleArn
	}

	cfg := buildExpressGatewayServiceConfiguration(
		svc, input.CPU, input.Memory, input.HealthCheckPath,
		input.ExecutionRoleArn, input.TaskRoleArn, input.TaskDefinitionArn,
		input.NetworkConfiguration, input.PrimaryContainer, input.ScalingTarget,
	)

	svc.ActiveConfigurations = []ExpressGatewayServiceConfiguration{cfg}
	svc.CurrentDeployment = cfg.ServiceRevisionArn
	svc.UpdatedAt = time.Now()

	out := *svc
	out.Tags = copyTags(b.resourceTags[resourceTagKey(svc.ServiceArn)])
	out.ActiveConfigurations = append(
		[]ExpressGatewayServiceConfiguration(nil), svc.ActiveConfigurations...,
	)

	return &out, nil
}

// CreateExpressGatewayService creates a new express gateway service.
func (b *InMemoryBackend) CreateExpressGatewayService(
	input CreateExpressGatewayServiceInput,
) (*ExpressGatewayService, error) {
	if input.InfrastructureRoleArn == "" {
		return nil, fmt.Errorf("%w: infrastructureRoleArn is required", ErrInvalidParameter)
	}

	// executionRoleArn is not documented as a structurally-required member on
	// CreateExpressGatewayServiceInput, but it is meaningless to omit unless a
	// self-managed task definition is supplied via taskDefinitionArn (which
	// carries its own execution role) -- see the mutual-exclusivity rule
	// enforced by validateExpressGatewayConfigInput below.
	if input.ExecutionRoleArn == "" && input.TaskDefinitionArn == "" {
		return nil, fmt.Errorf(
			"%w: executionRoleArn is required unless taskDefinitionArn is specified",
			ErrInvalidParameter,
		)
	}

	if err := validateExpressGatewayConfigInput(
		input.TaskDefinitionArn, input.PrimaryContainer,
		input.ExecutionRoleArn, input.TaskRoleArn, input.CPU, input.Memory,
	); err != nil {
		return nil, err
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
		return nil, fmt.Errorf("%w: express gateway service %s already exists", ErrInvalidParameter, serviceName)
	}

	now := time.Now()

	svc := &ExpressGatewayService{
		CreatedAt:             now,
		UpdatedAt:             now,
		ServiceArn:            serviceArn,
		ServiceName:           serviceName,
		Cluster:               clusterName,
		Status:                statusActive,
		InfrastructureRoleArn: input.InfrastructureRoleArn,
		Tags:                  copyTags(input.Tags),
	}

	cfg := buildExpressGatewayServiceConfiguration(
		svc, input.CPU, input.Memory, input.HealthCheckPath,
		input.ExecutionRoleArn, input.TaskRoleArn, input.TaskDefinitionArn,
		input.NetworkConfiguration, input.PrimaryContainer, input.ScalingTarget,
	)
	svc.ActiveConfigurations = []ExpressGatewayServiceConfiguration{cfg}
	svc.CurrentDeployment = cfg.ServiceRevisionArn

	b.expressGatewayServices.Put(svc)

	// Mirror tags into the resourceTags side map so TagResource/UntagResource/
	// ListTagsForResource (and DescribeExpressGatewayService, which reads tags
	// from this map -- see DescribeExpressGatewayService below) see the same
	// tags applied at creation. Previously svc.Tags and resourceTags were two
	// independent, never-synchronized copies: a TagResource call on this ARN
	// silently never showed up on Describe, and creation-time tags were
	// invisible to ListTagsForResource.
	if len(input.Tags) > 0 {
		b.setResourceTagsLocked(serviceArn, input.Tags)
	}

	out := *svc
	out.Tags = copyTags(svc.Tags)
	out.ActiveConfigurations = append(
		[]ExpressGatewayServiceConfiguration(nil), svc.ActiveConfigurations...,
	)

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
		return nil, fmt.Errorf("%w: %s", ErrServiceNotFound, serviceArn)
	}

	tags := copyTags(b.resourceTags[resourceTagKey(svc.ServiceArn)])

	b.expressGatewayServices.Delete(serviceArn)
	b.deleteResourceTagsLocked(serviceArn)

	out := *svc
	out.Tags = tags

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
		return nil, fmt.Errorf("%w: %s", ErrResourceNotFound, serviceArn)
	}

	out := *svc
	// resourceTags (kept in sync by TagResource/UntagResource) is authoritative,
	// not the creation-time svc.Tags snapshot -- see CreateExpressGatewayService.
	out.Tags = copyTags(b.resourceTags[resourceTagKey(svc.ServiceArn)])
	out.ActiveConfigurations = append(
		[]ExpressGatewayServiceConfiguration(nil), svc.ActiveConfigurations...,
	)

	return &out, nil
}

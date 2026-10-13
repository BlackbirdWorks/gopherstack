package ecs

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

const (
	cloudMapAttrIPv4       = "AWS_INSTANCE_IPV4"
	cloudMapAttrPort       = "AWS_INSTANCE_PORT"
	cloudMapAttrAZ         = "AVAILABILITY_ZONE"
	cloudMapAttrRegion     = "REGION"
	cloudMapAttrCluster    = "ECS_CLUSTER_NAME"
	cloudMapAttrService    = "ECS_SERVICE_NAME"
	cloudMapAttrTaskFamily = "ECS_TASK_DEFINITION_FAMILY"
)

// CloudMapRegistrar registers ECS tasks as AWS Cloud Map instances. The ECS backend resolves it
// at call time; ValidateRegistry rejects a registry ARN that names no Cloud Map service.
type CloudMapRegistrar interface {
	ValidateRegistry(ctx context.Context, registryArn string) error
	RegisterInstance(ctx context.Context, registryArn, instanceID string, attrs map[string]string) error
	DeregisterInstance(ctx context.Context, registryArn, instanceID string) error
}

// SetCloudMapRegistrar wires the Cloud Map registrar; nil disables service discovery registration.
func (b *InMemoryBackend) SetCloudMapRegistrar(r CloudMapRegistrar) {
	b.mu.Lock("SetCloudMapRegistrar")
	defer b.mu.Unlock()

	b.cloudMap = r
}

type cloudMapRegistration struct {
	attrs       map[string]string
	registryArn string
	instanceID  string
}

func (b *InMemoryBackend) validateServiceRegistriesLocked(regs []ServiceRegistry) error {
	if b.cloudMap == nil {
		return nil
	}

	for _, reg := range regs {
		if reg.RegistryArn == "" {
			return fmt.Errorf("%w: serviceRegistries registryArn is required", ErrInvalidParameter)
		}

		if err := b.cloudMap.ValidateRegistry(context.Background(), reg.RegistryArn); err != nil {
			return fmt.Errorf("%w: service registry %s is not valid: %w", ErrInvalidParameter, reg.RegistryArn, err)
		}
	}

	return nil
}

func (b *InMemoryBackend) taskCloudMapRegistrationsLocked(
	task *Task, clusterName string, regs []ServiceRegistry,
) []cloudMapRegistration {
	svc := b.serviceForTaskLocked(task, clusterName)
	if svc == nil || len(regs) == 0 {
		return nil
	}

	ip, hasIP := privateIPFromAttachments(task.Attachments)
	if !hasIP {
		return nil
	}

	out := make([]cloudMapRegistration, 0, len(regs))

	for _, reg := range regs {
		if reg.RegistryArn == "" {
			continue
		}

		attrs := map[string]string{
			cloudMapAttrIPv4:       ip,
			cloudMapAttrAZ:         b.region + "a",
			cloudMapAttrRegion:     b.region,
			cloudMapAttrCluster:    clusterName,
			cloudMapAttrService:    svc.ServiceName,
			cloudMapAttrTaskFamily: taskDefinitionFamily(task.TaskDefinitionArn),
		}

		if port := registryPort(reg); port > 0 {
			attrs[cloudMapAttrPort] = strconv.Itoa(port)
		}

		out = append(out, cloudMapRegistration{
			registryArn: reg.RegistryArn,
			instanceID:  taskID(task.TaskArn),
			attrs:       attrs,
		})
	}

	return out
}

func registryPort(reg ServiceRegistry) int {
	if reg.Port > 0 {
		return reg.Port
	}

	return reg.ContainerPort
}

func taskID(taskArn string) string {
	return taskArn[strings.LastIndex(taskArn, "/")+1:]
}

func taskDefinitionFamily(taskDefinitionArn string) string {
	name := taskDefinitionArn[strings.LastIndex(taskDefinitionArn, "/")+1:]
	family, _, _ := strings.Cut(name, ":")

	return family
}

// registerTaskWithCloudMapLocked registers a RUNNING task with each of its service's registries.
// Best-effort; caller holds b.mu.
func (b *InMemoryBackend) registerTaskWithCloudMapLocked(task *Task, clusterName string) {
	if b.cloudMap == nil {
		return
	}

	svc := b.serviceForTaskLocked(task, clusterName)
	if svc == nil {
		return
	}

	b.registerTaskInRegistriesLocked(task, clusterName, svc.ServiceRegistries)
}

func (b *InMemoryBackend) registerTaskInRegistriesLocked(task *Task, clusterName string, regs []ServiceRegistry) {
	ctx := context.Background()

	for _, r := range b.taskCloudMapRegistrationsLocked(task, clusterName, regs) {
		if err := b.cloudMap.RegisterInstance(ctx, r.registryArn, r.instanceID, r.attrs); err != nil {
			logger.Load(ctx).ErrorContext(ctx, "ecs: Cloud Map RegisterInstance failed",
				"error", err, "registryArn", r.registryArn, "taskArn", task.TaskArn)
		}
	}
}

// deregisterTaskFromCloudMapLocked removes a task from each of its service's registries.
// Best-effort; caller holds b.mu.
func (b *InMemoryBackend) deregisterTaskFromCloudMapLocked(task *Task, clusterName string) {
	if b.cloudMap == nil {
		return
	}

	svc := b.serviceForTaskLocked(task, clusterName)
	if svc == nil {
		return
	}

	b.deregisterTaskFromRegistriesLocked(task, svc.ServiceRegistries)
}

func (b *InMemoryBackend) deregisterTaskFromRegistriesLocked(task *Task, regs []ServiceRegistry) {
	ctx := context.Background()

	for _, reg := range regs {
		if reg.RegistryArn == "" {
			continue
		}

		if err := b.cloudMap.DeregisterInstance(ctx, reg.RegistryArn, taskID(task.TaskArn)); err != nil {
			logger.Load(ctx).ErrorContext(ctx, "ecs: Cloud Map DeregisterInstance failed",
				"error", err, "registryArn", reg.RegistryArn, "taskArn", task.TaskArn)
		}
	}
}

func (b *InMemoryBackend) runningServiceTasksLocked(clusterName, serviceName string) []*Task {
	group := "service:" + serviceName

	var out []*Task

	for _, t := range b.tasksByCluster.Get(clusterName) {
		if t.Group == group && t.LastStatus == statusRunning {
			out = append(out, t)
		}
	}

	return out
}

// moveServiceRegistrationsLocked swaps the running tasks of svc from oldRegs to its current registries.
func (b *InMemoryBackend) moveServiceRegistrationsLocked(clusterName string, svc *Service, oldRegs []ServiceRegistry) {
	if b.cloudMap == nil {
		return
	}

	for _, t := range b.runningServiceTasksLocked(clusterName, svc.ServiceName) {
		b.deregisterTaskFromRegistriesLocked(t, oldRegs)
		b.registerTaskInRegistriesLocked(t, clusterName, svc.ServiceRegistries)
	}
}

// deregisterServiceFromCloudMapLocked removes every running task of svc from its registries.
func (b *InMemoryBackend) deregisterServiceFromCloudMapLocked(clusterName string, svc *Service) {
	if b.cloudMap == nil {
		return
	}

	for _, t := range b.runningServiceTasksLocked(clusterName, svc.ServiceName) {
		b.deregisterTaskFromRegistriesLocked(t, svc.ServiceRegistries)
	}
}

func (b *InMemoryBackend) validateServiceReferencesLocked(
	strategy []CapacityProviderStrategyItem, regs []ServiceRegistry,
) error {
	if err := b.validateCapacityProviderStrategyLocked(strategy); err != nil {
		return err
	}

	return b.validateServiceRegistriesLocked(regs)
}

package ecs

import (
	"fmt"
	"time"
)

// CapacityProviderStrategyItem represents a single item in a default capacity provider strategy.
type CapacityProviderStrategyItem struct {
	CapacityProvider string `json:"capacityProvider"`
	Base             int    `json:"base,omitempty"`
	Weight           int    `json:"weight,omitempty"`
}

// ClusterSetting represents a single cluster setting name/value pair.
type ClusterSetting struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

// TaskProtection represents the protection state for a task.
type TaskProtection struct {
	ExpirationDate    *time.Time `json:"expirationDate,omitempty"`
	TaskArn           string     `json:"taskArn"`
	ProtectionEnabled bool       `json:"protectionEnabled"`
}

// UpdateExpressGatewayServiceInput holds input for UpdateExpressGatewayService.
type UpdateExpressGatewayServiceInput struct {
	ServiceArn            string
	ExecutionRoleArn      string
	InfrastructureRoleArn string
}

// ---- ListAccountSettings operation ----

// ListAccountSettings returns all account settings, optionally filtered by name and principal.
func (b *InMemoryBackend) ListAccountSettings(name, principalArn string) ([]AccountSetting, error) {
	b.mu.RLock("ListAccountSettings")
	defer b.mu.RUnlock()

	out := make([]AccountSetting, 0, len(b.accountSettings))

	for _, setting := range b.accountSettings {
		if name != "" && setting.Name != name {
			continue
		}

		if principalArn != "" && setting.PrincipalArn != principalArn {
			continue
		}

		out = append(out, *setting)
	}

	return out, nil
}

// ---- PutAccountSetting operation ----

// PutAccountSetting creates or updates an account setting for a specific principal.
func (b *InMemoryBackend) PutAccountSetting(name, value, principalArn string) (*AccountSetting, error) {
	if name == "" {
		return nil, fmt.Errorf("%w: name is required", ErrInvalidParameter)
	}

	if value == "" {
		return nil, fmt.Errorf("%w: value is required", ErrInvalidParameter)
	}

	key := accountSettingKey(name, principalArn)

	b.mu.Lock("PutAccountSetting")
	defer b.mu.Unlock()

	setting := &AccountSetting{
		Name:         name,
		Value:        value,
		PrincipalArn: principalArn,
	}

	b.accountSettings[key] = setting

	out := *setting

	return &out, nil
}

// ---- PutAccountSettingDefault operation ----

// PutAccountSettingDefault creates or updates an account-level default setting (no principal).
func (b *InMemoryBackend) PutAccountSettingDefault(name, value string) (*AccountSetting, error) {
	return b.PutAccountSetting(name, value, "")
}

// ---- ListAttributes operation ----

// ListAttributes returns attributes for resources in a cluster, optionally filtered by
// attribute name, target type, and target ID.
func (b *InMemoryBackend) ListAttributes(cluster, targetType, attributeName, targetID string) ([]Attribute, error) {
	clusterName := clusterKey(b.resolveCluster(cluster))

	b.mu.RLock("ListAttributes")
	defer b.mu.RUnlock()

	clusterAttrs := b.attributes[clusterName]

	out := make([]Attribute, 0, len(clusterAttrs))

	for _, attr := range clusterAttrs {
		if attributeName != "" && attr.Name != attributeName {
			continue
		}

		if targetType != "" && attr.TargetType != targetType {
			continue
		}

		if targetID != "" && attr.TargetID != targetID {
			continue
		}

		out = append(out, *attr)
	}

	return out, nil
}

// ---- PutAttributes operation ----

// PutAttributes creates or updates custom attributes on ECS resources in a cluster.
func (b *InMemoryBackend) PutAttributes(cluster string, attrs []Attribute) ([]Attribute, error) {
	clusterName := clusterKey(b.resolveCluster(cluster))

	b.mu.Lock("PutAttributes")
	defer b.mu.Unlock()

	if b.attributes[clusterName] == nil {
		b.attributes[clusterName] = make(map[string]*Attribute)
	}

	out := make([]Attribute, 0, len(attrs))

	for _, attr := range attrs {
		key := attributeKey(attr.Name, attr.TargetID)
		a := attr
		b.attributes[clusterName][key] = &a
		out = append(out, a)
	}

	return out, nil
}

// ---- PutClusterCapacityProviders operation ----

// PutClusterCapacityProviders associates capacity providers with a cluster.
func (b *InMemoryBackend) PutClusterCapacityProviders(
	cluster string,
	capacityProviders []string,
	defaultCapacityProviderStrategy []CapacityProviderStrategyItem,
) (*Cluster, error) {
	clusterName := clusterKey(b.resolveCluster(cluster))

	b.mu.Lock("PutClusterCapacityProviders")
	defer b.mu.Unlock()

	c, ok := b.clusters[clusterName]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrClusterNotFound, cluster)
	}

	c.CapacityProviders = capacityProviders
	c.DefaultCapacityProviderStrategy = defaultCapacityProviderStrategy

	cp := b.enrichCluster(c)

	return &cp, nil
}

// ---- UpdateClusterSettings operation ----

// UpdateClusterSettings updates the settings for an ECS cluster.
func (b *InMemoryBackend) UpdateClusterSettings(cluster string, settings []ClusterSetting) (*Cluster, error) {
	clusterName := clusterKey(b.resolveCluster(cluster))

	b.mu.Lock("UpdateClusterSettings")
	defer b.mu.Unlock()

	c, ok := b.clusters[clusterName]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrClusterNotFound, cluster)
	}

	c.Settings = settings

	cp := b.enrichCluster(c)

	return &cp, nil
}

// ---- UpdateContainerAgent operation ----

// UpdateContainerAgent initiates an update of the container agent on the given instance.
func (b *InMemoryBackend) UpdateContainerAgent(cluster, containerInstance string) (*ContainerInstance, error) {
	clusterName := clusterKey(b.resolveCluster(cluster))

	b.mu.Lock("UpdateContainerAgent")
	defer b.mu.Unlock()

	instances, ok := b.containerInstances[clusterName]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrClusterNotFound, cluster)
	}

	ci, ok := instances[containerInstance]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrContainerInstanceNotFound, containerInstance)
	}

	ci.AgentUpdateStatus = "PENDING"

	out := b.enrichContainerInstance(ci, clusterName)

	return &out, nil
}

// ---- UpdateExpressGatewayService operation ----

// UpdateExpressGatewayService updates an express gateway service.
func (b *InMemoryBackend) UpdateExpressGatewayService(
	input UpdateExpressGatewayServiceInput,
) (*ExpressGatewayService, error) {
	if input.ServiceArn == "" {
		return nil, fmt.Errorf("%w: serviceArn is required", ErrInvalidParameter)
	}

	b.mu.Lock("UpdateExpressGatewayService")
	defer b.mu.Unlock()

	svc, ok := b.expressGatewayServices[input.ServiceArn]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrExpressGatewayServiceNotFound, input.ServiceArn)
	}

	if input.ExecutionRoleArn != "" {
		svc.ExecutionRoleArn = input.ExecutionRoleArn
	}

	if input.InfrastructureRoleArn != "" {
		svc.InfrastructureRoleArn = input.InfrastructureRoleArn
	}

	out := *svc
	out.Tags = copyTags(svc.Tags)

	return &out, nil
}

// ---- GetTaskProtection operation ----

// GetTaskProtection returns the protection state for the given tasks on a cluster.
func (b *InMemoryBackend) GetTaskProtection(
	cluster string,
	taskArns []string,
) ([]TaskProtection, []Failure, error) {
	clusterName := clusterKey(b.resolveCluster(cluster))

	b.mu.RLock("GetTaskProtection")
	defer b.mu.RUnlock()

	tasks, ok := b.tasks[clusterName]
	if !ok {
		return nil, nil, fmt.Errorf("%w: %s", ErrClusterNotFound, cluster)
	}

	if len(taskArns) == 0 {
		out := make([]TaskProtection, 0, len(tasks))

		for arn := range tasks {
			out = append(out, b.taskProtectionLocked(arn))
		}

		return out, nil, nil
	}

	out := make([]TaskProtection, 0, len(taskArns))
	failures := make([]Failure, 0, len(taskArns))

	for _, arn := range taskArns {
		if _, found := tasks[arn]; !found {
			failures = append(failures, Failure{
				Arn:    arn,
				Reason: statusMissing,
				Detail: fmt.Sprintf("task %s not found", arn),
			})

			continue
		}

		out = append(out, b.taskProtectionLocked(arn))
	}

	return out, failures, nil
}

// taskProtectionLocked returns the protection for a task ARN (defaults to unprotected).
// Must be called with at least an RLock held.
func (b *InMemoryBackend) taskProtectionLocked(taskArn string) TaskProtection {
	if tp, ok := b.taskProtections[taskArn]; ok {
		return *tp
	}

	return TaskProtection{TaskArn: taskArn, ProtectionEnabled: false}
}

// ---- UpdateTaskProtection operation ----

// UpdateTaskProtection updates task protection for tasks in a cluster.
func (b *InMemoryBackend) UpdateTaskProtection(
	cluster string,
	taskArns []string,
	protectionEnabled bool,
	expiresInMinutes *int,
) ([]TaskProtection, []Failure, error) {
	clusterName := clusterKey(b.resolveCluster(cluster))

	if len(taskArns) == 0 {
		return nil, nil, fmt.Errorf("%w: tasks is required", ErrInvalidParameter)
	}

	b.mu.Lock("UpdateTaskProtection")
	defer b.mu.Unlock()

	tasks, ok := b.tasks[clusterName]
	if !ok {
		return nil, nil, fmt.Errorf("%w: %s", ErrClusterNotFound, cluster)
	}

	out := make([]TaskProtection, 0, len(taskArns))
	failures := make([]Failure, 0, len(taskArns))

	for _, arn := range taskArns {
		if _, found := tasks[arn]; !found {
			failures = append(failures, Failure{
				Arn:    arn,
				Reason: statusMissing,
				Detail: fmt.Sprintf("task %s not found", arn),
			})

			continue
		}

		tp := &TaskProtection{
			TaskArn:           arn,
			ProtectionEnabled: protectionEnabled,
		}

		if protectionEnabled && expiresInMinutes != nil {
			exp := time.Now().Add(time.Duration(*expiresInMinutes) * time.Minute)
			tp.ExpirationDate = &exp
		}

		b.taskProtections[arn] = tp
		out = append(out, *tp)
	}

	return out, failures, nil
}

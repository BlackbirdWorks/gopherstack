package cloudformation

import (
	"errors"
	"fmt"
	"strings"

	ecsbackend "github.com/blackbirdworks/gopherstack/services/ecs"
)

const resTypeECSTaskSet = "AWS::ECS::TaskSet"

// ---- ECS CapacityProvider ----

func autoScalingGroupProviderProp(
	m map[string]any, params, physicalIDs map[string]string,
) *ecsbackend.AutoScalingGroupProvider {
	if m == nil {
		return nil
	}

	out := &ecsbackend.AutoScalingGroupProvider{
		AutoScalingGroupArn:          strProp(m, "AutoScalingGroupArn", params, physicalIDs),
		ManagedTerminationProtection: strProp(m, "ManagedTerminationProtection", params, physicalIDs),
		ManagedDraining:              strProp(m, "ManagedDraining", params, physicalIDs),
	}

	if ms, ok := m["ManagedScaling"].(map[string]any); ok {
		out.ManagedScaling = &ecsbackend.ManagedScaling{
			Status:                    strProp(ms, "Status", params, physicalIDs),
			TargetCapacityPercent:     intProp(ms, "TargetCapacity"),
			MinimumScalingStepSize:    intProp(ms, "MinimumScalingStepSize"),
			MaximumScalingStepSize:    intProp(ms, "MaximumScalingStepSize"),
			InstanceWarmupPeriod:      intProp(ms, "InstanceWarmupPeriod"),
			TargetCapacityUtilization: intProp(ms, "TargetCapacityUtilization"),
		}
	}

	return out
}

func (rc *ResourceCreator) createECSCapacityProvider(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.ECS == nil {
		return logicalID + "-stub", nil
	}

	name := strProp(props, "Name", params, physicalIDs)
	if name == "" {
		name = logicalID
	}

	var asgProvider *ecsbackend.AutoScalingGroupProvider
	if m, ok := props["AutoScalingGroupProvider"].(map[string]any); ok {
		asgProvider = autoScalingGroupProviderProp(m, params, physicalIDs)
	}

	cp, err := rc.backends.ECS.Backend.CreateCapacityProvider(ecsbackend.CreateCapacityProviderInput{
		Name:                     name,
		AutoScalingGroupProvider: asgProvider,
		Tags:                     tagsFromKV(tagListProp(props, params, physicalIDs)),
	})
	if err != nil {
		return "", fmt.Errorf("create ECS capacity provider %s: %w", name, err)
	}

	return cp.Name, nil
}

func (rc *ResourceCreator) deleteECSCapacityProvider(physicalID string) error {
	if rc.backends.ECS == nil {
		return nil
	}

	_, err := rc.backends.ECS.Backend.DeleteCapacityProvider(physicalID)
	if errors.Is(err, ecsbackend.ErrInvalidParameter) {
		return nil
	}

	return err
}

// tagsFromKV converts a key/value tag map into the []Tag shape ECS's backend expects.
func tagsFromKV(kv map[string]string) []ecsbackend.Tag {
	if len(kv) == 0 {
		return nil
	}

	out := make([]ecsbackend.Tag, 0, len(kv))
	for k, v := range kv {
		out = append(out, ecsbackend.Tag{Key: k, Value: v})
	}

	return out
}

// ---- ECS ClusterCapacityProviderAssociations ----

// capacityProviderStrategyProp decodes a CapacityProviderStrategy-shaped array
// property. key differs by resource type: TaskSet's is "CapacityProviderStrategy",
// ClusterCapacityProviderAssociations' is "DefaultCapacityProviderStrategy".
func capacityProviderStrategyProp(
	props map[string]any, key string, params, physicalIDs map[string]string,
) []ecsbackend.CapacityProviderStrategyItem {
	raw, ok := props[key].([]any)
	if !ok {
		return nil
	}

	out := make([]ecsbackend.CapacityProviderStrategyItem, 0, len(raw))

	for _, item := range raw {
		m, isMap := item.(map[string]any)
		if !isMap {
			continue
		}

		out = append(out, ecsbackend.CapacityProviderStrategyItem{
			CapacityProvider: strProp(m, "CapacityProvider", params, physicalIDs),
			Base:             intProp(m, "Base"),
			Weight:           intProp(m, "Weight"),
		})
	}

	return out
}

func (rc *ResourceCreator) createECSClusterCapacityProviderAssociations(
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.ECS == nil {
		return "", nil
	}

	cluster := strProp(props, "Cluster", params, physicalIDs)
	providers := strSliceProp(props["CapacityProviders"], params, physicalIDs)
	strategy := capacityProviderStrategyProp(props, "DefaultCapacityProviderStrategy", params, physicalIDs)

	c, err := rc.backends.ECS.Backend.PutClusterCapacityProviders(cluster, providers, strategy)
	if err != nil {
		return "", fmt.Errorf("associate ECS capacity providers with cluster %s: %w", cluster, err)
	}

	return c.ClusterName, nil
}

func (rc *ResourceCreator) deleteECSClusterCapacityProviderAssociations(physicalID string) error {
	if rc.backends.ECS == nil {
		return nil
	}

	_, err := rc.backends.ECS.Backend.PutClusterCapacityProviders(physicalID, nil, nil)
	if errors.Is(err, ecsbackend.ErrClusterNotFound) {
		return nil
	}

	return err
}

// ---- ECS TaskSet ----

func (rc *ResourceCreator) createECSTaskSet(
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.ECS == nil {
		return "", nil
	}

	var scale *ecsbackend.TaskSetScale
	if s, ok := props["Scale"].(map[string]any); ok {
		v, _ := s["Value"].(float64)
		scale = &ecsbackend.TaskSetScale{Unit: strProp(s, "Unit", params, physicalIDs), Value: v}
	}

	ts, err := rc.backends.ECS.Backend.CreateTaskSet(ecsbackend.CreateTaskSetInput{
		Cluster:                  strProp(props, "Cluster", params, physicalIDs),
		Service:                  strProp(props, "Service", params, physicalIDs),
		TaskDefinition:           strProp(props, "TaskDefinition", params, physicalIDs),
		ExternalID:               strProp(props, "ExternalId", params, physicalIDs),
		PlatformVersion:          strProp(props, "PlatformVersion", params, physicalIDs),
		LaunchType:               strProp(props, "LaunchType", params, physicalIDs),
		Scale:                    scale,
		CapacityProviderStrategy: capacityProviderStrategyProp(props, "CapacityProviderStrategy", params, physicalIDs),
		Tags:                     tagsFromKV(tagListProp(props, params, physicalIDs)),
	})
	if err != nil {
		return "", fmt.Errorf("create ECS task set: %w", err)
	}

	return ts.TaskSetArn, nil
}

// taskSetInfoFromARN extracts cluster, service, and task set ID from a task set
// ARN of the form "arn:aws:ecs:{region}:{account}:task-set/{cluster}/{service}/{id}".
func taskSetInfoFromARN(taskSetARN string) (string, string, string, bool) {
	_, tail, found := strings.Cut(taskSetARN, "task-set/")
	if !found {
		return "", "", "", false
	}

	parts := strings.Split(tail, "/")
	const wantParts = 3
	if len(parts) != wantParts {
		return "", "", "", false
	}

	return parts[0], parts[1], parts[2], true
}

func (rc *ResourceCreator) deleteECSTaskSet(physicalID string) error {
	if rc.backends.ECS == nil {
		return nil
	}

	cluster, service, id, ok := taskSetInfoFromARN(physicalID)
	if !ok {
		return nil
	}

	_, err := rc.backends.ECS.Backend.DeleteTaskSet(cluster, service, id)
	if errors.Is(err, ecsbackend.ErrTaskSetNotFound) || errors.Is(err, ecsbackend.ErrServiceNotFound) ||
		errors.Is(err, ecsbackend.ErrClusterNotFound) {
		return nil
	}

	return err
}

// ---- ECS PrimaryTaskSet ----

func (rc *ResourceCreator) createECSPrimaryTaskSet(
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.ECS == nil {
		return "", nil
	}

	cluster := strProp(props, "Cluster", params, physicalIDs)
	service := strProp(props, "Service", params, physicalIDs)
	taskSetID := strProp(props, "TaskSetId", params, physicalIDs)

	ts, err := rc.backends.ECS.Backend.UpdateServicePrimaryTaskSet(cluster, service, taskSetID)
	if err != nil {
		return "", fmt.Errorf("set primary ECS task set for service %s: %w", service, err)
	}

	return ts.TaskSetArn, nil
}

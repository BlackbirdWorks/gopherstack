package cloudformation

import (
	"context"
	"fmt"
	"strings"

	ebbackend "github.com/blackbirdworks/gopherstack/services/eventbridge"
)

const resTypeEventsEndpoint = "AWS::Events::Endpoint"

// createEventsEndpointResource handles AWS::Events::Endpoint creation.
func (rc *ResourceCreator) createEventsEndpointResource(
	ctx context.Context,
	logicalID, resourceType string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, bool, error) {
	if resourceType != resTypeEventsEndpoint {
		return "", false, nil
	}

	id, err := rc.createEventsEndpoint(ctx, logicalID, props, params, physicalIDs)

	return id, true, err
}

// deleteEventsEndpointResource handles AWS::Events::Endpoint deletion.
func (rc *ResourceCreator) deleteEventsEndpointResource(
	ctx context.Context, resourceType, physicalID string,
) (bool, error) {
	if resourceType != resTypeEventsEndpoint {
		return false, nil
	}

	return true, rc.deleteEventsEndpoint(ctx, physicalID)
}

// ---- AWS::Events::Endpoint ----
// Ref returns the Endpoint ID (docs), which the backend derives as
// "<name>-<region>" (CreateEndpoint, endpoints.go) rather than storing it as
// its own key -- DeleteEndpoint is Name-keyed, so delete strips the
// deterministic "-<region>" suffix back off. Arn/EndpointUrl/State/
// StateReason aren't derivable from the ID alone, so they're stashed for
// Fn::GetAtt the same way MemoryDB::Cluster's side-channel attrs are.

func (rc *ResourceCreator) createEventsEndpoint(
	ctx context.Context,
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.EventBridge == nil {
		return logicalID + "-stub", nil
	}

	name := strProp(props, "Name", params, physicalIDs)
	if name == "" {
		name = logicalID
	}

	input := ebbackend.CreateEndpointInput{
		Name:        name,
		Description: strProp(props, "Description", params, physicalIDs),
		RoleArn:     strProp(props, "RoleArn", params, physicalIDs),
		EventBuses:  eventsEndpointBuses(props, params, physicalIDs),
	}

	if rc2, ok := props["RoutingConfig"].(map[string]any); ok {
		input.RoutingConfig = eventsEndpointRoutingConfig(rc2, params, physicalIDs)
	}

	if repl, ok := props["ReplicationConfig"].(map[string]any); ok {
		input.ReplicationConfig = &ebbackend.ReplicationConfig{
			State: strProp(repl, "State", params, physicalIDs),
		}
	}

	ep, err := rc.backends.EventBridge.Backend.CreateEndpoint(ctx, input)
	if err != nil {
		return "", fmt.Errorf("create Events endpoint %s: %w", name, err)
	}

	physicalIDs[logicalID+"/Arn"] = ep.Arn
	physicalIDs[logicalID+"/EndpointUrl"] = ep.EndpointURL
	physicalIDs[logicalID+"/State"] = ep.State
	physicalIDs[logicalID+"/StateReason"] = ep.StateReason

	return ep.EndpointID, nil
}

func eventsEndpointBuses(props map[string]any, params, physicalIDs map[string]string) []ebbackend.EndpointEventBus {
	list, ok := props["EventBuses"].([]any)
	if !ok {
		return nil
	}

	buses := make([]ebbackend.EndpointEventBus, 0, len(list))

	for _, v := range list {
		m, isMap := v.(map[string]any)
		if !isMap {
			continue
		}

		buses = append(buses, ebbackend.EndpointEventBus{
			EventBusArn: strProp(m, "EventBusArn", params, physicalIDs),
		})
	}

	return buses
}

func eventsEndpointRoutingConfig(
	m map[string]any, params, physicalIDs map[string]string,
) *ebbackend.RoutingConfig {
	fc, ok := m["FailoverConfig"].(map[string]any)
	if !ok {
		return nil
	}

	failover := &ebbackend.FailoverConfig{}

	if primary, ok2 := fc["Primary"].(map[string]any); ok2 {
		failover.Primary = &ebbackend.Primary{HealthCheck: strProp(primary, "HealthCheck", params, physicalIDs)}
	}

	if secondary, ok2 := fc["Secondary"].(map[string]any); ok2 {
		failover.Secondary = &ebbackend.Secondary{Route: strProp(secondary, "Route", params, physicalIDs)}
	}

	return &ebbackend.RoutingConfig{FailoverConfig: failover}
}

func (rc *ResourceCreator) deleteEventsEndpoint(ctx context.Context, physicalID string) error {
	if rc.backends.EventBridge == nil {
		return nil
	}

	name := strings.TrimSuffix(physicalID, "-"+rc.backends.Region)

	return ignoreNotFound(rc.backends.EventBridge.Backend.DeleteEndpoint(ctx, name), ebbackend.ErrNotFound)
}

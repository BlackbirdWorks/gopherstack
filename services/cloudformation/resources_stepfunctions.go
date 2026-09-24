package cloudformation

import (
	"errors"
	"fmt"

	sfnbackend "github.com/blackbirdworks/gopherstack/services/stepfunctions"
)

// ---- StepFunctions StateMachineVersion ----

func (rc *ResourceCreator) createSFNStateMachineVersion(
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.StepFunctions == nil {
		return "", nil
	}

	smARN := strProp(props, "StateMachineArn", params, physicalIDs)
	description := strProp(props, "Description", params, physicalIDs)
	revisionID := strProp(props, "StateMachineRevisionId", params, physicalIDs)

	v, err := rc.backends.StepFunctions.Backend.PublishStateMachineVersion(smARN, description, revisionID)
	if err != nil {
		return "", fmt.Errorf("publish state machine version for %s: %w", smARN, err)
	}

	return v.StateMachineVersionArn, nil
}

func (rc *ResourceCreator) deleteSFNStateMachineVersion(physicalID string) error {
	if rc.backends.StepFunctions == nil {
		return nil
	}

	return rc.backends.StepFunctions.Backend.DeleteStateMachineVersion(physicalID)
}

// ---- StepFunctions StateMachineAlias ----

func routingConfigProp(props map[string]any, params, physicalIDs map[string]string) []sfnbackend.AliasRoutingConfig {
	if raw, ok := props["RoutingConfiguration"].([]any); ok {
		out := make([]sfnbackend.AliasRoutingConfig, 0, len(raw))

		for _, item := range raw {
			m, isMap := item.(map[string]any)
			if !isMap {
				continue
			}

			out = append(out, sfnbackend.AliasRoutingConfig{
				StateMachineVersionArn: strProp(m, "StateMachineVersionArn", params, physicalIDs),
				Weight:                 intProp(m, "Weight"),
			})
		}

		return out
	}

	// DeploymentPreference and RoutingConfiguration are mutually exclusive; when only
	// DeploymentPreference is given, route 100% of traffic to its target version --
	// this backend has no gradual-traffic-shifting model to approximate LINEAR/CANARY with.
	if dp, ok := props["DeploymentPreference"].(map[string]any); ok {
		const fullWeight = 100

		return []sfnbackend.AliasRoutingConfig{{
			StateMachineVersionArn: strProp(dp, "StateMachineVersionArn", params, physicalIDs),
			Weight:                 fullWeight,
		}}
	}

	return nil
}

func (rc *ResourceCreator) createSFNStateMachineAlias(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.StepFunctions == nil {
		return "", nil
	}

	name := strProp(props, "Name", params, physicalIDs)
	if name == "" {
		name = logicalID
	}

	description := strProp(props, "Description", params, physicalIDs)
	routing := routingConfigProp(props, params, physicalIDs)

	alias, err := rc.backends.StepFunctions.Backend.CreateStateMachineAlias(name, description, routing)
	if err != nil {
		return "", fmt.Errorf("create state machine alias %s: %w", name, err)
	}

	return alias.StateMachineAliasArn, nil
}

func (rc *ResourceCreator) deleteSFNStateMachineAlias(physicalID string) error {
	if rc.backends.StepFunctions == nil {
		return nil
	}

	err := rc.backends.StepFunctions.Backend.DeleteStateMachineAlias(physicalID)
	if errors.Is(err, sfnbackend.ErrStateMachineAliasDoesNotExist) {
		return nil
	}

	return err
}

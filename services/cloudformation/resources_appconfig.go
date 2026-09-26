package cloudformation

import (
	"fmt"
)

// float32Prop reads a float-valued property, accepting JSON numbers.
func float32Prop(props map[string]any, key string) float32 {
	v, _ := props[key].(float64)

	return float32(v)
}

const (
	resTypeAppConfigApplication          = "AWS::AppConfig::Application"
	resTypeAppConfigEnvironment          = "AWS::AppConfig::Environment"
	resTypeAppConfigConfigurationProfile = "AWS::AppConfig::ConfigurationProfile"
	resTypeAppConfigDeploymentStrategy   = "AWS::AppConfig::DeploymentStrategy"
	appConfigDeletionProtectionBypass    = "BYPASS"
)

// createAppConfigResource handles the AppConfig resource types listed above.
// Environment and ConfigurationProfile delete through
// deleteNewestPropsBasedResource since DeleteEnvironment/
// DeleteConfigurationProfile need the owning ApplicationId, a sibling
// property not embedded in either type's Ref value.
func (rc *ResourceCreator) createAppConfigResource(
	logicalID, resourceType string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, bool, error) {
	switch resourceType {
	case resTypeAppConfigApplication:
		id, err := rc.createAppConfigApplication(logicalID, props, params, physicalIDs)

		return id, true, err
	case resTypeAppConfigEnvironment:
		id, err := rc.createAppConfigEnvironment(logicalID, props, params, physicalIDs)

		return id, true, err
	case resTypeAppConfigConfigurationProfile:
		id, err := rc.createAppConfigConfigurationProfile(logicalID, props, params, physicalIDs)

		return id, true, err
	case resTypeAppConfigDeploymentStrategy:
		id, err := rc.createAppConfigDeploymentStrategy(logicalID, props, params, physicalIDs)

		return id, true, err
	default:
		return "", false, nil
	}
}

// deleteAppConfigResource handles AWS::AppConfig::Application and
// ::DeploymentStrategy deletion only; Environment and ConfigurationProfile
// are handled by deleteNewestPropsBasedResource (see the doc comment above).
func (rc *ResourceCreator) deleteAppConfigResource(resourceType, physicalID string) (bool, error) {
	if rc.backends.AppConfig == nil {
		switch resourceType {
		case resTypeAppConfigApplication, resTypeAppConfigDeploymentStrategy:
			return true, nil
		default:
			return false, nil
		}
	}

	switch resourceType {
	case resTypeAppConfigApplication:
		return true, rc.backends.AppConfig.Backend.DeleteApplication(physicalID)
	case resTypeAppConfigDeploymentStrategy:
		return true, rc.backends.AppConfig.Backend.DeleteDeploymentStrategy(physicalID)
	default:
		return false, nil
	}
}

// ---- AWS::AppConfig::Application ----
// Ref returns the application ID (documented).

func (rc *ResourceCreator) createAppConfigApplication(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.AppConfig == nil {
		return logicalID + "-stub", nil
	}

	name := strProp(props, "Name", params, physicalIDs)
	if name == "" {
		name = logicalID
	}

	app, err := rc.backends.AppConfig.Backend.CreateApplication(
		name, strProp(props, "Description", params, physicalIDs), tagListProp(props, params, physicalIDs),
	)
	if err != nil {
		return "", fmt.Errorf("create AppConfig application %s: %w", name, err)
	}

	physicalIDs[logicalID+"/ApplicationId"] = app.ID

	return app.ID, nil
}

// ---- AWS::AppConfig::Environment ----
// Ref returns the environment ID (documented).

func (rc *ResourceCreator) createAppConfigEnvironment(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.AppConfig == nil {
		return logicalID + "-stub", nil
	}

	applicationID := strProp(props, "ApplicationId", params, physicalIDs)
	name := strProp(props, "Name", params, physicalIDs)
	if name == "" {
		name = logicalID
	}

	env, err := rc.backends.AppConfig.Backend.CreateEnvironment(
		applicationID, name, strProp(props, "Description", params, physicalIDs),
		nil, tagListProp(props, params, physicalIDs),
	)
	if err != nil {
		return "", fmt.Errorf("create AppConfig environment %s: %w", name, err)
	}

	physicalIDs[logicalID+"/EnvironmentId"] = env.ID

	return env.ID, nil
}

// deleteAppConfigEnvironment deletes an environment. EnvironmentId is
// system-generated (not a template property), so physicalID -- not
// props -- carries it; ApplicationId is a sibling property.
func (rc *ResourceCreator) deleteAppConfigEnvironment(
	physicalID string, props map[string]any, stackPhysicalIDs map[string]string,
) error {
	if rc.backends.AppConfig == nil {
		return nil
	}

	applicationID := strProp(props, "ApplicationId", nil, stackPhysicalIDs)

	return rc.backends.AppConfig.Backend.DeleteEnvironment(applicationID, physicalID, appConfigDeletionProtectionBypass)
}

// ---- AWS::AppConfig::ConfigurationProfile ----
// Ref returns the configuration profile ID (documented). KmsKeyArn is a
// documented Fn::GetAtt attribute this backend cannot honestly derive (see
// ConfigurationProfile.KmsKeyIdentifier's doc comment) and is left
// unimplemented rather than fabricated.

func (rc *ResourceCreator) createAppConfigConfigurationProfile(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.AppConfig == nil {
		return logicalID + "-stub", nil
	}

	applicationID := strProp(props, "ApplicationId", params, physicalIDs)
	name := strProp(props, "Name", params, physicalIDs)
	if name == "" {
		name = logicalID
	}

	profile, err := rc.backends.AppConfig.Backend.CreateConfigurationProfile(
		applicationID,
		name,
		strProp(props, "Description", params, physicalIDs),
		strProp(props, "LocationUri", params, physicalIDs),
		strProp(props, "Type", params, physicalIDs),
		strProp(props, "RetrievalRoleArn", params, physicalIDs),
		strProp(props, "KmsKeyIdentifier", params, physicalIDs),
		nil,
		tagListProp(props, params, physicalIDs),
	)
	if err != nil {
		return "", fmt.Errorf("create AppConfig configuration profile %s: %w", name, err)
	}

	physicalIDs[logicalID+"/ConfigurationProfileId"] = profile.ID

	return profile.ID, nil
}

// deleteAppConfigConfigurationProfile deletes a configuration profile.
// ConfigurationProfileId is system-generated (not a template property), so
// physicalID -- not props -- carries it; ApplicationId is a sibling property.
func (rc *ResourceCreator) deleteAppConfigConfigurationProfile(
	physicalID string, props map[string]any, stackPhysicalIDs map[string]string,
) error {
	if rc.backends.AppConfig == nil {
		return nil
	}

	applicationID := strProp(props, "ApplicationId", nil, stackPhysicalIDs)

	return rc.backends.AppConfig.Backend.DeleteConfigurationProfile(
		applicationID, physicalID, appConfigDeletionProtectionBypass,
	)
}

// ---- AWS::AppConfig::DeploymentStrategy ----
// Ref returns the deployment strategy ID (documented).

func (rc *ResourceCreator) createAppConfigDeploymentStrategy(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.AppConfig == nil {
		return logicalID + "-stub", nil
	}

	name := strProp(props, "Name", params, physicalIDs)
	if name == "" {
		name = logicalID
	}

	strategy, err := rc.backends.AppConfig.Backend.CreateDeploymentStrategy(
		name,
		strProp(props, "Description", params, physicalIDs),
		int32Prop(props, "DeploymentDurationInMinutes", params, physicalIDs),
		int32Prop(props, "FinalBakeTimeInMinutes", params, physicalIDs),
		float32Prop(props, "GrowthFactor"),
		strProp(props, "GrowthType", params, physicalIDs),
		strProp(props, "ReplicateTo", params, physicalIDs),
		tagListProp(props, params, physicalIDs),
	)
	if err != nil {
		return "", fmt.Errorf("create AppConfig deployment strategy %s: %w", name, err)
	}

	physicalIDs[logicalID+"/Id"] = strategy.ID

	return strategy.ID, nil
}

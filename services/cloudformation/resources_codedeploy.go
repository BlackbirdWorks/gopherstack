package cloudformation

import (
	"errors"
	"fmt"

	codedeploybackend "github.com/blackbirdworks/gopherstack/services/codedeploy"
)

const (
	resTypeCodeDeployApplication      = "AWS::CodeDeploy::Application"
	resTypeCodeDeployDeploymentConfig = "AWS::CodeDeploy::DeploymentConfig"
	resTypeCodeDeployDeploymentGroup  = "AWS::CodeDeploy::DeploymentGroup"
)

// createCodeDeployResource handles AWS::CodeDeploy::* resource creation.
func (rc *ResourceCreator) createCodeDeployResource(
	logicalID, resourceType string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, bool, error) {
	switch resourceType {
	case resTypeCodeDeployApplication:
		id, err := rc.createCodeDeployApplication(logicalID, props, params, physicalIDs)

		return id, true, err
	case resTypeCodeDeployDeploymentConfig:
		id, err := rc.createCodeDeployDeploymentConfig(logicalID, props, params, physicalIDs)

		return id, true, err
	case resTypeCodeDeployDeploymentGroup:
		id, err := rc.createCodeDeployDeploymentGroup(logicalID, props, params, physicalIDs)

		return id, true, err
	default:

		return "", false, nil
	}
}

// deleteCodeDeployResource handles AWS::CodeDeploy::Application/DeploymentConfig
// deletion. AWS::CodeDeploy::DeploymentGroup is NOT handled here: its delete
// needs the resource's ApplicationName property (see deleteCodeDeployDeploymentGroup's
// doc comment), which this prop-less hook point doesn't receive, so it's
// special-cased in deleteServiceResource instead (same as IAM AccessKey/
// UserToGroupAddition).
func (rc *ResourceCreator) deleteCodeDeployResource(resourceType, physicalID string) (bool, error) {
	switch resourceType {
	case resTypeCodeDeployApplication:

		return true, rc.deleteCodeDeployApplication(physicalID)
	case resTypeCodeDeployDeploymentConfig:

		return true, rc.deleteCodeDeployDeploymentConfig(physicalID)
	default:

		return false, nil
	}
}

// ---- AWS::CodeDeploy::Application ----
// Ref returns the application name (CFN docs: aws-resource-codedeploy-application.html).

func (rc *ResourceCreator) createCodeDeployApplication(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.CodeDeploy == nil {
		return logicalID + "-stub", nil
	}

	name := strProp(props, "ApplicationName", params, physicalIDs)
	if name == "" {
		name = logicalID
	}

	computePlatform := strProp(props, "ComputePlatform", params, physicalIDs)
	tags := tagListProp(props, params, physicalIDs)

	app, err := rc.backends.CodeDeploy.Backend.CreateApplication(name, computePlatform, tags)
	if err != nil {
		return "", fmt.Errorf("create CodeDeploy application %s: %w", name, err)
	}

	return app.ApplicationName, nil
}

func (rc *ResourceCreator) deleteCodeDeployApplication(name string) error {
	if rc.backends.CodeDeploy == nil {
		return nil
	}

	err := rc.backends.CodeDeploy.Backend.DeleteApplication(name)
	if errors.Is(err, codedeploybackend.ErrNotFound) {
		return nil
	}

	return err
}

// ---- AWS::CodeDeploy::DeploymentConfig ----
// Ref returns the deployment config name (CFN docs: aws-resource-codedeploy-deploymentconfig.html).

func (rc *ResourceCreator) createCodeDeployDeploymentConfig(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.CodeDeploy == nil {
		return logicalID + "-stub", nil
	}

	name := strProp(props, "DeploymentConfigName", params, physicalIDs)
	if name == "" {
		name = logicalID
	}

	computePlatform := strProp(props, "ComputePlatform", params, physicalIDs)

	cfg, err := rc.backends.CodeDeploy.Backend.CreateDeploymentConfig(
		name, computePlatform,
		minimumHealthyHostsFromProps(props),
		nil, nil,
	)
	if err != nil {
		return "", fmt.Errorf("create CodeDeploy deployment config %s: %w", name, err)
	}

	return cfg.DeploymentConfigName, nil
}

func minimumHealthyHostsFromProps(props map[string]any) *codedeploybackend.MinimumHealthyHosts {
	cfg, ok := props["MinimumHealthyHosts"].(map[string]any)
	if !ok {
		return nil
	}

	hostType, _ := cfg["Type"].(string)
	if hostType == "" {
		return nil
	}

	return &codedeploybackend.MinimumHealthyHosts{
		Type:  hostType,
		Value: intProp(cfg, "Value"),
	}
}

func (rc *ResourceCreator) deleteCodeDeployDeploymentConfig(name string) error {
	if rc.backends.CodeDeploy == nil {
		return nil
	}

	err := rc.backends.CodeDeploy.Backend.DeleteDeploymentConfig(name)
	if errors.Is(err, codedeploybackend.ErrDeploymentConfigNotFound) {
		return nil
	}

	return err
}

// ---- AWS::CodeDeploy::DeploymentGroup ----
// Ref returns the deployment group name (CFN docs: aws-resource-codedeploy-deploymentgroup.html).

func (rc *ResourceCreator) createCodeDeployDeploymentGroup(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.CodeDeploy == nil {
		return logicalID + "-stub", nil
	}

	appName := strProp(props, "ApplicationName", params, physicalIDs)

	dgName := strProp(props, "DeploymentGroupName", params, physicalIDs)
	if dgName == "" {
		dgName = logicalID
	}

	input := codedeploybackend.DeploymentGroupInput{
		ServiceRoleArn:       strProp(props, "ServiceRoleArn", params, physicalIDs),
		DeploymentConfigName: strProp(props, "DeploymentConfigName", params, physicalIDs),
	}

	tags := tagListProp(props, params, physicalIDs)

	dg, err := rc.backends.CodeDeploy.Backend.CreateDeploymentGroup(appName, dgName, input, tags)
	if err != nil {
		return "", fmt.Errorf("create CodeDeploy deployment group %s/%s: %w", appName, dgName, err)
	}

	return dg.DeploymentGroupName, nil
}

// deleteCodeDeployDeploymentGroup needs both ApplicationName and
// DeploymentGroupName, but Ref (and so physID) is only the bare group name
// per the CFN docs -- so ApplicationName is read from the resource's own
// Properties here instead of being folded into the physical ID (same
// resolve-from-props-at-delete-time approach as deleteIAMAccessKey's
// UserName / deleteIAMUserToGroupAddition's GroupName, which f774835c2 added
// stackPhysicalIDs to Delete's signature specifically to support).
func (rc *ResourceCreator) deleteCodeDeployDeploymentGroup(
	props map[string]any, stackPhysicalIDs map[string]string, physID string,
) error {
	if rc.backends.CodeDeploy == nil {
		return nil
	}

	appName := strProp(props, "ApplicationName", nil, stackPhysicalIDs)

	err := rc.backends.CodeDeploy.Backend.DeleteDeploymentGroup(appName, physID)
	if errors.Is(err, codedeploybackend.ErrDeploymentGroupNotFound) || errors.Is(err, codedeploybackend.ErrNotFound) {
		return nil
	}

	return err
}

package cloudformation

import (
	"fmt"

	lambdabackend "github.com/blackbirdworks/gopherstack/services/lambda"
)

const resTypeLambdaCodeSigningConfig = "AWS::Lambda::CodeSigningConfig"

// createLambdaCSCResource handles AWS::Lambda::CodeSigningConfig creation.
func (rc *ResourceCreator) createLambdaCSCResource(
	logicalID, resourceType string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, bool, error) {
	if resourceType != resTypeLambdaCodeSigningConfig {
		return "", false, nil
	}

	id, err := rc.createLambdaCodeSigningConfig(logicalID, props, params, physicalIDs)

	return id, true, err
}

// deleteLambdaCSCResource handles AWS::Lambda::CodeSigningConfig deletion.
func (rc *ResourceCreator) deleteLambdaCSCResource(resourceType, physicalID string) (bool, error) {
	if resourceType != resTypeLambdaCodeSigningConfig {
		return false, nil
	}

	return true, rc.deleteLambdaCodeSigningConfig(physicalID)
}

// ---- AWS::Lambda::CodeSigningConfig ----
// Docs literally say "Ref returns the resource name", but this type has no
// Name property and the backend is ARN-keyed (Create/Delete/
// GetCodeSigningConfig all take the ARN) -- same doc/identifier mismatch
// class as Athena::NamedQuery (resources_athena.go). CodeSigningConfigArn is
// used as Ref/physical ID; CodeSigningConfigId is stashed for Fn::GetAtt.

func (rc *ResourceCreator) createLambdaCodeSigningConfig(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.Lambda == nil {
		return logicalID + "-stub", nil
	}

	imb, ok := rc.backends.Lambda.Backend.(*lambdabackend.InMemoryBackend)
	if !ok {
		return logicalID + "-stub", nil
	}

	input := &lambdabackend.CreateCodeSigningConfigInput{
		Description: strProp(props, "Description", params, physicalIDs),
	}

	if ap, ok2 := props["AllowedPublishers"].(map[string]any); ok2 {
		input.AllowedPublishers = &lambdabackend.AllowedPublishers{
			SigningProfileVersionArns: strSliceProp(ap["SigningProfileVersionArns"], params, physicalIDs),
		}
	}

	if csp, ok2 := props["CodeSigningPolicies"].(map[string]any); ok2 {
		input.CodeSigningPolicies = &lambdabackend.CodeSigningPolicies{
			UntrustedArtifactOnDeployment: strProp(csp, "UntrustedArtifactOnDeployment", params, physicalIDs),
		}
	}

	cfg, err := imb.CreateCodeSigningConfig(input)
	if err != nil {
		return "", fmt.Errorf("create Lambda code signing config: %w", err)
	}

	physicalIDs[logicalID+"/CodeSigningConfigId"] = cfg.CodeSigningConfigID

	return cfg.CodeSigningConfigArn, nil
}

func (rc *ResourceCreator) deleteLambdaCodeSigningConfig(physicalID string) error {
	if rc.backends.Lambda == nil {
		return nil
	}

	imb, ok := rc.backends.Lambda.Backend.(*lambdabackend.InMemoryBackend)
	if !ok {
		return nil
	}

	// DeleteCodeSigningConfig returns ErrFunctionNotFound (not a dedicated
	// not-found sentinel) when the config doesn't exist -- verified in
	// services/lambda/code_signing.go.
	return ignoreNotFound(imb.DeleteCodeSigningConfig(physicalID), lambdabackend.ErrFunctionNotFound)
}

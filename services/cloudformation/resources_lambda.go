package cloudformation

import (
	"context"
	"errors"
	"fmt"
	"strings"

	lambdabackend "github.com/blackbirdworks/gopherstack/services/lambda"
)

// ErrInvalidLayerVersionARN is returned when a LayerVersionArn property is missing or malformed.
var ErrInvalidLayerVersionARN = errors.New("invalid or missing LayerVersionArn")

// parseLayerVersionARN parses a Lambda layer version ARN and returns the layer name and version.
// Expected format: arn:aws:lambda:{region}:{account}:layer:{name}:{version}.
func parseLayerVersionARN(arn string) (string, int64) {
	parts := strings.Split(arn, ":")
	const layerARNParts = 8
	if len(parts) != layerARNParts || parts[5] != "layer" {
		return "", 0
	}

	var v int64
	if _, err := fmt.Sscanf(parts[7], "%d", &v); err != nil {
		return "", 0
	}

	return parts[6], v
}

// ---- Lambda Layer Version ----

func (rc *ResourceCreator) createLambdaLayerVersion(
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

	layerName := strProp(props, "LayerName", params, physicalIDs)
	if layerName == "" {
		layerName = logicalID
	}

	description := strProp(props, "Description", params, physicalIDs)
	licenseInfo := strProp(props, "LicenseInfo", params, physicalIDs)

	var compatibleRuntimes []string
	if list, ok2 := props["CompatibleRuntimes"].([]any); ok2 {
		for _, v := range list {
			if s := resolve(v, params, physicalIDs); s != "" {
				compatibleRuntimes = append(compatibleRuntimes, s)
			}
		}
	}

	input := &lambdabackend.PublishLayerVersionInput{
		LayerName:          layerName,
		Description:        description,
		LicenseInfo:        licenseInfo,
		CompatibleRuntimes: compatibleRuntimes,
		Content: &lambdabackend.LayerVersionContentInput{
			ZipFile: []byte{},
		},
	}

	out, err := imb.PublishLayerVersion(input)
	if err != nil {
		return "", fmt.Errorf("publish Lambda layer version %s: %w", layerName, err)
	}

	return out.LayerVersionArn, nil
}

func (rc *ResourceCreator) deleteLambdaLayerVersion(layerVersionARN string) error {
	if rc.backends.Lambda == nil {
		return nil
	}

	imb, ok := rc.backends.Lambda.Backend.(*lambdabackend.InMemoryBackend)
	if !ok {
		return nil
	}

	// Parse ARN: arn:aws:lambda:{region}:{account}:layer:{name}:{version}
	layerName, version := parseLayerVersionARN(layerVersionARN)
	if layerName == "" {
		return nil
	}

	return imb.DeleteLayerVersion(layerName, version)
}

func (rc *ResourceCreator) createLambdaLayerVersionPermission(
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

	layerVersionARN := strProp(props, "LayerVersionArn", params, physicalIDs)
	statementID := strProp(props, "Id", params, physicalIDs)
	if statementID == "" {
		statementID = logicalID
	}

	action := strProp(props, "Action", params, physicalIDs)
	principal := strProp(props, "Principal", params, physicalIDs)
	orgID := strProp(props, "OrganizationId", params, physicalIDs)

	layerName, version := parseLayerVersionARN(layerVersionARN)
	if layerName == "" {
		return "", fmt.Errorf("%w: %q (resource %s)", ErrInvalidLayerVersionARN, layerVersionARN, logicalID)
	}

	_, err := imb.AddLayerVersionPermission(layerName, version, &lambdabackend.AddLayerVersionPermissionInput{
		Action:         action,
		Principal:      principal,
		StatementID:    statementID,
		OrganizationID: orgID,
	})
	if err != nil {
		return "", fmt.Errorf("add Lambda layer version permission: %w", err)
	}

	// Physical ID encodes layer ARN + statement ID.
	return layerVersionARN + ":" + statementID, nil
}

func (rc *ResourceCreator) deleteLambdaLayerVersionPermission(physicalID string) error {
	if rc.backends.Lambda == nil {
		return nil
	}

	imb, ok := rc.backends.Lambda.Backend.(*lambdabackend.InMemoryBackend)
	if !ok {
		return nil
	}

	// physicalID format: {layerVersionARN}:{statementID}
	lastColon := strings.LastIndex(physicalID, ":")
	if lastColon < 0 {
		return nil
	}

	layerVersionARN := physicalID[:lastColon]
	statementID := physicalID[lastColon+1:]

	layerName, version := parseLayerVersionARN(layerVersionARN)
	if layerName == "" {
		return nil
	}

	return imb.RemoveLayerVersionPermission(layerName, version, statementID)
}

// ---- Lambda EventInvokeConfig and Url ----

// createLambdaSupplementalResource handles Lambda EventInvokeConfig and Url resource creation.
func (rc *ResourceCreator) createLambdaSupplementalResource(
	ctx context.Context,
	logicalID, resourceType string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, bool, error) {
	switch resourceType {
	case "AWS::Lambda::EventInvokeConfig":
		id, err := rc.createLambdaEventInvokeConfig(logicalID, props, params, physicalIDs)

		return id, true, err
	case "AWS::Lambda::Url":
		id, err := rc.createLambdaURL(ctx, logicalID, props, params, physicalIDs)

		return id, true, err
	default:
		return "", false, nil
	}
}

// deleteLambdaSupplementalResource handles Lambda EventInvokeConfig and Url resource deletion.
func (rc *ResourceCreator) deleteLambdaSupplementalResource(
	resourceType, physicalID string,
) (bool, error) {
	switch resourceType {
	case "AWS::Lambda::EventInvokeConfig":
		return true, rc.deleteLambdaEventInvokeConfig(physicalID)
	case "AWS::Lambda::Url":
		return true, rc.deleteLambdaURL(physicalID)
	default:
		return false, nil
	}
}

func (rc *ResourceCreator) createLambdaEventInvokeConfig(
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
	functionName := strProp(props, "FunctionName", params, physicalIDs)
	input := &lambdabackend.PutFunctionEventInvokeConfigInput{}
	if v := intProp(props, "MaximumRetryAttempts"); v >= 0 {
		retries := v
		input.MaximumRetryAttempts = &retries
	}
	if v := intProp(props, "MaximumEventAgeInSeconds"); v > 0 {
		age := v
		input.MaximumEventAgeInSeconds = &age
	}
	if _, err := imb.PutFunctionEventInvokeConfig(functionName, input); err != nil {
		return "", fmt.Errorf("create Lambda event invoke config %s: %w", functionName, err)
	}

	return functionName, nil
}

func (rc *ResourceCreator) deleteLambdaEventInvokeConfig(physicalID string) error {
	if rc.backends.Lambda == nil {
		return nil
	}
	imb, ok := rc.backends.Lambda.Backend.(*lambdabackend.InMemoryBackend)
	if !ok {
		return nil
	}

	return imb.DeleteFunctionEventInvokeConfig(physicalID)
}

func (rc *ResourceCreator) createLambdaURL(
	ctx context.Context,
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
	functionName := strProp(props, "TargetFunctionArn", params, physicalIDs)
	if functionName == "" {
		functionName = logicalID
	}
	authType := strProp(props, "AuthType", params, physicalIDs)
	if authType == "" {
		authType = "AWS_IAM"
	}
	invokeMode := strProp(props, "InvokeMode", params, physicalIDs)
	var cors *lambdabackend.FunctionURLCors
	if corsMap, ok2 := props["Cors"].(map[string]any); ok2 {
		cors = &lambdabackend.FunctionURLCors{}
		if origins, ok3 := corsMap["AllowOrigins"].([]any); ok3 {
			for _, o := range origins {
				cors.AllowOrigins = append(cors.AllowOrigins, fmt.Sprintf("%v", o))
			}
		}
	}
	cfg, err := imb.CreateFunctionURLConfig(ctx, functionName, authType, cors, invokeMode)
	if err != nil {
		return "", fmt.Errorf("create Lambda function URL %s: %w", functionName, err)
	}

	return cfg.FunctionArn, nil
}

func (rc *ResourceCreator) deleteLambdaURL(physicalID string) error {
	if rc.backends.Lambda == nil {
		return nil
	}
	imb, ok := rc.backends.Lambda.Backend.(*lambdabackend.InMemoryBackend)
	if !ok {
		return nil
	}

	return imb.DeleteFunctionURLConfig(physicalID)
}

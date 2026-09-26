package cloudformation

import (
	"context"
	"errors"
	"fmt"

	codebuildbackend "github.com/blackbirdworks/gopherstack/services/codebuild"
	kinesisbackend "github.com/blackbirdworks/gopherstack/services/kinesis"
	lambdabackend "github.com/blackbirdworks/gopherstack/services/lambda"
)

const (
	resTypeCodeBuildReportGroup  = "AWS::CodeBuild::ReportGroup"
	resTypeKinesisResourcePolicy = "AWS::Kinesis::ResourcePolicy"
	resTypeLambdaResourcePolicy  = "AWS::Lambda::ResourcePolicy"
)

func (rc *ResourceCreator) createMiscMoreResource(
	ctx context.Context,
	logicalID, resourceType string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, bool, error) {
	switch resourceType {
	case resTypeCodeBuildReportGroup:
		id, err := rc.createCodeBuildReportGroup(logicalID, props, params, physicalIDs)

		return id, true, err
	case resTypeKinesisResourcePolicy:
		id, err := rc.createKinesisResourcePolicy(ctx, logicalID, props, params, physicalIDs)

		return id, true, err
	case resTypeLambdaResourcePolicy:
		id, err := rc.createLambdaResourcePolicy(logicalID, props, params, physicalIDs)

		return id, true, err
	default:
		return "", false, nil
	}
}

func (rc *ResourceCreator) deleteMiscMoreResource(ctx context.Context, resourceType, physicalID string) (bool, error) {
	switch resourceType {
	case resTypeCodeBuildReportGroup:
		return true, rc.deleteCodeBuildReportGroup(physicalID)
	case resTypeKinesisResourcePolicy:
		return true, rc.deleteKinesisResourcePolicy(ctx, physicalID)
	case resTypeLambdaResourcePolicy:
		return true, rc.deleteLambdaResourcePolicy(physicalID)
	default:
		return false, nil
	}
}

// ---- AWS::CodeBuild::ReportGroup ----
// Ref returns the report group ARN; Fn::GetAtt Arn returns the same value.

func (rc *ResourceCreator) createCodeBuildReportGroup(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.CodeBuild == nil {
		return logicalID + "-stub", nil
	}

	name := strProp(props, "Name", params, physicalIDs)
	if name == "" {
		name = logicalID
	}

	var exportConfig codebuildbackend.ReportExportConfig
	if ec, ok := props["ExportConfig"].(map[string]any); ok {
		exportConfig.ExportConfigType = strProp(ec, "ExportConfigType", params, physicalIDs)
	}

	rg, err := rc.backends.CodeBuild.Backend.CreateReportGroup(
		name, strProp(props, "Type", params, physicalIDs), exportConfig, tagListProp(props, params, physicalIDs),
	)
	if err != nil {
		return "", fmt.Errorf("create CodeBuild report group %s: %w", name, err)
	}

	physicalIDs[logicalID+"/Arn"] = rg.Arn

	return rg.Arn, nil
}

func (rc *ResourceCreator) deleteCodeBuildReportGroup(arn string) error {
	if rc.backends.CodeBuild == nil {
		return nil
	}

	err := rc.backends.CodeBuild.Backend.DeleteReportGroup(arn, true)
	if errors.Is(err, codebuildbackend.ErrNotFound) {
		return nil
	}

	return err
}

// ---- AWS::Kinesis::ResourcePolicy ----
// Ref is undocumented (the CFN reference lists "Return values Ref" with no
// description text); the target resource's own ARN (ResourceArn) is used as
// the primary identifier, matching this backend's own keying.

func (rc *ResourceCreator) createKinesisResourcePolicy(
	ctx context.Context,
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.Kinesis == nil {
		return logicalID + "-stub", nil
	}

	resourceARN := strProp(props, "ResourceArn", params, physicalIDs)

	err := rc.backends.Kinesis.Backend.PutResourcePolicy(ctx, &kinesisbackend.PutResourcePolicyInput{
		ResourceARN: resourceARN,
		Policy:      jsonProp(props, "ResourcePolicy"),
	})
	if err != nil {
		return "", fmt.Errorf("put Kinesis resource policy for %s: %w", resourceARN, err)
	}

	return resourceARN, nil
}

func (rc *ResourceCreator) deleteKinesisResourcePolicy(ctx context.Context, resourceARN string) error {
	if rc.backends.Kinesis == nil {
		return nil
	}

	err := rc.backends.Kinesis.Backend.DeleteResourcePolicy(ctx, &kinesisbackend.DeleteResourcePolicyInput{
		ResourceARN: resourceARN,
	})
	if errors.Is(err, kinesisbackend.ErrResourcePolicyNotFound) {
		return nil
	}

	return err
}

// ---- AWS::Lambda::ResourcePolicy ----
// Ref returns the primary ID of the resource (ResourceArn), i.e. the
// function's FunctionResourceArn.

func (rc *ResourceCreator) createLambdaResourcePolicy(
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

	resourceARN := strProp(props, "FunctionResourceArn", params, physicalIDs)

	_, err := imb.PutResourcePolicy(resourceARN, jsonProp(props, "PolicyDocument"), "")
	if err != nil {
		return "", fmt.Errorf("put Lambda resource policy for %s: %w", resourceARN, err)
	}

	return resourceARN, nil
}

func (rc *ResourceCreator) deleteLambdaResourcePolicy(resourceARN string) error {
	if rc.backends.Lambda == nil {
		return nil
	}

	imb, ok := rc.backends.Lambda.Backend.(*lambdabackend.InMemoryBackend)
	if !ok {
		return nil
	}

	err := imb.DeleteResourcePolicy(resourceARN, "")
	if errors.Is(err, lambdabackend.ErrFunctionNotFound) || errors.Is(err, lambdabackend.ErrNoPolicyFound) {
		return nil
	}

	return err
}

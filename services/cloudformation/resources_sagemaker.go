package cloudformation

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"

	sagemakerbackend "github.com/blackbirdworks/gopherstack/services/sagemaker"
)

const (
	resTypeSageMakerModel                           = "AWS::SageMaker::Model"
	resTypeSageMakerEndpointConfig                  = "AWS::SageMaker::EndpointConfig"
	resTypeSageMakerEndpoint                        = "AWS::SageMaker::Endpoint"
	resTypeSageMakerNotebookInstance                = "AWS::SageMaker::NotebookInstance"
	resTypeSageMakerNotebookInstanceLifecycleConfig = "AWS::SageMaker::NotebookInstanceLifecycleConfig"
	resTypeSageMakerCodeRepository                  = "AWS::SageMaker::CodeRepository"
	resTypeSageMakerDomain                          = "AWS::SageMaker::Domain"
	resTypeSageMakerPipeline                        = "AWS::SageMaker::Pipeline"
	resTypeSageMakerModelPackageGroup               = "AWS::SageMaker::ModelPackageGroup"
	resTypeSageMakerFeatureGroup                    = "AWS::SageMaker::FeatureGroup"
	resTypeSageMakerProject                         = "AWS::SageMaker::Project"
	resTypeSageMakerWorkteam                        = "AWS::SageMaker::Workteam"
	resTypeSageMakerImage                           = "AWS::SageMaker::Image"
	resTypeSageMakerImageVersion                    = "AWS::SageMaker::ImageVersion"
)

// createSageMakerResource handles the SageMaker resource types listed above.
// Returns handled=false when resourceType isn't one of them.
func (rc *ResourceCreator) createSageMakerResource(
	ctx context.Context,
	logicalID, resourceType string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, bool, error) {
	switch resourceType {
	case resTypeSageMakerModel:
		id, err := rc.createSageMakerModel(ctx, logicalID, props, params, physicalIDs)

		return id, true, err
	case resTypeSageMakerEndpointConfig:
		id, err := rc.createSageMakerEndpointConfig(ctx, logicalID, props, params, physicalIDs)

		return id, true, err
	case resTypeSageMakerEndpoint:
		id, err := rc.createSageMakerEndpoint(ctx, logicalID, props, params, physicalIDs)

		return id, true, err
	case resTypeSageMakerNotebookInstance:
		id, err := rc.createSageMakerNotebookInstance(ctx, logicalID, props, params, physicalIDs)

		return id, true, err
	case resTypeSageMakerNotebookInstanceLifecycleConfig:
		id, err := rc.createSageMakerNotebookInstanceLifecycleConfig(ctx, logicalID, props, params, physicalIDs)

		return id, true, err
	case resTypeSageMakerCodeRepository:
		id, err := rc.createSageMakerCodeRepository(ctx, logicalID, props, params, physicalIDs)

		return id, true, err
	default:
		return rc.createSageMakerResource2(ctx, logicalID, resourceType, props, params, physicalIDs)
	}
}

// createSageMakerResource2 is createSageMakerResource's overflow table, kept
// separate to stay under the cyclop/gocognit budget for a single switch.
func (rc *ResourceCreator) createSageMakerResource2(
	ctx context.Context,
	logicalID, resourceType string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, bool, error) {
	switch resourceType {
	case resTypeSageMakerDomain:
		id, err := rc.createSageMakerDomain(ctx, logicalID, props, params, physicalIDs)

		return id, true, err
	case resTypeSageMakerPipeline:
		id, err := rc.createSageMakerPipeline(ctx, logicalID, props, params, physicalIDs)

		return id, true, err
	case resTypeSageMakerModelPackageGroup:
		id, err := rc.createSageMakerModelPackageGroup(ctx, logicalID, props, params, physicalIDs)

		return id, true, err
	case resTypeSageMakerFeatureGroup:
		id, err := rc.createSageMakerFeatureGroup(ctx, logicalID, props, params, physicalIDs)

		return id, true, err
	case resTypeSageMakerProject:
		id, err := rc.createSageMakerProject(ctx, logicalID, props, params, physicalIDs)

		return id, true, err
	case resTypeSageMakerWorkteam:
		id, err := rc.createSageMakerWorkteam(ctx, logicalID, props, params, physicalIDs)

		return id, true, err
	case resTypeSageMakerImage:
		id, err := rc.createSageMakerImage(ctx, logicalID, props, params, physicalIDs)

		return id, true, err
	case resTypeSageMakerImageVersion:
		id, err := rc.createSageMakerImageVersion(ctx, logicalID, props, params, physicalIDs)

		return id, true, err
	default:
		return "", false, nil
	}
}

// deleteSageMakerResource handles deletion for the types described in
// createSageMakerResource.
func (rc *ResourceCreator) deleteSageMakerResource(
	ctx context.Context, resourceType, physicalID string,
) (bool, error) {
	if rc.backends.SageMaker == nil {
		switch resourceType {
		case resTypeSageMakerModel, resTypeSageMakerEndpointConfig, resTypeSageMakerEndpoint,
			resTypeSageMakerNotebookInstance, resTypeSageMakerNotebookInstanceLifecycleConfig,
			resTypeSageMakerCodeRepository, resTypeSageMakerDomain, resTypeSageMakerPipeline,
			resTypeSageMakerModelPackageGroup, resTypeSageMakerFeatureGroup, resTypeSageMakerProject,
			resTypeSageMakerWorkteam, resTypeSageMakerImage, resTypeSageMakerImageVersion:
			return true, nil
		default:
			return false, nil
		}
	}

	b := rc.backends.SageMaker.Backend

	switch resourceType {
	case resTypeSageMakerModel:
		err := b.DeleteModel(ctx, sagemakerNameFromARN(physicalID))

		return true, ignoreNotFound(err, sagemakerbackend.ErrModelNotFound)
	case resTypeSageMakerEndpointConfig:
		err := b.DeleteEndpointConfig(ctx, sagemakerNameFromARN(physicalID))

		return true, ignoreNotFound(err, sagemakerbackend.ErrEndpointConfigNotFound)
	case resTypeSageMakerEndpoint:
		err := b.DeleteEndpoint(ctx, sagemakerNameFromARN(physicalID))

		return true, ignoreNotFound(err, sagemakerbackend.ErrEndpointNotFound)
	case resTypeSageMakerNotebookInstance:
		// DeleteNotebookInstance requires Stopped status (real AWS semantics);
		// StopNotebookInstance is the synchronous, unconditional variant (as
		// opposed to StopNotebookInstanceFSM's InService-only/async one), so
		// stack teardown doesn't depend on a lifecycle tick.
		name := sagemakerNameFromARN(physicalID)
		if err := b.StopNotebookInstance(ctx, name); err != nil &&
			!errors.Is(err, sagemakerbackend.ErrNotebookNotFound) {
			return true, fmt.Errorf("stop SageMaker notebook instance %s before delete: %w", name, err)
		}

		return true, ignoreNotFound(b.DeleteNotebookInstance(ctx, name), sagemakerbackend.ErrNotebookNotFound)
	case resTypeSageMakerNotebookInstanceLifecycleConfig:
		err := b.DeleteNotebookInstanceLifecycleConfig(ctx, sagemakerNameFromARN(physicalID))

		return true, ignoreNotFound(err, sagemakerbackend.ErrNotebookLifecycleConfigNotFound)
	case resTypeSageMakerCodeRepository:
		err := b.DeleteCodeRepository(ctx, sagemakerNameFromARN(physicalID))

		return true, ignoreNotFound(err, sagemakerbackend.ErrCodeRepositoryNotFound)
	default:
		return rc.deleteSageMakerResource2(ctx, resourceType, physicalID)
	}
}

// deleteSageMakerResource2 is deleteSageMakerResource's overflow table.
func (rc *ResourceCreator) deleteSageMakerResource2(
	ctx context.Context, resourceType, physicalID string,
) (bool, error) {
	b := rc.backends.SageMaker.Backend

	switch resourceType {
	case resTypeSageMakerDomain:
		return true, ignoreNotFound(b.DeleteDomain(ctx, physicalID), sagemakerbackend.ErrDomainNotFound)
	case resTypeSageMakerPipeline:
		_, err := b.DeletePipeline(ctx, physicalID)

		return true, ignoreNotFound(err, sagemakerbackend.ErrPipelineNotFound)
	case resTypeSageMakerModelPackageGroup:
		err := b.DeleteModelPackageGroup(ctx, sagemakerNameFromARN(physicalID))

		return true, ignoreNotFound(err, sagemakerbackend.ErrModelPackageGroupNotFound)
	case resTypeSageMakerFeatureGroup:
		return true, ignoreNotFound(b.DeleteFeatureGroup(ctx, physicalID), sagemakerbackend.ErrFeatureGroupNotFound)
	case resTypeSageMakerProject:
		err := b.DeleteProject(ctx, sagemakerNameFromARN(physicalID))

		return true, ignoreNotFound(err, sagemakerbackend.ErrProjectNotFound)
	case resTypeSageMakerWorkteam:
		return true, ignoreNotFound(b.DeleteWorkteam(ctx, physicalID), sagemakerbackend.ErrWorkteamNotFound)
	case resTypeSageMakerImage:
		err := b.DeleteImage(ctx, sagemakerNameFromARN(physicalID))

		return true, ignoreNotFound(err, sagemakerbackend.ErrSMImageNotFound)
	case resTypeSageMakerImageVersion:
		imageName, version := sagemakerImageVersionFromARN(physicalID)
		err := b.DeleteImageVersion(ctx, imageName, "", version)

		return true, ignoreNotFound(err, sagemakerbackend.ErrImageVersionNotFound)
	default:
		return false, nil
	}
}

// ignoreNotFound treats a not-found error as a successful (idempotent) delete.
func ignoreNotFound(err error, sentinel error) error {
	if errors.Is(err, sentinel) {
		return nil
	}

	return err
}

// sagemakerNameFromARN extracts the trailing name segment from a SageMaker
// ARN, e.g. "arn:aws:sagemaker:...:model/my-model" -> "my-model". Several
// SageMaker CFN types document Ref as the resource ARN while the backend's
// Delete methods are name-keyed (same class as AWS::Backup::Framework's Ref,
// see PARITY.md).
func sagemakerNameFromARN(arnStr string) string {
	if i := strings.LastIndex(arnStr, "/"); i >= 0 {
		return arnStr[i+1:]
	}

	return arnStr
}

// sagemakerImageVersionFromARN extracts the image name and version number
// from an ImageVersionArn, e.g.
// "arn:aws:sagemaker:...:image-version/my-image/3" -> ("my-image", 3).
func sagemakerImageVersionFromARN(arnStr string) (string, int) {
	const marker = "image-version/"

	_, rest, ok := strings.Cut(arnStr, marker)
	if !ok {
		return "", 0
	}

	imageName, versionStr, ok := strings.Cut(rest, "/")
	if !ok {
		return imageName, 0
	}

	version, _ := strconv.Atoi(versionStr)

	return imageName, version
}

// sagemakerFloatProp reads a float-valued property, accepting JSON numbers.
func sagemakerFloatProp(props map[string]any, key string) float64 {
	if f, ok := props[key].(float64); ok {
		return f
	}

	return 0
}

// rawJSONProp reads a property as opaque JSON, returning nil when absent
// (unlike jsonProp's "", which is not valid json.RawMessage).
func rawJSONProp(props map[string]any, key string) json.RawMessage {
	s := jsonProp(props, key)
	if s == "" {
		return nil
	}

	return json.RawMessage(s)
}

// ---- AWS::SageMaker::Model ----
// Ref returns the model's ARN (docs); ModelName is also stashed as a GetAtt.

func (rc *ResourceCreator) createSageMakerModel(
	ctx context.Context,
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.SageMaker == nil {
		return logicalID + "-stub", nil
	}

	name := strProp(props, "ModelName", params, physicalIDs)
	if name == "" {
		name = logicalID
	}

	var primary *sagemakerbackend.ContainerDefinition

	if pc, ok := props["PrimaryContainer"].(map[string]any); ok {
		c := sagemakerContainerFromProps(pc, params, physicalIDs)
		primary = &c
	}

	var containers []sagemakerbackend.ContainerDefinition

	if list, ok := props["Containers"].([]any); ok {
		for _, item := range list {
			if m, isMap := item.(map[string]any); isMap {
				containers = append(containers, sagemakerContainerFromProps(m, params, physicalIDs))
			}
		}
	}

	m, err := rc.backends.SageMaker.Backend.CreateModel(
		ctx, name,
		strProp(props, "ExecutionRoleArn", params, physicalIDs),
		primary, containers,
		tagListProp(props, params, physicalIDs),
	)
	if err != nil {
		return "", fmt.Errorf("create SageMaker model %s: %w", name, err)
	}

	physicalIDs[logicalID+"/ModelName"] = m.ModelName

	return m.ModelARN, nil
}

func sagemakerContainerFromProps(
	m map[string]any, params, physicalIDs map[string]string,
) sagemakerbackend.ContainerDefinition {
	return sagemakerbackend.ContainerDefinition{
		Image:        strProp(m, "Image", params, physicalIDs),
		ModelDataURL: strProp(m, "ModelDataUrl", params, physicalIDs),
		Mode:         strProp(m, "Mode", params, physicalIDs),
	}
}

// ---- AWS::SageMaker::EndpointConfig ----
// Ref returns the endpoint config's ARN (docs).

func (rc *ResourceCreator) createSageMakerEndpointConfig(
	ctx context.Context,
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.SageMaker == nil {
		return logicalID + "-stub", nil
	}

	name := strProp(props, "EndpointConfigName", params, physicalIDs)
	if name == "" {
		name = logicalID
	}

	var variants []sagemakerbackend.ProductionVariant

	if list, ok := props["ProductionVariants"].([]any); ok {
		for _, item := range list {
			pv, isMap := item.(map[string]any)
			if !isMap {
				continue
			}

			variants = append(variants, sagemakerbackend.ProductionVariant{
				VariantName:  strProp(pv, "VariantName", params, physicalIDs),
				ModelName:    strProp(pv, "ModelName", params, physicalIDs),
				InstanceType: strProp(pv, "InstanceType", params, physicalIDs),
				// #nosec G115 -- instance count, not attacker-controlled range
				InitialInstanceCount: int32(intProp(pv, "InitialInstanceCount")),
				InitialVariantWeight: sagemakerFloatProp(pv, "InitialVariantWeight"),
			})
		}
	}

	ec, err := rc.backends.SageMaker.Backend.CreateEndpointConfig(
		ctx, name, variants, tagListProp(props, params, physicalIDs),
	)
	if err != nil {
		return "", fmt.Errorf("create SageMaker endpoint config %s: %w", name, err)
	}

	physicalIDs[logicalID+"/EndpointConfigName"] = ec.EndpointConfigName

	return ec.EndpointConfigARN, nil
}

// ---- AWS::SageMaker::Endpoint ----
// Ref returns the endpoint's ARN (docs).

func (rc *ResourceCreator) createSageMakerEndpoint(
	ctx context.Context,
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.SageMaker == nil {
		return logicalID + "-stub", nil
	}

	name := strProp(props, "EndpointName", params, physicalIDs)
	if name == "" {
		name = logicalID
	}

	ep, err := rc.backends.SageMaker.Backend.CreateEndpoint(ctx, sagemakerbackend.CreateEndpointOptions{
		Name:               name,
		EndpointConfigName: strProp(props, "EndpointConfigName", params, physicalIDs),
		Tags:               tagListProp(props, params, physicalIDs),
	})
	if err != nil {
		return "", fmt.Errorf("create SageMaker endpoint %s: %w", name, err)
	}

	physicalIDs[logicalID+"/EndpointName"] = ep.EndpointName

	return ep.EndpointArn, nil
}

// ---- AWS::SageMaker::NotebookInstance ----
// Ref returns the notebook instance's ARN (docs).

func (rc *ResourceCreator) createSageMakerNotebookInstance(
	ctx context.Context,
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.SageMaker == nil {
		return logicalID + "-stub", nil
	}

	name := strProp(props, "NotebookInstanceName", params, physicalIDs)
	if name == "" {
		name = logicalID
	}

	nb, err := rc.backends.SageMaker.Backend.CreateNotebookInstance(
		ctx, name,
		strProp(props, "InstanceType", params, physicalIDs),
		strProp(props, "RoleArn", params, physicalIDs),
		tagListProp(props, params, physicalIDs),
	)
	if err != nil {
		return "", fmt.Errorf("create SageMaker notebook instance %s: %w", name, err)
	}

	physicalIDs[logicalID+"/NotebookInstanceName"] = nb.NotebookInstanceName

	return nb.NotebookInstanceArn, nil
}

// ---- AWS::SageMaker::NotebookInstanceLifecycleConfig ----
// Ref returns the lifecycle config's ARN (docs).

func (rc *ResourceCreator) createSageMakerNotebookInstanceLifecycleConfig(
	ctx context.Context,
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.SageMaker == nil {
		return logicalID + "-stub", nil
	}

	name := strProp(props, "NotebookInstanceLifecycleConfigName", params, physicalIDs)
	if name == "" {
		name = logicalID
	}

	lc, err := rc.backends.SageMaker.Backend.CreateNotebookInstanceLifecycleConfig(
		ctx, name,
		sagemakerLifecycleHooks(props["OnCreate"]),
		sagemakerLifecycleHooks(props["OnStart"]),
		tagListProp(props, params, physicalIDs),
	)
	if err != nil {
		return "", fmt.Errorf("create SageMaker notebook instance lifecycle config %s: %w", name, err)
	}

	physicalIDs[logicalID+"/NotebookInstanceLifecycleConfigName"] = lc.Name

	return lc.ARN, nil
}

func sagemakerLifecycleHooks(v any) []sagemakerbackend.NotebookLifecycleHook {
	list, ok := v.([]any)
	if !ok {
		return nil
	}

	hooks := make([]sagemakerbackend.NotebookLifecycleHook, 0, len(list))

	for _, item := range list {
		if m, isMap := item.(map[string]any); isMap {
			if content, hasContent := m["Content"].(string); hasContent {
				hooks = append(hooks, sagemakerbackend.NotebookLifecycleHook{Content: content})
			}
		}
	}

	return hooks
}

// ---- AWS::SageMaker::CodeRepository ----
// Ref returns the code repository's ARN (docs).

func (rc *ResourceCreator) createSageMakerCodeRepository(
	ctx context.Context,
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.SageMaker == nil {
		return logicalID + "-stub", nil
	}

	name := strProp(props, "CodeRepositoryName", params, physicalIDs)
	if name == "" {
		name = logicalID
	}

	gitConfig := map[string]string{}

	if gc, ok := props["GitConfig"].(map[string]any); ok {
		gitConfig["RepositoryUrl"] = strProp(gc, "RepositoryUrl", params, physicalIDs)

		if branch := strProp(gc, "Branch", params, physicalIDs); branch != "" {
			gitConfig["Branch"] = branch
		}

		if secret := strProp(gc, "SecretArn", params, physicalIDs); secret != "" {
			gitConfig["SecretArn"] = secret
		}
	}

	r, err := rc.backends.SageMaker.Backend.CreateCodeRepository(
		ctx, name, gitConfig, tagListProp(props, params, physicalIDs),
	)
	if err != nil {
		return "", fmt.Errorf("create SageMaker code repository %s: %w", name, err)
	}

	physicalIDs[logicalID+"/CodeRepositoryName"] = r.CodeRepositoryName

	return r.CodeRepositoryArn, nil
}

// ---- AWS::SageMaker::Domain ----
// Ref returns the Domain ID (docs).

func (rc *ResourceCreator) createSageMakerDomain(
	ctx context.Context,
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.SageMaker == nil {
		return logicalID + "-stub", nil
	}

	name := strProp(props, "DomainName", params, physicalIDs)
	if name == "" {
		name = logicalID
	}

	opts := sagemakerbackend.CreateDomainOptions{
		AppNetworkAccessType: strProp(props, "AppNetworkAccessType", params, physicalIDs),
		KmsKeyID:             strProp(props, "KmsKeyId", params, physicalIDs),
		VpcID:                strProp(props, "VpcId", params, physicalIDs),
		SubnetIDs:            strSliceProp(props["SubnetIds"], params, physicalIDs),
		DefaultUserSettings:  rawJSONProp(props, "DefaultUserSettings"),
		DefaultSpaceSettings: rawJSONProp(props, "DefaultSpaceSettings"),
		DomainSettings:       rawJSONProp(props, "DomainSettings"),
	}

	d, err := rc.backends.SageMaker.Backend.CreateDomain(
		ctx, name, strProp(props, "AuthMode", params, physicalIDs), tagListProp(props, params, physicalIDs), opts,
	)
	if err != nil {
		return "", fmt.Errorf("create SageMaker domain %s: %w", name, err)
	}

	physicalIDs[logicalID+"/DomainArn"] = d.DomainArn
	physicalIDs[logicalID+"/Url"] = d.URL

	return d.DomainID, nil
}

// ---- AWS::SageMaker::Pipeline ----
// Ref returns the PipelineName (docs).

func (rc *ResourceCreator) createSageMakerPipeline(
	ctx context.Context,
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.SageMaker == nil {
		return logicalID + "-stub", nil
	}

	name := strProp(props, "PipelineName", params, physicalIDs)
	if name == "" {
		name = logicalID
	}

	definition := jsonProp(props, "PipelineDefinition")

	p, err := rc.backends.SageMaker.Backend.CreatePipeline(
		ctx, name, definition,
		strProp(props, "RoleArn", params, physicalIDs),
		tagListProp(props, params, physicalIDs),
	)
	if err != nil {
		return "", fmt.Errorf("create SageMaker pipeline %s: %w", name, err)
	}

	physicalIDs[logicalID+"/PipelineArn"] = p.PipelineArn

	return p.PipelineName, nil
}

// ---- AWS::SageMaker::ModelPackageGroup ----
// Ref returns the model package group's ARN (docs).

func (rc *ResourceCreator) createSageMakerModelPackageGroup(
	ctx context.Context,
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.SageMaker == nil {
		return logicalID + "-stub", nil
	}

	name := strProp(props, "ModelPackageGroupName", params, physicalIDs)
	if name == "" {
		name = logicalID
	}

	opts := sagemakerbackend.CreateModelPackageGroupOptions{
		Name:        name,
		Description: strProp(props, "ModelPackageGroupDescription", params, physicalIDs),
		Tags:        tagListProp(props, params, physicalIDs),
	}

	g, err := rc.backends.SageMaker.Backend.CreateModelPackageGroup(ctx, opts)
	if err != nil {
		return "", fmt.Errorf("create SageMaker model package group %s: %w", name, err)
	}

	physicalIDs[logicalID+"/ModelPackageGroupName"] = g.ModelPackageGroupName

	return g.ModelPackageGroupArn, nil
}

// ---- AWS::SageMaker::FeatureGroup ----
// Ref returns the FeatureGroupName (docs).

func (rc *ResourceCreator) createSageMakerFeatureGroup(
	ctx context.Context,
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.SageMaker == nil {
		return logicalID + "-stub", nil
	}

	name := strProp(props, "FeatureGroupName", params, physicalIDs)
	if name == "" {
		name = logicalID
	}

	var defs []sagemakerbackend.FeatureDefinition

	if list, ok := props["FeatureDefinitions"].([]any); ok {
		for _, item := range list {
			if m, isMap := item.(map[string]any); isMap {
				defs = append(defs, sagemakerbackend.FeatureDefinition{
					FeatureName: strProp(m, "FeatureName", params, physicalIDs),
					FeatureType: strProp(m, "FeatureType", params, physicalIDs),
				})
			}
		}
	}

	fg, err := rc.backends.SageMaker.Backend.CreateFeatureGroup(ctx, sagemakerbackend.CreateFeatureGroupOptions{
		FeatureGroupName:            name,
		RecordIdentifierFeatureName: strProp(props, "RecordIdentifierFeatureName", params, physicalIDs),
		EventTimeFeatureName:        strProp(props, "EventTimeFeatureName", params, physicalIDs),
		Description:                 strProp(props, "Description", params, physicalIDs),
		RoleArn:                     strProp(props, "RoleArn", params, physicalIDs),
		FeatureDefinitions:          defs,
		Tags:                        tagListProp(props, params, physicalIDs),
	})
	if err != nil {
		return "", fmt.Errorf("create SageMaker feature group %s: %w", name, err)
	}

	physicalIDs[logicalID+"/FeatureGroupArn"] = fg.FeatureGroupArn

	return fg.FeatureGroupName, nil
}

// ---- AWS::SageMaker::Project ----
// Ref returns the project's ARN (docs).

func (rc *ResourceCreator) createSageMakerProject(
	ctx context.Context,
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.SageMaker == nil {
		return logicalID + "-stub", nil
	}

	name := strProp(props, "ProjectName", params, physicalIDs)
	if name == "" {
		name = logicalID
	}

	p, err := rc.backends.SageMaker.Backend.CreateProject(ctx, sagemakerbackend.CreateProjectOptions{
		Name:                              name,
		Description:                       strProp(props, "ProjectDescription", params, physicalIDs),
		Tags:                              tagListProp(props, params, physicalIDs),
		ServiceCatalogProvisioningDetails: rawJSONProp(props, "ServiceCatalogProvisioningDetails"),
	})
	if err != nil {
		return "", fmt.Errorf("create SageMaker project %s: %w", name, err)
	}

	physicalIDs[logicalID+"/ProjectName"] = p.ProjectName
	physicalIDs[logicalID+"/ProjectId"] = p.ProjectID

	return p.ProjectArn, nil
}

// ---- AWS::SageMaker::Workteam ----
// Ref is undocumented on this type's Template Reference page (only
// Fn::GetAtt Id/WorkteamName are listed, both the work team's name) -- the
// WorkteamName primary identifier is used, per this task's fallback rule.

func (rc *ResourceCreator) createSageMakerWorkteam(
	ctx context.Context,
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.SageMaker == nil {
		return logicalID + "-stub", nil
	}

	name := strProp(props, "WorkteamName", params, physicalIDs)
	if name == "" {
		name = logicalID
	}

	var members []sagemakerbackend.MemberDefinition

	if list, ok := props["MemberDefinitions"].([]any); ok {
		for _, item := range list {
			m, isMap := item.(map[string]any)
			if !isMap {
				continue
			}

			members = append(members, sagemakerMemberDefinitionFromProps(m, params, physicalIDs))
		}
	}

	w, err := rc.backends.SageMaker.Backend.CreateWorkteam(ctx, sagemakerbackend.CreateWorkteamOptions{
		Name:              name,
		Description:       strProp(props, "Description", params, physicalIDs),
		MemberDefinitions: members,
		Tags:              tagListProp(props, params, physicalIDs),
	})
	if err != nil {
		return "", fmt.Errorf("create SageMaker workteam %s: %w", name, err)
	}

	physicalIDs[logicalID+"/WorkteamArn"] = w.WorkteamArn

	return w.WorkteamName, nil
}

func sagemakerMemberDefinitionFromProps(
	m map[string]any, params, physicalIDs map[string]string,
) sagemakerbackend.MemberDefinition {
	var md sagemakerbackend.MemberDefinition

	if c, ok := m["CognitoMemberDefinition"].(map[string]any); ok {
		md.CognitoMemberDefinition = &sagemakerbackend.CognitoMemberDefinition{
			UserPool:  strProp(c, "CognitoUserPool", params, physicalIDs),
			UserGroup: strProp(c, "CognitoUserGroup", params, physicalIDs),
			ClientID:  strProp(c, "CognitoClientId", params, physicalIDs),
		}
	}

	if o, ok := m["OidcMemberDefinition"].(map[string]any); ok {
		md.OidcMemberDefinition = &sagemakerbackend.OidcMemberDefinition{
			Groups: strSliceProp(o["OidcGroups"], params, physicalIDs),
		}
	}

	return md
}

// ---- AWS::SageMaker::Image ----
// Ref returns the ImageArn (docs).

func (rc *ResourceCreator) createSageMakerImage(
	ctx context.Context,
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.SageMaker == nil {
		return logicalID + "-stub", nil
	}

	name := strProp(props, "ImageName", params, physicalIDs)
	if name == "" {
		name = logicalID
	}

	img, err := rc.backends.SageMaker.Backend.CreateImage(
		ctx, name,
		strProp(props, "ImageDescription", params, physicalIDs),
		strProp(props, "ImageDisplayName", params, physicalIDs),
		strProp(props, "ImageRoleArn", params, physicalIDs),
		tagListProp(props, params, physicalIDs),
	)
	if err != nil {
		return "", fmt.Errorf("create SageMaker image %s: %w", name, err)
	}

	physicalIDs[logicalID+"/ImageName"] = img.ImageName

	return img.ImageArn, nil
}

// ---- AWS::SageMaker::ImageVersion ----
// Ref returns the ImageVersionArn (docs).

func (rc *ResourceCreator) createSageMakerImageVersion(
	ctx context.Context,
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.SageMaker == nil {
		return logicalID + "-stub", nil
	}

	imageName := strProp(props, "ImageName", params, physicalIDs)

	v, err := rc.backends.SageMaker.Backend.CreateImageVersion(
		ctx, imageName,
		strProp(props, "BaseImage", params, physicalIDs),
		sagemakerbackend.CreateImageVersionOptions{
			MLFramework:     strProp(props, "MLFramework", params, physicalIDs),
			Processor:       strProp(props, "Processor", params, physicalIDs),
			ProgrammingLang: strProp(props, "ProgrammingLang", params, physicalIDs),
			ReleaseNotes:    strProp(props, "ReleaseNotes", params, physicalIDs),
			VendorGuidance:  strProp(props, "VendorGuidance", params, physicalIDs),
			Aliases:         strSliceProp(props["Aliases"], params, physicalIDs),
		},
	)
	if err != nil {
		return "", fmt.Errorf("create SageMaker image version for %s: %w", imageName, err)
	}

	physicalIDs[logicalID+"/ImageArn"] = v.ImageArn
	physicalIDs[logicalID+"/Version"] = strconv.Itoa(v.Version)

	return v.ImageVersionArn, nil
}

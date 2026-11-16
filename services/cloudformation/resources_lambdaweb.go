package cloudformation

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	lambdawebbackend "github.com/blackbirdworks/gopherstack/services/lambdaweb"
)

const (
	resTypeLambdaWebFunction         = "AWS::Lambda::WebFunction"
	resTypeLambdaWebFunctionRevision = "AWS::Lambda::WebFunctionRevision"
	resTypeLambdaWebFunctionEndpoint = "AWS::Lambda::WebFunctionEndpoint"

	lambdaWebStateActive = "Active"
	lambdaWebStateFailed = "Failed"
	lambdaWebUpdateDone  = "Successful"
)

var errLambdaWebFailed = errors.New("lambda web resource entered the Failed state")

func (rc *ResourceCreator) lambdaWebBackend() lambdawebbackend.StorageBackend {
	if rc.backends == nil || rc.backends.LambdaWeb == nil {
		return nil
	}

	return rc.backends.LambdaWeb.Backend
}

func (rc *ResourceCreator) createLambdaWebResource(
	ctx context.Context,
	logicalID, resourceType string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, bool, error) {
	var (
		id  string
		err error
	)

	switch resourceType {
	case resTypeLambdaWebFunction:
		id, err = rc.createLambdaWebFunction(ctx, logicalID, props, params, physicalIDs)
	case resTypeLambdaWebFunctionRevision:
		id, err = rc.createLambdaWebRevision(ctx, logicalID, props, params, physicalIDs)
	case resTypeLambdaWebFunctionEndpoint:
		id, err = rc.createLambdaWebEndpoint(logicalID, props, params, physicalIDs)
	default:
		return "", false, nil
	}

	return id, true, err
}

func (rc *ResourceCreator) deleteLambdaWebResource(resourceType, physicalID string) (bool, error) {
	switch resourceType {
	case resTypeLambdaWebFunction, resTypeLambdaWebFunctionRevision, resTypeLambdaWebFunctionEndpoint:
	default:
		return false, nil
	}

	be := rc.lambdaWebBackend()
	if be == nil {
		return true, nil
	}

	region, _ := lambdawebbackend.RegionOf(physicalID)
	fnARN, _ := lambdawebbackend.FunctionARNOf(physicalID)

	switch resourceType {
	case resTypeLambdaWebFunction:
		return true, rc.deleteLambdaWebFunction(be, region, physicalID)
	case resTypeLambdaWebFunctionRevision:
		return true, rc.deleteLambdaWebRevision(be, region, fnARN, physicalID)
	default:
		return true, rc.deleteLambdaWebEndpoint(be, region, fnARN, physicalID)
	}
}

func lambdaWebNotFound(err error) bool { return errors.Is(err, awserr.ErrNotFound) }

func lambdaWebStamp(t time.Time) string { return t.UTC().Format(time.RFC3339) }

// ---- AWS::Lambda::WebFunction ----

func (rc *ResourceCreator) createLambdaWebFunction(
	ctx context.Context,
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	be := rc.lambdaWebBackend()
	if be == nil {
		return logicalID + "-stub", nil
	}

	name := strProp(props, "FunctionName", params, physicalIDs)
	if name == "" {
		name = logicalID
	}

	out, err := be.CreateWebFunction(ctx, rc.backends.Region, lambdawebbackend.CreateFunctionInput{
		Name: name, Tags: tagListProp(props, params, physicalIDs),
	})
	if err != nil {
		return "", fmt.Errorf("create Lambda web function %s: %w", name, err)
	}

	region := rc.backends.Region

	err = awaitResource("Lambda web function "+name, func() (bool, error) {
		f, getErr := be.GetWebFunction(region, name)
		if getErr != nil {
			return false, getErr
		}

		return lambdaWebSettled(f.State, f.StateReason)
	})
	if err != nil {
		return "", err
	}

	f, err := be.GetWebFunction(region, name)
	if err != nil {
		return "", fmt.Errorf("describe Lambda web function %s: %w", name, err)
	}

	physicalIDs[logicalID+"/FunctionArn"] = f.ARN
	physicalIDs[logicalID+"/State"] = f.State
	physicalIDs[logicalID+"/StateReason"] = f.StateReason
	physicalIDs[logicalID+"/CreatedAt"] = lambdaWebStamp(out.Function.CreatedAt)
	physicalIDs[logicalID+"/UpdatedAt"] = lambdaWebStamp(f.UpdatedAt)

	return f.ARN, nil
}

func lambdaWebSettled(state, reason string) (bool, error) {
	switch state {
	case lambdaWebStateActive:
		return true, nil
	case lambdaWebStateFailed:
		return false, fmt.Errorf("%w: %s", errLambdaWebFailed, reason)
	default:
		return false, nil
	}
}

func (rc *ResourceCreator) deleteLambdaWebFunction(
	be lambdawebbackend.StorageBackend, region, fnARN string,
) error {
	if err := be.DeleteWebFunction(region, fnARN); err != nil {
		if lambdaWebNotFound(err) {
			return nil
		}

		return fmt.Errorf("delete Lambda web function %s: %w", fnARN, err)
	}

	return awaitGone("Lambda web function "+fnARN, func() error {
		_, err := be.GetWebFunction(region, fnARN)

		return err
	}, awserr.ErrNotFound)
}

func (rc *ResourceCreator) updateLambdaWebFunction(
	physicalID string, props map[string]any, params, physicalIDs map[string]string,
) error {
	be := rc.lambdaWebBackend()
	if be == nil {
		return nil
	}

	want := tagListProp(props, params, physicalIDs)

	have, err := be.ListTags(physicalID)
	if err != nil {
		return fmt.Errorf("update Lambda web function: %w", err)
	}

	var drop []string

	for k := range have {
		if _, keep := want[k]; !keep {
			drop = append(drop, k)
		}
	}

	if len(drop) > 0 {
		if err = be.UntagResource(physicalID, drop); err != nil {
			return fmt.Errorf("update Lambda web function: %w", err)
		}
	}

	if len(want) > 0 {
		if err = be.TagResource(physicalID, maps.Clone(want)); err != nil {
			return fmt.Errorf("update Lambda web function: %w", err)
		}
	}

	return nil
}

// ---- AWS::Lambda::WebFunctionRevision ----

func lambdaWebRevisionInput(
	props map[string]any, params, physicalIDs map[string]string,
) lambdawebbackend.RevisionInput {
	in := lambdawebbackend.RevisionInput{
		Description: strProp(props, "Description", params, physicalIDs),
		KmsKeyArn:   strProp(props, "KmsKeyArn", params, physicalIDs),
	}

	build, _ := props["BuildConfig"].(map[string]any)
	codeCfg, _ := build["CodeConfig"].(map[string]any)
	s3, _ := codeCfg["S3Object"].(map[string]any)
	runtime, _ := build["RuntimeConfig"].(map[string]any)

	in.S3 = lambdawebbackend.S3Object{
		Bucket:    strProp(s3, "Bucket", params, physicalIDs),
		Key:       strProp(s3, "Key", params, physicalIDs),
		VersionID: strProp(s3, "VersionId", params, physicalIDs),
	}
	in.Runtime = strProp(runtime, "Runtime", params, physicalIDs)

	svc, _ := props["ServiceConfig"].(map[string]any)
	in.ServiceConfig = lambdawebbackend.ServiceConfig{
		ExecutionRoleArn:             strProp(svc, "ExecutionRoleArn", params, physicalIDs),
		EgressNetworkConnectorArn:    strProp(svc, "EgressNetworkConnectorArn", params, physicalIDs),
		TimeoutSeconds:               int32Prop(svc, "TimeoutSeconds", params, physicalIDs),
		MaxConcurrencyPerEnvironment: int32Prop(svc, "MaxConcurrencyPerEnvironment", params, physicalIDs),
		EnvironmentVariables:         lambdaWebStringMap(svc["EnvironmentVariables"], params, physicalIDs),
	}

	telemetry, _ := svc["TelemetryConfig"].(map[string]any)
	if logging, ok := telemetry["LoggingConfig"].(map[string]any); ok {
		in.ServiceConfig.Logging = &lambdawebbackend.LoggingConfig{
			ApplicationLogLevel: strProp(logging, "ApplicationLogLevel", params, physicalIDs),
			LogGroup:            strProp(logging, "LogGroup", params, physicalIDs),
			SystemLogLevel:      strProp(logging, "SystemLogLevel", params, physicalIDs),
		}
	}

	return in
}

func lambdaWebStringMap(v any, params, physicalIDs map[string]string) map[string]string {
	m, ok := v.(map[string]any)
	if !ok {
		return nil
	}

	out := make(map[string]string, len(m))
	for k := range m {
		out[k] = strProp(m, k, params, physicalIDs)
	}

	return out
}

func (rc *ResourceCreator) createLambdaWebRevision(
	ctx context.Context,
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	be := rc.lambdaWebBackend()
	if be == nil {
		return logicalID + "-stub", nil
	}

	fn := strProp(props, "FunctionName", params, physicalIDs)
	region := rc.backends.Region

	if r, ok := lambdawebbackend.RegionOf(fn); ok {
		region = r
	}

	rev, err := be.CreateWebFunctionRevision(ctx, region, fn, lambdaWebRevisionInput(props, params, physicalIDs))
	if err != nil {
		return "", fmt.Errorf("create Lambda web function revision for %s: %w", fn, err)
	}

	err = awaitResource("Lambda web function revision "+rev.ID, func() (bool, error) {
		cur, getErr := be.GetWebFunctionRevision(region, fn, rev.ID)
		if getErr != nil {
			return false, getErr
		}

		return lambdaWebSettled(cur.State, cur.StateReason)
	})
	if err != nil {
		return "", err
	}

	cur, err := be.GetWebFunctionRevision(region, fn, rev.ID)
	if err != nil {
		return "", fmt.Errorf("describe Lambda web function revision %s: %w", rev.ID, err)
	}

	fnARN, _ := lambdawebbackend.FunctionARNOf(cur.ARN)

	physicalIDs[logicalID+"/FunctionArn"] = fnARN
	physicalIDs[logicalID+"/RevisionArn"] = cur.ARN
	physicalIDs[logicalID+"/RevisionId"] = cur.ID
	physicalIDs[logicalID+"/State"] = cur.State
	physicalIDs[logicalID+"/StateReason"] = cur.StateReason
	physicalIDs[logicalID+"/CreatedAt"] = lambdaWebStamp(cur.CreatedAt)

	return cur.ARN, nil
}

func (rc *ResourceCreator) deleteLambdaWebRevision(
	be lambdawebbackend.StorageBackend, region, fnARN, revARN string,
) error {
	if err := be.DeleteWebFunctionRevision(region, fnARN, revARN); err != nil {
		if lambdaWebNotFound(err) {
			return nil
		}

		return fmt.Errorf("delete Lambda web function revision %s: %w", revARN, err)
	}

	return nil
}

// ---- AWS::Lambda::WebFunctionEndpoint ----

func lambdaWebWeights(props map[string]any, params, physicalIDs map[string]string) []lambdawebbackend.RevisionWeight {
	raw, _ := props["RevisionWeights"].([]any)

	out := make([]lambdawebbackend.RevisionWeight, 0, len(raw))

	for _, item := range raw {
		m, ok := item.(map[string]any)
		if !ok {
			continue
		}

		out = append(out, lambdawebbackend.RevisionWeight{
			RevisionID: strProp(m, "RevisionId", params, physicalIDs),
			Weight:     int32Prop(m, "Weight", params, physicalIDs),
		})
	}

	return out
}

func lambdaWebScaling(props map[string]any, params, physicalIDs map[string]string) *lambdawebbackend.ScalingInput {
	m, ok := props["ScalingConfig"].(map[string]any)
	if !ok {
		return nil
	}

	in := &lambdawebbackend.ScalingInput{}
	if _, has := m["MaxEnvironments"]; has {
		v := int32Prop(m, "MaxEnvironments", params, physicalIDs)
		in.MaxEnvironments = &v
	}

	return in
}

func lambdaWebThrottle(props map[string]any, params, physicalIDs map[string]string) *lambdawebbackend.ThrottleInput {
	m, ok := props["ThrottleConfig"].(map[string]any)
	if !ok {
		return nil
	}

	in := &lambdawebbackend.ThrottleInput{}
	if _, has := m["RateLimit"]; has {
		v := int32Prop(m, "RateLimit", params, physicalIDs)
		in.RateLimit = &v
	}

	return in
}

func (rc *ResourceCreator) createLambdaWebEndpoint(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	be := rc.lambdaWebBackend()
	if be == nil {
		return logicalID + "-stub", nil
	}

	fn := strProp(props, "FunctionName", params, physicalIDs)
	region := rc.backends.Region

	if r, ok := lambdawebbackend.RegionOf(fn); ok {
		region = r
	}

	in := lambdawebbackend.EndpointInput{
		Name:        strProp(props, "EndpointName", params, physicalIDs),
		Description: strProp(props, "Description", params, physicalIDs),
		Type:        strProp(props, "EndpointType", params, physicalIDs),
		AuthType:    strProp(props, "AuthType", params, physicalIDs),
		Regions:     strSliceProp(props["Regions"], params, physicalIDs),
		Weights:     lambdaWebWeights(props, params, physicalIDs),
		Scaling:     lambdaWebScaling(props, params, physicalIDs),
		Throttle:    lambdaWebThrottle(props, params, physicalIDs),
	}

	if len(in.Weights) == 0 && in.Type == "HomeRegion" {
		in.AutoDeploymentMode = "LatestRevision"
	}

	ep, err := be.CreateWebFunctionEndpoint(region, fn, in)
	if err != nil {
		return "", fmt.Errorf("create Lambda web function endpoint %s: %w", in.Name, err)
	}

	err = awaitResource("Lambda web function endpoint "+ep.Name, func() (bool, error) {
		cur, getErr := be.GetWebFunctionEndpoint(region, fn, ep.Name)
		if getErr != nil {
			return false, getErr
		}

		return lambdaWebSettled(cur.State, cur.StateReason)
	})
	if err != nil {
		return "", err
	}

	cur, err := be.GetWebFunctionEndpoint(region, fn, ep.Name)
	if err != nil {
		return "", fmt.Errorf("describe Lambda web function endpoint %s: %w", ep.Name, err)
	}

	rc.stashLambdaWebEndpoint(logicalID, cur, physicalIDs)

	return cur.ARN, nil
}

func (rc *ResourceCreator) stashLambdaWebEndpoint(
	logicalID string, ep *lambdawebbackend.Endpoint, physicalIDs map[string]string,
) {
	fnARN, _ := lambdawebbackend.FunctionARNOf(ep.ARN)

	physicalIDs[logicalID+"/FunctionArn"] = fnARN
	physicalIDs[logicalID+"/EndpointArn"] = ep.ARN
	physicalIDs[logicalID+"/DomainName"] = ep.DomainName
	physicalIDs[logicalID+"/State"] = ep.State
	physicalIDs[logicalID+"/StateReason"] = ep.StateReason
	physicalIDs[logicalID+"/UpdateStatus"] = ep.UpdateStatus
	physicalIDs[logicalID+"/UpdateStatusReason"] = ep.UpdateReason
	physicalIDs[logicalID+"/CreatedAt"] = lambdaWebStamp(ep.CreatedAt)
	physicalIDs[logicalID+"/UpdatedAt"] = lambdaWebStamp(ep.UpdatedAt)
}

func (rc *ResourceCreator) deleteLambdaWebEndpoint(
	be lambdawebbackend.StorageBackend, region, fnARN, epARN string,
) error {
	if err := be.DeleteWebFunctionEndpoint(region, fnARN, epARN); err != nil {
		if lambdaWebNotFound(err) {
			return nil
		}

		return fmt.Errorf("delete Lambda web function endpoint %s: %w", epARN, err)
	}

	return awaitGone("Lambda web function endpoint "+epARN, func() error {
		_, err := be.GetWebFunctionEndpoint(region, fnARN, epARN)

		return err
	}, awserr.ErrNotFound)
}

func (rc *ResourceCreator) updateLambdaWebEndpoint(
	physicalID string, props map[string]any, params, physicalIDs map[string]string,
) error {
	be := rc.lambdaWebBackend()
	if be == nil {
		return nil
	}

	region, _ := lambdawebbackend.RegionOf(physicalID)
	fnARN, _ := lambdawebbackend.FunctionARNOf(physicalID)

	cur, err := be.GetWebFunctionEndpoint(region, fnARN, physicalID)
	if err != nil {
		return fmt.Errorf("update Lambda web function endpoint: %w", err)
	}

	desc := strProp(props, "Description", params, physicalIDs)
	up := lambdawebbackend.EndpointUpdate{
		Description: &desc,
		AuthType:    strProp(props, "AuthType", params, physicalIDs),
		Scaling:     lambdaWebScaling(props, params, physicalIDs),
		Throttle:    lambdaWebThrottle(props, params, physicalIDs),
	}

	if up.Scaling == nil {
		up.Scaling = &lambdawebbackend.ScalingInput{}
	}

	if up.Throttle == nil {
		up.Throttle = &lambdawebbackend.ThrottleInput{}
	}

	if weights := lambdaWebWeights(props, params, physicalIDs); len(weights) > 0 {
		up.AutoDeploymentMode, up.Weights, up.WeightsSet = "Disabled", weights, true
	} else if cur.Type == "HomeRegion" {
		up.AutoDeploymentMode = "LatestRevision"
	}

	if _, err = be.UpdateWebFunctionEndpoint(region, fnARN, physicalID, up); err != nil {
		return fmt.Errorf("update Lambda web function endpoint: %w", err)
	}

	return awaitResource("Lambda web function endpoint update "+physicalID, func() (bool, error) {
		e, getErr := be.GetWebFunctionEndpoint(region, fnARN, physicalID)
		if getErr != nil {
			return false, getErr
		}

		return e.UpdateStatus == lambdaWebUpdateDone || e.UpdateStatus == "", nil
	})
}

// lambdaWebRecreation lists the properties whose change replaces the resource (UpdateType Immutable in the CFN spec).
func lambdaWebRecreation(resType, propName string) string {
	switch resType {
	case resTypeLambdaWebFunction:
		return alwaysIfOneOf(propName, "FunctionName")
	case resTypeLambdaWebFunctionRevision:
		return recreateAlways
	case resTypeLambdaWebFunctionEndpoint:
		return alwaysIfOneOf(propName, "EndpointName", "EndpointType", "FunctionName", "Regions")
	default:
		return recreateNever
	}
}

func extraRecreation(resType, propName string) string {
	if v := lambdaWebRecreation(resType, propName); v != recreateNever {
		return v
	}

	if resType == resTypeELBv2LB {
		return alwaysIfOneOf(propName, "Name", "Type", "Scheme")
	}

	return globalAcceleratorRecreation(resType, propName)
}

// createLambdaWebThenRest tries the Lambda web types, then the families after them in the dispatch chain.
func (rc *ResourceCreator) createLambdaWebThenRest(
	ctx context.Context,
	logicalID, resourceType string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, bool, error) {
	if id, ok, err := rc.createLambdaWebResource(ctx, logicalID, resourceType, props, params, physicalIDs); ok {
		return id, true, err
	}

	return rc.createGlobalAcceleratorThenNetworkManager(ctx, logicalID, resourceType, props, params, physicalIDs)
}

// deleteLambdaWebThenRest mirrors createLambdaWebThenRest.
func (rc *ResourceCreator) deleteLambdaWebThenRest(
	ctx context.Context, resourceType, physicalID string,
) (bool, error) {
	if handled, err := rc.deleteLambdaWebResource(resourceType, physicalID); handled {
		return true, err
	}

	return rc.deleteGlobalAcceleratorThenNetworkManager(ctx, resourceType, physicalID)
}

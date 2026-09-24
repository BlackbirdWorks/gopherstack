package cloudformation

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	autoscalingbackend "github.com/blackbirdworks/gopherstack/services/autoscaling"
	ec2backend "github.com/blackbirdworks/gopherstack/services/ec2"
	iambackend "github.com/blackbirdworks/gopherstack/services/iam"
	rdsbackend "github.com/blackbirdworks/gopherstack/services/rds"
)

const (
	resTypeEC2LaunchTemplate = "AWS::EC2::LaunchTemplate"
	resTypeEC2VPCEndpoint    = "AWS::EC2::VPCEndpoint"
	resTypeASGScalingPolicy  = "AWS::AutoScaling::ScalingPolicy"
	resTypeASGScheduledActn  = "AWS::AutoScaling::ScheduledAction"
	resTypeASGLifecycleHook  = "AWS::AutoScaling::LifecycleHook"
	resTypeIAMOIDCProvider   = "AWS::IAM::OIDCProvider"
	resTypeRDSDBProxy        = "AWS::RDS::DBProxy"
)

// int32Prop reads an integer-valued property, accepting JSON numbers or
// stringified numbers (some CloudFormation types, e.g. AutoScaling::ScalingPolicy's
// Cooldown, are typed String even though the underlying API field is numeric).
func int32Prop(props map[string]any, key string, params, physicalIDs map[string]string) int32 {
	if v, ok := props[key].(float64); ok {
		return int32(v)
	}

	if s := strProp(props, key, params, physicalIDs); s != "" {
		if n, err := strconv.ParseInt(s, 10, 32); err == nil {
			return int32(n)
		}
	}

	return 0
}

// int32PtrProp is like int32Prop but returns nil when the property is absent,
// distinguishing "not set" from "set to zero" for optional fields.
func int32PtrProp(props map[string]any, key string, params, physicalIDs map[string]string) *int32 {
	if v, ok := props[key].(float64); ok {
		n := int32(v)

		return &n
	}

	if s := strProp(props, key, params, physicalIDs); s != "" {
		if n, err := strconv.ParseInt(s, 10, 32); err == nil {
			v := int32(n)

			return &v
		}
	}

	return nil
}

// timeProp parses an RFC3339 timestamp property, returning the zero time if absent or unparseable.
func timeProp(props map[string]any, key string, params, physicalIDs map[string]string) time.Time {
	s := strProp(props, key, params, physicalIDs)
	if s == "" {
		return time.Time{}
	}

	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		return time.Time{}
	}

	return t
}

// tagListFromSlice decodes a resolved []any of {Key,Value} maps (the standard
// CloudFormation Tags shape) into a plain map.
func tagListFromSlice(raw []any, params, physicalIDs map[string]string) map[string]string {
	out := make(map[string]string, len(raw))

	for _, item := range raw {
		m, ok := item.(map[string]any)
		if !ok {
			continue
		}

		k := strProp(m, "Key", params, physicalIDs)
		if k == "" {
			continue
		}

		out[k] = strProp(m, "Value", params, physicalIDs)
	}

	return out
}

// tagListProp decodes props[key] as the standard array-of-{Key,Value} Tags shape.
func tagListProp(props map[string]any, key string, params, physicalIDs map[string]string) map[string]string {
	raw, ok := props[key].([]any)
	if !ok {
		return nil
	}

	return tagListFromSlice(raw, params, physicalIDs)
}

// ---- EC2 LaunchTemplate ----

func (rc *ResourceCreator) createEC2LaunchTemplate(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.EC2 == nil {
		return logicalID + "-stub", nil
	}

	name := strProp(props, "LaunchTemplateName", params, physicalIDs)
	if name == "" {
		name = logicalID
	}

	data, _ := props["LaunchTemplateData"].(map[string]any)
	imageID := strProp(data, "ImageId", params, physicalIDs)
	instanceType := strProp(data, "InstanceType", params, physicalIDs)

	lt, err := rc.backends.EC2.Backend.CreateLaunchTemplate(
		name, imageID, instanceType, launchTemplateTagsProp(props, params, physicalIDs),
	)
	if err != nil {
		return "", fmt.Errorf("create EC2 LaunchTemplate %s: %w", name, err)
	}

	return lt.ID, nil
}

func (rc *ResourceCreator) deleteEC2LaunchTemplate(id string) error {
	if rc.backends.EC2 == nil {
		return nil
	}

	_, err := rc.backends.EC2.Backend.DeleteLaunchTemplate(id)
	if errors.Is(err, ec2backend.ErrLaunchTemplateNotFound) {
		return nil
	}

	return err
}

// launchTemplateTagsProp decodes TagSpecifications, returning the Tags for the
// entry whose ResourceType is "launch-template" (the launch template itself,
// as opposed to tags applied to instances/volumes launched from it).
func launchTemplateTagsProp(props map[string]any, params, physicalIDs map[string]string) map[string]string {
	raw, ok := props["TagSpecifications"].([]any)
	if !ok {
		return nil
	}

	for _, item := range raw {
		spec, specOK := item.(map[string]any)
		if !specOK {
			continue
		}

		if strProp(spec, "ResourceType", params, physicalIDs) != "launch-template" {
			continue
		}

		if tagsList, tagsOK := spec["Tags"].([]any); tagsOK {
			return tagListFromSlice(tagsList, params, physicalIDs)
		}
	}

	return nil
}

// ---- EC2 VPCEndpoint ----

func (rc *ResourceCreator) createEC2VPCEndpoint(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.EC2 == nil {
		return logicalID + "-stub", nil
	}

	vpcID := strProp(props, "VpcId", params, physicalIDs)
	serviceName := strProp(props, "ServiceName", params, physicalIDs)
	endpointType := strProp(props, "VpcEndpointType", params, physicalIDs)
	subnetIDs := strSliceProp(props["SubnetIds"], params, physicalIDs)
	routeTableIDs := strSliceProp(props["RouteTableIds"], params, physicalIDs)

	opts := ec2backend.VpcEndpointCreateOptions{
		SecurityGroupIDs: strSliceProp(props["SecurityGroupIds"], params, physicalIDs),
		PolicyDocument:   strProp(props, "PolicyDocument", params, physicalIDs),
	}

	if v, ok := props["PrivateDnsEnabled"].(bool); ok {
		opts.PrivateDNSEnabled = &v
	}

	ep, err := rc.backends.EC2.Backend.CreateVpcEndpointWithRouteTableIDs(
		vpcID, serviceName, endpointType, subnetIDs, routeTableIDs, opts,
	)
	if err != nil {
		return "", fmt.Errorf("create EC2 VPCEndpoint: %w", err)
	}

	if tags := tagListProp(props, "Tags", params, physicalIDs); len(tags) > 0 {
		_ = rc.backends.EC2.Backend.CreateTags([]string{ep.ID}, tags)
	}

	return ep.ID, nil
}

func (rc *ResourceCreator) deleteEC2VPCEndpoint(id string) error {
	if rc.backends.EC2 == nil {
		return nil
	}

	// DeleteVpcEndpoints reports a not-found ID via its returned "unsuccessful"
	// list rather than an error, so a single-ID call already tolerates
	// already-gone endpoints without special-casing.
	_, err := rc.backends.EC2.Backend.DeleteVpcEndpoints([]string{id})

	return err
}

// ---- AutoScaling ScalingPolicy ----

func (rc *ResourceCreator) createASGScalingPolicy(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.Autoscaling == nil {
		return logicalID + "-stub", nil
	}

	policyName := strProp(props, "PolicyName", params, physicalIDs)
	if policyName == "" {
		policyName = logicalID
	}

	input := autoscalingbackend.ScalingPolicyInput{
		AutoScalingGroupName:   strProp(props, "AutoScalingGroupName", params, physicalIDs),
		PolicyName:             policyName,
		PolicyType:             strProp(props, "PolicyType", params, physicalIDs),
		AdjustmentType:         strProp(props, "AdjustmentType", params, physicalIDs),
		MetricAggregationType:  strProp(props, "MetricAggregationType", params, physicalIDs),
		ScalingAdjustment:      int32Prop(props, "ScalingAdjustment", params, physicalIDs),
		Cooldown:               int32Prop(props, "Cooldown", params, physicalIDs),
		EstimatedWarmup:        int32Prop(props, "EstimatedInstanceWarmup", params, physicalIDs),
		MinAdjustmentMagnitude: int32Prop(props, "MinAdjustmentMagnitude", params, physicalIDs),
	}

	if ttc, ttcOK := props["TargetTrackingConfiguration"].(map[string]any); ttcOK {
		if pm, pmOK := ttc["PredefinedMetricSpecification"].(map[string]any); pmOK {
			input.MetricType = strProp(pm, "PredefinedMetricType", params, physicalIDs)
			input.ResourceLabel = strProp(pm, "ResourceLabel", params, physicalIDs)
		}

		if v, vOK := ttc["TargetValue"].(float64); vOK {
			input.TargetValue = v
		}

		if v, vOK := ttc["DisableScaleIn"].(bool); vOK {
			input.DisableScaleIn = v
		}
	}

	policy, err := rc.backends.Autoscaling.Backend.PutScalingPolicy(input)
	if err != nil {
		return "", fmt.Errorf("create AutoScaling ScalingPolicy %s: %w", policyName, err)
	}

	return policy.PolicyARN, nil
}

func (rc *ResourceCreator) deleteASGScalingPolicy(policyARN string) error {
	if rc.backends.Autoscaling == nil {
		return nil
	}

	// DeletePolicy searches across every group when groupName is "", which
	// PolicyARN (our physical ID) is sufficient to resolve.
	err := rc.backends.Autoscaling.Backend.DeletePolicy("", policyARN)
	if errors.Is(err, autoscalingbackend.ErrPolicyNotFound) {
		return nil
	}

	return err
}

// ---- AutoScaling ScheduledAction ----

func (rc *ResourceCreator) createASGScheduledAction(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.Autoscaling == nil {
		return logicalID + "-stub", nil
	}

	groupName := strProp(props, "AutoScalingGroupName", params, physicalIDs)

	action := autoscalingbackend.ScheduledUpdateGroupAction{
		ScheduledActionName: logicalID,
		Recurrence:          strProp(props, "Recurrence", params, physicalIDs),
		TimeZone:            strProp(props, "TimeZone", params, physicalIDs),
		MinSize:             int32PtrProp(props, "MinSize", params, physicalIDs),
		MaxSize:             int32PtrProp(props, "MaxSize", params, physicalIDs),
		DesiredCapacity:     int32PtrProp(props, "DesiredCapacity", params, physicalIDs),
		StartTime:           timeProp(props, "StartTime", params, physicalIDs),
		EndTime:             timeProp(props, "EndTime", params, physicalIDs),
	}

	_, err := rc.backends.Autoscaling.Backend.BatchPutScheduledUpdateGroupAction(
		groupName, []autoscalingbackend.ScheduledUpdateGroupAction{action},
	)
	if err != nil {
		return "", fmt.Errorf("create AutoScaling ScheduledAction %s: %w", logicalID, err)
	}

	return logicalID, nil
}

func (rc *ResourceCreator) deleteASGScheduledAction(actionName string) error {
	if rc.backends.Autoscaling == nil {
		return nil
	}

	// groupName="" ranges every group's scheduled actions by name.
	actions, err := rc.backends.Autoscaling.Backend.DescribeScheduledActions(
		"", []string{actionName}, time.Time{}, time.Time{},
	)
	if err != nil {
		return err
	}

	if len(actions) == 0 {
		return nil
	}

	err = rc.backends.Autoscaling.Backend.DeleteScheduledAction(actions[0].AutoScalingGroupName, actionName)
	if errors.Is(err, autoscalingbackend.ErrInvalidParameter) || errors.Is(err, autoscalingbackend.ErrGroupNotFound) {
		return nil
	}

	return err
}

// ---- AutoScaling LifecycleHook ----

func (rc *ResourceCreator) createASGLifecycleHook(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.Autoscaling == nil {
		return logicalID + "-stub", nil
	}

	hookName := strProp(props, "LifecycleHookName", params, physicalIDs)
	if hookName == "" {
		hookName = logicalID
	}

	hook := autoscalingbackend.LifecycleHook{
		LifecycleHookName:     hookName,
		AutoScalingGroupName:  strProp(props, "AutoScalingGroupName", params, physicalIDs),
		LifecycleTransition:   strProp(props, "LifecycleTransition", params, physicalIDs),
		DefaultResult:         strProp(props, "DefaultResult", params, physicalIDs),
		NotificationTargetARN: strProp(props, "NotificationTargetARN", params, physicalIDs),
		NotificationMetadata:  strProp(props, "NotificationMetadata", params, physicalIDs),
		RoleARN:               strProp(props, "RoleARN", params, physicalIDs),
		HeartbeatTimeout:      int32Prop(props, "HeartbeatTimeout", params, physicalIDs),
	}

	if err := rc.backends.Autoscaling.Backend.PutLifecycleHook(hook); err != nil {
		return "", fmt.Errorf("create AutoScaling LifecycleHook %s: %w", hookName, err)
	}

	return hookName, nil
}

func (rc *ResourceCreator) deleteASGLifecycleHook(hookName string) error {
	if rc.backends.Autoscaling == nil {
		return nil
	}

	groups, err := rc.backends.Autoscaling.Backend.DescribeAutoScalingGroups(nil, nil)
	if err != nil {
		return err
	}

	for _, g := range groups {
		hooks, hErr := rc.backends.Autoscaling.Backend.DescribeLifecycleHooks(
			g.AutoScalingGroupName, []string{hookName},
		)
		if hErr != nil || len(hooks) == 0 {
			continue
		}

		delErr := rc.backends.Autoscaling.Backend.DeleteLifecycleHook(g.AutoScalingGroupName, hookName)
		if errors.Is(delErr, autoscalingbackend.ErrLifecycleHookNotFound) ||
			errors.Is(delErr, autoscalingbackend.ErrGroupNotFound) {
			return nil
		}

		return delErr
	}

	return nil
}

// ---- IAM OIDCProvider ----

func (rc *ResourceCreator) createIAMOIDCProvider(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.IAM == nil {
		return logicalID + "-stub", nil
	}

	url := strProp(props, "Url", params, physicalIDs)
	clientIDs := strSliceProp(props["ClientIdList"], params, physicalIDs)
	thumbprints := strSliceProp(props["ThumbprintList"], params, physicalIDs)

	provider, err := rc.backends.IAM.Backend.CreateOpenIDConnectProvider(url, clientIDs, thumbprints)
	if err != nil {
		return "", fmt.Errorf("create IAM OIDCProvider %s: %w", url, err)
	}

	return provider.Arn, nil
}

func (rc *ResourceCreator) deleteIAMOIDCProvider(providerARN string) error {
	if rc.backends.IAM == nil {
		return nil
	}

	err := rc.backends.IAM.Backend.DeleteOpenIDConnectProvider(providerARN)
	if errors.Is(err, iambackend.ErrOIDCProviderNotFound) {
		return nil
	}

	return err
}

// ---- RDS DBProxy ----

func (rc *ResourceCreator) createRDSDBProxy(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.RDS == nil {
		return logicalID + "-stub", nil
	}

	name := strProp(props, "DBProxyName", params, physicalIDs)
	if name == "" {
		name = logicalID
	}

	proxy, err := rc.backends.RDS.Backend.CreateDBProxy(
		name,
		strProp(props, "EngineFamily", params, physicalIDs),
		strProp(props, "RoleArn", params, physicalIDs),
		rdsDBProxyAuthProp(props, params, physicalIDs),
		strSliceProp(props["VpcSubnetIds"], params, physicalIDs),
		strSliceProp(props["VpcSecurityGroupIds"], params, physicalIDs),
		strProp(props, "DefaultAuthScheme", params, physicalIDs),
		strProp(props, "EndpointNetworkType", params, physicalIDs),
		strProp(props, "TargetConnectionNetworkType", params, physicalIDs),
	)
	if err != nil {
		return "", fmt.Errorf("create RDS DBProxy %s: %w", name, err)
	}

	if tags := tagListProp(props, "Tags", params, physicalIDs); len(tags) > 0 {
		rdsTags := make([]rdsbackend.Tag, 0, len(tags))
		for k, v := range tags {
			rdsTags = append(rdsTags, rdsbackend.Tag{Key: k, Value: v})
		}

		rc.backends.RDS.Backend.AddTagsToResource(proxy.DBProxyARN, rdsTags)
	}

	return proxy.DBProxyName, nil
}

func (rc *ResourceCreator) deleteRDSDBProxy(name string) error {
	if rc.backends.RDS == nil {
		return nil
	}

	_, err := rc.backends.RDS.Backend.DeleteDBProxy(name)
	if errors.Is(err, rdsbackend.ErrDBProxyNotFound) {
		return nil
	}

	return err
}

func rdsDBProxyAuthProp(props map[string]any, params, physicalIDs map[string]string) []rdsbackend.UserAuthConfig {
	raw, ok := props["Auth"].([]any)
	if !ok {
		return nil
	}

	out := make([]rdsbackend.UserAuthConfig, 0, len(raw))

	for _, item := range raw {
		m, mOK := item.(map[string]any)
		if !mOK {
			continue
		}

		out = append(out, rdsbackend.UserAuthConfig{
			AuthScheme:  strProp(m, "AuthScheme", params, physicalIDs),
			Description: strProp(m, "Description", params, physicalIDs),
			IAMAuth:     strProp(m, "IAMAuth", params, physicalIDs),
			SecretARN:   strProp(m, "SecretArn", params, physicalIDs),
			UserName:    strProp(m, "UserName", params, physicalIDs),
		})
	}

	return out
}

// scalingPolicyNameFromARN extracts the policy name from a scaling policy
// ARN of the form ".../policyName/<name>" (scaling_policies.go's PutScalingPolicy).
func scalingPolicyNameFromARN(policyARN string) string {
	const marker = "policyName/"
	if idx := strings.LastIndex(policyARN, marker); idx >= 0 {
		return policyARN[idx+len(marker):]
	}

	return policyARN
}

// rdsDBProxyArn reproduces the ARN formula RDS's CreateDBProxy computes
// (proxies.go), letting Fn::GetAtt derive DBProxyArn without a backend lookup.
func rdsDBProxyArn(name, accountID, region string) string {
	return arn.Build("rds", region, accountID, "db-proxy:prx-"+name)
}

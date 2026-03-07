package cloudformation

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/google/uuid"

	cloudwatchbackend "github.com/blackbirdworks/gopherstack/services/cloudwatch"
	kinesisbackend "github.com/blackbirdworks/gopherstack/services/kinesis"
	lambdabackend "github.com/blackbirdworks/gopherstack/services/lambda"
	route53backend "github.com/blackbirdworks/gopherstack/services/route53"
	schedulerbackend "github.com/blackbirdworks/gopherstack/services/scheduler"
	sqsbackend "github.com/blackbirdworks/gopherstack/services/sqs"
)

// Sentinel errors for CloudFormation resource creation validation.
var (
	// ErrFunctionNameRequired is returned when FunctionName is missing from Lambda::Permission.
	ErrFunctionNameRequired = errors.New("FunctionName is required for Lambda::Permission")
	// ErrRestAPIIDRequired is returned when RestApiId is missing from ApiGateway::Stage.
	ErrRestAPIIDRequired = errors.New("RestApiId is required for ApiGateway::Stage")
)

// ---- IAM ----

func (rc *ResourceCreator) createIAMRole(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.IAM == nil {
		return logicalID + "-stub", nil
	}

	roleName := strProp(props, "RoleName", params, physicalIDs)
	if roleName == "" {
		roleName = logicalID + "-" + uuid.New().String()[:8]
	}

	path := strProp(props, "Path", params, physicalIDs)
	if path == "" {
		path = "/"
	}

	assumeRolePolicyDocument := strProp(props, "AssumeRolePolicyDocument", params, physicalIDs)

	role, err := rc.backends.IAM.Backend.CreateRole(roleName, path, assumeRolePolicyDocument, "")
	if err != nil {
		return "", fmt.Errorf("create IAM role %s: %w", roleName, err)
	}

	return role.Arn, nil
}

func (rc *ResourceCreator) deleteIAMRole(arn string) error {
	if rc.backends.IAM == nil {
		return nil
	}

	roleName := resourceNameFromARN(arn)
	// Detach all policies before deleting to avoid conflicts.
	attached, _ := rc.backends.IAM.Backend.ListAttachedRolePolicies(roleName)
	for _, p := range attached {
		_ = rc.backends.IAM.Backend.DetachRolePolicy(roleName, p.PolicyArn)
	}

	return rc.backends.IAM.Backend.DeleteRole(roleName)
}

func (rc *ResourceCreator) createIAMPolicy(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.IAM == nil {
		return logicalID + "-stub", nil
	}

	policyName := strProp(props, "PolicyName", params, physicalIDs)
	if policyName == "" {
		policyName = logicalID + "-" + uuid.New().String()[:8]
	}

	path := strProp(props, "Path", params, physicalIDs)
	if path == "" {
		path = "/"
	}

	policyDocument := strProp(props, "PolicyDocument", params, physicalIDs)

	policy, err := rc.backends.IAM.Backend.CreatePolicy(policyName, path, policyDocument)
	if err != nil {
		return "", fmt.Errorf("create IAM policy %s: %w", policyName, err)
	}

	return policy.Arn, nil
}

func (rc *ResourceCreator) createIAMManagedPolicy(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.IAM == nil {
		return logicalID + "-stub", nil
	}

	policyName := strProp(props, "ManagedPolicyName", params, physicalIDs)
	if policyName == "" {
		policyName = logicalID + "-" + uuid.New().String()[:8]
	}

	path := strProp(props, "Path", params, physicalIDs)
	if path == "" {
		path = "/"
	}

	policyDocument := strProp(props, "PolicyDocument", params, physicalIDs)

	policy, err := rc.backends.IAM.Backend.CreatePolicy(policyName, path, policyDocument)
	if err != nil {
		return "", fmt.Errorf("create IAM managed policy %s: %w", policyName, err)
	}

	return policy.Arn, nil
}

func (rc *ResourceCreator) deleteIAMPolicy(arn string) error {
	if rc.backends.IAM == nil {
		return nil
	}

	return rc.backends.IAM.Backend.DeletePolicy(arn)
}

func (rc *ResourceCreator) createIAMInstanceProfile(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.IAM == nil {
		return logicalID + "-stub", nil
	}

	name := strProp(props, "InstanceProfileName", params, physicalIDs)
	if name == "" {
		name = logicalID
	}

	path := strProp(props, "Path", params, physicalIDs)
	if path == "" {
		path = "/"
	}

	profile, err := rc.backends.IAM.Backend.CreateInstanceProfile(name, path)
	if err != nil {
		return "", fmt.Errorf("create IAM instance profile %s: %w", name, err)
	}

	return profile.Arn, nil
}

func (rc *ResourceCreator) deleteIAMInstanceProfile(arn string) error {
	if rc.backends.IAM == nil {
		return nil
	}

	name := resourceNameFromARN(arn)

	return rc.backends.IAM.Backend.DeleteInstanceProfile(name)
}

// ---- Lambda extensions ----

func (rc *ResourceCreator) createLambdaEventSourceMapping(
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
	eventSourceARN := strProp(props, "EventSourceArn", params, physicalIDs)
	startingPosition := strProp(props, "StartingPosition", params, physicalIDs)
	if startingPosition == "" {
		startingPosition = "LATEST"
	}

	batchSize := 100
	if bv, ok2 := props["BatchSize"].(float64); ok2 {
		batchSize = int(bv)
	}

	enabled := true
	if ev, ok2 := props["Enabled"].(bool); ok2 {
		enabled = ev
	}

	esm, err := imb.CreateEventSourceMapping(&lambdabackend.CreateEventSourceMappingInput{
		FunctionName:     functionName,
		EventSourceARN:   eventSourceARN,
		StartingPosition: startingPosition,
		BatchSize:        batchSize,
		Enabled:          enabled,
	})
	if err != nil {
		return "", fmt.Errorf("create Lambda event source mapping: %w", err)
	}

	return esm.UUID, nil
}

func (rc *ResourceCreator) deleteLambdaEventSourceMapping(uuid string) error {
	if rc.backends.Lambda == nil {
		return nil
	}

	imb, ok := rc.backends.Lambda.Backend.(*lambdabackend.InMemoryBackend)
	if !ok {
		return nil
	}

	_, err := imb.DeleteEventSourceMapping(uuid)

	return err
}

func (rc *ResourceCreator) createLambdaPermission(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.Lambda == nil {
		return logicalID + "-stub", nil
	}

	// Lambda::Permission is a resource-policy statement; we generate a unique
	// physical ID that encodes function and statement for later deletion.
	functionName := strProp(props, "FunctionName", params, physicalIDs)
	if functionName == "" {
		return "", ErrFunctionNameRequired
	}

	statementID := logicalID + "-" + uuid.New().String()[:8]

	return functionName + ":" + statementID, nil
}

func (rc *ResourceCreator) deleteLambdaPermission(_ string) error {
	return nil
}

func (rc *ResourceCreator) createLambdaAlias(
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
	aliasName := strProp(props, "Name", params, physicalIDs)
	if aliasName == "" {
		aliasName = logicalID
	}

	functionVersion := strProp(props, "FunctionVersion", params, physicalIDs)
	description := strProp(props, "Description", params, physicalIDs)

	alias, err := imb.CreateAlias(functionName, &lambdabackend.CreateAliasInput{
		Name:            aliasName,
		FunctionVersion: functionVersion,
		Description:     description,
	})
	if err != nil {
		return "", fmt.Errorf("create Lambda alias %s: %w", aliasName, err)
	}

	return alias.AliasArn, nil
}

func (rc *ResourceCreator) deleteLambdaAlias(arn string) error {
	if rc.backends.Lambda == nil {
		return nil
	}

	imb, ok := rc.backends.Lambda.Backend.(*lambdabackend.InMemoryBackend)
	if !ok {
		return nil
	}

	// ARN format: arn:aws:lambda:region:account:function:name:alias — 8 segments.
	const lambdaAliasARNParts = 8

	parts := strings.Split(arn, ":")
	if len(parts) < lambdaAliasARNParts {
		return nil
	}

	functionName := parts[6]
	aliasName := parts[7]

	return imb.DeleteAlias(functionName, aliasName)
}

func (rc *ResourceCreator) createLambdaVersion(
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
	description := strProp(props, "Description", params, physicalIDs)

	ver, err := imb.PublishVersion(functionName, description)
	if err != nil {
		return "", fmt.Errorf("create Lambda version for %s: %w", functionName, err)
	}

	return ver.FunctionArn, nil
}

// ---- EventBridge EventBus ----

func (rc *ResourceCreator) createEventBus(
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

	bus, err := rc.backends.EventBridge.Backend.CreateEventBus(name, "")
	if err != nil {
		return "", fmt.Errorf("create EventBridge event bus %s: %w", name, err)
	}

	return bus.Arn, nil
}

func (rc *ResourceCreator) deleteEventBus(arn string) error {
	if rc.backends.EventBridge == nil {
		return nil
	}

	parts := strings.Split(arn, "/")
	name := parts[len(parts)-1]

	return rc.backends.EventBridge.Backend.DeleteEventBus(name)
}

// ---- API Gateway sub-resources ----

func (rc *ResourceCreator) createAPIGatewayResource(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.APIGateway == nil {
		return logicalID + "-stub", nil
	}

	restAPIID := strProp(props, "RestApiId", params, physicalIDs)
	parentID := strProp(props, "ParentId", params, physicalIDs)
	pathPart := strProp(props, "PathPart", params, physicalIDs)

	resource, err := rc.backends.APIGateway.Backend.CreateResource(restAPIID, parentID, pathPart)
	if err != nil {
		return "", fmt.Errorf("create API Gateway resource: %w", err)
	}

	return resource.ID, nil
}

func (rc *ResourceCreator) deleteAPIGatewayResource(resourceID string) error {
	if rc.backends.APIGateway == nil {
		return nil
	}

	// resourceID is encoded as "restApiId:resourceId"
	restAPIID, rid := splitCompositeID(resourceID)
	if restAPIID == "" {
		return nil
	}

	return rc.backends.APIGateway.Backend.DeleteResource(restAPIID, rid)
}

func (rc *ResourceCreator) createAPIGatewayMethod(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.APIGateway == nil {
		return logicalID + "-stub", nil
	}

	restAPIID := strProp(props, "RestApiId", params, physicalIDs)
	resourceID := strProp(props, "ResourceId", params, physicalIDs)
	httpMethod := strProp(props, "HttpMethod", params, physicalIDs)
	authorizationType := strProp(props, "AuthorizationType", params, physicalIDs)
	if authorizationType == "" {
		authorizationType = "NONE"
	}

	if _, err := rc.backends.APIGateway.Backend.PutMethod(
		restAPIID,
		resourceID,
		httpMethod,
		authorizationType,
		"",
		"",
		false,
	); err != nil {
		return "", fmt.Errorf("create API Gateway method: %w", err)
	}

	return restAPIID + ":" + resourceID + ":" + httpMethod, nil
}

func (rc *ResourceCreator) deleteAPIGatewayMethod(physicalID string) error {
	if rc.backends.APIGateway == nil {
		return nil
	}

	// physicalID format: "restApiId:resourceId:httpMethod" — 3 segments.
	const methodIDParts = 3

	parts := strings.SplitN(physicalID, ":", methodIDParts)
	if len(parts) < methodIDParts {
		return nil
	}

	return rc.backends.APIGateway.Backend.DeleteMethod(parts[0], parts[1], parts[2])
}

func (rc *ResourceCreator) createAPIGatewayDeployment(
	_ context.Context,
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.APIGateway == nil {
		return logicalID + "-stub", nil
	}

	restAPIID := strProp(props, "RestApiId", params, physicalIDs)
	stageName := strProp(props, "StageName", params, physicalIDs)
	description := strProp(props, "Description", params, physicalIDs)

	dep, err := rc.backends.APIGateway.Backend.CreateDeployment(restAPIID, stageName, description)
	if err != nil {
		return "", fmt.Errorf("create API Gateway deployment: %w", err)
	}

	return dep.ID, nil
}

func (rc *ResourceCreator) deleteAPIGatewayDeployment(physicalID string) error {
	if rc.backends.APIGateway == nil {
		return nil
	}

	restAPIID, depID := splitCompositeID(physicalID)
	if restAPIID == "" {
		return nil
	}

	return rc.backends.APIGateway.Backend.DeleteDeployment(restAPIID, depID)
}

func (rc *ResourceCreator) createAPIGatewayStage(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.APIGateway == nil {
		return logicalID + "-stub", nil
	}

	restAPIID := strProp(props, "RestApiId", params, physicalIDs)
	if restAPIID == "" {
		return "", ErrRestAPIIDRequired
	}

	stageName := strProp(props, "StageName", params, physicalIDs)
	if stageName == "" {
		stageName = logicalID
	}

	return restAPIID + ":" + stageName, nil
}

func (rc *ResourceCreator) deleteAPIGatewayStage(physicalID string) error {
	if rc.backends.APIGateway == nil {
		return nil
	}

	restAPIID, stageName := splitCompositeID(physicalID)
	if restAPIID == "" {
		return nil
	}

	return rc.backends.APIGateway.Backend.DeleteStage(restAPIID, stageName)
}

// ---- EC2 ----

func (rc *ResourceCreator) createEC2SecurityGroup(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.EC2 == nil {
		return logicalID + "-stub", nil
	}

	name := strProp(props, "GroupName", params, physicalIDs)
	if name == "" {
		name = logicalID
	}

	description := strProp(props, "GroupDescription", params, physicalIDs)
	vpcID := strProp(props, "VpcId", params, physicalIDs)

	sg, err := rc.backends.EC2.Backend.CreateSecurityGroup(name, description, vpcID)
	if err != nil {
		return "", fmt.Errorf("create EC2 security group %s: %w", name, err)
	}

	return sg.ID, nil
}

func (rc *ResourceCreator) deleteEC2SecurityGroup(id string) error {
	if rc.backends.EC2 == nil {
		return nil
	}

	return rc.backends.EC2.Backend.DeleteSecurityGroup(id)
}

func (rc *ResourceCreator) createEC2VPC(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.EC2 == nil {
		return logicalID + "-stub", nil
	}

	cidr := strProp(props, "CidrBlock", params, physicalIDs)
	if cidr == "" {
		cidr = "10.0.0.0/16"
	}

	vpc, err := rc.backends.EC2.Backend.CreateVpc(cidr)
	if err != nil {
		return "", fmt.Errorf("create EC2 VPC: %w", err)
	}

	return vpc.ID, nil
}

func (rc *ResourceCreator) deleteEC2VPC(id string) error {
	if rc.backends.EC2 == nil {
		return nil
	}

	return rc.backends.EC2.Backend.DeleteVpc(id)
}

func (rc *ResourceCreator) createEC2Subnet(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.EC2 == nil {
		return logicalID + "-stub", nil
	}

	vpcID := strProp(props, "VpcId", params, physicalIDs)
	cidr := strProp(props, "CidrBlock", params, physicalIDs)
	az := strProp(props, "AvailabilityZone", params, physicalIDs)

	subnet, err := rc.backends.EC2.Backend.CreateSubnet(vpcID, cidr, az)
	if err != nil {
		return "", fmt.Errorf("create EC2 subnet: %w", err)
	}

	return subnet.ID, nil
}

func (rc *ResourceCreator) deleteEC2Subnet(id string) error {
	if rc.backends.EC2 == nil {
		return nil
	}

	return rc.backends.EC2.Backend.DeleteSubnet(id)
}

func (rc *ResourceCreator) createEC2InternetGateway(logicalID string) (string, error) {
	if rc.backends.EC2 == nil {
		return logicalID + "-stub", nil
	}

	igw, err := rc.backends.EC2.Backend.CreateInternetGateway()
	if err != nil {
		return "", fmt.Errorf("create EC2 internet gateway: %w", err)
	}

	return igw.ID, nil
}

func (rc *ResourceCreator) deleteEC2InternetGateway(id string) error {
	if rc.backends.EC2 == nil {
		return nil
	}

	return rc.backends.EC2.Backend.DeleteInternetGateway(id)
}

func (rc *ResourceCreator) createEC2RouteTable(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.EC2 == nil {
		return logicalID + "-stub", nil
	}

	vpcID := strProp(props, "VpcId", params, physicalIDs)

	rt, err := rc.backends.EC2.Backend.CreateRouteTable(vpcID)
	if err != nil {
		return "", fmt.Errorf("create EC2 route table: %w", err)
	}

	return rt.ID, nil
}

func (rc *ResourceCreator) deleteEC2RouteTable(id string) error {
	if rc.backends.EC2 == nil {
		return nil
	}

	return rc.backends.EC2.Backend.DeleteRouteTable(id)
}

func (rc *ResourceCreator) createEC2Route(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.EC2 == nil {
		return logicalID + "-stub", nil
	}

	rtID := strProp(props, "RouteTableId", params, physicalIDs)
	destCIDR := strProp(props, "DestinationCidrBlock", params, physicalIDs)
	gatewayID := strProp(props, "GatewayId", params, physicalIDs)
	natGatewayID := strProp(props, "NatGatewayId", params, physicalIDs)

	if err := rc.backends.EC2.Backend.CreateRoute(rtID, destCIDR, gatewayID, natGatewayID); err != nil {
		return "", fmt.Errorf("create EC2 route: %w", err)
	}

	return rtID + ":" + destCIDR, nil
}

// ---- Kinesis ----

func (rc *ResourceCreator) createKinesisStream(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.Kinesis == nil {
		return logicalID + "-stub", nil
	}

	name := strProp(props, "Name", params, physicalIDs)
	if name == "" {
		name = logicalID
	}

	shardCount := 1
	if v, ok := props["ShardCount"].(float64); ok {
		shardCount = int(v)
	}

	if err := rc.backends.Kinesis.Backend.CreateStream(&kinesisbackend.CreateStreamInput{
		StreamName: name,
		ShardCount: shardCount,
	}); err != nil {
		return "", fmt.Errorf("create Kinesis stream %s: %w", name, err)
	}

	out, err := rc.backends.Kinesis.Backend.DescribeStream(&kinesisbackend.DescribeStreamInput{StreamName: name})
	if err != nil {
		// Fall back to stream name if describe fails; ARN may not be available yet.
		return name, nil //nolint:nilerr // describe can fail; stream was created, return name
	}

	return out.StreamARN, nil
}

func (rc *ResourceCreator) deleteKinesisStream(arn string) error {
	if rc.backends.Kinesis == nil {
		return nil
	}

	name := streamNameFromARN(arn)

	return rc.backends.Kinesis.Backend.DeleteStream(&kinesisbackend.DeleteStreamInput{StreamName: name})
}

// ---- CloudWatch ----

func (rc *ResourceCreator) createCloudWatchAlarm(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.CloudWatch == nil {
		return logicalID + "-stub", nil
	}

	alarmName := strProp(props, "AlarmName", params, physicalIDs)
	if alarmName == "" {
		alarmName = logicalID
	}

	namespace := strProp(props, "Namespace", params, physicalIDs)
	metricName := strProp(props, "MetricName", params, physicalIDs)
	comparisonOperator := strProp(props, "ComparisonOperator", params, physicalIDs)
	statistic := strProp(props, "Statistic", params, physicalIDs)
	description := strProp(props, "AlarmDescription", params, physicalIDs)

	var threshold float64
	if v, ok := props["Threshold"].(float64); ok {
		threshold = v
	}

	var evalPeriods int32
	if v, ok := props["EvaluationPeriods"].(float64); ok {
		evalPeriods = int32(v)
	}

	var period int32
	if v, ok := props["Period"].(float64); ok {
		period = int32(v)
	}

	alarm := &cloudwatchbackend.MetricAlarm{
		AlarmName:          alarmName,
		AlarmDescription:   description,
		Namespace:          namespace,
		MetricName:         metricName,
		ComparisonOperator: comparisonOperator,
		Statistic:          statistic,
		Threshold:          threshold,
		EvaluationPeriods:  evalPeriods,
		Period:             period,
		StateValue:         "INSUFFICIENT_DATA",
	}

	if err := rc.backends.CloudWatch.Backend.PutMetricAlarm(alarm); err != nil {
		return "", fmt.Errorf("create CloudWatch alarm %s: %w", alarmName, err)
	}

	return alarmName, nil
}

func (rc *ResourceCreator) deleteCloudWatchAlarm(name string) error {
	if rc.backends.CloudWatch == nil {
		return nil
	}

	return rc.backends.CloudWatch.Backend.DeleteAlarms([]string{name})
}

// ---- Route 53 ----

func (rc *ResourceCreator) createRoute53HostedZone(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.Route53 == nil {
		return logicalID + "-stub", nil
	}

	name := strProp(props, "Name", params, physicalIDs)
	if name == "" {
		name = logicalID + ".example.com"
	}

	comment := ""
	if cfg, ok := props["HostedZoneConfig"].(map[string]any); ok {
		comment = resolve(cfg["Comment"], params, physicalIDs)
	}

	zone, err := rc.backends.Route53.Backend.CreateHostedZone(name, uuid.New().String(), comment, false)
	if err != nil {
		return "", fmt.Errorf("create Route53 hosted zone %s: %w", name, err)
	}

	return zone.ID, nil
}

func (rc *ResourceCreator) deleteRoute53HostedZone(zoneID string) error {
	if rc.backends.Route53 == nil {
		return nil
	}

	return rc.backends.Route53.Backend.DeleteHostedZone(zoneID)
}

func (rc *ResourceCreator) createRoute53RecordSet(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.Route53 == nil {
		return logicalID + "-stub", nil
	}

	hostedZoneID := strProp(props, "HostedZoneId", params, physicalIDs)
	name := strProp(props, "Name", params, physicalIDs)
	rrType := strProp(props, "Type", params, physicalIDs)

	var ttl int64
	if v, ok := props["TTL"].(float64); ok {
		ttl = int64(v)
	}

	var records []route53backend.ResourceRecord
	if rawRecords, ok := props["ResourceRecords"].([]any); ok {
		for _, r := range rawRecords {
			if val := resolve(r, params, physicalIDs); val != "" {
				records = append(records, route53backend.ResourceRecord{Value: val})
			}
		}
	}

	change := route53backend.Change{
		Action: route53backend.ChangeActionCreate,
		ResourceRecordSet: route53backend.ResourceRecordSet{
			Name:    name,
			Type:    rrType,
			TTL:     ttl,
			Records: records,
		},
	}

	if err := rc.backends.Route53.Backend.ChangeResourceRecordSets(
		hostedZoneID,
		[]route53backend.Change{change},
	); err != nil {
		return "", fmt.Errorf("create Route53 record set %s: %w", name, err)
	}

	return hostedZoneID + ":" + name + ":" + rrType, nil
}

// ---- ElastiCache ----

func (rc *ResourceCreator) createElastiCacheCacheCluster(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.ElastiCache == nil {
		return logicalID + "-stub", nil
	}

	clusterID := strProp(props, "CacheClusterId", params, physicalIDs)
	if clusterID == "" {
		clusterID = strings.ToLower(logicalID)
	}

	engine := strProp(props, "Engine", params, physicalIDs)
	if engine == "" {
		engine = "redis"
	}

	nodeType := strProp(props, "CacheNodeType", params, physicalIDs)
	if nodeType == "" {
		nodeType = "cache.t3.micro"
	}

	cluster, err := rc.backends.ElastiCache.Backend.CreateCluster(clusterID, engine, nodeType, 0)
	if err != nil {
		return "", fmt.Errorf("create ElastiCache cluster %s: %w", clusterID, err)
	}

	return cluster.ClusterID, nil
}

func (rc *ResourceCreator) deleteElastiCacheCacheCluster(_ context.Context, id string) error {
	if rc.backends.ElastiCache == nil {
		return nil
	}

	return rc.backends.ElastiCache.Backend.DeleteCluster(id)
}

// ---- SNS Subscription ----

func (rc *ResourceCreator) createSNSSubscription(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.SNS == nil {
		return logicalID + "-stub", nil
	}

	topicARN := strProp(props, "TopicArn", params, physicalIDs)
	protocol := strProp(props, "Protocol", params, physicalIDs)
	endpoint := strProp(props, "Endpoint", params, physicalIDs)
	filterPolicy := strProp(props, "FilterPolicy", params, physicalIDs)

	sub, err := rc.backends.SNS.Backend.Subscribe(topicARN, protocol, endpoint, filterPolicy)
	if err != nil {
		return "", fmt.Errorf("create SNS subscription: %w", err)
	}

	return sub.SubscriptionArn, nil
}

func (rc *ResourceCreator) deleteSNSSubscription(arn string) error {
	if rc.backends.SNS == nil {
		return nil
	}

	return rc.backends.SNS.Backend.Unsubscribe(arn)
}

// ---- SQS Queue Policy ----

func (rc *ResourceCreator) createSQSQueuePolicy(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.SQS == nil {
		return logicalID + "-stub", nil
	}

	queues, _ := props["Queues"].([]any)
	policyDocument := strProp(props, "PolicyDocument", params, physicalIDs)

	physID := logicalID + "-" + uuid.New().String()[:8]

	for _, q := range queues {
		queueURL := resolve(q, params, physicalIDs)
		if queueURL == "" {
			continue
		}

		_ = rc.backends.SQS.Backend.SetQueueAttributes(&sqsbackend.SetQueueAttributesInput{
			QueueURL:   queueURL,
			Attributes: map[string]string{"Policy": policyDocument},
		})
	}

	return physID, nil
}

// ---- S3 Bucket Policy ----

func (rc *ResourceCreator) createS3BucketPolicy(
	ctx context.Context,
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.S3 == nil {
		return logicalID + "-stub", nil
	}

	bucket := strProp(props, "Bucket", params, physicalIDs)
	policyDocument := strProp(props, "PolicyDocument", params, physicalIDs)

	if err := rc.backends.S3.Backend.PutBucketPolicy(ctx, bucket, policyDocument); err != nil {
		return "", fmt.Errorf("create S3 bucket policy for %s: %w", bucket, err)
	}

	return bucket, nil
}

func (rc *ResourceCreator) deleteS3BucketPolicy(ctx context.Context, bucket string) error {
	if rc.backends.S3 == nil {
		return nil
	}

	return rc.backends.S3.Backend.DeleteBucketPolicy(ctx, bucket)
}

// ---- Scheduler ----

func (rc *ResourceCreator) createSchedulerSchedule(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.Scheduler == nil {
		return logicalID + "-stub", nil
	}

	name := strProp(props, "Name", params, physicalIDs)
	if name == "" {
		name = logicalID
	}

	scheduleExpression := strProp(props, "ScheduleExpression", params, physicalIDs)
	state := strProp(props, "State", params, physicalIDs)
	if state == "" {
		state = "ENABLED"
	}

	var target schedulerbackend.Target
	if rawTarget, ok := props["Target"].(map[string]any); ok {
		target.ARN = resolve(rawTarget["Arn"], params, physicalIDs)
		target.RoleARN = resolve(rawTarget["RoleArn"], params, physicalIDs)
	}

	sched, err := rc.backends.Scheduler.Backend.CreateSchedule(
		name,
		scheduleExpression,
		target,
		state,
		schedulerbackend.FlexibleTimeWindow{Mode: "OFF"},
	)
	if err != nil {
		return "", fmt.Errorf("create Scheduler schedule %s: %w", name, err)
	}

	return sched.ARN, nil
}

func (rc *ResourceCreator) deleteSchedulerSchedule(arn string) error {
	if rc.backends.Scheduler == nil {
		return nil
	}

	name := resourceNameFromARN(arn)

	return rc.backends.Scheduler.Backend.DeleteSchedule(name)
}

// ---- helpers ----

// resourceNameFromARN extracts the last segment after "/" or ":" in an ARN.
func resourceNameFromARN(arn string) string {
	if idx := strings.LastIndexByte(arn, '/'); idx >= 0 {
		return arn[idx+1:]
	}

	if idx := strings.LastIndexByte(arn, ':'); idx >= 0 {
		return arn[idx+1:]
	}

	return arn
}

// streamNameFromARN extracts the stream name from a Kinesis stream ARN.
// Format: arn:aws:kinesis:region:account:stream/name.
func streamNameFromARN(arn string) string {
	if idx := strings.LastIndex(arn, "stream/"); idx >= 0 {
		return arn[idx+7:]
	}

	return arn
}

// splitCompositeID splits a composite "part1:part2" physical ID.
func splitCompositeID(id string) (string, string) {
	before, after, ok := strings.Cut(id, ":")
	if !ok {
		return "", id
	}

	return before, after
}

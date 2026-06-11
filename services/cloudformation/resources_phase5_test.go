package cloudformation_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	apigatewayv2backend "github.com/blackbirdworks/gopherstack/services/apigatewayv2"
	"github.com/blackbirdworks/gopherstack/services/cloudformation"
	cwlogsbackend "github.com/blackbirdworks/gopherstack/services/cloudwatchlogs"
	ec2backend "github.com/blackbirdworks/gopherstack/services/ec2"
	kmsbackend "github.com/blackbirdworks/gopherstack/services/kms"
)

// TestResourceCreator_Phase5Types_NilBackends ensures every phase-5 resource type returns
// a stub physical ID (no panic, no error) when the backing service is nil.
func TestResourceCreator_Phase5Types_NilBackends(t *testing.T) {
	t.Parallel()

	tests := []struct {
		props        map[string]any
		name         string
		logicalID    string
		resourceType string
	}{
		{name: "logs_log_stream", logicalID: "Stream", resourceType: "AWS::Logs::LogStream",
			props: map[string]any{"LogGroupName": "/g", "LogStreamName": "s"}},
		{name: "logs_metric_filter", logicalID: "MF", resourceType: "AWS::Logs::MetricFilter",
			props: map[string]any{"LogGroupName": "/g", "FilterName": "mf"}},
		{name: "logs_subscription_filter", logicalID: "SF", resourceType: "AWS::Logs::SubscriptionFilter",
			props: map[string]any{"LogGroupName": "/g", "DestinationArn": "arn:aws:lambda:::f"}},
		{name: "logs_resource_policy", logicalID: "RP", resourceType: "AWS::Logs::ResourcePolicy",
			props: map[string]any{"PolicyName": "p", "PolicyDocument": "{}"}},
		{name: "logs_query_definition", logicalID: "QD", resourceType: "AWS::Logs::QueryDefinition",
			props: map[string]any{"Name": "q", "QueryString": "fields @message"}},
		{name: "ec2_volume", logicalID: "Vol", resourceType: "AWS::EC2::Volume",
			props: map[string]any{"AvailabilityZone": "us-east-1a", "Size": float64(10)}},
		{name: "ec2_volume_attachment", logicalID: "VA", resourceType: "AWS::EC2::VolumeAttachment",
			props: map[string]any{"VolumeId": "vol-1", "InstanceId": "i-1"}},
		{name: "ec2_network_interface", logicalID: "ENI", resourceType: "AWS::EC2::NetworkInterface",
			props: map[string]any{"SubnetId": "subnet-1"}},
		{name: "apigwv2_integration", logicalID: "Int", resourceType: "AWS::ApiGatewayV2::Integration",
			props: map[string]any{"ApiId": "api-1", "IntegrationType": "AWS_PROXY"}},
		{name: "apigwv2_route", logicalID: "Route", resourceType: "AWS::ApiGatewayV2::Route",
			props: map[string]any{"ApiId": "api-1", "RouteKey": "GET /"}},
		{name: "apigwv2_authorizer", logicalID: "Auth", resourceType: "AWS::ApiGatewayV2::Authorizer",
			props: map[string]any{"ApiId": "api-1", "Name": "a", "AuthorizerType": "REQUEST"}},
		{name: "kms_alias", logicalID: "Alias", resourceType: "AWS::KMS::Alias",
			props: map[string]any{"AliasName": "alias/k", "TargetKeyId": "key-1"}},
		{name: "sns_topic_policy", logicalID: "TP", resourceType: "AWS::SNS::TopicPolicy",
			props: map[string]any{"Topics": []any{"arn:aws:sns:::t"}, "PolicyDocument": "{}"}},
		{name: "events_connection", logicalID: "Conn", resourceType: "AWS::Events::Connection",
			props: map[string]any{"Name": "c", "AuthorizationType": "API_KEY"}},
		{name: "events_archive", logicalID: "Arch", resourceType: "AWS::Events::Archive",
			props: map[string]any{"ArchiveName": "a", "SourceArn": "arn:aws:events:::event-bus/default"}},
		{name: "sfn_activity", logicalID: "Act", resourceType: "AWS::StepFunctions::Activity",
			props: map[string]any{"Name": "act"}},
		{name: "ssm_document", logicalID: "Doc", resourceType: "AWS::SSM::Document",
			props: map[string]any{"Name": "d", "Content": "{}", "DocumentType": "Command"}},
		{name: "secrets_resource_policy", logicalID: "SRP", resourceType: "AWS::SecretsManager::ResourcePolicy",
			props: map[string]any{"SecretId": "s", "ResourcePolicy": "{}"}},
		{name: "cloudfront_function", logicalID: "Fn", resourceType: "AWS::CloudFront::Function",
			props: map[string]any{"Name": "fn"}},
		{name: "cloudfront_cache_policy", logicalID: "CP", resourceType: "AWS::CloudFront::CachePolicy",
			props: map[string]any{"CachePolicyConfig": map[string]any{"Name": "cp"}}},
		{name: "cloudfront_oac", logicalID: "OAC", resourceType: "AWS::CloudFront::OriginAccessControl",
			props: map[string]any{"OriginAccessControlConfig": map[string]any{"Name": "oac"}}},
		{name: "cloudfront_rhp", logicalID: "RHP", resourceType: "AWS::CloudFront::ResponseHeadersPolicy",
			props: map[string]any{"ResponseHeadersPolicyConfig": map[string]any{"Name": "rhp"}}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rc := cloudformation.NewResourceCreator(&cloudformation.ServiceBackends{
				AccountID: "000000000000",
				Region:    "us-east-1",
			})

			physID, err := rc.Create(t.Context(), tt.logicalID, tt.resourceType, tt.props, nil, nil)
			require.NoError(t, err)
			assert.NotEmpty(t, physID)

			require.NoError(t, rc.Delete(t.Context(), tt.resourceType, physID, tt.props))
		})
	}
}

// TestResourceCreator_Phase5_LogsResources verifies that Logs child resources are created in
// the real CloudWatch Logs backend and removed on delete.
func TestResourceCreator_Phase5_LogsResources(t *testing.T) {
	t.Parallel()

	backends := newPhase3ServiceBackends()
	rc := cloudformation.NewResourceCreator(backends)
	ctx := t.Context()
	cw, ok := backends.CloudWatchLogs.Backend.(*cwlogsbackend.InMemoryBackend)
	require.True(t, ok)

	const group = "/aws/cfn/phase5"
	_, err := cw.CreateLogGroup(ctx, group, "", "")
	require.NoError(t, err)

	// LogStream round trip.
	streamPhys, err := rc.Create(ctx, "MyStream", "AWS::Logs::LogStream",
		map[string]any{"LogGroupName": group, "LogStreamName": "app-logs"}, nil, nil)
	require.NoError(t, err)

	streams, _, err := cw.DescribeLogStreams(ctx, group, "", "", "", false, 0)
	require.NoError(t, err)
	require.Len(t, streams, 1)
	assert.Equal(t, "app-logs", streams[0].LogStreamName)

	require.NoError(t, rc.Delete(ctx, "AWS::Logs::LogStream", streamPhys, nil))
	streams, _, err = cw.DescribeLogStreams(ctx, group, "", "", "", false, 0)
	require.NoError(t, err)
	assert.Empty(t, streams)

	// MetricFilter round trip.
	mfPhys, err := rc.Create(ctx, "MyMF", "AWS::Logs::MetricFilter",
		map[string]any{
			"LogGroupName":  group,
			"FilterName":    "errors",
			"FilterPattern": "ERROR",
			"MetricTransformations": []any{
				map[string]any{"MetricName": "ErrorCount", "MetricNamespace": "App", "MetricValue": "1"},
			},
		}, nil, nil)
	require.NoError(t, err)

	filters, _, err := cw.DescribeMetricFilters(ctx, group, "", "", "", "", 0)
	require.NoError(t, err)
	require.Len(t, filters, 1)
	assert.Equal(t, "errors", filters[0].FilterName)

	require.NoError(t, rc.Delete(ctx, "AWS::Logs::MetricFilter", mfPhys, nil))
	filters, _, err = cw.DescribeMetricFilters(ctx, group, "", "", "", "", 0)
	require.NoError(t, err)
	assert.Empty(t, filters)

	// QueryDefinition round trip.
	qdPhys, err := rc.Create(ctx, "MyQD", "AWS::Logs::QueryDefinition",
		map[string]any{"Name": "slow-queries", "QueryString": "fields @message", "LogGroupNames": []any{group}},
		nil, nil)
	require.NoError(t, err)
	require.NotEmpty(t, qdPhys)

	defs, _, err := cw.DescribeQueryDefinitions("", 0, "")
	require.NoError(t, err)
	require.Len(t, defs, 1)

	require.NoError(t, rc.Delete(ctx, "AWS::Logs::QueryDefinition", qdPhys, nil))
	defs, _, err = cw.DescribeQueryDefinitions("", 0, "")
	require.NoError(t, err)
	assert.Empty(t, defs)
}

// TestResourceCreator_Phase5_EC2Volume verifies a real EBS volume is created and deleted, and
// that Fn::GetAtt VolumeId returns the real physical ID.
func TestResourceCreator_Phase5_EC2Volume(t *testing.T) {
	t.Parallel()

	backends := newPhase3ServiceBackends()
	rc := cloudformation.NewResourceCreator(backends)
	ctx := t.Context()
	ec2b, ok := backends.EC2.Backend.(*ec2backend.InMemoryBackend)
	require.True(t, ok)

	volPhys, err := rc.Create(ctx, "DataVol", "AWS::EC2::Volume",
		map[string]any{"AvailabilityZone": "us-east-1a", "Size": float64(20), "VolumeType": "gp3"}, nil, nil)
	require.NoError(t, err)
	require.NotEmpty(t, volPhys)

	vols := ec2b.DescribeVolumes([]string{volPhys})
	require.Len(t, vols, 1)
	assert.Equal(t, 20, vols[0].Size)

	// GetAtt VolumeId returns the physical volume ID.
	got := cloudformation.GetResourceAttribute("AWS::EC2::Volume", volPhys, "VolumeId", "000000000000", "us-east-1")
	assert.Equal(t, volPhys, got)

	require.NoError(t, rc.Delete(ctx, "AWS::EC2::Volume", volPhys, nil))
	assert.Empty(t, ec2b.DescribeVolumes([]string{volPhys}))
}

// TestResourceCreator_Phase5_KMSAlias verifies an alias is created against a real key and that
// Fn::GetAtt Arn returns a real KMS ARN.
func TestResourceCreator_Phase5_KMSAlias(t *testing.T) {
	t.Parallel()

	backends := newPhase3ServiceBackends()
	rc := cloudformation.NewResourceCreator(backends)
	ctx := t.Context()
	kmsb, ok := backends.KMS.Backend.(*kmsbackend.InMemoryBackend)
	require.True(t, ok)

	// Create a real key to point the alias at.
	keyPhys, err := rc.Create(ctx, "MyKey", "AWS::KMS::Key", map[string]any{}, nil, nil)
	require.NoError(t, err)
	require.NotEmpty(t, keyPhys)

	aliasPhys, err := rc.Create(ctx, "MyAlias", "AWS::KMS::Alias",
		map[string]any{"AliasName": "alias/phase5", "TargetKeyId": keyPhys}, nil, nil)
	require.NoError(t, err)
	assert.Equal(t, "alias/phase5", aliasPhys)

	aliases, err := kmsb.ListAliases(&kmsbackend.ListAliasesInput{})
	require.NoError(t, err)
	found := false
	for _, a := range aliases.Aliases {
		if a.AliasName == "alias/phase5" {
			found = true
		}
	}
	assert.True(t, found, "alias should exist in KMS backend")

	got := cloudformation.GetResourceAttribute("AWS::KMS::Alias", aliasPhys, "Arn", "000000000000", "us-east-1")
	assert.Contains(t, got, "alias/phase5")
	assert.Contains(t, got, "arn:aws:kms")

	require.NoError(t, rc.Delete(ctx, "AWS::KMS::Alias", aliasPhys, nil))
}

// TestResourceCreator_Phase5_APIGatewayV2Children verifies Integration, Route, and Authorizer are
// created against a real HTTP API and removed on delete.
func TestResourceCreator_Phase5_APIGatewayV2Children(t *testing.T) {
	t.Parallel()

	backends := newPhase3ServiceBackends()
	rc := cloudformation.NewResourceCreator(backends)
	ctx := t.Context()
	apigw, ok := backends.APIGatewayV2.Backend.(*apigatewayv2backend.InMemoryBackend)
	require.True(t, ok)

	apiID, err := rc.Create(ctx, "Api", "AWS::ApiGatewayV2::Api",
		map[string]any{"Name": "phase5-http", "ProtocolType": "HTTP"}, nil, nil)
	require.NoError(t, err)
	physIDs := map[string]string{"Api": apiID}

	authPhys, err := rc.Create(ctx, "Authz", "AWS::ApiGatewayV2::Authorizer",
		map[string]any{"ApiId": apiID, "Name": "jwt-less", "AuthorizerType": "REQUEST"}, nil, physIDs)
	require.NoError(t, err)

	intPhys, err := rc.Create(ctx, "Integ", "AWS::ApiGatewayV2::Integration",
		map[string]any{"ApiId": apiID, "IntegrationType": "HTTP_PROXY", "IntegrationUri": "https://example.com"},
		nil, physIDs)
	require.NoError(t, err)

	routePhys, err := rc.Create(ctx, "Route", "AWS::ApiGatewayV2::Route",
		map[string]any{"ApiId": apiID, "RouteKey": "GET /items"}, nil, physIDs)
	require.NoError(t, err)

	routes, err := apigw.GetRoutes(apiID)
	require.NoError(t, err)
	require.Len(t, routes, 1)
	assert.Equal(t, "GET /items", routes[0].RouteKey)

	require.NoError(t, rc.Delete(ctx, "AWS::ApiGatewayV2::Route", routePhys, nil))
	require.NoError(t, rc.Delete(ctx, "AWS::ApiGatewayV2::Integration", intPhys, nil))
	require.NoError(t, rc.Delete(ctx, "AWS::ApiGatewayV2::Authorizer", authPhys, nil))

	routes, err = apigw.GetRoutes(apiID)
	require.NoError(t, err)
	assert.Empty(t, routes)
}

// TestResourceCreator_Phase5_SecretsManagerResourcePolicy verifies a resource policy is attached to
// a real secret and removed on delete.
func TestResourceCreator_Phase5_SecretsManagerResourcePolicy(t *testing.T) {
	t.Parallel()

	backends := newPhase3ServiceBackends()
	rc := cloudformation.NewResourceCreator(backends)
	ctx := t.Context()

	secretPhys, err := rc.Create(ctx, "MySecret", "AWS::SecretsManager::Secret",
		map[string]any{"Name": "phase5-secret"}, nil, nil)
	require.NoError(t, err)
	require.NotEmpty(t, secretPhys)

	policyPhys, err := rc.Create(ctx, "MyPolicy", "AWS::SecretsManager::ResourcePolicy",
		map[string]any{
			"SecretId":       secretPhys,
			"ResourcePolicy": `{"Version":"2012-10-17","Statement":[]}`,
		}, nil, nil)
	require.NoError(t, err)
	assert.Equal(t, secretPhys, policyPhys)

	require.NoError(t, rc.Delete(ctx, "AWS::SecretsManager::ResourcePolicy", policyPhys, nil))
}

// TestResourceCreator_Phase5_GetAtt verifies Fn::GetAtt resolution for phase-5 resource types.
func TestResourceCreator_Phase5_GetAtt(t *testing.T) {
	t.Parallel()

	const (
		account = "000000000000"
		region  = "us-east-1"
	)

	tests := []struct {
		name     string
		resType  string
		physID   string
		attrName string
		want     string
	}{
		{name: "volume_id", resType: "AWS::EC2::Volume", physID: "vol-abc", attrName: "VolumeId", want: "vol-abc"},
		{name: "eni_id", resType: "AWS::EC2::NetworkInterface", physID: "eni-abc", attrName: "Id", want: "eni-abc"},
		{
			name: "activity_arn", resType: "AWS::StepFunctions::Activity",
			physID: "arn:aws:states:us-east-1:000000000000:activity:proc", attrName: "Arn",
			want: "arn:aws:states:us-east-1:000000000000:activity:proc",
		},
		{
			name: "activity_name", resType: "AWS::StepFunctions::Activity",
			physID: "arn:aws:states:us-east-1:000000000000:activity:proc", attrName: "Name", want: "proc",
		},
		{
			name: "connection_arn", resType: "AWS::Events::Connection", physID: "my-conn", attrName: "Arn",
			want: "arn:aws:events:us-east-1:000000000000:connection/my-conn",
		},
		{
			name: "logstream_name", resType: "AWS::Logs::LogStream", physID: "/grp|stream-1",
			attrName: "LogStreamName", want: "stream-1",
		},
		{
			name: "kms_alias_arn", resType: "AWS::KMS::Alias", physID: "alias/x", attrName: "Arn",
			want: "arn:aws:kms:us-east-1:000000000000:alias/x",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := cloudformation.GetResourceAttribute(tt.resType, tt.physID, tt.attrName, account, region)
			assert.Equal(t, tt.want, got)
		})
	}
}

package cloudformation_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cloudformation"
	cognitoidentitybackend "github.com/blackbirdworks/gopherstack/services/cognitoidentity"
	ec2backend "github.com/blackbirdworks/gopherstack/services/ec2"
	kmsbackend "github.com/blackbirdworks/gopherstack/services/kms"
)

// newPhase6ServiceBackends creates a ServiceBackends with all phase-6 backends populated.
func newPhase6ServiceBackends() *cloudformation.ServiceBackends {
	b := newPhase5ServiceBackends()
	b.CognitoIdentity = cognitoidentitybackend.NewHandler(
		cognitoidentitybackend.NewInMemoryBackend("000000000000", "us-east-1"),
		"us-east-1",
	)

	return b
}

// TestResourceCreator_Phase6Types_NilBackends ensures every phase-6 resource type returns a stub
// physical ID (no panic, no error) when the backing service is nil.
func TestResourceCreator_Phase6Types_NilBackends(t *testing.T) {
	t.Parallel()

	tests := []struct {
		props        map[string]any
		name         string
		logicalID    string
		resourceType string
	}{
		{
			name: "apigw_model", logicalID: "Model", resourceType: "AWS::ApiGateway::Model",
			props: map[string]any{
				"RestApiId":   "api-1",
				"Name":        "MyModel",
				"ContentType": "application/json",
			},
		},
		{
			name: "apigw_request_validator", logicalID: "Validator", resourceType: "AWS::ApiGateway::RequestValidator",
			props: map[string]any{"RestApiId": "api-1", "Name": "MyValidator"},
		},
		{
			name: "apigw_authorizer", logicalID: "Auth", resourceType: "AWS::ApiGateway::Authorizer",
			props: map[string]any{"RestApiId": "api-1", "Name": "MyAuth", "Type": "TOKEN"},
		},
		{
			name: "apigw_api_key", logicalID: "Key", resourceType: "AWS::ApiGateway::ApiKey",
			props: map[string]any{"Name": "MyKey"},
		},
		{
			name: "apigw_usage_plan", logicalID: "Plan", resourceType: "AWS::ApiGateway::UsagePlan",
			props: map[string]any{"UsagePlanName": "MyPlan"},
		},
		{
			name: "apigw_usage_plan_key", logicalID: "PlanKey", resourceType: "AWS::ApiGateway::UsagePlanKey",
			props: map[string]any{"UsagePlanId": "plan-1", "KeyId": "key-1", "KeyType": "API_KEY"},
		},
		{
			name: "apigw_domain_name", logicalID: "Domain", resourceType: "AWS::ApiGateway::DomainName",
			props: map[string]any{"DomainName": "api.example.com"},
		},
		{
			name: "apigw_base_path_mapping", logicalID: "BPM", resourceType: "AWS::ApiGateway::BasePathMapping",
			props: map[string]any{"DomainName": "api.example.com", "RestApiId": "api-1"},
		},
		{
			name: "apigw_account", logicalID: "Account", resourceType: "AWS::ApiGateway::Account",
			props: map[string]any{"CloudWatchRoleArn": "arn:aws:iam:::role/APIGatewayCloudWatch"},
		},
		{
			name: "apigw_gateway_response", logicalID: "GR", resourceType: "AWS::ApiGateway::GatewayResponse",
			props: map[string]any{"RestApiId": "api-1", "ResponseType": "DEFAULT_4XX"},
		},
		{
			name: "apigwv2_domain_name", logicalID: "V2Domain", resourceType: "AWS::ApiGatewayV2::DomainName",
			props: map[string]any{"DomainName": "ws.example.com"},
		},
		{
			name: "apigwv2_api_mapping", logicalID: "Mapping", resourceType: "AWS::ApiGatewayV2::ApiMapping",
			props: map[string]any{
				"DomainName": "ws.example.com",
				"ApiId":      "api-2",
				"Stage":      "$default",
			},
		},
		{
			name: "events_api_destination", logicalID: "Dest", resourceType: "AWS::Events::ApiDestination",
			props: map[string]any{
				"Name":               "MyDest",
				"ConnectionArn":      "arn:aws:events:::connection/c",
				"HttpMethod":         "POST",
				"InvocationEndpoint": "https://example.com/api",
			},
		},
		{
			name: "events_event_bus_policy", logicalID: "Policy", resourceType: "AWS::Events::EventBusPolicy",
			props: map[string]any{"StatementId": "allow-put-events", "EventBusName": "default"},
		},
		{
			name: "kms_replica_key", logicalID: "Replica", resourceType: "AWS::KMS::ReplicaKey",
			props: map[string]any{"PrimaryKeyArn": "arn:aws:kms:us-east-1:000000000000:key/key-1"},
		},
		{
			name: "cognito_identity_pool", logicalID: "Pool", resourceType: "AWS::Cognito::IdentityPool",
			props: map[string]any{
				"IdentityPoolName":               "MyPool",
				"AllowUnauthenticatedIdentities": false,
			},
		},
		{
			name:         "cognito_identity_pool_role_attachment",
			logicalID:    "Roles",
			resourceType: "AWS::Cognito::IdentityPoolRoleAttachment",
			props:        map[string]any{"IdentityPoolId": "pool-1"},
		},
		{
			name: "cognito_user_pool_domain", logicalID: "Domain", resourceType: "AWS::Cognito::UserPoolDomain",
			props: map[string]any{"Domain": "myapp", "UserPoolId": "pool-1"},
		},
		{
			name: "cognito_user_pool_group", logicalID: "Group", resourceType: "AWS::Cognito::UserPoolGroup",
			props: map[string]any{"UserPoolId": "pool-1", "GroupName": "admins"},
		},
		{
			name: "ec2_vpc_peering_connection", logicalID: "Peering", resourceType: "AWS::EC2::VPCPeeringConnection",
			props: map[string]any{"VpcId": "vpc-1", "PeerVpcId": "vpc-2"},
		},
		{
			name: "ec2_network_acl", logicalID: "ACL", resourceType: "AWS::EC2::NetworkAcl",
			props: map[string]any{"VpcId": "vpc-1"},
		},
		{
			name: "ec2_network_acl_entry", logicalID: "Entry", resourceType: "AWS::EC2::NetworkAclEntry",
			props: map[string]any{
				"NetworkAclId": "acl-1",
				"RuleNumber":   float64(100),
				"Protocol":     "-1",
				"RuleAction":   "allow",
			},
		},
		{
			name: "ec2_key_pair", logicalID: "KP", resourceType: "AWS::EC2::KeyPair",
			props: map[string]any{"KeyName": "my-key"},
		},
		{
			name: "ec2_security_group_ingress", logicalID: "SGI", resourceType: "AWS::EC2::SecurityGroupIngress",
			props: map[string]any{
				"GroupId":    "sg-1",
				"IpProtocol": "tcp",
				"FromPort":   float64(80),
				"ToPort":     float64(80),
			},
		},
		{
			name: "ec2_security_group_egress", logicalID: "SGE", resourceType: "AWS::EC2::SecurityGroupEgress",
			props: map[string]any{"GroupId": "sg-1", "IpProtocol": "-1"},
		},
		{
			name: "ec2_flow_log", logicalID: "FL", resourceType: "AWS::EC2::FlowLog",
			props: map[string]any{"ResourceId": "vpc-1", "TrafficType": "ALL"},
		},
		{
			name: "elbv2_listener_rule", logicalID: "Rule", resourceType: "AWS::ElasticLoadBalancingV2::ListenerRule",
			props: map[string]any{
				"ListenerArn": "arn:aws:elasticloadbalancing:::listener/l-1",
				"Priority":    "10",
				"Actions": []any{
					map[string]any{
						"Type":           "forward",
						"TargetGroupArn": "arn:aws:elasticloadbalancing:::targetgroup/tg",
					},
				},
			},
		},
		{
			name: "lambda_event_invoke_config", logicalID: "EIC", resourceType: "AWS::Lambda::EventInvokeConfig",
			props: map[string]any{"FunctionName": "my-fn"},
		},
		{
			name: "lambda_url", logicalID: "Url", resourceType: "AWS::Lambda::Url",
			props: map[string]any{"TargetFunctionArn": "my-fn", "AuthType": "NONE"},
		},
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

// TestResourceCreator_Phase6Types_RealBackends validates create and delete round-trips with
// real in-memory backends for a representative subset of phase-6 resource types.
func TestResourceCreator_Phase6Types_RealBackends(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setupFn      func(*cloudformation.ServiceBackends)
		props        map[string]any
		name         string
		logicalID    string
		resourceType string
	}{
		{
			name:         "ec2_key_pair",
			logicalID:    "MyKeyPair",
			resourceType: "AWS::EC2::KeyPair",
			props:        map[string]any{"KeyName": "unit-cfn-keypair"},
		},
		{
			name:         "cognito_identity_pool",
			logicalID:    "MyIdentityPool",
			resourceType: "AWS::Cognito::IdentityPool",
			props: map[string]any{
				"IdentityPoolName":               "unit-cfn-pool",
				"AllowUnauthenticatedIdentities": false,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backends := newPhase6ServiceBackends()
			rc := cloudformation.NewResourceCreator(backends)

			physID, err := rc.Create(t.Context(), tt.logicalID, tt.resourceType, tt.props, nil, nil)
			require.NoError(t, err)
			assert.NotEmpty(t, physID)

			err = rc.Delete(t.Context(), tt.resourceType, physID, nil)
			require.NoError(t, err)
		})
	}
}

// TestResourceCreator_Phase6_EC2NetworkAcl validates NetworkAcl round-trip with a real VPC.
func TestResourceCreator_Phase6_EC2NetworkAcl(t *testing.T) {
	t.Parallel()

	backends := newPhase6ServiceBackends()
	ec2b, ok := backends.EC2.Backend.(*ec2backend.InMemoryBackend)
	require.True(t, ok)

	vpc, err := ec2b.CreateVpc("10.0.0.0/16")
	require.NoError(t, err)

	rc := cloudformation.NewResourceCreator(backends)
	ctx := t.Context()

	aclPhys, err := rc.Create(ctx, "MyACL", "AWS::EC2::NetworkAcl",
		map[string]any{"VpcId": vpc.ID}, nil, nil)
	require.NoError(t, err)
	assert.NotEmpty(t, aclPhys)

	// Create an entry in the ACL.
	entryPhys, err := rc.Create(ctx, "MyEntry", "AWS::EC2::NetworkAclEntry", map[string]any{
		"NetworkAclId": aclPhys,
		"RuleNumber":   float64(100),
		"Protocol":     "-1",
		"RuleAction":   "allow",
		"Egress":       false,
		"CidrBlock":    "0.0.0.0/0",
	}, nil, nil)
	require.NoError(t, err)
	assert.NotEmpty(t, entryPhys)

	require.NoError(t, rc.Delete(ctx, "AWS::EC2::NetworkAclEntry", entryPhys, nil))
	require.NoError(t, rc.Delete(ctx, "AWS::EC2::NetworkAcl", aclPhys, nil))
}

// TestResourceCreator_Phase6_KMSReplicaKey validates ReplicaKey round-trip with a real KMS key.
func TestResourceCreator_Phase6_KMSReplicaKey(t *testing.T) {
	t.Parallel()

	backends := newPhase6ServiceBackends()
	kmsb, ok := backends.KMS.Backend.(*kmsbackend.InMemoryBackend)
	require.True(t, ok)

	// Create a primary key.
	keyOut, err := kmsb.CreateKey(
		t.Context(),
		&kmsbackend.CreateKeyInput{Description: "primary", MultiRegion: true},
	)
	require.NoError(t, err)

	rc := cloudformation.NewResourceCreator(backends)
	ctx := t.Context()

	replicaPhys, err := rc.Create(ctx, "MyReplica", "AWS::KMS::ReplicaKey", map[string]any{
		"PrimaryKeyArn": keyOut.KeyMetadata.Arn,
		"Description":   "replica",
		"ReplicaRegion": "eu-west-1",
	}, nil, nil)
	require.NoError(t, err)
	assert.NotEmpty(t, replicaPhys)

	require.NoError(t, rc.Delete(ctx, "AWS::KMS::ReplicaKey", replicaPhys, nil))
}

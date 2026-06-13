package cloudformation_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cloudformation"
	cognitoidentitybackend "github.com/blackbirdworks/gopherstack/services/cognitoidentity"
	elbv2backend "github.com/blackbirdworks/gopherstack/services/elbv2"
	lambdabackend "github.com/blackbirdworks/gopherstack/services/lambda"
)

// newPhase5ServiceBackends creates a ServiceBackends with all phase-5 backends populated.
func newPhase5ServiceBackends() *cloudformation.ServiceBackends {
	b := newPhase4ServiceBackends()
	b.APIGatewayV2 = newPhase3ServiceBackends().APIGatewayV2
	b.CognitoIdentity = cognitoidentitybackend.NewHandler(
		cognitoidentitybackend.NewInMemoryBackend("000000000000", "us-east-1"),
		"us-east-1",
	)
	b.Lambda = lambdabackend.NewHandler(
		lambdabackend.NewInMemoryBackend(
			nil,
			nil,
			lambdabackend.DefaultSettings(),
			"000000000000",
			"us-east-1",
		),
	)

	return b
}

// TestResourceCreator_Phase5Types_NilBackends ensures all phase-5 resource types return a stub
// physical ID when the corresponding backend is nil.
func TestResourceCreator_Phase5Types_NilBackends(t *testing.T) {
	t.Parallel()

	tests := []struct {
		props        map[string]any
		name         string
		logicalID    string
		resourceType string
	}{
		{
			name:         "apigw_model",
			logicalID:    "MyModel",
			resourceType: "AWS::ApiGateway::Model",
			props: map[string]any{
				"RestApiId":   "api-stub",
				"Name":        "stub-model",
				"ContentType": "application/json",
			},
		},
		{
			name:         "apigw_request_validator",
			logicalID:    "MyValidator",
			resourceType: "AWS::ApiGateway::RequestValidator",
			props:        map[string]any{"RestApiId": "api-stub", "Name": "stub-validator"},
		},
		{
			name:         "apigw_authorizer",
			logicalID:    "MyAuth",
			resourceType: "AWS::ApiGateway::Authorizer",
			props: map[string]any{
				"RestApiId": "api-stub",
				"Name":      "stub-auth",
				"Type":      "TOKEN",
			},
		},
		{
			name:         "apigw_api_key",
			logicalID:    "MyKey",
			resourceType: "AWS::ApiGateway::ApiKey",
			props:        map[string]any{"Name": "stub-key"},
		},
		{
			name:         "apigw_usage_plan",
			logicalID:    "MyUsagePlan",
			resourceType: "AWS::ApiGateway::UsagePlan",
			props:        map[string]any{"UsagePlanName": "stub-plan"},
		},
		{
			name:         "apigw_usage_plan_key",
			logicalID:    "MyUPKey",
			resourceType: "AWS::ApiGateway::UsagePlanKey",
			props: map[string]any{
				"UsagePlanId": "plan-stub",
				"KeyId":       "key-stub",
				"KeyType":     "API_KEY",
			},
		},
		{
			name:         "apigw_domain_name",
			logicalID:    "MyDomain",
			resourceType: "AWS::ApiGateway::DomainName",
			props:        map[string]any{"DomainName": "stub.example.com"},
		},
		{
			name:         "apigw_base_path_mapping",
			logicalID:    "MyBPM",
			resourceType: "AWS::ApiGateway::BasePathMapping",
			props:        map[string]any{"DomainName": "stub.example.com", "RestApiId": "api-stub"},
		},
		{
			name:         "apigw_account",
			logicalID:    "MyAccount",
			resourceType: "AWS::ApiGateway::Account",
			props:        map[string]any{},
		},
		{
			name:         "apigw_gateway_response",
			logicalID:    "MyGatewayResponse",
			resourceType: "AWS::ApiGateway::GatewayResponse",
			props:        map[string]any{"RestApiId": "api-stub", "ResponseType": "DEFAULT_4XX"},
		},
		{
			name:         "apigwv2_domain_name",
			logicalID:    "MyV2Domain",
			resourceType: "AWS::ApiGatewayV2::DomainName",
			props:        map[string]any{"DomainName": "v2.example.com"},
		},
		{
			name:         "apigwv2_api_mapping",
			logicalID:    "MyAPIMapping",
			resourceType: "AWS::ApiGatewayV2::ApiMapping",
			props: map[string]any{
				"DomainName": "v2.example.com",
				"ApiId":      "api-stub",
				"Stage":      "prod",
			},
		},
		{
			name:         "events_api_destination",
			logicalID:    "MyDest",
			resourceType: "AWS::Events::ApiDestination",
			props: map[string]any{
				"Name":               "stub-dest",
				"ConnectionArn":      "arn:stub",
				"InvocationEndpoint": "https://example.com",
				"HttpMethod":         "POST",
			},
		},
		{
			name:         "events_event_bus_policy",
			logicalID:    "MyPolicy",
			resourceType: "AWS::Events::EventBusPolicy",
			props: map[string]any{
				"StatementId":  "stub-stmt",
				"EventBusName": "default",
				"Action":       "events:PutEvents",
				"Principal":    "123456789012",
			},
		},
		{
			name:         "kms_replica_key",
			logicalID:    "MyReplicaKey",
			resourceType: "AWS::KMS::ReplicaKey",
			props: map[string]any{
				"PrimaryKeyArn": "arn:aws:kms:us-west-2:000000000000:key/stub",
			},
		},
		{
			name:         "cognito_identity_pool",
			logicalID:    "MyPool",
			resourceType: "AWS::Cognito::IdentityPool",
			props: map[string]any{
				"IdentityPoolName":               "stub-pool",
				"AllowUnauthenticatedIdentities": false,
			},
		},
		{
			name:         "cognito_identity_pool_role_attachment",
			logicalID:    "MyRoleAttach",
			resourceType: "AWS::Cognito::IdentityPoolRoleAttachment",
			props:        map[string]any{"IdentityPoolId": "us-east-1:stub-pool-id"},
		},
		{
			name:         "cognito_user_pool_domain",
			logicalID:    "MyUPDomain",
			resourceType: "AWS::Cognito::UserPoolDomain",
			props:        map[string]any{"Domain": "stub-domain", "UserPoolId": "us-east-1_stub"},
		},
		{
			name:         "cognito_user_pool_group",
			logicalID:    "MyGroup",
			resourceType: "AWS::Cognito::UserPoolGroup",
			props:        map[string]any{"GroupName": "stub-group", "UserPoolId": "us-east-1_stub"},
		},
		{
			name:         "ec2_vpc_peering",
			logicalID:    "MyPeering",
			resourceType: "AWS::EC2::VPCPeeringConnection",
			props:        map[string]any{"VpcId": "vpc-stub", "PeerVpcId": "vpc-peer"},
		},
		{
			name:         "ec2_network_acl",
			logicalID:    "MyACL",
			resourceType: "AWS::EC2::NetworkAcl",
			props:        map[string]any{"VpcId": "vpc-stub"},
		},
		{
			name:         "ec2_network_acl_entry",
			logicalID:    "MyACLEntry",
			resourceType: "AWS::EC2::NetworkAclEntry",
			props: map[string]any{
				"NetworkAclId": "acl-stub",
				"RuleNumber":   float64(100),
				"Protocol":     float64(6),
				"RuleAction":   "allow",
				"Egress":       false,
				"CidrBlock":    "0.0.0.0/0",
			},
		},
		{
			name:         "ec2_key_pair",
			logicalID:    "MyKeyPair",
			resourceType: "AWS::EC2::KeyPair",
			props:        map[string]any{"KeyName": "stub-key"},
		},
		{
			name:         "ec2_sg_ingress",
			logicalID:    "MyIngress",
			resourceType: "AWS::EC2::SecurityGroupIngress",
			props: map[string]any{
				"GroupId":    "sg-stub",
				"IpProtocol": "tcp",
				"FromPort":   float64(80),
				"ToPort":     float64(80),
				"CidrIp":     "0.0.0.0/0",
			},
		},
		{
			name:         "ec2_sg_egress",
			logicalID:    "MyEgress",
			resourceType: "AWS::EC2::SecurityGroupEgress",
			props: map[string]any{
				"GroupId":    "sg-stub",
				"IpProtocol": "tcp",
				"FromPort":   float64(443),
				"ToPort":     float64(443),
				"CidrIp":     "0.0.0.0/0",
			},
		},
		{
			name:         "ec2_flow_log",
			logicalID:    "MyFlowLog",
			resourceType: "AWS::EC2::FlowLog",
			props: map[string]any{
				"ResourceId":   "vpc-stub",
				"ResourceType": "VPC",
				"TrafficType":  "ALL",
				"LogGroupName": "stub-lg",
			},
		},
		{
			name:         "elbv2_listener_rule",
			logicalID:    "MyRule",
			resourceType: "AWS::ElasticLoadBalancingV2::ListenerRule",
			props: map[string]any{
				"ListenerArn": "arn:stub",
				"Priority":    float64(10),
				"Conditions": []any{map[string]any{
					"Field":  "path-pattern",
					"Values": []any{"/api/*"},
				}},
				"Actions": []any{map[string]any{
					"Type":                "fixed-response",
					"FixedResponseConfig": map[string]any{"StatusCode": "200"},
				}},
			},
		},
		{
			name:         "lambda_event_invoke_config",
			logicalID:    "MyEIC",
			resourceType: "AWS::Lambda::EventInvokeConfig",
			props: map[string]any{
				"FunctionName":         "stub-fn",
				"MaximumRetryAttempts": float64(2),
			},
		},
		{
			name:         "lambda_url",
			logicalID:    "MyURL",
			resourceType: "AWS::Lambda::Url",
			props:        map[string]any{"TargetFunctionArn": "stub-fn", "AuthType": "NONE"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backends := newServiceBackends()
			backends.APIGateway = nil
			backends.APIGatewayV2 = nil
			backends.EventBridge = nil
			backends.KMS = nil
			backends.CognitoIdentity = nil
			backends.CognitoIDP = nil
			backends.EC2 = nil
			backends.ELBv2 = nil
			backends.Lambda = nil

			rc := cloudformation.NewResourceCreator(backends)

			physID, err := rc.Create(t.Context(), tt.logicalID, tt.resourceType, tt.props, nil, nil)
			require.NoError(t, err)
			assert.NotEmpty(t, physID)

			err = rc.Delete(t.Context(), tt.resourceType, physID, nil)
			require.NoError(t, err)
		})
	}
}

// TestResourceCreator_Phase5Types_RealBackends validates create and delete for phase-5 resource types
// with real in-memory backends.
func TestResourceCreator_Phase5Types_RealBackends(t *testing.T) {
	t.Parallel()

	backends := newPhase5ServiceBackends()
	rc := cloudformation.NewResourceCreator(backends)

	// First create a real REST API so child resources have a valid restAPIID.
	restAPIID, err := rc.Create(t.Context(), "MyAPI", "AWS::ApiGateway::RestApi",
		map[string]any{"Name": "phase5-test-api"}, nil, nil)
	require.NoError(t, err)
	require.NotEmpty(t, restAPIID)

	// And a real API key to use in UsagePlanKey.
	apiKeyID, err := rc.Create(t.Context(), "MyKey", "AWS::ApiGateway::ApiKey",
		map[string]any{"Name": "phase5-key", "Enabled": true}, nil, nil)
	require.NoError(t, err)
	require.NotEmpty(t, apiKeyID)

	t.Run("apigw_model", func(t *testing.T) {
		t.Parallel()
		rc2 := cloudformation.NewResourceCreator(backends)
		physID, err := rc2.Create(
			t.Context(),
			"MyModel",
			"AWS::ApiGateway::Model",
			map[string]any{
				"RestApiId":   restAPIID,
				"Name":        "MyModel",
				"ContentType": "application/json",
				"Schema":      `{}`,
			},
			nil,
			nil,
		)
		require.NoError(t, err)
		assert.Contains(t, physID, restAPIID)
		require.NoError(t, rc2.Delete(t.Context(), "AWS::ApiGateway::Model", physID, nil))
	})

	t.Run("apigw_request_validator", func(t *testing.T) {
		t.Parallel()
		rc2 := cloudformation.NewResourceCreator(backends)
		physID, err := rc2.Create(t.Context(), "MyValidator", "AWS::ApiGateway::RequestValidator",
			map[string]any{"RestApiId": restAPIID, "Name": "MyValidator"},
			nil, nil)
		require.NoError(t, err)
		assert.Contains(t, physID, restAPIID)
		require.NoError(
			t,
			rc2.Delete(t.Context(), "AWS::ApiGateway::RequestValidator", physID, nil),
		)
	})

	t.Run("apigw_domain_name", func(t *testing.T) {
		t.Parallel()
		rc2 := cloudformation.NewResourceCreator(backends)
		physID, err := rc2.Create(t.Context(), "MyDomain", "AWS::ApiGateway::DomainName",
			map[string]any{"DomainName": "cfn-phase5-test.example.com"},
			nil, nil)
		require.NoError(t, err)
		assert.NotEmpty(t, physID)
		require.NoError(t, rc2.Delete(t.Context(), "AWS::ApiGateway::DomainName", physID, nil))
	})

	t.Run("apigw_usage_plan", func(t *testing.T) {
		t.Parallel()
		rc2 := cloudformation.NewResourceCreator(backends)
		physID, err := rc2.Create(t.Context(), "MyPlan", "AWS::ApiGateway::UsagePlan",
			map[string]any{"UsagePlanName": "phase5-test-plan"},
			nil, nil)
		require.NoError(t, err)
		assert.NotEmpty(t, physID)
		require.NoError(t, rc2.Delete(t.Context(), "AWS::ApiGateway::UsagePlan", physID, nil))
	})

	t.Run("cognito_identity_pool_create_delete", func(t *testing.T) {
		t.Parallel()
		rc2 := cloudformation.NewResourceCreator(backends)
		physID, err := rc2.Create(
			t.Context(),
			"MyPool",
			"AWS::Cognito::IdentityPool",
			map[string]any{
				"IdentityPoolName":               "phase5-test-pool",
				"AllowUnauthenticatedIdentities": true,
			},
			nil,
			nil,
		)
		require.NoError(t, err)
		assert.NotEmpty(t, physID)
		require.NoError(t, rc2.Delete(t.Context(), "AWS::Cognito::IdentityPool", physID, nil))
	})

	t.Run("ec2_network_acl", func(t *testing.T) {
		t.Parallel()
		rc2 := cloudformation.NewResourceCreator(backends)

		// Create a VPC first.
		vpcID, err := rc2.Create(t.Context(), "MyVPC", "AWS::EC2::VPC",
			map[string]any{"CidrBlock": "10.99.0.0/16"}, nil, nil)
		require.NoError(t, err)

		physID, err := rc2.Create(t.Context(), "MyACL", "AWS::EC2::NetworkAcl",
			map[string]any{"VpcId": vpcID}, nil, nil)
		require.NoError(t, err)
		assert.NotEmpty(t, physID)
		require.NoError(t, rc2.Delete(t.Context(), "AWS::EC2::NetworkAcl", physID, nil))
	})

	t.Run("elbv2_listener_rule", func(t *testing.T) {
		t.Parallel()
		elbBackend := elbv2backend.NewInMemoryBackend("000000000000", "us-east-1")
		elbHandler := elbv2backend.NewHandler(elbBackend)
		b2 := newPhase5ServiceBackends()
		b2.ELBv2 = elbHandler
		rc2 := cloudformation.NewResourceCreator(b2)

		// Create LB + listener first.
		lbARN, err := rc2.Create(t.Context(), "MyLB", "AWS::ElasticLoadBalancingV2::LoadBalancer",
			map[string]any{"Name": "phase5-test-lb", "Type": "application"}, nil, nil)
		require.NoError(t, err)

		listenerARN, err := rc2.Create(
			t.Context(),
			"MyListener",
			"AWS::ElasticLoadBalancingV2::Listener",
			map[string]any{
				"LoadBalancerArn": lbARN,
				"Protocol":        "HTTP",
				"Port":            float64(80),
				"DefaultActions": []any{map[string]any{
					"Type":                "fixed-response",
					"FixedResponseConfig": map[string]any{"StatusCode": "200"},
				}},
			},
			nil,
			nil,
		)
		require.NoError(t, err)

		physID, err := rc2.Create(
			t.Context(),
			"MyRule",
			"AWS::ElasticLoadBalancingV2::ListenerRule",
			map[string]any{
				"ListenerArn": listenerARN,
				"Priority":    float64(10),
				"Conditions": []any{map[string]any{
					"Field":  "path-pattern",
					"Values": []any{"/api/*"},
				}},
				"Actions": []any{map[string]any{
					"Type":                "fixed-response",
					"FixedResponseConfig": map[string]any{"StatusCode": "200"},
				}},
			},
			nil,
			nil,
		)
		require.NoError(t, err)
		assert.NotEmpty(t, physID)
		require.NoError(
			t,
			rc2.Delete(t.Context(), "AWS::ElasticLoadBalancingV2::ListenerRule", physID, nil),
		)
	})

	t.Run("lambda_event_invoke_config", func(t *testing.T) {
		t.Parallel()
		rc2 := cloudformation.NewResourceCreator(backends)

		// Create a Lambda function first.
		fnARN, err := rc2.Create(t.Context(), "MyFn", "AWS::Lambda::Function",
			map[string]any{
				"FunctionName": "phase5-test-fn",
				"Runtime":      "python3.11",
				"Handler":      "index.handler",
				"Role":         "arn:aws:iam::000000000000:role/lambda-role",
				"Code":         map[string]any{"ZipFile": "def handler(e, c): pass"},
			}, nil, nil)
		require.NoError(t, err)
		require.NotEmpty(t, fnARN)

		physID, err := rc2.Create(t.Context(), "MyEIC", "AWS::Lambda::EventInvokeConfig",
			map[string]any{"FunctionName": "phase5-test-fn", "MaximumRetryAttempts": float64(1)},
			nil, nil)
		require.NoError(t, err)
		assert.NotEmpty(t, physID)
		require.NoError(t, rc2.Delete(t.Context(), "AWS::Lambda::EventInvokeConfig", physID, nil))
	})

	t.Run("lambda_url", func(t *testing.T) {
		t.Parallel()
		rc2 := cloudformation.NewResourceCreator(backends)

		fnARN, err := rc2.Create(t.Context(), "MyFn2", "AWS::Lambda::Function",
			map[string]any{
				"FunctionName": "phase5-url-test-fn",
				"Runtime":      "python3.11",
				"Handler":      "index.handler",
				"Role":         "arn:aws:iam::000000000000:role/lambda-role",
				"Code":         map[string]any{"ZipFile": "def handler(e, c): pass"},
			}, nil, nil)
		require.NoError(t, err)
		require.NotEmpty(t, fnARN)

		physID, err := rc2.Create(t.Context(), "MyURL", "AWS::Lambda::Url",
			map[string]any{"TargetFunctionArn": "phase5-url-test-fn", "AuthType": "NONE"},
			nil, nil)
		require.NoError(t, err)
		assert.NotEmpty(t, physID)
		require.NoError(t, rc2.Delete(t.Context(), "AWS::Lambda::Url", physID, nil))
	})
}

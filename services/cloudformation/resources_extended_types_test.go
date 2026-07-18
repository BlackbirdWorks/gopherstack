package cloudformation_test

import (
	"log/slog"
	"testing"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/cloudformation"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestProvider_Init_WithConfig(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		config  any
		wantSvc string
	}{
		{
			name:    "with_backends_provider",
			config:  newMockBackendsProvider(),
			wantSvc: "CloudFormation",
		},
		{
			name:    "with_config_provider",
			config:  &mockConfigProvider{},
			wantSvc: "CloudFormation",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			p := &cloudformation.Provider{}
			appCtx := &service.AppContext{
				Logger: slog.Default(),
				Config: tt.config,
			}
			svc, err := p.Init(appCtx)
			require.NoError(t, err)
			require.NotNil(t, svc)
			assert.Equal(t, tt.wantSvc, svc.Name())
		})
	}
}

func TestResourceNameFromARN(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		input string
		want  string
	}{
		{
			name:  "arn_with_slash",
			input: "arn:aws:scheduler:us-east-1:000000000000:schedule/default/my-sched",
			want:  "my-sched",
		},
		{
			name:  "arn_with_colon_no_slash",
			input: "arn:aws:sns:us-east-1:000000000000:my-topic",
			want:  "my-topic",
		},
		{
			name:  "plain_name_no_separator",
			input: "my-plain-resource",
			want:  "my-plain-resource",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// Indirectly exercise resourceNameFromARN via the Scheduler delete path.
			// We drive it through deleteSchedulerSchedule by creating and deleting a schedule.
			if tt.input == "my-plain-resource" {
				// Exercise plain-name case directly via Scheduler ARN that is already a name.
				backends := newExtendedServiceBackends()
				rc := cloudformation.NewResourceCreator(backends)

				physID, err := rc.Create(t.Context(), "PlainSched", "AWS::Scheduler::Schedule",
					map[string]any{
						"Name":               "my-plain-resource",
						"ScheduleExpression": "rate(1 minute)",
						"Target": map[string]any{
							"Arn":     "arn:aws:lambda:us-east-1:000000000000:function:fn",
							"RoleArn": "arn:aws:iam::000000000000:role/r",
						},
					}, nil, nil)
				require.NoError(t, err)

				// Delete using the ARN form (physID) to confirm extraction works.
				err = rc.Delete(t.Context(), "AWS::Scheduler::Schedule", physID, nil)
				require.NoError(t, err)

				return
			}

			// For ARN forms, just verify the ARN is used in scheduler create/delete cycle.
			backends := newExtendedServiceBackends()
			rc := cloudformation.NewResourceCreator(backends)

			schedName := tt.want
			physID, err := rc.Create(t.Context(), "MySched", "AWS::Scheduler::Schedule",
				map[string]any{
					"Name":               schedName,
					"ScheduleExpression": "rate(1 minute)",
					"Target": map[string]any{
						"Arn":     "arn:aws:lambda:us-east-1:000000000000:function:fn",
						"RoleArn": "arn:aws:iam::000000000000:role/r",
					},
				}, nil, nil)
			require.NoError(t, err)
			assert.Contains(t, physID, schedName)

			err = rc.Delete(t.Context(), "AWS::Scheduler::Schedule", physID, nil)
			require.NoError(t, err)
		})
	}
}

func TestStreamNameFromARN(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		input string
		want  string
	}{
		{
			name:  "full_kinesis_arn",
			input: "arn:aws:kinesis:us-east-1:000000000000:stream/my-stream",
			want:  "my-stream",
		},
		{
			name:  "plain_name_fallback",
			input: "my-plain-stream",
			want:  "my-plain-stream",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// Exercise streamNameFromARN indirectly via Kinesis delete path.
			backends := newExtendedServiceBackends()
			rc := cloudformation.NewResourceCreator(backends)

			streamName := tt.want
			physID, err := rc.Create(t.Context(), "MyStream", "AWS::Kinesis::Stream",
				map[string]any{"Name": streamName}, nil, nil)
			require.NoError(t, err)

			// For the plain-name case, deleteKinesisStream receives the stream name (not ARN).
			deleteID := physID
			if tt.input == "my-plain-stream" {
				deleteID = tt.input // pass plain name so fallback branch is hit
			}

			err = rc.Delete(t.Context(), "AWS::Kinesis::Stream", deleteID, nil)
			require.NoError(t, err)
		})
	}
}

func TestResourceCreator_AdditionalTypes_NilBackends(t *testing.T) {
	t.Parallel()

	// When all Phase 2 backends are nil (only core backends), create should return stub IDs.
	tests := []struct {
		props        map[string]any
		name         string
		logicalID    string
		resourceType string
	}{
		{
			name: "rds_db_instance", logicalID: "MyDB", resourceType: "AWS::RDS::DBInstance",
			props: map[string]any{"DBInstanceIdentifier": "stub-db", "Engine": "postgres"},
		},
		{
			name:         "rds_subnet_group",
			logicalID:    "MySG",
			resourceType: "AWS::RDS::DBSubnetGroup",
			props: map[string]any{
				"DBSubnetGroupName":        "stub-sg",
				"DBSubnetGroupDescription": "desc",
				"SubnetIds":                []any{"s-1"},
			},
		},
		{
			name: "rds_parameter_group", logicalID: "MyPG", resourceType: "AWS::RDS::DBParameterGroup",
			props: map[string]any{
				"DBParameterGroupName": "stub-pg",
				"Family":               "postgres14",
				"Description":          "desc",
			},
		},
		{
			name:         "elasticache_replication_group",
			logicalID:    "MyRG",
			resourceType: "AWS::ElastiCache::ReplicationGroup",

			props: map[string]any{
				"ReplicationGroupId":          "stub-rg",
				"ReplicationGroupDescription": "desc",
			},
		},
		{
			name:         "elasticache_subnet_group",
			logicalID:    "MyECSubnet",
			resourceType: "AWS::ElastiCache::SubnetGroup",
			props: map[string]any{
				"CacheSubnetGroupName":        "stub-ecsg",
				"CacheSubnetGroupDescription": "desc",
				"SubnetIds":                   []any{"s-1"},
			},
		},
		{
			name: "ecs_cluster", logicalID: "MyCluster", resourceType: "AWS::ECS::Cluster",
			props: map[string]any{"ClusterName": "stub-cluster"},
		},
		{
			name: "ecs_task_definition", logicalID: "MyTD", resourceType: "AWS::ECS::TaskDefinition",
			props: map[string]any{"Family": "stub-family"},
		},
		{
			name:         "ecs_service",
			logicalID:    "MySvc",
			resourceType: "AWS::ECS::Service",
			props: map[string]any{
				"ServiceName":    "stub-service",
				"Cluster":        "stub-cluster",
				"TaskDefinition": "stub-td",
			},
		},
		{
			name: "ecr_repository", logicalID: "MyRepo", resourceType: "AWS::ECR::Repository",
			props: map[string]any{"RepositoryName": "stub-repo"},
		},
		{
			name:         "redshift_cluster",
			logicalID:    "MyRS",
			resourceType: "AWS::Redshift::Cluster",
			props: map[string]any{
				"ClusterIdentifier": "stub-rs",
				"NodeType":          "dc2.large",
				"DBName":            "mydb",
				"MasterUsername":    "admin",
			},
		},
		{
			name: "opensearch_domain", logicalID: "MyDomain", resourceType: "AWS::OpenSearch::Domain",
			props: map[string]any{"DomainName": "stub-os"},
		},
		{
			name: "firehose_delivery_stream", logicalID: "MyStream", resourceType: "AWS::Firehose::DeliveryStream",
			props: map[string]any{"DeliveryStreamName": "stub-fh"},
		},
		{
			name: "route53_healthcheck", logicalID: "MyHC", resourceType: "AWS::Route53::HealthCheck",
			props: map[string]any{"HealthCheckConfig": map[string]any{"Type": "HTTPS"}},
		},
		{
			name: "route53resolver_endpoint", logicalID: "MyEP", resourceType: "AWS::Route53Resolver::ResolverEndpoint",
			props: map[string]any{"Name": "stub-ep", "Direction": "INBOUND"},
		},
		{
			name: "route53resolver_rule", logicalID: "MyRule", resourceType: "AWS::Route53Resolver::ResolverRule",
			props: map[string]any{
				"Name":       "stub-rule",
				"DomainName": "example.internal",
				"RuleType":   "FORWARD",
			},
		},
		{
			name: "swf_domain", logicalID: "MyDomain", resourceType: "AWS::SWF::Domain",
			props: map[string]any{"Name": "stub-domain"},
		},
		{
			name: "appsync_graphql_api", logicalID: "MyAPI", resourceType: "AWS::AppSync::GraphQLApi",
			props: map[string]any{"Name": "stub-api", "AuthenticationType": "API_KEY"},
		},
		{
			name: "ses_email_identity", logicalID: "MyId", resourceType: "AWS::SES::EmailIdentity",
			props: map[string]any{"EmailIdentity": "stub@example.com"},
		},
		{
			name: "acm_certificate", logicalID: "MyCert", resourceType: "AWS::ACM::Certificate",
			props: map[string]any{"DomainName": "stub.example.com"},
		},
		{
			name: "cognito_user_pool", logicalID: "MyPool", resourceType: "AWS::Cognito::UserPool",
			props: map[string]any{"PoolName": "stub-pool"},
		},
		{
			name: "cognito_user_pool_client", logicalID: "MyClient", resourceType: "AWS::Cognito::UserPoolClient",
			props: map[string]any{"ClientName": "stub-client", "UserPoolId": "us-east-1_stubpool"},
		},
		{
			name:         "ec2_eip",
			logicalID:    "MyEIP",
			resourceType: "AWS::EC2::EIP",
			props:        map[string]any{},
		},
		{
			name: "ec2_nat_gateway", logicalID: "MyNGW", resourceType: "AWS::EC2::NatGateway",
			props: map[string]any{"SubnetId": "subnet-1", "AllocationId": "eipalloc-abc123"},
		},
		{
			name: "cloudwatch_composite_alarm", logicalID: "MyCA", resourceType: "AWS::CloudWatch::CompositeAlarm",
			props: map[string]any{"AlarmName": "stub-composite", "AlarmRule": "ALARM(foo)"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// newServiceBackends() leaves all Phase 2 backends nil → stub path.
			backends := newServiceBackends()
			rc := cloudformation.NewResourceCreator(backends)

			physID, err := rc.Create(t.Context(), tt.logicalID, tt.resourceType, tt.props, nil, nil)
			require.NoError(t, err)
			assert.NotEmpty(t, physID)

			// Delete should also be a no-op without a backend.
			err = rc.Delete(t.Context(), tt.resourceType, physID, nil)
			require.NoError(t, err)
		})
	}
}

func TestResourceCreator_AdditionalTypes_RealBackends(t *testing.T) {
	t.Parallel()

	tests := []struct {
		props        map[string]any
		name         string
		logicalID    string
		resourceType string
		wantContains string
		wantPhysID   string
		wantNotEmpty bool
	}{
		{
			name:         "rds_db_instance",
			logicalID:    "MyDB",
			resourceType: "AWS::RDS::DBInstance",
			props: map[string]any{
				"DBInstanceIdentifier": "unit-test-db",
				"Engine":               "postgres",
				"DBInstanceClass":      "db.t3.micro",
			},
			wantPhysID: "unit-test-db",
		},
		{
			name:         "rds_subnet_group",
			logicalID:    "MySG",
			resourceType: "AWS::RDS::DBSubnetGroup",
			props: map[string]any{
				"DBSubnetGroupName":        "unit-test-sg",
				"DBSubnetGroupDescription": "desc",
				"SubnetIds":                []any{"subnet-1"},
			},
			wantPhysID: "unit-test-sg",
		},
		{
			name:         "rds_parameter_group",
			logicalID:    "MyPG",
			resourceType: "AWS::RDS::DBParameterGroup",
			props: map[string]any{
				"DBParameterGroupName": "unit-test-pg",
				"Family":               "postgres14",
				"Description":          "desc",
			},
			wantPhysID: "unit-test-pg",
		},
		{
			name:         "elasticache_replication_group",
			logicalID:    "MyRG",
			resourceType: "AWS::ElastiCache::ReplicationGroup",
			props: map[string]any{
				"ReplicationGroupId":          "unit-test-rg",
				"ReplicationGroupDescription": "desc",
			},
			wantNotEmpty: true,
		},
		{
			name:         "elasticache_subnet_group",
			logicalID:    "MyECSubnet",
			resourceType: "AWS::ElastiCache::SubnetGroup",
			props: map[string]any{
				"CacheSubnetGroupName":        "unit-test-ecsg",
				"CacheSubnetGroupDescription": "desc",
				"SubnetIds":                   []any{"subnet-1"},
			},
			wantPhysID: "unit-test-ecsg",
		},
		{
			name:         "ecs_cluster",
			logicalID:    "MyCluster",
			resourceType: "AWS::ECS::Cluster",
			props:        map[string]any{"ClusterName": "unit-test-cluster"},
			wantNotEmpty: true,
		},
		{
			name:         "ecs_task_definition",
			logicalID:    "MyTD",
			resourceType: "AWS::ECS::TaskDefinition",
			props: map[string]any{
				"Family":      "unit-test-family",
				"NetworkMode": "awsvpc",
				"ContainerDefinitions": []any{
					map[string]any{"Name": "app", "Image": "nginx:latest"},
				},
			},
			wantNotEmpty: true,
		},
		{
			name:         "ecr_repository",
			logicalID:    "MyRepo",
			resourceType: "AWS::ECR::Repository",
			props:        map[string]any{"RepositoryName": "unit-test-repo"},
			wantNotEmpty: true,
		},
		{
			name:         "redshift_cluster",
			logicalID:    "MyRS",
			resourceType: "AWS::Redshift::Cluster",
			props: map[string]any{
				"ClusterIdentifier": "unit-test-rs",
				"NodeType":          "dc2.large",
				"DBName":            "mydb",
				"MasterUsername":    "admin",
			},
			wantNotEmpty: true,
		},
		{
			name:         "opensearch_domain",
			logicalID:    "MyDomain",
			resourceType: "AWS::OpenSearch::Domain",
			props:        map[string]any{"DomainName": "unit-test-os"},
			wantNotEmpty: true,
		},
		{
			name:         "firehose_delivery_stream",
			logicalID:    "MyStream",
			resourceType: "AWS::Firehose::DeliveryStream",
			props:        map[string]any{"DeliveryStreamName": "unit-test-fh"},
			wantNotEmpty: true,
		},
		{
			name:         "route53_healthcheck",
			logicalID:    "MyHC",
			resourceType: "AWS::Route53::HealthCheck",
			props: map[string]any{
				"HealthCheckConfig": map[string]any{
					"Type":                     "HTTPS",
					"FullyQualifiedDomainName": "example.com",
				},
			},
			wantNotEmpty: true,
		},
		{
			name:         "route53resolver_endpoint",
			logicalID:    "MyEP",
			resourceType: "AWS::Route53Resolver::ResolverEndpoint",
			props:        map[string]any{"Name": "unit-test-ep", "Direction": "INBOUND"},
			wantNotEmpty: true,
		},
		{
			name:         "route53resolver_rule",
			logicalID:    "MyRule",
			resourceType: "AWS::Route53Resolver::ResolverRule",
			props: map[string]any{
				"Name":       "unit-test-rule",
				"DomainName": "example.internal",
				"RuleType":   "FORWARD",
			},
			wantNotEmpty: true,
		},
		{
			name:         "swf_domain",
			logicalID:    "MyDomain",
			resourceType: "AWS::SWF::Domain",
			props:        map[string]any{"Name": "unit-test-domain"},
			wantNotEmpty: true,
		},
		{
			name:         "appsync_graphql_api",
			logicalID:    "MyAPI",
			resourceType: "AWS::AppSync::GraphQLApi",
			props:        map[string]any{"Name": "unit-test-api", "AuthenticationType": "API_KEY"},
			wantNotEmpty: true,
		},
		{
			name:         "ses_email_identity",
			logicalID:    "MyId",
			resourceType: "AWS::SES::EmailIdentity",
			props:        map[string]any{"EmailIdentity": "unit@example.com"},
			wantPhysID:   "unit@example.com",
		},
		{
			name:         "acm_certificate",
			logicalID:    "MyCert",
			resourceType: "AWS::ACM::Certificate",
			props:        map[string]any{"DomainName": "unit.example.com"},
			wantNotEmpty: true,
		},
		{
			name:         "cognito_user_pool",
			logicalID:    "MyPool",
			resourceType: "AWS::Cognito::UserPool",
			props:        map[string]any{"PoolName": "unit-test-pool"},
			wantNotEmpty: true,
		},
		{
			name:         "ec2_eip",
			logicalID:    "MyEIP",
			resourceType: "AWS::EC2::EIP",
			props:        map[string]any{},
			wantContains: "eipalloc-",
		},
		{
			name:         "cloudwatch_composite_alarm",
			logicalID:    "MyCA",
			resourceType: "AWS::CloudWatch::CompositeAlarm",
			props:        map[string]any{"AlarmName": "unit-composite", "AlarmRule": "ALARM(foo)"},
			wantPhysID:   "unit-composite",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backends := newAdditionalServiceBackends()
			rc := cloudformation.NewResourceCreator(backends)

			physID, err := rc.Create(t.Context(), tt.logicalID, tt.resourceType, tt.props, nil, nil)
			require.NoError(t, err)

			if tt.wantPhysID != "" {
				assert.Equal(t, tt.wantPhysID, physID)
			}

			if tt.wantContains != "" {
				assert.Contains(t, physID, tt.wantContains)
			}

			if tt.wantNotEmpty {
				assert.NotEmpty(t, physID)
			}

			err = rc.Delete(t.Context(), tt.resourceType, physID, tt.props)
			require.NoError(t, err)
		})
	}
}

func TestResourceCreator_CognitoUserPoolWithClient(t *testing.T) {
	t.Parallel()

	backends := newAdditionalServiceBackends()
	rc := cloudformation.NewResourceCreator(backends)
	ctx := t.Context()

	// Create pool first.
	poolID, err := rc.Create(ctx, "MyPool", "AWS::Cognito::UserPool",
		map[string]any{"PoolName": "test-pool"}, nil, nil)
	require.NoError(t, err)
	require.NotEmpty(t, poolID)

	// Create client referencing the pool.
	clientPhysID, err := rc.Create(ctx, "MyClient", "AWS::Cognito::UserPoolClient",
		map[string]any{"ClientName": "test-client", "UserPoolId": poolID}, nil, nil)
	require.NoError(t, err)
	require.NotEmpty(t, clientPhysID)

	// Delete pool — should also clean up the client.
	err = rc.Delete(ctx, "AWS::Cognito::UserPool", poolID, nil)
	require.NoError(t, err)

	// Attempting to delete the client again should either succeed (already gone) or fail gracefully.
	_ = rc.Delete(ctx, "AWS::Cognito::UserPoolClient", clientPhysID, nil)
}

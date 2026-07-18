package cloudformation_test

import (
	"testing"

	"github.com/blackbirdworks/gopherstack/services/cloudformation"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewInMemoryBackend(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		wantEmpty bool
	}{
		{
			name:      "empty_on_creation",
			wantEmpty: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := cloudformation.NewInMemoryBackend()
			require.NotNil(t, b)

			all := b.ListAll()

			if tt.wantEmpty {
				assert.Empty(t, all)
			}
		})
	}
}

func TestResourceCreator_NilBackends(t *testing.T) {
	t.Parallel()

	tests := []struct {
		props        map[string]any
		name         string
		logicalID    string
		resourceType string
		physID       string
		wantContains string
		wantNotEq    string
		isDelete     bool
	}{
		{
			name:         "create_s3",
			logicalID:    "MyBucket",
			resourceType: "AWS::S3::Bucket",
			wantContains: "MyBucket",
		},
		{
			name:         "delete_s3",
			resourceType: "AWS::S3::Bucket",
			physID:       "my-bucket",
			isDelete:     true,
		},
		{
			name:         "create_default_resource",
			logicalID:    "MyRole",
			resourceType: "AWS::IAM::Role",
			wantContains: "MyRole-",
			wantNotEq:    "MyRole-stub",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rc := cloudformation.NewResourceCreator(nil)

			if tt.isDelete {
				err := rc.Delete(t.Context(), tt.resourceType, tt.physID, tt.props)
				require.NoError(t, err)

				return
			}

			physID, err := rc.Create(t.Context(), tt.logicalID, tt.resourceType, tt.props, nil, nil)
			require.NoError(t, err)

			if tt.wantContains != "" {
				assert.Contains(t, physID, tt.wantContains)
			}

			if tt.wantNotEq != "" {
				assert.NotEqual(t, tt.wantNotEq, physID)
			}
		})
	}
}

func TestResourceCreator_UnknownType(t *testing.T) {
	t.Parallel()

	tests := []struct {
		props        map[string]any
		name         string
		logicalID    string
		resourceType string
		physID       string
		wantPhysID   string
		isDelete     bool
	}{
		{
			name:         "create_returns_stub",
			logicalID:    "MyRole",
			resourceType: "AWS::IAM::Role",
			props:        map[string]any{},
			wantPhysID:   "MyRole-stub",
		},
		{
			name:         "delete_no_error",
			resourceType: "AWS::IAM::Role",
			physID:       "some-role",
			props:        map[string]any{},
			isDelete:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backends := newServiceBackends()
			rc := cloudformation.NewResourceCreator(backends)

			if tt.isDelete {
				err := rc.Delete(t.Context(), tt.resourceType, tt.physID, tt.props)
				require.NoError(t, err)

				return
			}

			physID, err := rc.Create(t.Context(), tt.logicalID, tt.resourceType, tt.props, nil, nil)
			require.NoError(t, err)
			assert.Equal(t, tt.wantPhysID, physID)
		})
	}
}

func TestBackend_CreateStack_RealResources(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		stackName  string
		template   string
		wantStatus string
	}{
		{
			name:      "s3_bucket",
			stackName: "real-stack",
			template: `{
"AWSTemplateFormatVersion": "2010-09-09",
"Resources": {
"MyBucket": {
"Type": "AWS::S3::Bucket",
"Properties": {"BucketName": "real-cfn-bucket"}
}
}
}`,
			wantStatus: "CREATE_COMPLETE",
		},
		{
			name:      "ssm_and_sns",
			stackName: "ssm-sns-stack",
			template: `{
"AWSTemplateFormatVersion": "2010-09-09",
"Resources": {
"MyParam": {
"Type": "AWS::SSM::Parameter",
"Properties": {
"Name": "/test/cfn/param",
"Type": "String",
"Value": "test-value"
}
},
"MyTopic": {
"Type": "AWS::SNS::Topic",
"Properties": {"TopicName": "real-cfn-topic"}
}
}
}`,
			wantStatus: "CREATE_COMPLETE",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backends := newServiceBackends()
			creator := cloudformation.NewResourceCreator(backends)
			backend := cloudformation.NewInMemoryBackendWithConfig(
				"000000000000",
				"us-east-1",
				creator,
			)

			stack, err := backend.CreateStack(
				t.Context(),
				tt.stackName,
				tt.template,
				nil,
				cloudformation.StackOptions{},
			)
			require.NoError(t, err)
			assert.Equal(t, tt.wantStatus, stack.StackStatus)

			if tt.stackName == "real-stack" {
				err = backend.DeleteStack(t.Context(), tt.stackName)
				require.NoError(t, err)
			}
		})
	}
}

func TestBackend_UpdateStack_WithNewResource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		stackName  string
		tmpl1      string
		tmpl2      string
		wantStatus string
	}{
		{
			name:      "add_sqs_queue",
			stackName: "upd-real-stack",
			tmpl1: `{
"AWSTemplateFormatVersion": "2010-09-09",
"Resources": {
"MyBucket": {
"Type": "AWS::S3::Bucket",
"Properties": {"BucketName": "upd-cfn-bucket"}
}
}
}`,
			tmpl2: `{
"AWSTemplateFormatVersion": "2010-09-09",
"Resources": {
"MyBucket": {
"Type": "AWS::S3::Bucket",
"Properties": {"BucketName": "upd-cfn-bucket"}
},
"MyQueue": {
"Type": "AWS::SQS::Queue",
"Properties": {"QueueName": "upd-cfn-queue"}
}
}
}`,
			wantStatus: "UPDATE_COMPLETE",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backends := newServiceBackends()
			creator := cloudformation.NewResourceCreator(backends)
			backend := cloudformation.NewInMemoryBackendWithConfig(
				"000000000000",
				"us-east-1",
				creator,
			)

			_, err := backend.CreateStack(
				t.Context(),
				tt.stackName,
				tt.tmpl1,
				nil,
				cloudformation.StackOptions{},
			)
			require.NoError(t, err)

			updated, err := backend.UpdateStack(
				t.Context(),
				tt.stackName,
				tt.tmpl2,
				nil,
				cloudformation.StackOptions{},
			)
			require.NoError(t, err)
			assert.Equal(t, tt.wantStatus, updated.StackStatus)
		})
	}
}

func TestResourceCreator_ExtendedTypes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		props        map[string]any
		name         string
		logicalID    string
		resourceType string
		physID       string
		wantPhysID   string
		wantContains string
		isDelete     bool
		wantNotEmpty bool
	}{
		{
			name:         "event_bridge_rule",
			logicalID:    "MyRule",
			resourceType: "AWS::Events::Rule",
			props: map[string]any{
				"Name":         "my-cfn-rule",
				"EventPattern": `{"source":["aws.s3"]}`,
				"State":        "ENABLED",
			},
			wantContains: "my-cfn-rule",
		},
		{
			name:         "step_functions_state_machine",
			logicalID:    "MyStateMachine",
			resourceType: "AWS::StepFunctions::StateMachine",
			props: map[string]any{
				"StateMachineName": "cfn-sm",
				"DefinitionString": `{"Comment":"test","StartAt":"Done","States":{"Done":{"Type":"Succeed"}}}`,
				"RoleArn":          "arn:aws:iam::000000000000:role/sfn-role",
			},
			wantContains: "cfn-sm",
		},
		{
			name:         "cloudwatch_log_group",
			logicalID:    "MyLogGroup",
			resourceType: "AWS::Logs::LogGroup",
			props:        map[string]any{"LogGroupName": "/cfn/my-log-group"},
			wantPhysID:   "/cfn/my-log-group",
		},
		{
			name:         "api_gateway_rest_api",
			logicalID:    "MyAPI",
			resourceType: "AWS::ApiGateway::RestApi",
			props:        map[string]any{"Name": "cfn-rest-api", "Description": "created by cfn"},
			wantNotEmpty: true,
		},
		{
			name:         "default_stub",
			logicalID:    "MyWhatever",
			resourceType: "AWS::Whatever::Thing",
			props:        nil,
			wantPhysID:   "MyWhatever-stub",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backends := newExtendedServiceBackends()
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

			// delete test (skip for default_stub since it uses a generic type)
			if tt.resourceType != "AWS::Whatever::Thing" {
				err = rc.Delete(t.Context(), tt.resourceType, physID, tt.props)
				require.NoError(t, err)
			} else {
				err = rc.Delete(t.Context(), tt.resourceType, "whatever-id", nil)
				require.NoError(t, err)
			}
		})
	}
}

func TestResourceCreator_CloudWatchAlarm(t *testing.T) {
	t.Parallel()

	tests := []struct {
		props      map[string]any
		name       string
		logicalID  string
		wantPhysID string
	}{
		{
			name:      "basic_alarm",
			logicalID: "MyAlarm",
			props: map[string]any{
				"AlarmName":          "cfn-test-alarm",
				"Namespace":          "AWS/Lambda",
				"MetricName":         "Errors",
				"ComparisonOperator": "GreaterThanThreshold",
				"Statistic":          "Sum",
				"Threshold":          float64(10),
				"EvaluationPeriods":  float64(1),
				"Period":             float64(60),
			},
			wantPhysID: "cfn-test-alarm",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backends := newExtendedServiceBackends()
			rc := cloudformation.NewResourceCreator(backends)

			physID, err := rc.Create(
				t.Context(),
				tt.logicalID,
				"AWS::CloudWatch::Alarm",
				tt.props,
				nil,
				nil,
			)
			require.NoError(t, err)
			assert.Equal(t, tt.wantPhysID, physID)

			err = rc.Delete(t.Context(), "AWS::CloudWatch::Alarm", physID, nil)
			require.NoError(t, err)
		})
	}
}

func TestResourceCreator_Route53HostedZone(t *testing.T) {
	t.Parallel()

	tests := []struct {
		props     map[string]any
		name      string
		logicalID string
	}{
		{
			name:      "basic_hosted_zone",
			logicalID: "MyZone",
			props:     map[string]any{"Name": "example.com"},
		},
		{
			name:      "with_comment",
			logicalID: "MyZoneWithComment",
			props: map[string]any{
				"Name": "test.example.com",
				"HostedZoneConfig": map[string]any{
					"Comment": "Test zone",
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backends := newExtendedServiceBackends()
			rc := cloudformation.NewResourceCreator(backends)

			physID, err := rc.Create(
				t.Context(),
				tt.logicalID,
				"AWS::Route53::HostedZone",
				tt.props,
				nil,
				nil,
			)
			require.NoError(t, err)
			assert.NotEmpty(t, physID)

			err = rc.Delete(t.Context(), "AWS::Route53::HostedZone", physID, nil)
			require.NoError(t, err)
		})
	}
}

func TestResourceCreator_Route53RecordSet(t *testing.T) {
	t.Parallel()

	backends := newExtendedServiceBackends()
	rc := cloudformation.NewResourceCreator(backends)

	// Create hosted zone first.
	zoneID, err := rc.Create(t.Context(), "MyZone", "AWS::Route53::HostedZone",
		map[string]any{"Name": "example.com"}, nil, nil)
	require.NoError(t, err)

	// Create record set.
	recordPhysID, err := rc.Create(t.Context(), "MyRecord", "AWS::Route53::RecordSet",
		map[string]any{
			"HostedZoneId":    zoneID,
			"Name":            "api.example.com",
			"Type":            "A",
			"TTL":             float64(300),
			"ResourceRecords": []any{"1.2.3.4"},
		}, nil, nil)
	require.NoError(t, err)
	assert.NotEmpty(t, recordPhysID)

	err = rc.Delete(t.Context(), "AWS::Route53::RecordSet", recordPhysID, nil)
	require.NoError(t, err)
}

func TestResourceCreator_ElastiCacheCacheCluster(t *testing.T) {
	t.Parallel()

	tests := []struct {
		props      map[string]any
		name       string
		logicalID  string
		wantPhysID string
	}{
		{
			name:      "redis_cluster",
			logicalID: "MyCluster",
			props: map[string]any{
				"CacheClusterId": "cfn-redis-cluster",
				"Engine":         "redis",
				"CacheNodeType":  "cache.t3.micro",
			},
			wantPhysID: "cfn-redis-cluster",
		},
		{
			name:       "default_engine",
			logicalID:  "MyCluster2",
			props:      map[string]any{"CacheClusterId": "cfn-default-cluster"},
			wantPhysID: "cfn-default-cluster",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backends := newExtendedServiceBackends()
			rc := cloudformation.NewResourceCreator(backends)

			physID, err := rc.Create(
				t.Context(),
				tt.logicalID,
				"AWS::ElastiCache::CacheCluster",
				tt.props,
				nil,
				nil,
			)
			require.NoError(t, err)
			assert.Equal(t, tt.wantPhysID, physID)

			err = rc.Delete(t.Context(), "AWS::ElastiCache::CacheCluster", physID, nil)
			require.NoError(t, err)
		})
	}
}

func TestResourceCreator_SchedulerSchedule(t *testing.T) {
	t.Parallel()

	tests := []struct {
		props        map[string]any
		name         string
		logicalID    string
		wantContains string
	}{
		{
			name:      "basic_schedule",
			logicalID: "MySchedule",
			props: map[string]any{
				"Name":               "cfn-schedule",
				"ScheduleExpression": "rate(5 minutes)",
				"State":              "ENABLED",
				"Target": map[string]any{
					"Arn":     "arn:aws:lambda:us-east-1:000000000000:function:my-fn",
					"RoleArn": "arn:aws:iam::000000000000:role/my-role",
				},
			},
			wantContains: "cfn-schedule",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backends := newExtendedServiceBackends()
			rc := cloudformation.NewResourceCreator(backends)

			physID, err := rc.Create(
				t.Context(),
				tt.logicalID,
				"AWS::Scheduler::Schedule",
				tt.props,
				nil,
				nil,
			)
			require.NoError(t, err)
			assert.Contains(t, physID, tt.wantContains)

			err = rc.Delete(t.Context(), "AWS::Scheduler::Schedule", physID, nil)
			require.NoError(t, err)
		})
	}
}

func TestResourceCreator_NewTypes_NilBackends(t *testing.T) {
	t.Parallel()

	tests := []struct {
		props        map[string]any
		name         string
		logicalID    string
		resourceType string
	}{
		{
			name:         "iam_role_nil",
			logicalID:    "MyRole",
			resourceType: "AWS::IAM::Role",
			props:        map[string]any{},
		},
		{
			name:         "iam_policy_nil",
			logicalID:    "MyPolicy",
			resourceType: "AWS::IAM::Policy",
			props:        map[string]any{},
		},
		{
			name:         "iam_managed_policy_nil",
			logicalID:    "MyManagedPolicy",
			resourceType: "AWS::IAM::ManagedPolicy",
			props:        map[string]any{},
		},
		{
			name:         "iam_instance_profile_nil",
			logicalID:    "MyInstanceProfile",
			resourceType: "AWS::IAM::InstanceProfile",
			props:        map[string]any{},
		},
		{
			name:         "ec2_vpc_nil",
			logicalID:    "MyVPC",
			resourceType: "AWS::EC2::VPC",
			props:        map[string]any{"CidrBlock": "10.0.0.0/16"},
		},
		{
			name:         "ec2_subnet_nil",
			logicalID:    "MySubnet",
			resourceType: "AWS::EC2::Subnet",
			props:        map[string]any{"VpcId": "vpc-abc", "CidrBlock": "10.0.1.0/24"},
		},
		{
			name:         "ec2_security_group_nil",
			logicalID:    "MySG",
			resourceType: "AWS::EC2::SecurityGroup",
			props:        map[string]any{"GroupDescription": "test"},
		},
		{
			name:         "ec2_igw_nil",
			logicalID:    "MyIGW",
			resourceType: "AWS::EC2::InternetGateway",
			props:        map[string]any{},
		},
		{
			name:         "ec2_route_table_nil",
			logicalID:    "MyRT",
			resourceType: "AWS::EC2::RouteTable",
			props:        map[string]any{"VpcId": "vpc-abc"},
		},
		{
			name:         "ec2_route_nil",
			logicalID:    "MyRoute",
			resourceType: "AWS::EC2::Route",
			props: map[string]any{
				"RouteTableId":         "rtb-abc",
				"DestinationCidrBlock": "0.0.0.0/0",
			},
		},
		{
			name:         "kinesis_stream_nil",
			logicalID:    "MyStream",
			resourceType: "AWS::Kinesis::Stream",
			props:        map[string]any{"Name": "my-stream"},
		},
		{
			name:         "cloudwatch_alarm_nil",
			logicalID:    "MyAlarm",
			resourceType: "AWS::CloudWatch::Alarm",
			props:        map[string]any{},
		},
		{
			name:         "route53_hosted_zone_nil",
			logicalID:    "MyZone",
			resourceType: "AWS::Route53::HostedZone",
			props:        map[string]any{"Name": "example.com"},
		},
		{
			name:         "route53_record_set_nil",
			logicalID:    "MyRecord",
			resourceType: "AWS::Route53::RecordSet",
			props: map[string]any{
				"HostedZoneId": "Z123",
				"Name":         "api.example.com",
				"Type":         "A",
			},
		},
		{
			name:         "elasticache_cluster_nil",
			logicalID:    "MyCluster",
			resourceType: "AWS::ElastiCache::CacheCluster",
			props:        map[string]any{"Engine": "redis"},
		},
		{
			name:         "events_eventbus_nil",
			logicalID:    "MyEventBus",
			resourceType: "AWS::Events::EventBus",
			props:        map[string]any{"Name": "my-bus"},
		},
		{
			name:         "scheduler_schedule_nil",
			logicalID:    "MySchedule",
			resourceType: "AWS::Scheduler::Schedule",
			props: map[string]any{
				"Name":               "my-schedule",
				"ScheduleExpression": "rate(5 minutes)",
			},
		},
		{
			name:         "lambda_esm_nil",
			logicalID:    "MyESM",
			resourceType: "AWS::Lambda::EventSourceMapping",
			props: map[string]any{
				"FunctionName":   "my-fn",
				"EventSourceArn": "arn:aws:kinesis:us-east-1:000:stream/my-stream",
			},
		},
		{
			name:         "lambda_permission_nil",
			logicalID:    "MyPermission",
			resourceType: "AWS::Lambda::Permission",
			props:        map[string]any{"FunctionName": "my-fn"},
		},
		{
			name:         "lambda_alias_nil",
			logicalID:    "MyAlias",
			resourceType: "AWS::Lambda::Alias",
			props: map[string]any{
				"FunctionName":    "my-fn",
				"Name":            "prod",
				"FunctionVersion": "$LATEST",
			},
		},
		{
			name:         "lambda_version_nil",
			logicalID:    "MyVersion",
			resourceType: "AWS::Lambda::Version",
			props:        map[string]any{"FunctionName": "my-fn"},
		},
		{
			name:         "apigw_resource_nil",
			logicalID:    "MyResource",
			resourceType: "AWS::ApiGateway::Resource",
			props: map[string]any{
				"RestApiId": "abc123",
				"ParentId":  "root",
				"PathPart":  "items",
			},
		},
		{
			name:         "apigw_method_nil",
			logicalID:    "MyMethod",
			resourceType: "AWS::ApiGateway::Method",
			props: map[string]any{
				"RestApiId":  "abc123",
				"ResourceId": "res1",
				"HttpMethod": "GET",
			},
		},
		{
			name:         "apigw_deployment_nil",
			logicalID:    "MyDeployment",
			resourceType: "AWS::ApiGateway::Deployment",
			props:        map[string]any{"RestApiId": "abc123"},
		},
		{
			name:         "apigw_stage_nil",
			logicalID:    "MyStage",
			resourceType: "AWS::ApiGateway::Stage",
			props:        map[string]any{"RestApiId": "abc123", "StageName": "prod"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// Use base backends (no IAM/EC2/Kinesis/etc.)
			backends := newServiceBackends()
			rc := cloudformation.NewResourceCreator(backends)

			physID, err := rc.Create(t.Context(), tt.logicalID, tt.resourceType, tt.props, nil, nil)
			require.NoError(t, err)
			assert.Contains(t, physID, tt.logicalID, "stub physID should contain logicalID")

			err = rc.Delete(t.Context(), tt.resourceType, physID, nil)
			require.NoError(t, err)
		})
	}
}

package integration_test

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	cloudformationsdk "github.com/aws/aws-sdk-go-v2/service/cloudformation"
	cftypes "github.com/aws/aws-sdk-go-v2/service/cloudformation/types"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// cfnStackID returns an 8-char UUID suffix for unique resource naming.
func cfnStackID() string { return uuid.NewString()[:8] }

// deployStackPhase2 creates a CloudFormation stack and waits for it to reach
// a terminal state.
func deployStackPhase2(
	t *testing.T,
	client *cloudformationsdk.Client,
	stackName, templateBody string,
) string {
	t.Helper()

	ctx := t.Context()

	_, err := client.CreateStack(ctx, &cloudformationsdk.CreateStackInput{
		StackName:    aws.String(stackName),
		TemplateBody: aws.String(templateBody),
	})
	require.NoError(t, err)

	return waitForStackStatus(t, client, stackName, 10*time.Second)
}

// deleteStackPhase2 deletes a stack and waits for DELETE_COMPLETE.
func deleteStackPhase2(t *testing.T, client *cloudformationsdk.Client, stackName string) {
	t.Helper()

	ctx := t.Context()

	_, err := client.DeleteStack(ctx, &cloudformationsdk.DeleteStackInput{
		StackName: aws.String(stackName),
	})
	require.NoError(t, err)

	waitForStackStatus(t, client, stackName, 10*time.Second)
}

func TestIntegration_CFN_Phase2_RDS(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	tests := []struct {
		template func() string
		name     string
	}{
		{
			name: "DBInstance",
			template: func() string {
				return `{
"AWSTemplateFormatVersion": "2010-09-09",
"Resources": {
"MyDB": {
"Type": "AWS::RDS::DBInstance",
"Properties": {
"DBInstanceIdentifier": "cfn-db-` + cfnStackID() + `",
"Engine": "postgres",
"DBInstanceClass": "db.t3.micro",
"AllocatedStorage": "20",
"MasterUsername": "admin"
}
}
}
}`
			},
		},
		{
			name: "DBSubnetGroup",
			template: func() string {
				return `{
"AWSTemplateFormatVersion": "2010-09-09",
"Resources": {
"MySubnetGrp": {
"Type": "AWS::RDS::DBSubnetGroup",
"Properties": {
"DBSubnetGroupName": "cfn-sg-` + cfnStackID() + `",
"DBSubnetGroupDescription": "test subnet group",
"SubnetIds": ["subnet-default"]
}
}
}
}`
			},
		},
		{
			name: "DBParameterGroup",
			template: func() string {
				return `{
"AWSTemplateFormatVersion": "2010-09-09",
"Resources": {
"MyPG": {
"Type": "AWS::RDS::DBParameterGroup",
"Properties": {
"DBParameterGroupName": "cfn-pg-` + cfnStackID() + `",
"Family": "postgres14",
"Description": "test parameter group"
}
}
}
}`
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := createCloudFormationClient(t)
			stackName := "test-rds-" + cfnStackID()

			status := deployStackPhase2(t, client, stackName, tt.template())
			assert.Equal(t, string(cftypes.StackStatusCreateComplete), status)

			deleteStackPhase2(t, client, stackName)
		})
	}
}

func TestIntegration_CFN_Phase2_ElastiCache(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	tests := []struct {
		template func() string
		name     string
	}{
		{
			name: "ReplicationGroup",
			template: func() string {
				return `{
"AWSTemplateFormatVersion": "2010-09-09",
"Resources": {
"MyRG": {
"Type": "AWS::ElastiCache::ReplicationGroup",
"Properties": {
"ReplicationGroupId": "cfn-rg-` + cfnStackID() + `",
"ReplicationGroupDescription": "test replication group"
}
}
}
}`
			},
		},
		{
			name: "SubnetGroup",
			template: func() string {
				return `{
"AWSTemplateFormatVersion": "2010-09-09",
"Resources": {
"MySG": {
"Type": "AWS::ElastiCache::SubnetGroup",
"Properties": {
"CacheSubnetGroupName": "cfn-ecsg-` + cfnStackID() + `",
"CacheSubnetGroupDescription": "test subnet group",
"SubnetIds": ["subnet-default"]
}
}
}
}`
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := createCloudFormationClient(t)
			stackName := "test-ec-" + cfnStackID()

			status := deployStackPhase2(t, client, stackName, tt.template())
			assert.Equal(t, string(cftypes.StackStatusCreateComplete), status)

			deleteStackPhase2(t, client, stackName)
		})
	}
}

func TestIntegration_CFN_Phase2_ECS(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	tests := []struct {
		template func() string
		name     string
	}{
		{
			name: "Cluster",
			template: func() string {
				return `{
"AWSTemplateFormatVersion": "2010-09-09",
"Resources": {
"MyCluster": {
"Type": "AWS::ECS::Cluster",
"Properties": {
"ClusterName": "cfn-cluster-` + cfnStackID() + `"
}
}
}
}`
			},
		},
		{
			name: "TaskDefinition",
			template: func() string {
				return `{
"AWSTemplateFormatVersion": "2010-09-09",
"Resources": {
"MyTD": {
"Type": "AWS::ECS::TaskDefinition",
"Properties": {
"Family": "cfn-td-` + cfnStackID() + `",
"NetworkMode": "awsvpc",
"ContainerDefinitions": [
{
"Name": "app",
"Image": "nginx:latest",
"Cpu": 256,
"Memory": 512
}
]
}
}
}
}`
			},
		},
		{
			name: "Service",
			template: func() string {
				clusterName := "cfn-cluster-" + cfnStackID()
				family := "cfn-ecs-family-" + cfnStackID()
				serviceName := "cfn-ecs-service-" + cfnStackID()

				return `{
"AWSTemplateFormatVersion": "2010-09-09",
"Resources": {
"MyCluster": {
"Type": "AWS::ECS::Cluster",
"Properties": {
"ClusterName": "` + clusterName + `"
}
},
"MyTD": {
"Type": "AWS::ECS::TaskDefinition",
"Properties": {
"Family": "` + family + `",
"NetworkMode": "bridge",
"ContainerDefinitions": [
{
"Name": "app",
"Image": "nginx:latest",
"Cpu": 256,
"Memory": 512
}
]
}
},
"MySvc": {
"Type": "AWS::ECS::Service",
"DependsOn": ["MyCluster", "MyTD"],
"Properties": {
"ServiceName": "` + serviceName + `",
"Cluster": {"Ref": "MyCluster"},
"TaskDefinition": {"Ref": "MyTD"},
"DesiredCount": 0,
"LaunchType": "EC2"
}
}
}
}`
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := createCloudFormationClient(t)
			stackName := "test-ecs-" + cfnStackID()

			status := deployStackPhase2(t, client, stackName, tt.template())
			assert.Equal(t, string(cftypes.StackStatusCreateComplete), status)

			deleteStackPhase2(t, client, stackName)
		})
	}
}

func TestIntegration_CFN_Phase2_ECR(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	client := createCloudFormationClient(t)
	stackName := "test-ecr-" + cfnStackID()
	template := `{
"AWSTemplateFormatVersion": "2010-09-09",
"Resources": {
"MyRepo": {
"Type": "AWS::ECR::Repository",
"Properties": {
"RepositoryName": "cfn-repo-` + cfnStackID() + `"
}
}
}
}`

	status := deployStackPhase2(t, client, stackName, template)
	assert.Equal(t, string(cftypes.StackStatusCreateComplete), status)

	deleteStackPhase2(t, client, stackName)
}

func TestIntegration_CFN_Phase2_Redshift(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	client := createCloudFormationClient(t)
	stackName := "test-redshift-" + cfnStackID()
	template := `{
"AWSTemplateFormatVersion": "2010-09-09",
"Resources": {
"MyCluster": {
"Type": "AWS::Redshift::Cluster",
"Properties": {
"ClusterIdentifier": "cfn-rs-` + cfnStackID() + `",
"NodeType": "dc2.large",
"DBName": "mydb",
"MasterUsername": "admin"
}
}
}
}`

	status := deployStackPhase2(t, client, stackName, template)
	assert.Equal(t, string(cftypes.StackStatusCreateComplete), status)

	deleteStackPhase2(t, client, stackName)
}

func TestIntegration_CFN_Phase2_OpenSearch(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	client := createCloudFormationClient(t)
	stackName := "test-opensearch-" + cfnStackID()
	template := `{
"AWSTemplateFormatVersion": "2010-09-09",
"Resources": {
"MyDomain": {
"Type": "AWS::OpenSearch::Domain",
"Properties": {
"DomainName": "cfn-os-` + cfnStackID() + `",
"EngineVersion": "OpenSearch_2.11"
}
}
}
}`

	status := deployStackPhase2(t, client, stackName, template)
	assert.Equal(t, string(cftypes.StackStatusCreateComplete), status)

	deleteStackPhase2(t, client, stackName)
}

func TestIntegration_CFN_Phase2_Firehose(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	client := createCloudFormationClient(t)
	stackName := "test-firehose-" + cfnStackID()
	template := `{
"AWSTemplateFormatVersion": "2010-09-09",
"Resources": {
"MyStream": {
"Type": "AWS::Firehose::DeliveryStream",
"Properties": {
"DeliveryStreamName": "cfn-fh-` + cfnStackID() + `"
}
}
}
}`

	status := deployStackPhase2(t, client, stackName, template)
	assert.Equal(t, string(cftypes.StackStatusCreateComplete), status)

	deleteStackPhase2(t, client, stackName)
}

func TestIntegration_CFN_Phase2_Route53HealthCheck(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	client := createCloudFormationClient(t)
	stackName := "test-r53hc-" + cfnStackID()
	template := `{
"AWSTemplateFormatVersion": "2010-09-09",
"Resources": {
"MyHC": {
"Type": "AWS::Route53::HealthCheck",
"Properties": {
"HealthCheckConfig": {
"Type": "HTTPS",
"FullyQualifiedDomainName": "example.com",
"Port": "443"
}
}
}
}
}`

	status := deployStackPhase2(t, client, stackName, template)
	assert.Equal(t, string(cftypes.StackStatusCreateComplete), status)

	deleteStackPhase2(t, client, stackName)
}

func TestIntegration_CFN_Phase2_Route53Resolver(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	tests := []struct {
		template func() string
		name     string
	}{
		{
			name: "ResolverEndpoint",
			template: func() string {
				return `{
"AWSTemplateFormatVersion": "2010-09-09",
"Resources": {
"MyEP": {
"Type": "AWS::Route53Resolver::ResolverEndpoint",
"Properties": {
"Name": "cfn-ep-` + cfnStackID() + `",
"Direction": "INBOUND"
}
}
}
}`
			},
		},
		{
			name: "ResolverRule",
			template: func() string {
				return `{
"AWSTemplateFormatVersion": "2010-09-09",
"Resources": {
"MyRule": {
"Type": "AWS::Route53Resolver::ResolverRule",
"Properties": {
"Name": "cfn-rule-` + cfnStackID() + `",
"DomainName": "example.internal",
"RuleType": "FORWARD"
}
}
}
}`
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := createCloudFormationClient(t)
			stackName := "test-r53r-" + cfnStackID()

			status := deployStackPhase2(t, client, stackName, tt.template())
			assert.Equal(t, string(cftypes.StackStatusCreateComplete), status)

			deleteStackPhase2(t, client, stackName)
		})
	}
}

func TestIntegration_CFN_Phase2_SWF(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	client := createCloudFormationClient(t)
	stackName := "test-swf-" + cfnStackID()
	template := `{
"AWSTemplateFormatVersion": "2010-09-09",
"Resources": {
"MyDomain": {
"Type": "AWS::SWF::Domain",
"Properties": {
"Name": "cfn-domain-` + cfnStackID() + `",
"Description": "test SWF domain"
}
}
}
}`

	status := deployStackPhase2(t, client, stackName, template)
	assert.Equal(t, string(cftypes.StackStatusCreateComplete), status)

	deleteStackPhase2(t, client, stackName)
}

func TestIntegration_CFN_Phase2_AppSync(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	client := createCloudFormationClient(t)
	stackName := "test-appsync-" + cfnStackID()
	template := `{
"AWSTemplateFormatVersion": "2010-09-09",
"Resources": {
"MyAPI": {
"Type": "AWS::AppSync::GraphQLApi",
"Properties": {
"Name": "cfn-api-` + cfnStackID() + `",
"AuthenticationType": "API_KEY"
}
}
}
}`

	status := deployStackPhase2(t, client, stackName, template)
	assert.Equal(t, string(cftypes.StackStatusCreateComplete), status)

	deleteStackPhase2(t, client, stackName)
}

func TestIntegration_CFN_Phase2_SES(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	client := createCloudFormationClient(t)
	stackName := "test-ses-" + cfnStackID()
	template := `{
"AWSTemplateFormatVersion": "2010-09-09",
"Resources": {
"MyIdentity": {
"Type": "AWS::SES::EmailIdentity",
"Properties": {
"EmailIdentity": "cfn-test-` + cfnStackID() + `@example.com"
}
}
}
}`

	status := deployStackPhase2(t, client, stackName, template)
	assert.Equal(t, string(cftypes.StackStatusCreateComplete), status)

	deleteStackPhase2(t, client, stackName)
}

func TestIntegration_CFN_Phase2_ACM(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	client := createCloudFormationClient(t)
	stackName := "test-acm-" + cfnStackID()
	template := `{
"AWSTemplateFormatVersion": "2010-09-09",
"Resources": {
"MyCert": {
"Type": "AWS::ACM::Certificate",
"Properties": {
"DomainName": "cfn-` + cfnStackID() + `.example.com"
}
}
}
}`

	status := deployStackPhase2(t, client, stackName, template)
	assert.Equal(t, string(cftypes.StackStatusCreateComplete), status)

	deleteStackPhase2(t, client, stackName)
}

func TestIntegration_CFN_Phase2_Cognito(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	client := createCloudFormationClient(t)
	stackName := "test-cognito-" + cfnStackID()
	template := `{
"AWSTemplateFormatVersion": "2010-09-09",
"Resources": {
"MyPool": {
"Type": "AWS::Cognito::UserPool",
"Properties": {
"PoolName": "cfn-pool-` + cfnStackID() + `"
}
}
}
}`

	status := deployStackPhase2(t, client, stackName, template)
	assert.Equal(t, string(cftypes.StackStatusCreateComplete), status)

	deleteStackPhase2(t, client, stackName)
}

func TestIntegration_CFN_Phase2_EC2Extended(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	tests := []struct {
		template func() string
		name     string
	}{
		{
			name: "EIP",
			template: func() string {
				return `{
"AWSTemplateFormatVersion": "2010-09-09",
"Resources": {
"MyEIP": {
"Type": "AWS::EC2::EIP",
"Properties": {}
}
}
}`
			},
		},
		{
			name: "NatGateway",
			template: func() string {
				return `{
"AWSTemplateFormatVersion": "2010-09-09",
"Resources": {
"MyEIP": {
"Type": "AWS::EC2::EIP",
"Properties": {}
},
"MyNGW": {
"Type": "AWS::EC2::NatGateway",
"DependsOn": ["MyEIP"],
"Properties": {
"SubnetId": "subnet-default",
"AllocationId": {"Ref": "MyEIP"}
}
}
}
}`
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := createCloudFormationClient(t)
			stackName := "test-ec2ext-" + cfnStackID()

			status := deployStackPhase2(t, client, stackName, tt.template())
			assert.Equal(t, string(cftypes.StackStatusCreateComplete), status)

			deleteStackPhase2(t, client, stackName)
		})
	}
}

func TestIntegration_CFN_Phase2_CloudWatchCompositeAlarm(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	client := createCloudFormationClient(t)
	id := cfnStackID()
	stackName := "test-cw-composite-" + id
	alarmName := "cfn-base-alarm-" + id
	compositeAlarmName := "cfn-composite-" + id
	template := `{
"AWSTemplateFormatVersion": "2010-09-09",
"Resources": {
"BaseAlarm": {
"Type": "AWS::CloudWatch::Alarm",
"Properties": {
"AlarmName": "` + alarmName + `",
"Namespace": "AWS/EC2",
"MetricName": "CPUUtilization",
"ComparisonOperator": "GreaterThanThreshold",
"Threshold": 90,
"EvaluationPeriods": 1,
"Period": 60,
"Statistic": "Average"
}
},
"CompositeAlarm": {
"Type": "AWS::CloudWatch::CompositeAlarm",
"DependsOn": ["BaseAlarm"],
"Properties": {
"AlarmName": "` + compositeAlarmName + `",
"AlarmRule": "ALARM(` + alarmName + `)"
}
}
}
}`

	status := deployStackPhase2(t, client, stackName, template)
	assert.Equal(t, string(cftypes.StackStatusCreateComplete), status)

	deleteStackPhase2(t, client, stackName)
}

func TestIntegration_CFN_Phase2_CognitoUserPoolClient(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	client := createCloudFormationClient(t)
	id := cfnStackID()
	stackName := "test-cognito-client-" + id
	poolName := "cfn-pool-" + id
	clientName := "cfn-client-" + id
	template := `{
"AWSTemplateFormatVersion": "2010-09-09",
"Resources": {
"MyPool": {
"Type": "AWS::Cognito::UserPool",
"Properties": {
"PoolName": "` + poolName + `"
}
},
"MyClient": {
"Type": "AWS::Cognito::UserPoolClient",
"DependsOn": ["MyPool"],
"Properties": {
"ClientName": "` + clientName + `",
"UserPoolId": {"Ref": "MyPool"}
}
}
}
}`

	status := deployStackPhase2(t, client, stackName, template)
	assert.Equal(t, string(cftypes.StackStatusCreateComplete), status)

	deleteStackPhase2(t, client, stackName)
}

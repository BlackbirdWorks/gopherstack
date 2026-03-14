package cloudformation_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	apigatewayv2backend "github.com/blackbirdworks/gopherstack/services/apigatewayv2"
	autoscalingbackend "github.com/blackbirdworks/gopherstack/services/autoscaling"
	batchbackend "github.com/blackbirdworks/gopherstack/services/batch"
	"github.com/blackbirdworks/gopherstack/services/cloudformation"
	cloudfrontbackend "github.com/blackbirdworks/gopherstack/services/cloudfront"
	cloudtrailbackend "github.com/blackbirdworks/gopherstack/services/cloudtrail"
	codebuildbackend "github.com/blackbirdworks/gopherstack/services/codebuild"
	codepipelinebackend "github.com/blackbirdworks/gopherstack/services/codepipeline"
	docdbbackend "github.com/blackbirdworks/gopherstack/services/docdb"
	efsbackend "github.com/blackbirdworks/gopherstack/services/efs"
	eksbackend "github.com/blackbirdworks/gopherstack/services/eks"
	emrbackend "github.com/blackbirdworks/gopherstack/services/emr"
	gluebackend "github.com/blackbirdworks/gopherstack/services/glue"
	iotbackend "github.com/blackbirdworks/gopherstack/services/iot"
	kafkabackend "github.com/blackbirdworks/gopherstack/services/kafka"
	neptunebackend "github.com/blackbirdworks/gopherstack/services/neptune"
	pipesbackend "github.com/blackbirdworks/gopherstack/services/pipes"
	transferbackend "github.com/blackbirdworks/gopherstack/services/transfer"
)

// newPhase3ServiceBackends creates a ServiceBackends with all phase-3 backends populated.
func newPhase3ServiceBackends() *cloudformation.ServiceBackends {
	b := newPhase2ServiceBackends()
	b.EKS = eksbackend.NewHandler(eksbackend.NewInMemoryBackend("000000000000", "us-east-1"))
	b.EFS = efsbackend.NewHandler(efsbackend.NewInMemoryBackend("000000000000", "us-east-1"))
	b.Batch = batchbackend.NewHandler(batchbackend.NewInMemoryBackend("000000000000", "us-east-1"))
	b.CloudFront = cloudfrontbackend.NewHandler(
		cloudfrontbackend.NewInMemoryBackend("000000000000", "us-east-1"))
	b.Autoscaling = autoscalingbackend.NewHandler(autoscalingbackend.NewInMemoryBackend())
	b.APIGatewayV2 = apigatewayv2backend.NewHandler(apigatewayv2backend.NewInMemoryBackend())
	b.CodeBuild = codebuildbackend.NewHandler(
		codebuildbackend.NewInMemoryBackend("000000000000", "us-east-1"))
	b.Glue = gluebackend.NewHandler(gluebackend.NewInMemoryBackend("000000000000", "us-east-1"))
	b.DocDB = docdbbackend.NewHandler(docdbbackend.NewInMemoryBackend("000000000000", "us-east-1"))
	b.Neptune = neptunebackend.NewHandler(
		neptunebackend.NewInMemoryBackend("000000000000", "us-east-1"))
	b.Kafka = kafkabackend.NewHandler(kafkabackend.NewInMemoryBackend("000000000000", "us-east-1"))
	b.Transfer = transferbackend.NewHandler(
		transferbackend.NewInMemoryBackend("000000000000", "us-east-1"))
	b.CloudTrail = cloudtrailbackend.NewHandler(
		cloudtrailbackend.NewInMemoryBackend("000000000000", "us-east-1"))
	b.CodePipeline = codepipelinebackend.NewHandler(
		codepipelinebackend.NewInMemoryBackend("000000000000", "us-east-1"))
	b.IoT = iotbackend.NewHandler(iotbackend.NewInMemoryBackendWithConfig("000000000000", "us-east-1"), nil)
	b.Pipes = pipesbackend.NewHandler(pipesbackend.NewInMemoryBackend("000000000000", "us-east-1"))
	b.EMR = emrbackend.NewHandler(emrbackend.NewInMemoryBackend("000000000000", "us-east-1"))

	return b
}

// TestResourceCreator_Phase3Types_NilBackends ensures all phase-3 resource types return a stub
// physical ID when the corresponding backend is nil.
func TestResourceCreator_Phase3Types_NilBackends(t *testing.T) {
	t.Parallel()

	tests := []struct {
		props        map[string]any
		name         string
		logicalID    string
		resourceType string
	}{
		{name: "eks_cluster", logicalID: "MyCluster", resourceType: "AWS::EKS::Cluster",
			props: map[string]any{"Name": "stub-cluster"}},
		{name: "eks_nodegroup", logicalID: "MyNg", resourceType: "AWS::EKS::Nodegroup",
			props: map[string]any{"ClusterName": "stub-cluster", "NodegroupName": "stub-ng"}},
		{name: "efs_filesystem", logicalID: "MyFS", resourceType: "AWS::EFS::FileSystem",
			props: map[string]any{"PerformanceMode": "generalPurpose"}},
		{name: "efs_mounttarget", logicalID: "MyMT", resourceType: "AWS::EFS::MountTarget",
			props: map[string]any{"FileSystemId": "fs-123", "SubnetId": "subnet-1"}},
		{name: "batch_compute_env", logicalID: "MyCE", resourceType: "AWS::Batch::ComputeEnvironment",
			props: map[string]any{"ComputeEnvironmentName": "stub-ce", "Type": "MANAGED"}},
		{name: "batch_job_queue", logicalID: "MyJQ", resourceType: "AWS::Batch::JobQueue",
			props: map[string]any{"JobQueueName": "stub-jq", "Priority": float64(1)}},
		{name: "batch_job_def", logicalID: "MyJD", resourceType: "AWS::Batch::JobDefinition",
			props: map[string]any{"JobDefinitionName": "stub-jd", "Type": "container"}},
		{name: "cloudfront_distribution", logicalID: "MyDist", resourceType: "AWS::CloudFront::Distribution",
			props: map[string]any{"DistributionConfig": map[string]any{"Enabled": true}}},
		{name: "autoscaling_group", logicalID: "MyASG", resourceType: "AWS::AutoScaling::AutoScalingGroup",
			props: map[string]any{"MinSize": float64(1), "MaxSize": float64(3)}},
		{name: "launch_configuration", logicalID: "MyLC", resourceType: "AWS::AutoScaling::LaunchConfiguration",
			props: map[string]any{"ImageId": "ami-stub", "InstanceType": "t2.micro"}},
		{name: "apigwv2_api", logicalID: "MyAPI", resourceType: "AWS::ApiGatewayV2::Api",
			props: map[string]any{"Name": "stub-api", "ProtocolType": "HTTP"}},
		{name: "apigwv2_stage", logicalID: "MyStage", resourceType: "AWS::ApiGatewayV2::Stage",
			props: map[string]any{"ApiId": "abc123", "StageName": "prod"}},
		{name: "codebuild_project", logicalID: "MyProject", resourceType: "AWS::CodeBuild::Project",
			props: map[string]any{"Name": "stub-project"}},
		{name: "glue_database", logicalID: "MyDB", resourceType: "AWS::Glue::Database",
			props: map[string]any{"DatabaseInput": map[string]any{"Name": "stub-db"}}},
		{name: "glue_job", logicalID: "MyJob", resourceType: "AWS::Glue::Job",
			props: map[string]any{"Name": "stub-job", "Role": "arn:aws:iam::000000000000:role/GlueRole"}},
		{name: "docdb_cluster", logicalID: "MyCluster", resourceType: "AWS::DocDB::DBCluster",
			props: map[string]any{"DBClusterIdentifier": "stub-docdb"}},
		{name: "docdb_instance", logicalID: "MyInst", resourceType: "AWS::DocDB::DBInstance",
			props: map[string]any{"DBInstanceIdentifier": "stub-inst", "DBClusterIdentifier": "stub-docdb"}},
		{name: "neptune_cluster", logicalID: "MyNeptune", resourceType: "AWS::Neptune::DBCluster",
			props: map[string]any{"DBClusterIdentifier": "stub-neptune"}},
		{name: "neptune_instance", logicalID: "MyNeptuneInst", resourceType: "AWS::Neptune::DBInstance",
			props: map[string]any{"DBInstanceIdentifier": "stub-neptune-inst"}},
		{name: "msk_cluster", logicalID: "MyMSK", resourceType: "AWS::MSK::Cluster",
			props: map[string]any{"ClusterName": "stub-msk"}},
		{name: "transfer_server", logicalID: "MyServer", resourceType: "AWS::Transfer::Server",
			props: map[string]any{"Protocols": []any{"SFTP"}}},
		{name: "cloudtrail_trail", logicalID: "MyTrail", resourceType: "AWS::CloudTrail::Trail",
			props: map[string]any{"TrailName": "stub-trail", "S3BucketName": "my-bucket"}},
		{name: "codepipeline_pipeline", logicalID: "MyPipeline", resourceType: "AWS::CodePipeline::Pipeline",
			props: map[string]any{"Pipeline": map[string]any{"Name": "stub-pipeline"}}},
		{name: "iot_thing", logicalID: "MyThing", resourceType: "AWS::IoT::Thing",
			props: map[string]any{"ThingName": "stub-thing"}},
		{name: "iot_topic_rule", logicalID: "MyRule", resourceType: "AWS::IoT::TopicRule",
			props: map[string]any{
				"RuleName":         "stub-rule",
				"TopicRulePayload": map[string]any{"SQL": "SELECT * FROM 'topic'"},
			},
		},
		{
			name:         "pipes_pipe",
			logicalID:    "MyPipe",
			resourceType: "AWS::Pipes::Pipe",
			props: map[string]any{
				"Name":   "stub-pipe",
				"Source": "arn:aws:sqs:us-east-1:000000000000:q",
				"Target": "arn:aws:sqs:us-east-1:000000000000:t",
			},
		},
		{name: "emr_cluster", logicalID: "MyEMR", resourceType: "AWS::EMR::Cluster",
			props: map[string]any{"Name": "stub-emr", "ReleaseLabel": "emr-6.0.0"}},
		{name: "cloudwatch_dashboard", logicalID: "MyDash", resourceType: "AWS::CloudWatch::Dashboard",
			props: map[string]any{"DashboardName": "stub-dash"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// newServiceBackends() leaves all Phase 3 backends nil → stub path.
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

// TestResourceCreator_Phase3Types_RealBackends validates create and delete
// for all phase-3 resource types with real in-memory backends.
func TestResourceCreator_Phase3Types_RealBackends(t *testing.T) {
	t.Parallel()

	tests := []struct {
		props        map[string]any
		name         string
		logicalID    string
		resourceType string
		wantNotEmpty bool
	}{
		{
			name:         "eks_cluster",
			logicalID:    "MyEKSCluster",
			resourceType: "AWS::EKS::Cluster",
			props: map[string]any{
				"Name":    "unit-eks-cluster",
				"RoleArn": "arn:aws:iam::000000000000:role/EKSRole",
			},
			wantNotEmpty: true,
		},
		{
			name: "efs_filesystem", logicalID: "MyEFS", resourceType: "AWS::EFS::FileSystem",
			props:        map[string]any{"PerformanceMode": "generalPurpose"},
			wantNotEmpty: true,
		},
		{
			name: "batch_compute_env", logicalID: "MyCE", resourceType: "AWS::Batch::ComputeEnvironment",
			props:        map[string]any{"ComputeEnvironmentName": "unit-ce", "Type": "MANAGED"},
			wantNotEmpty: true,
		},
		{
			name: "cloudfront_distribution", logicalID: "MyDist", resourceType: "AWS::CloudFront::Distribution",
			props:        map[string]any{"DistributionConfig": map[string]any{"Enabled": true, "Comment": "test"}},
			wantNotEmpty: true,
		},
		{
			name:         "launch_configuration",
			logicalID:    "MyLC",
			resourceType: "AWS::AutoScaling::LaunchConfiguration",
			props: map[string]any{
				"LaunchConfigurationName": "unit-lc",
				"ImageId":                 "ami-12345",
				"InstanceType":            "t2.micro",
			},
			wantNotEmpty: true,
		},
		{
			name:         "autoscaling_group",
			logicalID:    "MyASG",
			resourceType: "AWS::AutoScaling::AutoScalingGroup",
			props: map[string]any{
				"AutoScalingGroupName":    "unit-asg",
				"LaunchConfigurationName": "unit-lc",
				"MinSize":                 float64(1),
				"MaxSize":                 float64(3),
			},
			wantNotEmpty: true,
		},
		{
			name: "apigwv2_api", logicalID: "MyAPI", resourceType: "AWS::ApiGatewayV2::Api",
			props:        map[string]any{"Name": "unit-apigwv2-api", "ProtocolType": "HTTP"},
			wantNotEmpty: true,
		},
		{
			name: "codebuild_project", logicalID: "MyProject", resourceType: "AWS::CodeBuild::Project",
			props: map[string]any{
				"Name":        "unit-codebuild-project",
				"ServiceRole": "arn:aws:iam::000000000000:role/CodeBuildRole",
			},
			wantNotEmpty: true,
		},
		{
			name: "glue_database", logicalID: "MyDB", resourceType: "AWS::Glue::Database",
			props:        map[string]any{"DatabaseInput": map[string]any{"Name": "unit-glue-db"}},
			wantNotEmpty: true,
		},
		{
			name: "glue_job", logicalID: "MyJob", resourceType: "AWS::Glue::Job",
			props: map[string]any{
				"Name": "unit-glue-job",
				"Role": "arn:aws:iam::000000000000:role/GlueRole",
				"Command": map[string]any{
					"Name":           "glueetl",
					"ScriptLocation": "s3://bucket/script.py",
				},
			},
			wantNotEmpty: true,
		},
		{
			name: "docdb_cluster", logicalID: "MyDocDB", resourceType: "AWS::DocDB::DBCluster",
			props:        map[string]any{"DBClusterIdentifier": "unit-docdb-cluster"},
			wantNotEmpty: true,
		},
		{
			name: "neptune_cluster", logicalID: "MyNeptune", resourceType: "AWS::Neptune::DBCluster",
			props:        map[string]any{"DBClusterIdentifier": "unit-neptune-cluster"},
			wantNotEmpty: true,
		},
		{
			name: "msk_cluster", logicalID: "MyMSK", resourceType: "AWS::MSK::Cluster",
			props: map[string]any{
				"ClusterName":         "unit-msk-cluster",
				"KafkaVersion":        "3.4.0",
				"NumberOfBrokerNodes": float64(3),
			},
			wantNotEmpty: true,
		},
		{
			name: "transfer_server", logicalID: "MyTransfer", resourceType: "AWS::Transfer::Server",
			props:        map[string]any{"Protocols": []any{"SFTP"}},
			wantNotEmpty: true,
		},
		{
			name: "cloudtrail_trail", logicalID: "MyTrail", resourceType: "AWS::CloudTrail::Trail",
			props: map[string]any{
				"TrailName":    "unit-cloudtrail",
				"S3BucketName": "my-cloudtrail-bucket",
			},
			wantNotEmpty: true,
		},
		{
			name: "codepipeline_pipeline", logicalID: "MyPipeline", resourceType: "AWS::CodePipeline::Pipeline",
			props: map[string]any{
				"Pipeline": map[string]any{
					"Name":    "unit-codepipeline",
					"RoleArn": "arn:aws:iam::000000000000:role/PipelineRole",
				},
			},
			wantNotEmpty: true,
		},
		{
			name: "iot_thing", logicalID: "MyThing", resourceType: "AWS::IoT::Thing",
			props:        map[string]any{"ThingName": "unit-iot-thing"},
			wantNotEmpty: true,
		},
		{
			name: "iot_topic_rule", logicalID: "MyRule", resourceType: "AWS::IoT::TopicRule",
			props: map[string]any{
				"RuleName": "unit-iot-rule",
				"TopicRulePayload": map[string]any{
					"SQL": "SELECT * FROM 'test/topic'",
				},
			},
			wantNotEmpty: true,
		},
		{
			name: "pipes_pipe", logicalID: "MyPipe", resourceType: "AWS::Pipes::Pipe",
			props: map[string]any{
				"Name":    "unit-pipe",
				"Source":  "arn:aws:sqs:us-east-1:000000000000:source-queue",
				"Target":  "arn:aws:sqs:us-east-1:000000000000:target-queue",
				"RoleArn": "arn:aws:iam::000000000000:role/PipesRole",
			},
			wantNotEmpty: true,
		},
		{
			name: "emr_cluster", logicalID: "MyEMR", resourceType: "AWS::EMR::Cluster",
			props:        map[string]any{"Name": "unit-emr-cluster", "ReleaseLabel": "emr-6.0.0"},
			wantNotEmpty: true,
		},
		{
			name: "cloudwatch_dashboard", logicalID: "MyDash", resourceType: "AWS::CloudWatch::Dashboard",
			props:        map[string]any{"DashboardName": "unit-cw-dashboard", "DashboardBody": `{"widgets":[]}`},
			wantNotEmpty: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backends := newPhase3ServiceBackends()
			rc := cloudformation.NewResourceCreator(backends)
			ctx := t.Context()

			physID, err := rc.Create(ctx, tt.logicalID, tt.resourceType, tt.props, nil, nil)
			require.NoError(t, err)

			if tt.wantNotEmpty {
				assert.NotEmpty(t, physID, "expected non-empty physID for %s", tt.resourceType)
			}

			// Delete should succeed.
			err = rc.Delete(ctx, tt.resourceType, physID, nil)
			require.NoError(t, err)
		})
	}
}

// TestResourceCreator_Phase3_EFSMountTargetAfterFileSystem verifies mount target creation
// succeeds when the file system exists first.
func TestResourceCreator_Phase3_EFSMountTargetAfterFileSystem(t *testing.T) {
	t.Parallel()

	backends := newPhase3ServiceBackends()
	rc := cloudformation.NewResourceCreator(backends)
	ctx := t.Context()

	// Create file system first.
	fsID, err := rc.Create(ctx, "MyFS", "AWS::EFS::FileSystem",
		map[string]any{"PerformanceMode": "generalPurpose"}, nil, nil)
	require.NoError(t, err)
	require.NotEmpty(t, fsID)

	physIDs := map[string]string{"MyFS": fsID}

	// Create mount target referencing the file system.
	mtID, err := rc.Create(ctx, "MyMT", "AWS::EFS::MountTarget",
		map[string]any{
			"FileSystemId": fsID,
			"SubnetId":     "subnet-12345",
		}, nil, physIDs)
	require.NoError(t, err)
	require.NotEmpty(t, mtID)

	// Delete mount target.
	err = rc.Delete(ctx, "AWS::EFS::MountTarget", mtID, nil)
	require.NoError(t, err)

	// Delete file system.
	err = rc.Delete(ctx, "AWS::EFS::FileSystem", fsID, nil)
	require.NoError(t, err)
}

// TestResourceCreator_Phase3_BatchJobQueueWithCE verifies job queue creation
// succeeds when a compute environment exists.
func TestResourceCreator_Phase3_BatchJobQueueWithCE(t *testing.T) {
	t.Parallel()

	backends := newPhase3ServiceBackends()
	rc := cloudformation.NewResourceCreator(backends)
	ctx := t.Context()

	// Create compute environment first.
	ceARN, err := rc.Create(ctx, "MyCE", "AWS::Batch::ComputeEnvironment",
		map[string]any{"ComputeEnvironmentName": "unit-batch-ce", "Type": "MANAGED"}, nil, nil)
	require.NoError(t, err)
	require.NotEmpty(t, ceARN)

	physIDs := map[string]string{"MyCE": ceARN}

	// Create job queue referencing CE.
	jqARN, err := rc.Create(ctx, "MyJQ", "AWS::Batch::JobQueue",
		map[string]any{
			"JobQueueName": "unit-batch-jq",
			"Priority":     float64(10),
			"ComputeEnvironmentOrder": []any{
				map[string]any{"ComputeEnvironment": ceARN},
			},
		}, nil, physIDs)
	require.NoError(t, err)
	require.NotEmpty(t, jqARN)

	// Create job definition.
	jdARN, err := rc.Create(ctx, "MyJD", "AWS::Batch::JobDefinition",
		map[string]any{"JobDefinitionName": "unit-batch-jd", "Type": "container"}, nil, nil)
	require.NoError(t, err)
	require.NotEmpty(t, jdARN)

	// Delete resources.
	require.NoError(t, rc.Delete(ctx, "AWS::Batch::JobDefinition", jdARN, nil))
	require.NoError(t, rc.Delete(ctx, "AWS::Batch::JobQueue", jqARN, nil))
	require.NoError(t, rc.Delete(ctx, "AWS::Batch::ComputeEnvironment", ceARN, nil))
}

// TestResourceCreator_Phase3_EKSNodegroupAfterCluster verifies nodegroup creation
// succeeds when the cluster exists.
func TestResourceCreator_Phase3_EKSNodegroupAfterCluster(t *testing.T) {
	t.Parallel()

	backends := newPhase3ServiceBackends()
	rc := cloudformation.NewResourceCreator(backends)
	ctx := t.Context()

	// Create EKS cluster first.
	clusterARN, err := rc.Create(ctx, "MyCluster", "AWS::EKS::Cluster",
		map[string]any{
			"Name":    "unit-eks-cluster",
			"RoleArn": "arn:aws:iam::000000000000:role/EKSRole",
		}, nil, nil)
	require.NoError(t, err)
	require.NotEmpty(t, clusterARN)

	// Create nodegroup.
	ngARN, err := rc.Create(ctx, "MyNG", "AWS::EKS::Nodegroup",
		map[string]any{
			"ClusterName":   "unit-eks-cluster",
			"NodegroupName": "unit-nodegroup",
			"NodeRole":      "arn:aws:iam::000000000000:role/NodeRole",
		}, nil, nil)
	require.NoError(t, err)
	require.NotEmpty(t, ngARN)

	// Delete nodegroup then cluster.
	require.NoError(t, rc.Delete(ctx, "AWS::EKS::Nodegroup", ngARN, nil))
	require.NoError(t, rc.Delete(ctx, "AWS::EKS::Cluster", clusterARN, nil))
}

// TestResourceCreator_Phase3_APIGatewayV2StageAfterAPI verifies stage creation
// succeeds when the API exists.
func TestResourceCreator_Phase3_APIGatewayV2StageAfterAPI(t *testing.T) {
	t.Parallel()

	backends := newPhase3ServiceBackends()
	rc := cloudformation.NewResourceCreator(backends)
	ctx := t.Context()

	// Create API first.
	apiID, err := rc.Create(ctx, "MyAPI", "AWS::ApiGatewayV2::Api",
		map[string]any{
			"Name":         "unit-apigwv2",
			"ProtocolType": "HTTP",
		}, nil, nil)
	require.NoError(t, err)
	require.NotEmpty(t, apiID)

	physIDs := map[string]string{"MyAPI": apiID}

	// Create stage.
	stageID, err := rc.Create(ctx, "MyStage", "AWS::ApiGatewayV2::Stage",
		map[string]any{
			"ApiId":     apiID,
			"StageName": "prod",
		}, nil, physIDs)
	require.NoError(t, err)
	require.NotEmpty(t, stageID)

	// Delete stage then API.
	require.NoError(t, rc.Delete(ctx, "AWS::ApiGatewayV2::Stage", stageID, nil))
	require.NoError(t, rc.Delete(ctx, "AWS::ApiGatewayV2::Api", apiID, nil))
}

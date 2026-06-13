package cloudformation_test

import (
	"maps"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	appautoscalingbackend "github.com/blackbirdworks/gopherstack/services/applicationautoscaling"
	"github.com/blackbirdworks/gopherstack/services/cloudformation"
)

// newPhase5ServiceBackends creates a ServiceBackends with all phase-5 backends populated.
func newPhase5ServiceBackends() *cloudformation.ServiceBackends {
	b := newPhase4ServiceBackends()
	b.AppAutoScaling = appautoscalingbackend.NewHandler(
		appautoscalingbackend.NewInMemoryBackend("000000000000", "us-east-1"),
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
			name:         "app_autoscaling_scalable_target",
			logicalID:    "MyScalableTarget",
			resourceType: "AWS::ApplicationAutoScaling::ScalableTarget",
			props: map[string]any{
				"ServiceNamespace":  "ecs",
				"ResourceId":        "service/default/web",
				"ScalableDimension": "ecs:service:DesiredCount",
				"MinCapacity":       float64(1),
				"MaxCapacity":       float64(5),
			},
		},
		{
			name:         "app_autoscaling_scaling_policy",
			logicalID:    "MyScalingPolicy",
			resourceType: "AWS::ApplicationAutoScaling::ScalingPolicy",
			props: map[string]any{
				"PolicyName":        "stub-policy",
				"ServiceNamespace":  "ecs",
				"ResourceId":        "service/default/web",
				"ScalableDimension": "ecs:service:DesiredCount",
			},
		},
		{
			name:         "secretsmanager_rotation_schedule",
			logicalID:    "MyRotation",
			resourceType: "AWS::SecretsManager::RotationSchedule",
			props:        map[string]any{"SecretId": "arn:aws:secretsmanager:us-east-1:000000000000:secret:MySecret"},
		},
		{
			name:         "secretsmanager_secret_target_attachment",
			logicalID:    "MyAttachment",
			resourceType: "AWS::SecretsManager::SecretTargetAttachment",
			props: map[string]any{
				"SecretId":   "arn:aws:secretsmanager:us-east-1:000000000000:secret:MySecret",
				"TargetId":   "db-instance-id",
				"TargetType": "AWS::RDS::DBInstance",
			},
		},
		{
			name:         "ssm_maintenance_window",
			logicalID:    "MyMW",
			resourceType: "AWS::SSM::MaintenanceWindow",
			props: map[string]any{
				"Name":     "stub-mw",
				"Schedule": "cron(0 2 ? * SUN *)",
				"Duration": float64(4),
				"Cutoff":   float64(1),
			},
		},
		{
			name:         "ssm_association",
			logicalID:    "MyAssociation",
			resourceType: "AWS::SSM::Association",
			props:        map[string]any{"Name": "AWS-RunShellScript"},
		},
		{
			name:         "dynamodb_global_table",
			logicalID:    "MyGlobalTable",
			resourceType: "AWS::DynamoDB::GlobalTable",
			props:        map[string]any{"TableName": "stub-global"},
		},
		{
			name:         "glue_crawler",
			logicalID:    "MyCrawler",
			resourceType: "AWS::Glue::Crawler",
			props:        map[string]any{"Name": "stub-crawler", "Role": "AWSGlueRole"},
		},
		{
			name:         "glue_table",
			logicalID:    "MyTable",
			resourceType: "AWS::Glue::Table",
			props: map[string]any{
				"DatabaseName": "default",
				"TableInput":   map[string]any{"Name": "stub-table"},
			},
		},
		{
			name:         "glue_trigger",
			logicalID:    "MyTrigger",
			resourceType: "AWS::Glue::Trigger",
			props:        map[string]any{"Name": "stub-trigger", "Type": "ON_DEMAND"},
		},
		{
			name:         "glue_connection",
			logicalID:    "MyConnection",
			resourceType: "AWS::Glue::Connection",
			props: map[string]any{
				"ConnectionInput": map[string]any{
					"Name":           "stub-conn",
					"ConnectionType": "JDBC",
				},
			},
		},
		{
			name:         "glue_partition",
			logicalID:    "MyPartition",
			resourceType: "AWS::Glue::Partition",
			props: map[string]any{
				"DatabaseName": "default",
				"TableName":    "stub-table",
				"PartitionInput": map[string]any{
					"Values": []any{"2024"},
				},
			},
		},
		{
			name:         "appsync_datasource",
			logicalID:    "MyDS",
			resourceType: "AWS::AppSync::DataSource",
			props:        map[string]any{"ApiId": "api-stub", "Name": "stub-ds", "Type": "NONE"},
		},
		{
			name:         "appsync_resolver",
			logicalID:    "MyResolver",
			resourceType: "AWS::AppSync::Resolver",
			props: map[string]any{
				"ApiId":          "api-stub",
				"TypeName":       "Query",
				"FieldName":      "getStub",
				"DataSourceName": "stub-ds",
			},
		},
		{
			name:         "appsync_function_configuration",
			logicalID:    "MyFunction",
			resourceType: "AWS::AppSync::FunctionConfiguration",
			props: map[string]any{
				"ApiId":          "api-stub",
				"Name":           "stub-fn",
				"DataSourceName": "stub-ds",
			},
		},
		{
			name:         "appsync_api_key",
			logicalID:    "MyApiKey",
			resourceType: "AWS::AppSync::ApiKey",
			props:        map[string]any{"ApiId": "api-stub"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// nil backends → stub path
			backends := &cloudformation.ServiceBackends{}
			rc := cloudformation.NewResourceCreator(backends)

			physID, err := rc.Create(t.Context(), tt.logicalID, tt.resourceType, tt.props, nil, nil)
			require.NoError(t, err)
			assert.NotEmpty(t, physID)

			err = rc.Delete(t.Context(), tt.resourceType, physID, nil)
			require.NoError(t, err)
		})
	}
}

// TestResourceCreator_Phase5Types_RealBackends validates create and delete with real in-memory backends.
func TestResourceCreator_Phase5Types_RealBackends(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setupFn      func(*cloudformation.ServiceBackends)
		props        map[string]any
		name         string
		logicalID    string
		resourceType string
	}{
		{
			name:         "app_autoscaling_scalable_target",
			logicalID:    "MyScalableTarget",
			resourceType: "AWS::ApplicationAutoScaling::ScalableTarget",
			props: map[string]any{
				"ServiceNamespace":  "ecs",
				"ResourceId":        "service/default/web",
				"ScalableDimension": "ecs:service:DesiredCount",
				"MinCapacity":       float64(1),
				"MaxCapacity":       float64(10),
			},
		},
		{
			name:         "app_autoscaling_scaling_policy",
			logicalID:    "MyScalingPolicy",
			resourceType: "AWS::ApplicationAutoScaling::ScalingPolicy",
			props: map[string]any{
				"PolicyName":        "unit-cfn-policy",
				"ServiceNamespace":  "ecs",
				"ResourceId":        "service/default/web",
				"ScalableDimension": "ecs:service:DesiredCount",
				"PolicyType":        "StepScaling",
			},
		},
		{
			name:         "secretsmanager_rotation_schedule",
			logicalID:    "MyRotation",
			resourceType: "AWS::SecretsManager::RotationSchedule",
			props: map[string]any{
				"SecretId": "arn:aws:secretsmanager:us-east-1:000000000000:secret:MySecret",
			},
		},
		{
			name:         "secretsmanager_secret_target_attachment",
			logicalID:    "MyAttachment",
			resourceType: "AWS::SecretsManager::SecretTargetAttachment",
			props: map[string]any{
				"SecretId":   "arn:aws:secretsmanager:us-east-1:000000000000:secret:MySecret",
				"TargetId":   "db-12345",
				"TargetType": "AWS::RDS::DBInstance",
			},
		},
		{
			name:         "ssm_maintenance_window",
			logicalID:    "MyMW",
			resourceType: "AWS::SSM::MaintenanceWindow",
			props: map[string]any{
				"Name":     "unit-cfn-mw",
				"Schedule": "cron(0 2 ? * SUN *)",
				"Duration": float64(4),
				"Cutoff":   float64(1),
			},
		},
		{
			name:         "glue_crawler",
			logicalID:    "MyCrawler",
			resourceType: "AWS::Glue::Crawler",
			props: map[string]any{
				"Name": "unit-cfn-crawler",
				"Role": "AWSGlueServiceRole",
			},
		},
		{
			name:         "glue_trigger",
			logicalID:    "MyTrigger",
			resourceType: "AWS::Glue::Trigger",
			props: map[string]any{
				"Name": "unit-cfn-trigger",
				"Type": "ON_DEMAND",
			},
		},
		{
			name:         "glue_connection",
			logicalID:    "MyConnection",
			resourceType: "AWS::Glue::Connection",
			props: map[string]any{
				"ConnectionInput": map[string]any{
					"Name":           "unit-cfn-conn",
					"ConnectionType": "JDBC",
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backends := newPhase5ServiceBackends()
			rc := cloudformation.NewResourceCreator(backends)

			physID, err := rc.Create(t.Context(), tt.logicalID, tt.resourceType, tt.props, nil, nil)
			require.NoError(t, err)
			assert.NotEmpty(t, physID)

			err = rc.Delete(t.Context(), tt.resourceType, physID, nil)
			require.NoError(t, err)
		})
	}
}

// TestResourceCreator_AppAutoScaling_ScalableTarget_CreateDelete verifies
// scalable target is registered in the backend and deregistered on delete.
func TestResourceCreator_AppAutoScaling_ScalableTarget_CreateDelete(t *testing.T) {
	t.Parallel()

	backends := newPhase5ServiceBackends()
	rc := cloudformation.NewResourceCreator(backends)

	props := map[string]any{
		"ServiceNamespace":  "ecs",
		"ResourceId":        "service/my-cluster/my-service",
		"ScalableDimension": "ecs:service:DesiredCount",
		"MinCapacity":       float64(2),
		"MaxCapacity":       float64(20),
	}

	physID, err := rc.Create(t.Context(), "MyTarget", "AWS::ApplicationAutoScaling::ScalableTarget", props, nil, nil)
	require.NoError(t, err)
	assert.NotEmpty(t, physID)
	assert.Contains(t, physID, "scalable-target")

	// Verify it is registered in the backend.
	targets := backends.AppAutoScaling.Backend.DescribeScalableTargets(
		appautoscalingbackend.DescribeScalableTargetsFilter{},
	)
	assert.Len(t, targets, 1)
	assert.Equal(t, "service/my-cluster/my-service", targets[0].ResourceID)

	err = rc.Delete(t.Context(), "AWS::ApplicationAutoScaling::ScalableTarget", physID, nil)
	require.NoError(t, err)

	// Verify it is deregistered.
	targets = backends.AppAutoScaling.Backend.DescribeScalableTargets(
		appautoscalingbackend.DescribeScalableTargetsFilter{},
	)
	assert.Empty(t, targets)
}

// TestResourceCreator_AppAutoScaling_ScalingPolicy_CreateDelete verifies
// scaling policy is created and deleted correctly.
func TestResourceCreator_AppAutoScaling_ScalingPolicy_CreateDelete(t *testing.T) {
	t.Parallel()

	backends := newPhase5ServiceBackends()
	rc := cloudformation.NewResourceCreator(backends)

	props := map[string]any{
		"PolicyName":        "cfn-test-policy",
		"ServiceNamespace":  "ecs",
		"ResourceId":        "service/default/svc",
		"ScalableDimension": "ecs:service:DesiredCount",
		"PolicyType":        "TargetTrackingScaling",
	}

	physID, err := rc.Create(t.Context(), "MyPolicy", "AWS::ApplicationAutoScaling::ScalingPolicy", props, nil, nil)
	require.NoError(t, err)
	assert.NotEmpty(t, physID)

	policies := backends.AppAutoScaling.Backend.DescribeScalingPolicies(
		appautoscalingbackend.DescribeScalingPoliciesFilter{},
	)
	assert.Len(t, policies, 1)
	assert.Equal(t, "cfn-test-policy", policies[0].PolicyName)

	err = rc.Delete(t.Context(), "AWS::ApplicationAutoScaling::ScalingPolicy", physID, nil)
	require.NoError(t, err)

	policies = backends.AppAutoScaling.Backend.DescribeScalingPolicies(
		appautoscalingbackend.DescribeScalingPoliciesFilter{},
	)
	assert.Empty(t, policies)
}

// TestResourceCreator_SSM_MaintenanceWindow_CreateDelete verifies the maintenance
// window is created in the SSM backend and removed on delete.
func TestResourceCreator_SSM_MaintenanceWindow_CreateDelete(t *testing.T) {
	t.Parallel()

	backends := newPhase5ServiceBackends()
	rc := cloudformation.NewResourceCreator(backends)

	props := map[string]any{
		"Name":     "cfn-test-mw",
		"Schedule": "cron(0 2 ? * SUN *)",
		"Duration": float64(4),
		"Cutoff":   float64(1),
	}

	physID, err := rc.Create(t.Context(), "MyMW", "AWS::SSM::MaintenanceWindow", props, nil, nil)
	require.NoError(t, err)
	assert.NotEmpty(t, physID)
	assert.Contains(t, physID, "mw-")

	err = rc.Delete(t.Context(), "AWS::SSM::MaintenanceWindow", physID, nil)
	require.NoError(t, err)
}

// TestResourceCreator_Glue_Crawler_CreateDelete verifies the crawler is created
// in the Glue backend and removed on delete.
func TestResourceCreator_Glue_Crawler_CreateDelete(t *testing.T) {
	t.Parallel()

	backends := newPhase5ServiceBackends()
	rc := cloudformation.NewResourceCreator(backends)

	props := map[string]any{
		"Name": "cfn-test-crawler",
		"Role": "AWSGlueServiceRole",
	}

	physID, err := rc.Create(t.Context(), "MyCrawler", "AWS::Glue::Crawler", props, nil, nil)
	require.NoError(t, err)
	assert.Equal(t, "cfn-test-crawler", physID)

	crawler, err := backends.Glue.Backend.GetCrawler("cfn-test-crawler")
	require.NoError(t, err)
	assert.Equal(t, "cfn-test-crawler", crawler.Name)

	err = rc.Delete(t.Context(), "AWS::Glue::Crawler", physID, nil)
	require.NoError(t, err)
}

// TestResourceCreator_Glue_Trigger_CreateDelete verifies the trigger is created
// and deleted.
func TestResourceCreator_Glue_Trigger_CreateDelete(t *testing.T) {
	t.Parallel()

	backends := newPhase5ServiceBackends()
	rc := cloudformation.NewResourceCreator(backends)

	props := map[string]any{
		"Name": "cfn-test-trigger",
		"Type": "ON_DEMAND",
	}

	physID, err := rc.Create(t.Context(), "MyTrigger", "AWS::Glue::Trigger", props, nil, nil)
	require.NoError(t, err)
	assert.Equal(t, "cfn-test-trigger", physID)

	trigger, err := backends.Glue.Backend.GetTrigger("cfn-test-trigger")
	require.NoError(t, err)
	assert.Equal(t, "ON_DEMAND", trigger.Type)

	err = rc.Delete(t.Context(), "AWS::Glue::Trigger", physID, nil)
	require.NoError(t, err)
}

// TestResourceCreator_Glue_Connection_CreateDelete verifies the connection is
// created and deleted.
func TestResourceCreator_Glue_Connection_CreateDelete(t *testing.T) {
	t.Parallel()

	backends := newPhase5ServiceBackends()
	rc := cloudformation.NewResourceCreator(backends)

	props := map[string]any{
		"ConnectionInput": map[string]any{
			"Name":           "cfn-test-conn",
			"ConnectionType": "JDBC",
		},
	}

	physID, err := rc.Create(t.Context(), "MyConn", "AWS::Glue::Connection", props, nil, nil)
	require.NoError(t, err)
	assert.Equal(t, "cfn-test-conn", physID)

	conn, err := backends.Glue.Backend.GetConnection("cfn-test-conn")
	require.NoError(t, err)
	assert.Equal(t, "JDBC", conn.ConnectionType)

	err = rc.Delete(t.Context(), "AWS::Glue::Connection", physID, nil)
	require.NoError(t, err)
}

// TestResourceCreator_SecretsManager_RotationSchedule_CreateDelete verifies
// that rotation schedule and secret target attachment return stable physical IDs.
func TestResourceCreator_SecretsManager_RotationSchedule_CreateDelete(t *testing.T) {
	t.Parallel()

	tests := []struct {
		props        map[string]any
		name         string
		logicalID    string
		resourceType string
	}{
		{
			name:         "rotation_schedule",
			logicalID:    "MyRotation",
			resourceType: "AWS::SecretsManager::RotationSchedule",
			props:        map[string]any{"SecretId": "my-secret"},
		},
		{
			name:         "secret_target_attachment",
			logicalID:    "MyAttachment",
			resourceType: "AWS::SecretsManager::SecretTargetAttachment",
			props: map[string]any{
				"SecretId":   "my-secret",
				"TargetId":   "db-1234",
				"TargetType": "AWS::RDS::DBInstance",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backends := newPhase5ServiceBackends()
			rc := cloudformation.NewResourceCreator(backends)

			physID, err := rc.Create(t.Context(), tt.logicalID, tt.resourceType, tt.props, nil, nil)
			require.NoError(t, err)
			assert.NotEmpty(t, physID)

			err = rc.Delete(t.Context(), tt.resourceType, physID, nil)
			require.NoError(t, err)
		})
	}
}

// TestResourceCreator_AppSync_Supplemental_CreateDelete tests AppSync data source,
// resolver, function, and API key via CFN resource types.
func TestResourceCreator_AppSync_Supplemental_CreateDelete(t *testing.T) {
	t.Parallel()

	backends := newPhase5ServiceBackends()

	// First create a GraphQL API to use as parent.
	rc := cloudformation.NewResourceCreator(backends)
	apiPhysID, setupErr := rc.Create(t.Context(), "MyAPI", "AWS::AppSync::GraphQLApi", map[string]any{
		"Name":               "cfn-test-api",
		"AuthenticationType": "API_KEY",
	}, nil, nil)
	require.NoError(t, setupErr)
	require.NotEmpty(t, apiPhysID)

	tests := []struct {
		props        map[string]any
		name         string
		logicalID    string
		resourceType string
	}{
		{
			name:         "datasource",
			logicalID:    "MyDS",
			resourceType: "AWS::AppSync::DataSource",
			props: map[string]any{
				"ApiId": apiPhysID,
				"Name":  "cfn-test-ds",
				"Type":  "NONE",
			},
		},
		{
			name:         "api_key",
			logicalID:    "MyKey",
			resourceType: "AWS::AppSync::ApiKey",
			props: map[string]any{
				"ApiId":       apiPhysID,
				"Description": "test key",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backends2 := newPhase5ServiceBackends()
			// Re-create API in this backend so each subtest is independent.
			rc2 := cloudformation.NewResourceCreator(backends2)
			api2PhysID, err := rc2.Create(t.Context(), "MyAPI2", "AWS::AppSync::GraphQLApi", map[string]any{
				"Name":               "cfn-test-api-2-" + tt.name,
				"AuthenticationType": "API_KEY",
			}, nil, nil)
			require.NoError(t, err)

			props2 := maps.Clone(tt.props)
			props2["ApiId"] = api2PhysID

			physID, err := rc2.Create(t.Context(), tt.logicalID, tt.resourceType, props2, nil, nil)
			require.NoError(t, err)
			assert.NotEmpty(t, physID)

			err = rc2.Delete(t.Context(), tt.resourceType, physID, nil)
			require.NoError(t, err)
		})
	}
}

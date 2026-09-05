package ecs_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	ecssdk "github.com/aws/aws-sdk-go-v2/service/ecs"
	ecstypes "github.com/aws/aws-sdk-go-v2/service/ecs/types"
	"github.com/stretchr/testify/require"
)

// TestECS_ClusterServiceConnectDefaults_Echoed proves CreateCluster and
// UpdateCluster's ServiceConnectDefaults (undeclared before this fix -- see
// cmd/reqfielddiff) round-trips: CreateClusterInput.ServiceConnectDefaults's
// own doc comment describes the cluster-level default Service Connect
// namespace, and this is a documented-default-shaped field whose value must
// simply be reflected back, not fabricated behaviour.
func TestECS_ClusterServiceConnectDefaults_Echoed(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestECSClient(t, h)
	ctx := t.Context()

	createOut, err := client.CreateCluster(ctx, &ecssdk.CreateClusterInput{
		ClusterName: aws.String("scd-cluster"),
		ServiceConnectDefaults: &ecstypes.ClusterServiceConnectDefaultsRequest{
			Namespace: aws.String("scd-namespace"),
		},
	})
	require.NoError(t, err)
	require.NotNil(t, createOut.Cluster.ServiceConnectDefaults, "CreateCluster must echo ServiceConnectDefaults")
	require.Equal(t, "scd-namespace", *createOut.Cluster.ServiceConnectDefaults.Namespace)

	describeOut, err := client.DescribeClusters(ctx, &ecssdk.DescribeClustersInput{
		Clusters: []string{"scd-cluster"},
	})
	require.NoError(t, err)
	require.Len(t, describeOut.Clusters, 1)
	require.NotNil(t, describeOut.Clusters[0].ServiceConnectDefaults)
	require.Equal(t, "scd-namespace", *describeOut.Clusters[0].ServiceConnectDefaults.Namespace)

	updateOut, err := client.UpdateCluster(ctx, &ecssdk.UpdateClusterInput{
		Cluster: aws.String("scd-cluster"),
		ServiceConnectDefaults: &ecstypes.ClusterServiceConnectDefaultsRequest{
			Namespace: aws.String("scd-namespace-2"),
		},
	})
	require.NoError(t, err)
	require.NotNil(t, updateOut.Cluster.ServiceConnectDefaults)
	require.Equal(t, "scd-namespace-2", *updateOut.Cluster.ServiceConnectDefaults.Namespace)

	// UpdateCluster without ServiceConnectDefaults must leave the existing
	// value untouched (matches Settings' own if-non-nil precedent).
	update2Out, err := client.UpdateCluster(ctx, &ecssdk.UpdateClusterInput{
		Cluster: aws.String("scd-cluster"),
	})
	require.NoError(t, err)
	require.NotNil(t, update2Out.Cluster.ServiceConnectDefaults)
	require.Equal(t, "scd-namespace-2", *update2Out.Cluster.ServiceConnectDefaults.Namespace)
}

// TestECS_CreateService_AvailabilityZoneRebalancing_DefaultsToEnabled proves
// CreateServiceInput.AvailabilityZoneRebalancing's own doc comment: "For
// create service requests, when no value is specified ... Amazon ECS
// defaults the value to ENABLED." The test omits the field entirely.
func TestECS_CreateService_AvailabilityZoneRebalancing_DefaultsToEnabled(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestECSClient(t, h)
	ctx := t.Context()

	tdArn := registerTestTaskDef(t, h, "azr-default-family")

	out, err := client.CreateService(ctx, &ecssdk.CreateServiceInput{
		ServiceName:    aws.String("azr-default-svc"),
		TaskDefinition: aws.String(tdArn),
	})
	require.NoError(t, err)
	require.Equal(t, ecstypes.AvailabilityZoneRebalancingEnabled, out.Service.AvailabilityZoneRebalancing)
}

// TestECS_CreateService_AvailabilityZoneRebalancing_ExplicitValueHonored
// proves an explicitly supplied AvailabilityZoneRebalancing is stored and
// echoed, not silently dropped (the field was entirely undeclared before
// this fix).
func TestECS_CreateService_AvailabilityZoneRebalancing_ExplicitValueHonored(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestECSClient(t, h)
	ctx := t.Context()

	tdArn := registerTestTaskDef(t, h, "azr-explicit-family")

	out, err := client.CreateService(ctx, &ecssdk.CreateServiceInput{
		ServiceName:                 aws.String("azr-explicit-svc"),
		TaskDefinition:              aws.String(tdArn),
		AvailabilityZoneRebalancing: ecstypes.AvailabilityZoneRebalancingDisabled,
	})
	require.NoError(t, err)
	require.Equal(t, ecstypes.AvailabilityZoneRebalancingDisabled, out.Service.AvailabilityZoneRebalancing)
}

// TestECS_UpdateService_AvailabilityZoneRebalancing_DefaultsToExisting
// proves UpdateServiceInput.AvailabilityZoneRebalancing's own doc comment:
// "For update service requests, when no value is specified ... Amazon ECS
// defaults to the existing service's AvailabilityZoneRebalancing value." An
// update that omits the field must leave the create-time ENABLED default
// untouched; an update that sets it explicitly must change it.
func TestECS_UpdateService_AvailabilityZoneRebalancing_DefaultsToExisting(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestECSClient(t, h)
	ctx := t.Context()

	tdArn := registerTestTaskDef(t, h, "azr-update-family")

	_, err := client.CreateService(ctx, &ecssdk.CreateServiceInput{
		ServiceName:    aws.String("azr-update-svc"),
		TaskDefinition: aws.String(tdArn),
	})
	require.NoError(t, err)

	desired := int32(2)

	unchangedOut, err := client.UpdateService(ctx, &ecssdk.UpdateServiceInput{
		Service:      aws.String("azr-update-svc"),
		DesiredCount: &desired,
	})
	require.NoError(t, err)
	require.Equal(t, ecstypes.AvailabilityZoneRebalancingEnabled, unchangedOut.Service.AvailabilityZoneRebalancing,
		"omitting AvailabilityZoneRebalancing on update must keep the existing value")

	changedOut, err := client.UpdateService(ctx, &ecssdk.UpdateServiceInput{
		Service:                     aws.String("azr-update-svc"),
		AvailabilityZoneRebalancing: ecstypes.AvailabilityZoneRebalancingDisabled,
	})
	require.NoError(t, err)
	require.Equal(t, ecstypes.AvailabilityZoneRebalancingDisabled, changedOut.Service.AvailabilityZoneRebalancing)
}

// TestECS_CreateService_HealthCheckGracePeriodSeconds_DefaultsToZero proves
// CreateServiceInput.HealthCheckGracePeriodSeconds's own doc comment: "If you
// do not specify a health check grace period value, the default value of 0
// is used." The test omits the field entirely and asserts the response
// carries an explicit 0, not a dropped/nil field.
func TestECS_CreateService_HealthCheckGracePeriodSeconds_DefaultsToZero(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestECSClient(t, h)
	ctx := t.Context()

	tdArn := registerTestTaskDef(t, h, "hcgp-default-family")

	out, err := client.CreateService(ctx, &ecssdk.CreateServiceInput{
		ServiceName:    aws.String("hcgp-default-svc"),
		TaskDefinition: aws.String(tdArn),
	})
	require.NoError(t, err)
	require.NotNil(t, out.Service.HealthCheckGracePeriodSeconds, "must default to 0, not be omitted")
	require.Equal(t, int32(0), *out.Service.HealthCheckGracePeriodSeconds)
}

// TestECS_Service_HealthCheckGracePeriodSeconds_ExplicitValueHonored proves
// an explicit grace period round-trips on both CreateService and
// UpdateService (the field was entirely undeclared before this fix).
func TestECS_Service_HealthCheckGracePeriodSeconds_ExplicitValueHonored(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestECSClient(t, h)
	ctx := t.Context()

	tdArn := registerTestTaskDef(t, h, "hcgp-explicit-family")

	grace := int32(90)

	createOut, err := client.CreateService(ctx, &ecssdk.CreateServiceInput{
		ServiceName:                   aws.String("hcgp-explicit-svc"),
		TaskDefinition:                aws.String(tdArn),
		HealthCheckGracePeriodSeconds: &grace,
	})
	require.NoError(t, err)
	require.NotNil(t, createOut.Service.HealthCheckGracePeriodSeconds)
	require.Equal(t, grace, *createOut.Service.HealthCheckGracePeriodSeconds)

	updated := int32(120)

	updateOut, err := client.UpdateService(ctx, &ecssdk.UpdateServiceInput{
		Service:                       aws.String("hcgp-explicit-svc"),
		HealthCheckGracePeriodSeconds: &updated,
	})
	require.NoError(t, err)
	require.NotNil(t, updateOut.Service.HealthCheckGracePeriodSeconds)
	require.Equal(t, updated, *updateOut.Service.HealthCheckGracePeriodSeconds)
}

// TestECS_Service_Monitoring_Echoed proves CreateServiceInput.Monitoring and
// UpdateServiceInput.Monitoring (undeclared before this fix) round-trip via
// ServiceRevision (types.ServiceRevision.Monitoring -- real AWS surfaces this
// config on the revision, not on types.Service itself). This backend does
// not emit real CloudWatch metrics, so the fix stores and echoes the given
// configuration without simulating resolution behaviour.
func TestECS_Service_Monitoring_Echoed(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestECSClient(t, h)
	ctx := t.Context()

	tdArn := registerTestTaskDef(t, h, "mon-family")

	monitoring := &ecstypes.MonitoringConfiguration{
		MetricConfigurations: []ecstypes.MetricConfiguration{
			{MetricNames: []string{"CPUUtilization"}, ResolutionSeconds: aws.Int32(20)},
		},
	}

	_, err := client.CreateService(ctx, &ecssdk.CreateServiceInput{
		ServiceName:    aws.String("mon-svc"),
		TaskDefinition: aws.String(tdArn),
		Monitoring:     monitoring,
	})
	require.NoError(t, err)

	backendSvcs, _, err := h.Backend.DescribeServices("default", []string{"mon-svc"})
	require.NoError(t, err)
	require.Len(t, backendSvcs, 1)
	require.Len(t, backendSvcs[0].Deployments, 1)
	revisionArn := backendSvcs[0].Deployments[0].ServiceRevisionArn
	require.NotEmpty(t, revisionArn)

	revOut, err := client.DescribeServiceRevisions(ctx, &ecssdk.DescribeServiceRevisionsInput{
		ServiceRevisionArns: []string{revisionArn},
	})
	require.NoError(t, err)
	require.Len(t, revOut.ServiceRevisions, 1)
	require.NotNil(t, revOut.ServiceRevisions[0].Monitoring)
	require.Len(t, revOut.ServiceRevisions[0].Monitoring.MetricConfigurations, 1)
	metricConfig := revOut.ServiceRevisions[0].Monitoring.MetricConfigurations[0]
	require.Equal(t, []string{"CPUUtilization"}, metricConfig.MetricNames)
	require.Equal(t, int32(20), *metricConfig.ResolutionSeconds)
}

// TestECS_UpdateService_ForceNewDeployment_RotatesDeploymentWithoutTaskDefChange
// proves UpdateServiceInput.ForceNewDeployment's own doc comment: "you can
// use this option to start a new deployment with no service definition
// changes." Before the fix, UpdateService only rotated the PRIMARY
// deployment when TaskDefinition itself changed, so a ForceNewDeployment=true
// update with no other change was silently a no-op.
func TestECS_UpdateService_ForceNewDeployment_RotatesDeploymentWithoutTaskDefChange(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestECSClient(t, h)
	ctx := t.Context()

	tdArn := registerTestTaskDef(t, h, "fnd-family")

	createOut, err := client.CreateService(ctx, &ecssdk.CreateServiceInput{
		ServiceName:    aws.String("fnd-svc"),
		TaskDefinition: aws.String(tdArn),
	})
	require.NoError(t, err)
	require.Len(t, createOut.Service.Deployments, 1)
	originalDeploymentID := *createOut.Service.Deployments[0].Id

	updateOut, err := client.UpdateService(ctx, &ecssdk.UpdateServiceInput{
		Service:            aws.String("fnd-svc"),
		ForceNewDeployment: true,
	})
	require.NoError(t, err)

	var primaryID string

	for _, d := range updateOut.Service.Deployments {
		if d.Status != nil && *d.Status == "PRIMARY" {
			primaryID = *d.Id
		}
	}

	require.NotEmpty(t, primaryID)
	require.NotEqual(t, originalDeploymentID, primaryID,
		"ForceNewDeployment must rotate the PRIMARY deployment even without a task definition change")
	require.Len(t, updateOut.Service.Deployments, 2, "the old PRIMARY must be demoted to ACTIVE, not discarded")
}

// TestECS_RegisterTaskDefinition_IpcModePidMode_Echoed proves
// RegisterTaskDefinitionInput.IpcMode and .PidMode (undeclared before this
// fix) round-trip through DescribeTaskDefinition. Neither field has a fixed
// documented default value for this operation (IpcMode: "depends on the
// Docker daemon setting"; PidMode: no named enum value for "unset"), so no
// default is fabricated -- only explicit values are asserted here.
func TestECS_RegisterTaskDefinition_IpcModePidMode_Echoed(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestECSClient(t, h)
	ctx := t.Context()

	regOut, err := client.RegisterTaskDefinition(ctx, &ecssdk.RegisterTaskDefinitionInput{
		Family: aws.String("ipcpid-family"),
		ContainerDefinitions: []ecstypes.ContainerDefinition{
			{Name: aws.String("app"), Image: aws.String("nginx:latest")},
		},
		IpcMode: ecstypes.IpcModeTask,
		PidMode: ecstypes.PidModeHost,
	})
	require.NoError(t, err)
	require.Equal(t, ecstypes.IpcModeTask, regOut.TaskDefinition.IpcMode)
	require.Equal(t, ecstypes.PidModeHost, regOut.TaskDefinition.PidMode)

	descOut, err := client.DescribeTaskDefinition(ctx, &ecssdk.DescribeTaskDefinitionInput{
		TaskDefinition: aws.String("ipcpid-family"),
	})
	require.NoError(t, err)
	require.Equal(t, ecstypes.IpcModeTask, descOut.TaskDefinition.IpcMode)
	require.Equal(t, ecstypes.PidModeHost, descOut.TaskDefinition.PidMode)
}

// TestECS_RegisterTaskDefinition_EnableFaultInjection_Echoed proves
// RegisterTaskDefinitionInput.EnableFaultInjection (undeclared before this
// fix) round-trips. Its documented default (false) is Go's zero value, so no
// explicit defaulting code is needed -- this test only proves the explicit
// true case, since the field is dropped either way if never declared.
func TestECS_RegisterTaskDefinition_EnableFaultInjection_Echoed(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestECSClient(t, h)
	ctx := t.Context()

	regOut, err := client.RegisterTaskDefinition(ctx, &ecssdk.RegisterTaskDefinitionInput{
		Family: aws.String("fault-injection-family"),
		ContainerDefinitions: []ecstypes.ContainerDefinition{
			{Name: aws.String("app"), Image: aws.String("nginx:latest")},
		},
		EnableFaultInjection: aws.Bool(true),
	})
	require.NoError(t, err)
	require.NotNil(t, regOut.TaskDefinition.EnableFaultInjection)
	require.True(t, *regOut.TaskDefinition.EnableFaultInjection)
}

// TestECS_RegisterDaemonTaskDefinition_IpcModePidMode_DefaultsToNone proves
// RegisterDaemonTaskDefinitionInput.IpcMode and .PidMode's own doc comments:
// "The default is none." The test omits both fields entirely.
func TestECS_RegisterDaemonTaskDefinition_IpcModePidMode_DefaultsToNone(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	ctx := t.Context()
	client := newTestECSClient(t, h)

	regOut, err := client.RegisterDaemonTaskDefinition(ctx, &ecssdk.RegisterDaemonTaskDefinitionInput{
		Family: aws.String("daemon-ipcpid-default-family"),
		ContainerDefinitions: []ecstypes.DaemonContainerDefinition{
			{Name: aws.String("agent"), Image: aws.String("busybox:latest")},
		},
	})
	require.NoError(t, err)

	descOut, err := client.DescribeDaemonTaskDefinition(ctx, &ecssdk.DescribeDaemonTaskDefinitionInput{
		DaemonTaskDefinition: regOut.DaemonTaskDefinitionArn,
	})
	require.NoError(t, err)
	require.Equal(t, ecstypes.DaemonIpcModeNone, descOut.DaemonTaskDefinition.IpcMode)
	require.Equal(t, ecstypes.DaemonPidModeNone, descOut.DaemonTaskDefinition.PidMode)
}

// TestECS_RegisterDaemonTaskDefinition_IpcModePidMode_ExplicitSharedHonored
// proves an explicit "shared" value is stored and echoed rather than
// silently dropped (the field was entirely undeclared before this fix).
func TestECS_RegisterDaemonTaskDefinition_IpcModePidMode_ExplicitSharedHonored(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	ctx := t.Context()
	client := newTestECSClient(t, h)

	regOut, err := client.RegisterDaemonTaskDefinition(ctx, &ecssdk.RegisterDaemonTaskDefinitionInput{
		Family: aws.String("daemon-ipcpid-shared-family"),
		ContainerDefinitions: []ecstypes.DaemonContainerDefinition{
			{Name: aws.String("agent"), Image: aws.String("busybox:latest")},
		},
		IpcMode: ecstypes.DaemonIpcModeShared,
		PidMode: ecstypes.DaemonPidModeShared,
	})
	require.NoError(t, err)

	descOut, err := client.DescribeDaemonTaskDefinition(ctx, &ecssdk.DescribeDaemonTaskDefinitionInput{
		DaemonTaskDefinition: regOut.DaemonTaskDefinitionArn,
	})
	require.NoError(t, err)
	require.Equal(t, ecstypes.DaemonIpcModeShared, descOut.DaemonTaskDefinition.IpcMode)
	require.Equal(t, ecstypes.DaemonPidModeShared, descOut.DaemonTaskDefinition.PidMode)
}

// TestECS_ListAccountSettings_EffectiveSettings_FallsBackToDefault proves
// ListAccountSettingsInput.EffectiveSettings's own doc comment: "If true, the
// account settings for the root user or the default setting for the
// principalArn are returned." Two settings of the same name are seeded on
// both sides of the distinction -- an account-level default and a
// principal-specific override for a different name -- so the test can tell
// "fell back to default" apart from "returned everything".
func TestECS_ListAccountSettings_EffectiveSettings_FallsBackToDefault(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestECSClient(t, h)
	ctx := t.Context()

	principal := "arn:aws:iam::000000000000:user/effective-settings-user"

	_, err := client.PutAccountSettingDefault(ctx, &ecssdk.PutAccountSettingDefaultInput{
		Name:  ecstypes.SettingNameContainerInsights,
		Value: aws.String("enabled"),
	})
	require.NoError(t, err)

	_, err = client.PutAccountSetting(ctx, &ecssdk.PutAccountSettingInput{
		Name:         ecstypes.SettingNameServiceLongArnFormat,
		Value:        aws.String("enabled"),
		PrincipalArn: aws.String(principal),
	})
	require.NoError(t, err)

	out, err := client.ListAccountSettings(ctx, &ecssdk.ListAccountSettingsInput{
		PrincipalArn:      aws.String(principal),
		EffectiveSettings: true,
	})
	require.NoError(t, err)

	byName := map[string]ecstypes.Setting{}
	for _, s := range out.Settings {
		byName[string(s.Name)] = s
	}

	containerInsights, ok := byName[string(ecstypes.SettingNameContainerInsights)]
	require.True(t, ok, "effective settings must fall back to the account-level default")
	require.Equal(t, "enabled", *containerInsights.Value)

	longArn, ok := byName[string(ecstypes.SettingNameServiceLongArnFormat)]
	require.True(t, ok, "effective settings must still surface the principal's own explicit setting")
	require.Equal(t, "enabled", *longArn.Value)

	// Without effectiveSettings, only the principal's own explicit setting is
	// returned -- the account-level default must NOT leak in.
	falseOut, err := client.ListAccountSettings(ctx, &ecssdk.ListAccountSettingsInput{
		PrincipalArn: aws.String(principal),
	})
	require.NoError(t, err)

	falseNames := map[string]bool{}
	for _, s := range falseOut.Settings {
		falseNames[string(s.Name)] = true
	}

	require.False(t, falseNames[string(ecstypes.SettingNameContainerInsights)],
		"effectiveSettings=false must not fall back to the account-level default")
	require.True(t, falseNames[string(ecstypes.SettingNameServiceLongArnFormat)])
}

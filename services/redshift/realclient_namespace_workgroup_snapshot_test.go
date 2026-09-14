package redshift_test

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	redshiftserverlesssdk "github.com/aws/aws-sdk-go-v2/service/redshiftserverless"
	"github.com/aws/aws-sdk-go-v2/service/redshiftserverless/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/redshift"
)

// TestRealClient_NamespaceLifecycle drives Create/Get/List/Update/DeleteNamespace
// through the real redshiftserverless client (gopherstack-n3zi).
func TestRealClient_NamespaceLifecycle(t *testing.T) {
	t.Parallel()

	h := redshift.NewServerlessHandler(redshift.NewInMemoryBackend("000000000000", rtTestRegion))
	client := newTestServerlessClient(t, h)
	ctx := t.Context()

	created, err := client.CreateNamespace(ctx, &redshiftserverlesssdk.CreateNamespaceInput{
		NamespaceName: aws.String("ns-slice32"),
		AdminUsername: aws.String("admin"),
		DbName:        aws.String("dev"),
		IamRoles:      []string{"arn:aws:iam::000000000000:role/redshift-role"},
		LogExports:    []types.LogExport{types.LogExportUserLog},
		Tags:          []types.Tag{{Key: aws.String("env"), Value: aws.String("test")}},
	})
	require.NoError(t, err)
	require.NotNil(t, created.Namespace)

	ns := created.Namespace
	assert.Equal(t, "ns-slice32", aws.ToString(ns.NamespaceName))
	assert.Equal(t, "admin", aws.ToString(ns.AdminUsername))
	assert.Equal(t, "dev", aws.ToString(ns.DbName))
	assert.Contains(t, aws.ToString(ns.NamespaceArn), "namespace/")
	assert.Equal(t, types.NamespaceStatusAvailable, ns.Status)
	require.Len(t, ns.IamRoles, 1)
	require.Len(t, ns.LogExports, 1)
	assert.Equal(t, types.LogExportUserLog, ns.LogExports[0])

	got, err := client.GetNamespace(ctx, &redshiftserverlesssdk.GetNamespaceInput{
		NamespaceName: aws.String("ns-slice32"),
	})
	require.NoError(t, err)
	assert.Equal(t, aws.ToString(ns.NamespaceArn), aws.ToString(got.Namespace.NamespaceArn))

	listed, err := client.ListNamespaces(ctx, &redshiftserverlesssdk.ListNamespacesInput{})
	require.NoError(t, err)
	require.Len(t, listed.Namespaces, 1)
	assert.Equal(t, "ns-slice32", aws.ToString(listed.Namespaces[0].NamespaceName))

	updated, err := client.UpdateNamespace(ctx, &redshiftserverlesssdk.UpdateNamespaceInput{
		NamespaceName: aws.String("ns-slice32"),
		AdminUsername: aws.String("newadmin"),
	})
	require.NoError(t, err)
	assert.Equal(t, "newadmin", aws.ToString(updated.Namespace.AdminUsername))

	deleted, err := client.DeleteNamespace(ctx, &redshiftserverlesssdk.DeleteNamespaceInput{
		NamespaceName: aws.String("ns-slice32"),
	})
	require.NoError(t, err)
	assert.Equal(t, "ns-slice32", aws.ToString(deleted.Namespace.NamespaceName))

	_, err = client.GetNamespace(ctx, &redshiftserverlesssdk.GetNamespaceInput{
		NamespaceName: aws.String("ns-slice32"),
	})
	require.Error(t, err)

	var notFound *types.ResourceNotFoundException
	require.ErrorAs(t, err, &notFound)
}

// TestRealClient_WorkgroupLifecycle drives Create/Get/List/Update/DeleteWorkgroup.
func TestRealClient_WorkgroupLifecycle(t *testing.T) {
	t.Parallel()

	h := redshift.NewServerlessHandler(redshift.NewInMemoryBackend("000000000000", rtTestRegion))
	client := newTestServerlessClient(t, h)
	ctx := t.Context()

	_, err := client.CreateNamespace(ctx, &redshiftserverlesssdk.CreateNamespaceInput{
		NamespaceName: aws.String("wg-ns"),
	})
	require.NoError(t, err)

	created, err := client.CreateWorkgroup(ctx, &redshiftserverlesssdk.CreateWorkgroupInput{
		WorkgroupName: aws.String("wg-slice32"),
		NamespaceName: aws.String("wg-ns"),
		BaseCapacity:  aws.Int32(64),
	})
	require.NoError(t, err)
	require.NotNil(t, created.Workgroup)

	wg := created.Workgroup
	assert.Equal(t, "wg-slice32", aws.ToString(wg.WorkgroupName))
	assert.Equal(t, "wg-ns", aws.ToString(wg.NamespaceName))
	assert.EqualValues(t, 64, aws.ToInt32(wg.BaseCapacity))
	assert.Contains(t, aws.ToString(wg.WorkgroupArn), "workgroup/")
	assert.NotEmpty(t, aws.ToString(wg.Endpoint.Address))

	got, err := client.GetWorkgroup(ctx, &redshiftserverlesssdk.GetWorkgroupInput{
		WorkgroupName: aws.String("wg-slice32"),
	})
	require.NoError(t, err)
	assert.Equal(t, aws.ToString(wg.WorkgroupArn), aws.ToString(got.Workgroup.WorkgroupArn))

	listed, err := client.ListWorkgroups(ctx, &redshiftserverlesssdk.ListWorkgroupsInput{})
	require.NoError(t, err)
	require.Len(t, listed.Workgroups, 1)
	assert.Equal(t, "wg-slice32", aws.ToString(listed.Workgroups[0].WorkgroupName))

	updated, err := client.UpdateWorkgroup(ctx, &redshiftserverlesssdk.UpdateWorkgroupInput{
		WorkgroupName: aws.String("wg-slice32"),
		BaseCapacity:  aws.Int32(128),
	})
	require.NoError(t, err)
	assert.EqualValues(t, 128, aws.ToInt32(updated.Workgroup.BaseCapacity))

	deleted, err := client.DeleteWorkgroup(ctx, &redshiftserverlesssdk.DeleteWorkgroupInput{
		WorkgroupName: aws.String("wg-slice32"),
	})
	require.NoError(t, err)
	assert.Equal(t, "wg-slice32", aws.ToString(deleted.Workgroup.WorkgroupName))

	_, err = client.GetWorkgroup(ctx, &redshiftserverlesssdk.GetWorkgroupInput{
		WorkgroupName: aws.String("wg-slice32"),
	})
	require.Error(t, err)

	var notFound *types.ResourceNotFoundException
	require.ErrorAs(t, err, &notFound)
}

// TestRealClient_GetCredentials proves GetCredentials returns a decoded
// DbUser/DbPassword/Expiration/NextRefreshTime that a real client can read.
func TestRealClient_GetCredentials(t *testing.T) {
	t.Parallel()

	h := redshift.NewServerlessHandler(redshift.NewInMemoryBackend("000000000000", rtTestRegion))
	client := newTestServerlessClient(t, h)
	ctx := t.Context()

	_, err := client.CreateNamespace(ctx, &redshiftserverlesssdk.CreateNamespaceInput{
		NamespaceName: aws.String("cred-ns"),
		DbName:        aws.String("mydb"),
	})
	require.NoError(t, err)

	_, err = client.CreateWorkgroup(ctx, &redshiftserverlesssdk.CreateWorkgroupInput{
		WorkgroupName: aws.String("cred-wg"),
		NamespaceName: aws.String("cred-ns"),
	})
	require.NoError(t, err)

	before := time.Now()

	out, err := client.GetCredentials(ctx, &redshiftserverlesssdk.GetCredentialsInput{
		WorkgroupName:   aws.String("cred-wg"),
		DurationSeconds: aws.Int32(1800),
	})
	require.NoError(t, err)
	assert.Contains(t, aws.ToString(out.DbUser), "mydb")
	assert.NotEmpty(t, aws.ToString(out.DbPassword))
	require.NotNil(t, out.Expiration)
	require.NotNil(t, out.NextRefreshTime)
	assert.WithinDuration(t, before.Add(1800*time.Second), *out.Expiration, 5*time.Second)
	assert.True(t, out.NextRefreshTime.Before(*out.Expiration))
}

// TestRealClient_SnapshotLifecycle drives Create/Get/List/Update/DeleteSnapshot.
func TestRealClient_SnapshotLifecycle(t *testing.T) {
	t.Parallel()

	h := redshift.NewServerlessHandler(redshift.NewInMemoryBackend("000000000000", rtTestRegion))
	client := newTestServerlessClient(t, h)
	ctx := t.Context()

	_, err := client.CreateNamespace(ctx, &redshiftserverlesssdk.CreateNamespaceInput{
		NamespaceName: aws.String("snap-ns"),
		AdminUsername: aws.String("admin"),
	})
	require.NoError(t, err)

	created, err := client.CreateSnapshot(ctx, &redshiftserverlesssdk.CreateSnapshotInput{
		SnapshotName:    aws.String("snap-slice32"),
		NamespaceName:   aws.String("snap-ns"),
		RetentionPeriod: aws.Int32(7),
	})
	require.NoError(t, err)
	require.NotNil(t, created.Snapshot)

	snap := created.Snapshot
	assert.Equal(t, "snap-slice32", aws.ToString(snap.SnapshotName))
	assert.Equal(t, "snap-ns", aws.ToString(snap.NamespaceName))
	assert.Equal(t, "admin", aws.ToString(snap.AdminUsername))
	assert.EqualValues(t, 7, aws.ToInt32(snap.SnapshotRetentionPeriod))
	assert.Equal(t, types.SnapshotStatusAvailable, snap.Status)

	got, err := client.GetSnapshot(ctx, &redshiftserverlesssdk.GetSnapshotInput{
		SnapshotName: aws.String("snap-slice32"),
	})
	require.NoError(t, err)
	assert.Equal(t, aws.ToString(snap.SnapshotArn), aws.ToString(got.Snapshot.SnapshotArn))

	listed, err := client.ListSnapshots(ctx, &redshiftserverlesssdk.ListSnapshotsInput{
		NamespaceName: aws.String("snap-ns"),
	})
	require.NoError(t, err)
	require.Len(t, listed.Snapshots, 1)
	assert.Equal(t, "snap-slice32", aws.ToString(listed.Snapshots[0].SnapshotName))

	updated, err := client.UpdateSnapshot(ctx, &redshiftserverlesssdk.UpdateSnapshotInput{
		SnapshotName:    aws.String("snap-slice32"),
		RetentionPeriod: aws.Int32(14),
	})
	require.NoError(t, err)
	assert.EqualValues(t, 14, aws.ToInt32(updated.Snapshot.SnapshotRetentionPeriod))

	deleted, err := client.DeleteSnapshot(ctx, &redshiftserverlesssdk.DeleteSnapshotInput{
		SnapshotName: aws.String("snap-slice32"),
	})
	require.NoError(t, err)
	assert.Equal(t, "snap-slice32", aws.ToString(deleted.Snapshot.SnapshotName))

	_, err = client.GetSnapshot(ctx, &redshiftserverlesssdk.GetSnapshotInput{
		SnapshotName: aws.String("snap-slice32"),
	})
	require.Error(t, err)

	var notFound *types.ResourceNotFoundException
	require.ErrorAs(t, err, &notFound)
}

// TestRealClient_UsageLimitGetListUpdate covers Get/List/UpdateUsageLimit
// (Create/Delete are already typed-covered elsewhere).
func TestRealClient_UsageLimitGetListUpdate(t *testing.T) {
	t.Parallel()

	h := redshift.NewServerlessHandler(redshift.NewInMemoryBackend("000000000000", rtTestRegion))
	client := newTestServerlessClient(t, h)
	ctx := t.Context()

	resourceArn := "arn:aws:redshift-serverless:us-east-1:000000000000:namespace/fake"

	created, err := client.CreateUsageLimit(ctx, &redshiftserverlesssdk.CreateUsageLimitInput{
		ResourceArn:  aws.String(resourceArn),
		UsageType:    types.UsageLimitUsageTypeServerlessCompute,
		Amount:       aws.Int64(100),
		Period:       types.UsageLimitPeriodMonthly,
		BreachAction: types.UsageLimitBreachActionLog,
	})
	require.NoError(t, err)
	require.NotNil(t, created.UsageLimit)

	id := created.UsageLimit.UsageLimitId

	got, err := client.GetUsageLimit(
		ctx,
		&redshiftserverlesssdk.GetUsageLimitInput{UsageLimitId: id},
	)
	require.NoError(t, err)
	assert.Equal(t, resourceArn, aws.ToString(got.UsageLimit.ResourceArn))
	assert.EqualValues(t, 100, aws.ToInt64(got.UsageLimit.Amount))
	assert.Equal(t, types.UsageLimitPeriodMonthly, got.UsageLimit.Period)

	listed, err := client.ListUsageLimits(ctx, &redshiftserverlesssdk.ListUsageLimitsInput{
		ResourceArn: aws.String(resourceArn),
	})
	require.NoError(t, err)
	require.Len(t, listed.UsageLimits, 1)
	assert.Equal(t, aws.ToString(id), aws.ToString(listed.UsageLimits[0].UsageLimitId))

	updated, err := client.UpdateUsageLimit(ctx, &redshiftserverlesssdk.UpdateUsageLimitInput{
		UsageLimitId: id,
		Amount:       aws.Int64(500),
		BreachAction: types.UsageLimitBreachActionDeactivate,
	})
	require.NoError(t, err)
	assert.EqualValues(t, 500, aws.ToInt64(updated.UsageLimit.Amount))
	assert.Equal(t, types.UsageLimitBreachActionDeactivate, updated.UsageLimit.BreachAction)

	_, err = client.GetUsageLimit(ctx, &redshiftserverlesssdk.GetUsageLimitInput{
		UsageLimitId: aws.String("no-such-limit"),
	})
	require.Error(t, err)

	var notFound *types.ResourceNotFoundException
	require.ErrorAs(t, err, &notFound)
}

// TestRealClient_ScheduledActionGetListUpdate covers Get/List/UpdateScheduledAction.
// ListScheduledActions asserts the real ScheduledActionAssociation summary
// shape (only namespaceName/scheduledActionName) -- this test caught and
// locked a real bug: the handler previously echoed the full
// ScheduledActionResponse object into the list, which is not the wire shape
// (aws-sdk-go-v2/service/redshiftserverless@v1.38.5/deserializers.go:10298
// awsAwsjson11_deserializeDocumentScheduledActionAssociation only reads
// "namespaceName"/"scheduledActionName").
func TestRealClient_ScheduledActionGetListUpdate(t *testing.T) {
	t.Parallel()

	h := redshift.NewServerlessHandler(redshift.NewInMemoryBackend("000000000000", rtTestRegion))
	client := newTestServerlessClient(t, h)
	ctx := t.Context()

	_, err := client.CreateNamespace(ctx, &redshiftserverlesssdk.CreateNamespaceInput{
		NamespaceName: aws.String("sa-ns"),
	})
	require.NoError(t, err)

	created, err := client.CreateScheduledAction(
		ctx,
		&redshiftserverlesssdk.CreateScheduledActionInput{
			ScheduledActionName: aws.String("slice32-action"),
			NamespaceName:       aws.String("sa-ns"),
			RoleArn:             aws.String("arn:aws:iam::000000000000:role/scheduler"),
			Schedule: &types.ScheduleMemberCron{
				Value: "(0 10 ? * MON *)",
			},
			TargetAction: &types.TargetActionMemberCreateSnapshot{
				Value: types.CreateSnapshotScheduleActionParameters{
					NamespaceName:      aws.String("sa-ns"),
					SnapshotNamePrefix: aws.String("scheduled-"),
				},
			},
		},
	)
	require.NoError(t, err)
	require.NotNil(t, created.ScheduledAction)
	assert.Equal(t, "slice32-action", aws.ToString(created.ScheduledAction.ScheduledActionName))

	got, err := client.GetScheduledAction(ctx, &redshiftserverlesssdk.GetScheduledActionInput{
		ScheduledActionName: aws.String("slice32-action"),
	})
	require.NoError(t, err)
	assert.Equal(t, "sa-ns", aws.ToString(got.ScheduledAction.NamespaceName))
	assert.Equal(t, types.StateActive, got.ScheduledAction.State)

	listed, err := client.ListScheduledActions(
		ctx,
		&redshiftserverlesssdk.ListScheduledActionsInput{
			NamespaceName: aws.String("sa-ns"),
		},
	)
	require.NoError(t, err)
	require.Len(t, listed.ScheduledActions, 1)
	assert.Equal(t, "slice32-action", aws.ToString(listed.ScheduledActions[0].ScheduledActionName))
	assert.Equal(t, "sa-ns", aws.ToString(listed.ScheduledActions[0].NamespaceName))

	updated, err := client.UpdateScheduledAction(
		ctx,
		&redshiftserverlesssdk.UpdateScheduledActionInput{
			ScheduledActionName: aws.String("slice32-action"),
			Enabled:             aws.Bool(false),
		},
	)
	require.NoError(t, err)
	assert.Equal(t, types.StateDisabled, updated.ScheduledAction.State)

	_, err = client.GetScheduledAction(ctx, &redshiftserverlesssdk.GetScheduledActionInput{
		ScheduledActionName: aws.String("no-such-action"),
	})
	require.Error(t, err)

	var notFound *types.ResourceNotFoundException
	require.ErrorAs(t, err, &notFound)
}

// TestRealClient_CustomDomainGetListUpdate covers Get/List/UpdateCustomDomainAssociation
// (Create/Delete already typed-covered elsewhere).
func TestRealClient_CustomDomainGetListUpdate(t *testing.T) {
	t.Parallel()

	h := redshift.NewServerlessHandler(redshift.NewInMemoryBackend("000000000000", rtTestRegion))
	client := newTestServerlessClient(t, h)
	ctx := t.Context()

	_, err := client.CreateNamespace(ctx, &redshiftserverlesssdk.CreateNamespaceInput{
		NamespaceName: aws.String("cd-ns"),
	})
	require.NoError(t, err)

	_, err = client.CreateWorkgroup(ctx, &redshiftserverlesssdk.CreateWorkgroupInput{
		WorkgroupName: aws.String("cd-wg"),
		NamespaceName: aws.String("cd-ns"),
	})
	require.NoError(t, err)

	certArn := "arn:aws:acm:us-east-1:000000000000:certificate/abc123"

	_, err = client.CreateCustomDomainAssociation(
		ctx,
		&redshiftserverlesssdk.CreateCustomDomainAssociationInput{
			CustomDomainName:           aws.String("db.example.com"),
			CustomDomainCertificateArn: aws.String(certArn),
			WorkgroupName:              aws.String("cd-wg"),
		},
	)
	require.NoError(t, err)

	got, err := client.GetCustomDomainAssociation(
		ctx,
		&redshiftserverlesssdk.GetCustomDomainAssociationInput{
			CustomDomainName: aws.String("db.example.com"),
			WorkgroupName:    aws.String("cd-wg"),
		},
	)
	require.NoError(t, err)
	assert.Equal(t, certArn, aws.ToString(got.CustomDomainCertificateArn))
	assert.Equal(t, "cd-wg", aws.ToString(got.WorkgroupName))
	require.NotNil(t, got.CustomDomainCertificateExpiryTime)

	listed, err := client.ListCustomDomainAssociations(
		ctx,
		&redshiftserverlesssdk.ListCustomDomainAssociationsInput{},
	)
	require.NoError(t, err)
	require.Len(t, listed.Associations, 1)
	assert.Equal(t, "db.example.com", aws.ToString(listed.Associations[0].CustomDomainName))

	newCertArn := "arn:aws:acm:us-east-1:000000000000:certificate/def456"

	updated, err := client.UpdateCustomDomainAssociation(
		ctx,
		&redshiftserverlesssdk.UpdateCustomDomainAssociationInput{
			CustomDomainName:           aws.String("db.example.com"),
			WorkgroupName:              aws.String("cd-wg"),
			CustomDomainCertificateArn: aws.String(newCertArn),
		},
	)
	require.NoError(t, err)
	assert.Equal(t, newCertArn, aws.ToString(updated.CustomDomainCertificateArn))

	_, err = client.GetCustomDomainAssociation(
		ctx,
		&redshiftserverlesssdk.GetCustomDomainAssociationInput{
			CustomDomainName: aws.String("no-such-domain"),
			WorkgroupName:    aws.String("cd-wg"),
		},
	)
	require.Error(t, err)

	var notFound *types.ResourceNotFoundException
	require.ErrorAs(t, err, &notFound)
}

// TestRealClient_EndpointAccessGetListUpdate covers Get/List/UpdateEndpointAccess
// (Create/Delete already typed-covered elsewhere), and asserts
// VpcSecurityGroups decodes as the real {status,vpcSecurityGroupId} list.
func TestRealClient_EndpointAccessGetListUpdate(t *testing.T) {
	t.Parallel()

	h := redshift.NewServerlessHandler(redshift.NewInMemoryBackend("000000000000", rtTestRegion))
	client := newTestServerlessClient(t, h)
	ctx := t.Context()

	_, err := client.CreateNamespace(ctx, &redshiftserverlesssdk.CreateNamespaceInput{
		NamespaceName: aws.String("ep-ns"),
	})
	require.NoError(t, err)

	_, err = client.CreateWorkgroup(ctx, &redshiftserverlesssdk.CreateWorkgroupInput{
		WorkgroupName: aws.String("ep-wg"),
		NamespaceName: aws.String("ep-ns"),
	})
	require.NoError(t, err)

	created, err := client.CreateEndpointAccess(
		ctx,
		&redshiftserverlesssdk.CreateEndpointAccessInput{
			EndpointName:        aws.String("ep-slice32"),
			WorkgroupName:       aws.String("ep-wg"),
			SubnetIds:           []string{"subnet-1", "subnet-2"},
			VpcSecurityGroupIds: []string{"sg-1"},
		},
	)
	require.NoError(t, err)
	require.NotNil(t, created.Endpoint)
	require.Len(t, created.Endpoint.VpcSecurityGroups, 1)
	assert.Equal(t, "sg-1", aws.ToString(created.Endpoint.VpcSecurityGroups[0].VpcSecurityGroupId))
	assert.Equal(t, "active", aws.ToString(created.Endpoint.VpcSecurityGroups[0].Status))

	got, err := client.GetEndpointAccess(ctx, &redshiftserverlesssdk.GetEndpointAccessInput{
		EndpointName: aws.String("ep-slice32"),
	})
	require.NoError(t, err)
	assert.Equal(t, "ep-wg", aws.ToString(got.Endpoint.WorkgroupName))
	assert.ElementsMatch(t, []string{"subnet-1", "subnet-2"}, got.Endpoint.SubnetIds)

	listed, err := client.ListEndpointAccess(ctx, &redshiftserverlesssdk.ListEndpointAccessInput{
		WorkgroupName: aws.String("ep-wg"),
	})
	require.NoError(t, err)
	require.Len(t, listed.Endpoints, 1)
	assert.Equal(t, "ep-slice32", aws.ToString(listed.Endpoints[0].EndpointName))

	updated, err := client.UpdateEndpointAccess(
		ctx,
		&redshiftserverlesssdk.UpdateEndpointAccessInput{
			EndpointName:        aws.String("ep-slice32"),
			VpcSecurityGroupIds: []string{"sg-2", "sg-3"},
		},
	)
	require.NoError(t, err)
	require.Len(t, updated.Endpoint.VpcSecurityGroups, 2)

	_, err = client.GetEndpointAccess(ctx, &redshiftserverlesssdk.GetEndpointAccessInput{
		EndpointName: aws.String("no-such-endpoint"),
	})
	require.Error(t, err)

	var notFound *types.ResourceNotFoundException
	require.ErrorAs(t, err, &notFound)
}

// TestRealClient_ListManagedWorkgroups proves the honest-empty-list shape decodes
// cleanly through the real client (this backend has no Glue/Lake Formation
// integration to ever populate one, see ManagedWorkgroupListItem's doc comment).
func TestRealClient_ListManagedWorkgroups(t *testing.T) {
	t.Parallel()

	h := redshift.NewServerlessHandler(redshift.NewInMemoryBackend("000000000000", rtTestRegion))
	client := newTestServerlessClient(t, h)

	out, err := client.ListManagedWorkgroups(
		t.Context(),
		&redshiftserverlesssdk.ListManagedWorkgroupsInput{},
	)
	require.NoError(t, err)
	assert.Empty(t, out.ManagedWorkgroups)
	assert.Nil(t, out.NextToken)
}

// TestRealClient_SnapshotCopyConfigLifecycle covers Create/Update/Delete/List
// SnapshotCopyConfiguration.
func TestRealClient_SnapshotCopyConfigLifecycle(t *testing.T) {
	t.Parallel()

	h := redshift.NewServerlessHandler(redshift.NewInMemoryBackend("000000000000", rtTestRegion))
	client := newTestServerlessClient(t, h)
	ctx := t.Context()

	_, err := client.CreateNamespace(ctx, &redshiftserverlesssdk.CreateNamespaceInput{
		NamespaceName: aws.String("scc-ns"),
	})
	require.NoError(t, err)

	created, err := client.CreateSnapshotCopyConfiguration(
		ctx,
		&redshiftserverlesssdk.CreateSnapshotCopyConfigurationInput{
			NamespaceName:           aws.String("scc-ns"),
			DestinationRegion:       aws.String("us-west-2"),
			SnapshotRetentionPeriod: aws.Int32(7),
		},
	)
	require.NoError(t, err)
	require.NotNil(t, created.SnapshotCopyConfiguration)

	cfg := created.SnapshotCopyConfiguration
	assert.Equal(t, "scc-ns", aws.ToString(cfg.NamespaceName))
	assert.Equal(t, "us-west-2", aws.ToString(cfg.DestinationRegion))
	assert.EqualValues(t, 7, aws.ToInt32(cfg.SnapshotRetentionPeriod))
	assert.Contains(t, aws.ToString(cfg.SnapshotCopyConfigurationArn), "snapshotcopyconfiguration/")

	listed, err := client.ListSnapshotCopyConfigurations(
		ctx,
		&redshiftserverlesssdk.ListSnapshotCopyConfigurationsInput{
			NamespaceName: aws.String("scc-ns"),
		},
	)
	require.NoError(t, err)
	require.Len(t, listed.SnapshotCopyConfigurations, 1)

	updated, err := client.UpdateSnapshotCopyConfiguration(
		ctx,
		&redshiftserverlesssdk.UpdateSnapshotCopyConfigurationInput{
			SnapshotCopyConfigurationId: cfg.SnapshotCopyConfigurationId,
			SnapshotRetentionPeriod:     aws.Int32(30),
		},
	)
	require.NoError(t, err)
	assert.EqualValues(
		t,
		30,
		aws.ToInt32(updated.SnapshotCopyConfiguration.SnapshotRetentionPeriod),
	)

	deleted, err := client.DeleteSnapshotCopyConfiguration(
		ctx,
		&redshiftserverlesssdk.DeleteSnapshotCopyConfigurationInput{
			SnapshotCopyConfigurationId: cfg.SnapshotCopyConfigurationId,
		},
	)
	require.NoError(t, err)
	assert.Equal(
		t,
		aws.ToString(cfg.SnapshotCopyConfigurationId),
		aws.ToString(deleted.SnapshotCopyConfiguration.SnapshotCopyConfigurationId),
	)

	listed, err = client.ListSnapshotCopyConfigurations(
		ctx,
		&redshiftserverlesssdk.ListSnapshotCopyConfigurationsInput{
			NamespaceName: aws.String("scc-ns"),
		},
	)
	require.NoError(t, err)
	assert.Empty(t, listed.SnapshotCopyConfigurations)
}

// TestRealClient_RecoveryPointsAndRestore covers Get/ListRecoveryPoint and
// RestoreFromRecoveryPoint. A recovery point is generated automatically at
// CreateWorkgroup time (see generateRecoveryPointLocked's doc comment) --
// there is no CreateRecoveryPoint operation in the real API.
func TestRealClient_RecoveryPointsAndRestore(t *testing.T) {
	t.Parallel()

	h := redshift.NewServerlessHandler(redshift.NewInMemoryBackend("000000000000", rtTestRegion))
	client := newTestServerlessClient(t, h)
	ctx := t.Context()

	_, err := client.CreateNamespace(ctx, &redshiftserverlesssdk.CreateNamespaceInput{
		NamespaceName: aws.String("rp-ns"),
	})
	require.NoError(t, err)

	_, err = client.CreateWorkgroup(ctx, &redshiftserverlesssdk.CreateWorkgroupInput{
		WorkgroupName: aws.String("rp-wg"),
		NamespaceName: aws.String("rp-ns"),
	})
	require.NoError(t, err)

	listed, err := client.ListRecoveryPoints(ctx, &redshiftserverlesssdk.ListRecoveryPointsInput{
		NamespaceName: aws.String("rp-ns"),
	})
	require.NoError(t, err)
	require.Len(t, listed.RecoveryPoints, 1)

	rp := listed.RecoveryPoints[0]
	assert.Equal(t, "rp-ns", aws.ToString(rp.NamespaceName))
	assert.Equal(t, "rp-wg", aws.ToString(rp.WorkgroupName))
	require.NotNil(t, rp.RecoveryPointCreateTime)

	got, err := client.GetRecoveryPoint(ctx, &redshiftserverlesssdk.GetRecoveryPointInput{
		RecoveryPointId: rp.RecoveryPointId,
	})
	require.NoError(t, err)
	assert.Equal(
		t,
		aws.ToString(rp.RecoveryPointId),
		aws.ToString(got.RecoveryPoint.RecoveryPointId),
	)

	restored, err := client.RestoreFromRecoveryPoint(
		ctx,
		&redshiftserverlesssdk.RestoreFromRecoveryPointInput{
			NamespaceName:   aws.String("rp-ns"),
			WorkgroupName:   aws.String("rp-wg"),
			RecoveryPointId: rp.RecoveryPointId,
		},
	)
	require.NoError(t, err)
	assert.Equal(t, "rp-ns", aws.ToString(restored.Namespace.NamespaceName))
	assert.Equal(t, aws.ToString(rp.RecoveryPointId), aws.ToString(restored.RecoveryPointId))

	_, err = client.GetRecoveryPoint(ctx, &redshiftserverlesssdk.GetRecoveryPointInput{
		RecoveryPointId: aws.String("no-such-recovery-point"),
	})
	require.Error(t, err)

	var notFound *types.ResourceNotFoundException
	require.ErrorAs(t, err, &notFound)
}

// TestRealClient_RestoreFromSnapshot covers RestoreFromSnapshot.
func TestRealClient_RestoreFromSnapshot(t *testing.T) {
	t.Parallel()

	h := redshift.NewServerlessHandler(redshift.NewInMemoryBackend("000000000000", rtTestRegion))
	client := newTestServerlessClient(t, h)
	ctx := t.Context()

	_, err := client.CreateNamespace(ctx, &redshiftserverlesssdk.CreateNamespaceInput{
		NamespaceName: aws.String("rfs-ns"),
	})
	require.NoError(t, err)

	_, err = client.CreateWorkgroup(ctx, &redshiftserverlesssdk.CreateWorkgroupInput{
		WorkgroupName: aws.String("rfs-wg"),
		NamespaceName: aws.String("rfs-ns"),
	})
	require.NoError(t, err)

	_, err = client.CreateSnapshot(ctx, &redshiftserverlesssdk.CreateSnapshotInput{
		SnapshotName:  aws.String("rfs-snap"),
		NamespaceName: aws.String("rfs-ns"),
	})
	require.NoError(t, err)

	out, err := client.RestoreFromSnapshot(ctx, &redshiftserverlesssdk.RestoreFromSnapshotInput{
		NamespaceName: aws.String("rfs-ns"),
		WorkgroupName: aws.String("rfs-wg"),
		SnapshotName:  aws.String("rfs-snap"),
		OwnerAccount:  aws.String("000000000000"),
	})
	require.NoError(t, err)
	require.NotNil(t, out.Namespace)
	assert.Equal(t, "rfs-ns", aws.ToString(out.Namespace.NamespaceName))
	assert.Equal(t, "rfs-snap", aws.ToString(out.SnapshotName))
	assert.Equal(t, "000000000000", aws.ToString(out.OwnerAccount))

	_, err = client.RestoreFromSnapshot(ctx, &redshiftserverlesssdk.RestoreFromSnapshotInput{
		NamespaceName: aws.String("rfs-ns"),
		WorkgroupName: aws.String("rfs-wg"),
		SnapshotName:  aws.String("no-such-snapshot"),
	})
	require.Error(t, err)

	var notFound *types.ResourceNotFoundException
	require.ErrorAs(t, err, &notFound)
}

// TestRealClient_TableRestoreLifecycle covers RestoreTableFromSnapshot,
// RestoreTableFromRecoveryPoint, GetTableRestoreStatus, ListTableRestoreStatus.
func TestRealClient_TableRestoreLifecycle(t *testing.T) {
	t.Parallel()

	h := redshift.NewServerlessHandler(redshift.NewInMemoryBackend("000000000000", rtTestRegion))
	client := newTestServerlessClient(t, h)
	ctx := t.Context()

	_, err := client.CreateNamespace(ctx, &redshiftserverlesssdk.CreateNamespaceInput{
		NamespaceName: aws.String("tr-ns"),
	})
	require.NoError(t, err)

	_, err = client.CreateWorkgroup(ctx, &redshiftserverlesssdk.CreateWorkgroupInput{
		WorkgroupName: aws.String("tr-wg"),
		NamespaceName: aws.String("tr-ns"),
	})
	require.NoError(t, err)

	_, err = client.CreateSnapshot(ctx, &redshiftserverlesssdk.CreateSnapshotInput{
		SnapshotName:  aws.String("tr-snap"),
		NamespaceName: aws.String("tr-ns"),
	})
	require.NoError(t, err)

	fromSnap, err := client.RestoreTableFromSnapshot(
		ctx,
		&redshiftserverlesssdk.RestoreTableFromSnapshotInput{
			NamespaceName:      aws.String("tr-ns"),
			WorkgroupName:      aws.String("tr-wg"),
			SnapshotName:       aws.String("tr-snap"),
			NewTableName:       aws.String("orders_restored"),
			SourceDatabaseName: aws.String("dev"),
			SourceTableName:    aws.String("orders"),
		},
	)
	require.NoError(t, err)
	require.NotNil(t, fromSnap.TableRestoreStatus)
	assert.Equal(t, "orders_restored", aws.ToString(fromSnap.TableRestoreStatus.NewTableName))
	assert.Equal(t, "SUCCEEDED", aws.ToString(fromSnap.TableRestoreStatus.Status))
	require.NotNil(t, fromSnap.TableRestoreStatus.RequestTime)

	rpList, err := client.ListRecoveryPoints(ctx, &redshiftserverlesssdk.ListRecoveryPointsInput{
		NamespaceName: aws.String("tr-ns"),
	})
	require.NoError(t, err)
	require.NotEmpty(t, rpList.RecoveryPoints)
	recoveryPointID := rpList.RecoveryPoints[0].RecoveryPointId

	fromRP, err := client.RestoreTableFromRecoveryPoint(
		ctx,
		&redshiftserverlesssdk.RestoreTableFromRecoveryPointInput{
			NamespaceName:      aws.String("tr-ns"),
			WorkgroupName:      aws.String("tr-wg"),
			RecoveryPointId:    recoveryPointID,
			NewTableName:       aws.String("orders_from_rp"),
			SourceDatabaseName: aws.String("dev"),
			SourceTableName:    aws.String("orders"),
		},
	)
	require.NoError(t, err)
	assert.Equal(t, "orders_from_rp", aws.ToString(fromRP.TableRestoreStatus.NewTableName))

	got, err := client.GetTableRestoreStatus(ctx, &redshiftserverlesssdk.GetTableRestoreStatusInput{
		TableRestoreRequestId: fromSnap.TableRestoreStatus.TableRestoreRequestId,
	})
	require.NoError(t, err)
	assert.Equal(t, "orders_restored", aws.ToString(got.TableRestoreStatus.NewTableName))

	listed, err := client.ListTableRestoreStatus(
		ctx,
		&redshiftserverlesssdk.ListTableRestoreStatusInput{
			NamespaceName: aws.String("tr-ns"),
		},
	)
	require.NoError(t, err)
	require.Len(t, listed.TableRestoreStatuses, 2)

	_, err = client.GetTableRestoreStatus(ctx, &redshiftserverlesssdk.GetTableRestoreStatusInput{
		TableRestoreRequestId: aws.String("no-such-request"),
	})
	require.Error(t, err)

	var notFound *types.ResourceNotFoundException
	require.ErrorAs(t, err, &notFound)
}

// TestRealClient_ConvertRecoveryPointToSnapshot covers
// ConvertRecoveryPointToSnapshot.
func TestRealClient_ConvertRecoveryPointToSnapshot(t *testing.T) {
	t.Parallel()

	h := redshift.NewServerlessHandler(redshift.NewInMemoryBackend("000000000000", rtTestRegion))
	client := newTestServerlessClient(t, h)
	ctx := t.Context()

	_, err := client.CreateNamespace(ctx, &redshiftserverlesssdk.CreateNamespaceInput{
		NamespaceName: aws.String("crp-ns"),
		AdminUsername: aws.String("admin"),
	})
	require.NoError(t, err)

	_, err = client.CreateWorkgroup(ctx, &redshiftserverlesssdk.CreateWorkgroupInput{
		WorkgroupName: aws.String("crp-wg"),
		NamespaceName: aws.String("crp-ns"),
	})
	require.NoError(t, err)

	rpList, err := client.ListRecoveryPoints(ctx, &redshiftserverlesssdk.ListRecoveryPointsInput{
		NamespaceName: aws.String("crp-ns"),
	})
	require.NoError(t, err)
	require.NotEmpty(t, rpList.RecoveryPoints)

	out, err := client.ConvertRecoveryPointToSnapshot(
		ctx,
		&redshiftserverlesssdk.ConvertRecoveryPointToSnapshotInput{
			RecoveryPointId: rpList.RecoveryPoints[0].RecoveryPointId,
			SnapshotName:    aws.String("converted-snap"),
			RetentionPeriod: aws.Int32(3),
		},
	)
	require.NoError(t, err)
	require.NotNil(t, out.Snapshot)
	assert.Equal(t, "converted-snap", aws.ToString(out.Snapshot.SnapshotName))
	assert.Equal(t, "crp-ns", aws.ToString(out.Snapshot.NamespaceName))
	assert.Equal(t, "admin", aws.ToString(out.Snapshot.AdminUsername))
	assert.EqualValues(t, 3, aws.ToInt32(out.Snapshot.SnapshotRetentionPeriod))

	_, err = client.ConvertRecoveryPointToSnapshot(
		ctx,
		&redshiftserverlesssdk.ConvertRecoveryPointToSnapshotInput{
			RecoveryPointId: aws.String("no-such-recovery-point"),
			SnapshotName:    aws.String("other-snap"),
		},
	)
	require.Error(t, err)

	var notFound *types.ResourceNotFoundException
	require.ErrorAs(t, err, &notFound)
}

// TestRealClient_Tracks covers GetTrack and ListTracks against the fixed
// current/trailing catalog.
func TestRealClient_Tracks(t *testing.T) {
	t.Parallel()

	h := redshift.NewServerlessHandler(redshift.NewInMemoryBackend("000000000000", rtTestRegion))
	client := newTestServerlessClient(t, h)
	ctx := t.Context()

	got, err := client.GetTrack(ctx, &redshiftserverlesssdk.GetTrackInput{
		TrackName: aws.String("current"),
	})
	require.NoError(t, err)
	require.NotNil(t, got.Track)
	assert.Equal(t, "current", aws.ToString(got.Track.TrackName))
	assert.NotEmpty(t, aws.ToString(got.Track.WorkgroupVersion))

	listed, err := client.ListTracks(ctx, &redshiftserverlesssdk.ListTracksInput{})
	require.NoError(t, err)
	require.Len(t, listed.Tracks, 2)

	names := []string{
		aws.ToString(listed.Tracks[0].TrackName),
		aws.ToString(listed.Tracks[1].TrackName),
	}
	assert.ElementsMatch(t, []string{"current", "trailing"}, names)

	_, err = client.GetTrack(ctx, &redshiftserverlesssdk.GetTrackInput{
		TrackName: aws.String("no-such-track"),
	})
	require.Error(t, err)

	var notFound *types.ResourceNotFoundException
	require.ErrorAs(t, err, &notFound)
}

// TestRealClient_UpdateLakehouseConfiguration covers UpdateLakehouseConfiguration,
// including its flat (non-enveloped) response shape and DryRun behavior.
func TestRealClient_UpdateLakehouseConfiguration(t *testing.T) {
	t.Parallel()

	h := redshift.NewServerlessHandler(redshift.NewInMemoryBackend("000000000000", rtTestRegion))
	client := newTestServerlessClient(t, h)
	ctx := t.Context()

	_, err := client.CreateNamespace(ctx, &redshiftserverlesssdk.CreateNamespaceInput{
		NamespaceName: aws.String("lh-ns"),
	})
	require.NoError(t, err)

	out, err := client.UpdateLakehouseConfiguration(
		ctx,
		&redshiftserverlesssdk.UpdateLakehouseConfigurationInput{
			NamespaceName:         aws.String("lh-ns"),
			CatalogName:           aws.String("my-catalog"),
			LakehouseRegistration: types.LakehouseRegistrationRegister,
		},
	)
	require.NoError(t, err)
	assert.Equal(t, "lh-ns", aws.ToString(out.NamespaceName))
	assert.Contains(t, aws.ToString(out.CatalogArn), "catalog/my-catalog")
	assert.Equal(t, "Registered", aws.ToString(out.LakehouseRegistrationStatus))

	_, err = client.UpdateLakehouseConfiguration(
		ctx,
		&redshiftserverlesssdk.UpdateLakehouseConfigurationInput{
			NamespaceName: aws.String("lh-ns"),
			CatalogName:   aws.String("other-catalog"),
			DryRun:        aws.Bool(true),
		},
	)
	require.Error(t, err)

	var dryRun *types.DryRunException
	require.ErrorAs(t, err, &dryRun)

	unchanged, err := client.GetNamespace(ctx, &redshiftserverlesssdk.GetNamespaceInput{
		NamespaceName: aws.String("lh-ns"),
	})
	require.NoError(t, err)
	assert.Contains(t, aws.ToString(unchanged.Namespace.CatalogArn), "my-catalog",
		"DryRun must not mutate state")
}

// TestRealClient_ResourceTags covers TagResource, UntagResource and
// ListTagsForResource against a real namespace ARN.
func TestRealClient_ResourceTags(t *testing.T) {
	t.Parallel()

	h := redshift.NewServerlessHandler(redshift.NewInMemoryBackend("000000000000", rtTestRegion))
	client := newTestServerlessClient(t, h)
	ctx := t.Context()

	created, err := client.CreateNamespace(ctx, &redshiftserverlesssdk.CreateNamespaceInput{
		NamespaceName: aws.String("tag-ns"),
	})
	require.NoError(t, err)

	resourceArn := aws.ToString(created.Namespace.NamespaceArn)

	_, err = client.TagResource(ctx, &redshiftserverlesssdk.TagResourceInput{
		ResourceArn: aws.String(resourceArn),
		Tags: []types.Tag{
			{Key: aws.String("team"), Value: aws.String("data")},
			{Key: aws.String("env"), Value: aws.String("prod")},
		},
	})
	require.NoError(t, err)

	listed, err := client.ListTagsForResource(ctx, &redshiftserverlesssdk.ListTagsForResourceInput{
		ResourceArn: aws.String(resourceArn),
	})
	require.NoError(t, err)
	require.Len(t, listed.Tags, 2)

	got := map[string]string{}
	for _, tag := range listed.Tags {
		got[aws.ToString(tag.Key)] = aws.ToString(tag.Value)
	}
	assert.Equal(t, map[string]string{"team": "data", "env": "prod"}, got)

	_, err = client.UntagResource(ctx, &redshiftserverlesssdk.UntagResourceInput{
		ResourceArn: aws.String(resourceArn),
		TagKeys:     []string{"env"},
	})
	require.NoError(t, err)

	listed, err = client.ListTagsForResource(ctx, &redshiftserverlesssdk.ListTagsForResourceInput{
		ResourceArn: aws.String(resourceArn),
	})
	require.NoError(t, err)
	require.Len(t, listed.Tags, 1)
	assert.Equal(t, "team", aws.ToString(listed.Tags[0].Key))

	_, err = client.TagResource(ctx, &redshiftserverlesssdk.TagResourceInput{
		ResourceArn: aws.String(
			"arn:aws:redshift-serverless:us-east-1:000000000000:namespace/no-such",
		),
		Tags: []types.Tag{{Key: aws.String("k"), Value: aws.String("v")}},
	})
	require.Error(t, err)

	var notFound *types.ResourceNotFoundException
	require.ErrorAs(t, err, &notFound)
}

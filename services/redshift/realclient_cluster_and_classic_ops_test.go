package redshift_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	redshiftsdk "github.com/aws/aws-sdk-go-v2/service/redshift"
	"github.com/aws/aws-sdk-go-v2/service/redshift/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/redshift"
)

// TestRealClient_ClusterAndClassicOps covers redshift's highest-priority typed-
// client-uncovered classic-Redshift op families (gopherstack-n3zi):
// cluster lifecycle, parameter/subnet/security groups, snapshots and copy
// grants/schedules, event subscriptions, HSM, usage limits, scheduled
// actions, reserved nodes, tags, endpoint access, data shares, partners,
// redshift-idc/qev2-idc applications, integrations, authentication
// profiles, resource policy, custom domains and misc account/cluster info.
// Redshift Serverless ops (CreateNamespace/CreateWorkgroup/... -- a
// distinct real AWS service and SDK module, served by this package's
// separate ServerlessHandler) are out of scope: not named in this task's
// priority families and already covered by serverless_*_test.go.
// Each subtest creates real state through the typed aws-sdk-go-v2 client
// (or, for setup-only steps whose own op is already typed-covered, directly
// via the backend) and asserts decoded response values.
func TestRealClient_ClusterAndClassicOps(t *testing.T) {
	t.Parallel()

	cases := []struct {
		fn   func(t *testing.T)
		name string
	}{
		{testSlice5ClusterLifecycleRealClient, "cluster_lifecycle"},
		{testSlice5ParamSubnetSecurityGroupsRealClient, "param_subnet_security_groups"},
		{testSlice5SnapshotsCopyRealClient, "snapshots_copy"},
		{testSlice5EventSubscriptionsRealClient, "event_subscriptions"},
		{testSlice5HSMRealClient, "hsm"},
		{testSlice5UsageLimitsScheduledActionsRealClient, "usage_limits_scheduled_actions"},
		{testSlice5ReservedNodesRealClient, "reserved_nodes"},
		{testSlice5TagsRealClient, "tags"},
		{testSlice5EndpointAccessRealClient, "endpoint_access"},
		{testSlice5DataSharesRealClient, "data_shares"},
		{testSlice5PartnersRealClient, "partners"},
		{testSlice5IdcApplicationsRealClient, "idc_applications"},
		{testSlice5IntegrationsRealClient, "integrations"},
		{testSlice5AuthProfilesRealClient, "auth_profiles"},
		{testSlice5ResourcePolicyCustomDomainRealClient, "resource_policy_custom_domain"},
		{testSlice5AccountClusterInfoRealClient, "account_cluster_info"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.fn(t)
		})
	}
}

func newSlice5RedshiftBackendAndClient(t *testing.T) (*redshift.InMemoryBackend, *redshiftsdk.Client) {
	t.Helper()

	backend := redshift.NewInMemoryBackend("000000000000", rtTestRegion)
	client := newTestRedshiftClient(t, redshift.NewHandler(backend))

	return backend, client
}

func testSlice5ClusterLifecycleRealClient(t *testing.T) {
	t.Helper()

	backend, client := newSlice5RedshiftBackendAndClient(t)
	ctx := t.Context()

	_, err := backend.CreateCluster("slice5-c1", "dc2.large", "dev", "admin", nil, "")
	require.NoError(t, err)

	modified, err := client.ModifyCluster(ctx, &redshiftsdk.ModifyClusterInput{
		ClusterIdentifier: aws.String("slice5-c1"),
		NumberOfNodes:     aws.Int32(1),
		NodeType:          aws.String("dc2.large"),
	})
	require.NoError(t, err)
	require.NotNil(t, modified.Cluster)
	assert.Equal(t, "slice5-c1", aws.ToString(modified.Cluster.ClusterIdentifier))

	roleArn := "arn:aws:iam::000000000000:role/redshift-role"

	iamModified, err := client.ModifyClusterIamRoles(ctx, &redshiftsdk.ModifyClusterIamRolesInput{
		ClusterIdentifier: aws.String("slice5-c1"),
		AddIamRoles:       []string{roleArn},
	})
	require.NoError(t, err)
	require.NotNil(t, iamModified.Cluster)
	require.Len(t, iamModified.Cluster.IamRoles, 1)
	assert.Equal(t, roleArn, aws.ToString(iamModified.Cluster.IamRoles[0].IamRoleArn))

	maintModified, err := client.ModifyClusterMaintenance(ctx, &redshiftsdk.ModifyClusterMaintenanceInput{
		ClusterIdentifier:          aws.String("slice5-c1"),
		DeferMaintenance:           aws.Bool(true),
		DeferMaintenanceDuration:   aws.Int32(1),
		DeferMaintenanceIdentifier: aws.String("slice5-deferral"),
	})
	require.NoError(t, err)
	require.NotNil(t, maintModified.Cluster)

	aquaModified, err := client.ModifyAquaConfiguration(ctx, &redshiftsdk.ModifyAquaConfigurationInput{
		ClusterIdentifier: aws.String("slice5-c1"),
	})
	require.NoError(t, err)
	assert.Equal(t, types.AquaStatusDisabled, aquaModified.AquaConfiguration.AquaStatus)

	rebooted, err := client.RebootCluster(ctx, &redshiftsdk.RebootClusterInput{
		ClusterIdentifier: aws.String("slice5-c1"),
	})
	require.NoError(t, err)
	require.NotNil(t, rebooted.Cluster)

	resized, err := client.ResizeCluster(ctx, &redshiftsdk.ResizeClusterInput{
		ClusterIdentifier: aws.String("slice5-c1"),
		NodeType:          aws.String("dc2.large"),
		NumberOfNodes:     aws.Int32(2),
	})
	require.NoError(t, err)
	require.NotNil(t, resized.Cluster)

	resizeInfo, err := client.DescribeResize(ctx, &redshiftsdk.DescribeResizeInput{
		ClusterIdentifier: aws.String("slice5-c1"),
	})
	require.NoError(t, err)
	assert.NotEmpty(t, aws.ToString(resizeInfo.Status))

	// This backend applies ResizeCluster synchronously (cluster_mgmt.go), so
	// by the time a real client's CancelResize round-trips, the resize has
	// already completed and AllowCancelResize is correctly false -- matching
	// real AWS's rejection of cancelling an already-finished resize. Still
	// proves the InvalidClusterState error shape decodes correctly.
	_, err = client.CancelResize(ctx, &redshiftsdk.CancelResizeInput{
		ClusterIdentifier: aws.String("slice5-c1"),
	})
	require.Error(t, err)

	var invalidState *types.InvalidClusterStateFault
	require.ErrorAs(t, err, &invalidState)

	paused, err := client.PauseCluster(ctx, &redshiftsdk.PauseClusterInput{
		ClusterIdentifier: aws.String("slice5-c1"),
	})
	require.NoError(t, err)
	require.NotNil(t, paused.Cluster)

	resumed, err := client.ResumeCluster(ctx, &redshiftsdk.ResumeClusterInput{
		ClusterIdentifier: aws.String("slice5-c1"),
	})
	require.NoError(t, err)
	require.NotNil(t, resumed.Cluster)

	rotated, err := client.RotateEncryptionKey(ctx, &redshiftsdk.RotateEncryptionKeyInput{
		ClusterIdentifier: aws.String("slice5-c1"),
	})
	require.NoError(t, err)
	require.NotNil(t, rotated.Cluster)

	snap, err := backend.CreateClusterSnapshot("slice5-snap1", "slice5-c1")
	require.NoError(t, err)
	_ = snap

	restored, err := client.RestoreFromClusterSnapshot(ctx, &redshiftsdk.RestoreFromClusterSnapshotInput{
		ClusterIdentifier:  aws.String("slice5-c2"),
		SnapshotIdentifier: aws.String("slice5-snap1"),
	})
	require.NoError(t, err)
	require.NotNil(t, restored.Cluster)
	assert.Equal(t, "slice5-c2", aws.ToString(restored.Cluster.ClusterIdentifier))

	_, err = client.DeleteCluster(ctx, &redshiftsdk.DeleteClusterInput{
		ClusterIdentifier:        aws.String("slice5-c1"),
		SkipFinalClusterSnapshot: aws.Bool(true),
	})
	require.NoError(t, err)
}

func testSlice5ParamSubnetSecurityGroupsRealClient(t *testing.T) {
	t.Helper()

	backend, client := newSlice5RedshiftBackendAndClient(t)
	ctx := t.Context()

	_, err := backend.CreateClusterParameterGroup("slice5-pg", "redshift-1.0", "test param group")
	require.NoError(t, err)

	pgList, err := client.DescribeClusterParameterGroups(ctx, &redshiftsdk.DescribeClusterParameterGroupsInput{
		ParameterGroupName: aws.String("slice5-pg"),
	})
	require.NoError(t, err)
	require.Len(t, pgList.ParameterGroups, 1)
	assert.Equal(t, "redshift-1.0", aws.ToString(pgList.ParameterGroups[0].ParameterGroupFamily))

	defaults, err := client.DescribeDefaultClusterParameters(ctx, &redshiftsdk.DescribeDefaultClusterParametersInput{
		ParameterGroupFamily: aws.String("redshift-1.0"),
	})
	require.NoError(t, err)
	assert.Equal(t, "redshift-1.0", aws.ToString(defaults.DefaultClusterParameters.ParameterGroupFamily))

	_, err = client.ResetClusterParameterGroup(ctx, &redshiftsdk.ResetClusterParameterGroupInput{
		ParameterGroupName: aws.String("slice5-pg"),
		ResetAllParameters: aws.Bool(true),
	})
	require.NoError(t, err)

	_, err = client.DeleteClusterParameterGroup(ctx, &redshiftsdk.DeleteClusterParameterGroupInput{
		ParameterGroupName: aws.String("slice5-pg"),
	})
	require.NoError(t, err)

	_, err = backend.CreateClusterSubnetGroup(
		"slice5-subnetgrp", "test subnet group", "vpc-1", []string{"subnet-1", "subnet-2"}, nil,
	)
	require.NoError(t, err)

	subnetModified, err := client.ModifyClusterSubnetGroup(ctx, &redshiftsdk.ModifyClusterSubnetGroupInput{
		ClusterSubnetGroupName: aws.String("slice5-subnetgrp"),
		Description:            aws.String("updated subnet group"),
		SubnetIds:              []string{"subnet-1"},
	})
	require.NoError(t, err)
	assert.Equal(t, "updated subnet group", aws.ToString(subnetModified.ClusterSubnetGroup.Description))

	_, err = client.DeleteClusterSubnetGroup(ctx, &redshiftsdk.DeleteClusterSubnetGroupInput{
		ClusterSubnetGroupName: aws.String("slice5-subnetgrp"),
	})
	require.NoError(t, err)

	_, err = backend.CreateClusterSecurityGroup("slice5-secgrp", "test security group", nil)
	require.NoError(t, err)

	_, err = client.DeleteClusterSecurityGroup(ctx, &redshiftsdk.DeleteClusterSecurityGroupInput{
		ClusterSecurityGroupName: aws.String("slice5-secgrp"),
	})
	require.NoError(t, err)
}

func testSlice5SnapshotsCopyRealClient(t *testing.T) {
	t.Helper()

	backend, client := newSlice5RedshiftBackendAndClient(t)
	ctx := t.Context()

	_, err := backend.CreateCluster("slice5-snapc", "dc2.large", "dev", "admin", nil, "")
	require.NoError(t, err)

	_, err = backend.CreateClusterSnapshot("slice5-snap-src", "slice5-snapc")
	require.NoError(t, err)

	copied, err := client.CopyClusterSnapshot(ctx, &redshiftsdk.CopyClusterSnapshotInput{
		SourceSnapshotIdentifier: aws.String("slice5-snap-src"),
		TargetSnapshotIdentifier: aws.String("slice5-snap-copy"),
	})
	require.NoError(t, err)
	require.NotNil(t, copied.Snapshot)
	assert.Equal(t, "slice5-snap-copy", aws.ToString(copied.Snapshot.SnapshotIdentifier))

	_, err = client.DeleteClusterSnapshot(ctx, &redshiftsdk.DeleteClusterSnapshotInput{
		SnapshotIdentifier: aws.String("slice5-snap-copy"),
	})
	require.NoError(t, err)

	_, err = client.EnableSnapshotCopy(ctx, &redshiftsdk.EnableSnapshotCopyInput{
		ClusterIdentifier: aws.String("slice5-snapc"),
		DestinationRegion: aws.String("us-west-2"),
	})
	require.NoError(t, err)

	_, err = client.ModifySnapshotCopyRetentionPeriod(ctx, &redshiftsdk.ModifySnapshotCopyRetentionPeriodInput{
		ClusterIdentifier: aws.String("slice5-snapc"),
		RetentionPeriod:   aws.Int32(5),
	})
	require.NoError(t, err)

	_, err = client.DisableSnapshotCopy(ctx, &redshiftsdk.DisableSnapshotCopyInput{
		ClusterIdentifier: aws.String("slice5-snapc"),
	})
	require.NoError(t, err)

	_, err = backend.CreateSnapshotCopyGrant("slice5-copygrant", "kms-key-1", nil)
	require.NoError(t, err)

	_, err = client.DeleteSnapshotCopyGrant(ctx, &redshiftsdk.DeleteSnapshotCopyGrantInput{
		SnapshotCopyGrantName: aws.String("slice5-copygrant"),
	})
	require.NoError(t, err)

	_, err = backend.CreateSnapshotSchedule("slice5-schedule", "test schedule", []string{"rate(12 hours)"}, nil)
	require.NoError(t, err)

	scheduleModified, err := client.ModifySnapshotSchedule(ctx, &redshiftsdk.ModifySnapshotScheduleInput{
		ScheduleIdentifier:  aws.String("slice5-schedule"),
		ScheduleDefinitions: []string{"rate(6 hours)"},
	})
	require.NoError(t, err)
	assert.Equal(t, []string{"rate(6 hours)"}, scheduleModified.ScheduleDefinitions)

	_, err = client.DeleteSnapshotSchedule(ctx, &redshiftsdk.DeleteSnapshotScheduleInput{
		ScheduleIdentifier: aws.String("slice5-schedule"),
	})
	require.NoError(t, err)
}

func testSlice5EventSubscriptionsRealClient(t *testing.T) {
	t.Helper()

	_, client := newSlice5RedshiftBackendAndClient(t)
	ctx := t.Context()

	created, err := client.CreateEventSubscription(ctx, &redshiftsdk.CreateEventSubscriptionInput{
		SubscriptionName: aws.String("slice5-sub"),
		SnsTopicArn:      aws.String("arn:aws:sns:us-east-1:000000000000:topic1"),
		SourceType:       aws.String("cluster"),
		Severity:         aws.String("INFO"),
		Enabled:          aws.Bool(true),
	})
	require.NoError(t, err)
	require.NotNil(t, created.EventSubscription)
	assert.Equal(t, "slice5-sub", aws.ToString(created.EventSubscription.CustSubscriptionId))

	modified, err := client.ModifyEventSubscription(ctx, &redshiftsdk.ModifyEventSubscriptionInput{
		SubscriptionName: aws.String("slice5-sub"),
		Severity:         aws.String("ERROR"),
	})
	require.NoError(t, err)
	assert.Equal(t, "ERROR", aws.ToString(modified.EventSubscription.Severity))

	subs, err := client.DescribeEventSubscriptions(ctx, &redshiftsdk.DescribeEventSubscriptionsInput{
		SubscriptionName: aws.String("slice5-sub"),
	})
	require.NoError(t, err)
	require.Len(t, subs.EventSubscriptionsList, 1)

	_, err = client.DescribeEvents(ctx, &redshiftsdk.DescribeEventsInput{})
	require.NoError(t, err)

	_, err = client.DeleteEventSubscription(ctx, &redshiftsdk.DeleteEventSubscriptionInput{
		SubscriptionName: aws.String("slice5-sub"),
	})
	require.NoError(t, err)
}

func testSlice5HSMRealClient(t *testing.T) {
	t.Helper()

	backend, client := newSlice5RedshiftBackendAndClient(t)
	ctx := t.Context()

	_, err := backend.CreateHsmClientCertificate("slice5-hsmcert", nil)
	require.NoError(t, err)

	_, err = client.DeleteHsmClientCertificate(ctx, &redshiftsdk.DeleteHsmClientCertificateInput{
		HsmClientCertificateIdentifier: aws.String("slice5-hsmcert"),
	})
	require.NoError(t, err)

	_, err = backend.CreateHsmConfiguration("slice5-hsmcfg", "test hsm config", "10.0.0.1", "partition1", nil)
	require.NoError(t, err)

	_, err = client.DeleteHsmConfiguration(ctx, &redshiftsdk.DeleteHsmConfigurationInput{
		HsmConfigurationIdentifier: aws.String("slice5-hsmcfg"),
	})
	require.NoError(t, err)
}

func testSlice5UsageLimitsScheduledActionsRealClient(t *testing.T) {
	t.Helper()

	backend, client := newSlice5RedshiftBackendAndClient(t)
	ctx := t.Context()

	_, err := backend.CreateCluster("slice5-ulc", "dc2.large", "dev", "admin", nil, "")
	require.NoError(t, err)

	ul, err := backend.CreateUsageLimit("slice5-ulc", "concurrency-scaling", "time", "log", 60, nil)
	require.NoError(t, err)

	modified, err := client.ModifyUsageLimit(ctx, &redshiftsdk.ModifyUsageLimitInput{
		UsageLimitId: aws.String(ul.UsageLimitID),
		Amount:       aws.Int64(120),
	})
	require.NoError(t, err)
	assert.Equal(t, int64(120), aws.ToInt64(modified.Amount))

	_, err = client.DeleteUsageLimit(ctx, &redshiftsdk.DeleteUsageLimitInput{
		UsageLimitId: aws.String(ul.UsageLimitID),
	})
	require.NoError(t, err)

	_, err = backend.CreateScheduledAction(
		"slice5-schedaction", "at(2030-01-01T00:00:00)", "arn:aws:iam::000000000000:role/redshift-role",
		"test scheduled action", nil, aws.Bool(true),
	)
	require.NoError(t, err)

	actionModified, err := client.ModifyScheduledAction(ctx, &redshiftsdk.ModifyScheduledActionInput{
		ScheduledActionName:        aws.String("slice5-schedaction"),
		ScheduledActionDescription: aws.String("updated scheduled action"),
	})
	require.NoError(t, err)
	assert.Equal(t, "updated scheduled action", aws.ToString(actionModified.ScheduledActionDescription))

	_, err = client.DeleteScheduledAction(ctx, &redshiftsdk.DeleteScheduledActionInput{
		ScheduledActionName: aws.String("slice5-schedaction"),
	})
	require.NoError(t, err)
}

func testSlice5ReservedNodesRealClient(t *testing.T) {
	t.Helper()

	backend, client := newSlice5RedshiftBackendAndClient(t)
	ctx := t.Context()

	backend.AddReservedNodeInternal(&redshift.ReservedNode{
		ReservedNodeID:         "slice5-rn1",
		ReservedNodeOfferingID: "slice5-offering1",
		NodeType:               "dc2.large",
		State:                  "active",
		CurrencyCode:           "USD",
		OfferingType:           "All Upfront",
		NodeCount:              1,
	})

	nodes, err := client.DescribeReservedNodes(ctx, &redshiftsdk.DescribeReservedNodesInput{
		ReservedNodeId: aws.String("slice5-rn1"),
	})
	require.NoError(t, err)
	require.Len(t, nodes.ReservedNodes, 1)
	assert.Equal(t, "dc2.large", aws.ToString(nodes.ReservedNodes[0].NodeType))

	offerings, err := client.GetReservedNodeExchangeOfferings(ctx, &redshiftsdk.GetReservedNodeExchangeOfferingsInput{
		ReservedNodeId: aws.String("slice5-rn1"),
	})
	require.NoError(t, err)
	require.NotEmpty(t, offerings.ReservedNodeOfferings)

	targetOfferingID := aws.ToString(offerings.ReservedNodeOfferings[0].ReservedNodeOfferingId)

	exchangeOpts, err := client.GetReservedNodeExchangeConfigurationOptions(
		ctx, &redshiftsdk.GetReservedNodeExchangeConfigurationOptionsInput{
			ActionType: types.ReservedNodeExchangeActionTypeRestoreCluster,
		},
	)
	require.NoError(t, err)
	assert.NotNil(t, exchangeOpts)

	exchanged, err := client.AcceptReservedNodeExchange(ctx, &redshiftsdk.AcceptReservedNodeExchangeInput{
		ReservedNodeId:               aws.String("slice5-rn1"),
		TargetReservedNodeOfferingId: aws.String(targetOfferingID),
	})
	require.NoError(t, err)
	require.NotNil(t, exchanged.ExchangedReservedNode)
	assert.Equal(t, targetOfferingID, aws.ToString(exchanged.ExchangedReservedNode.ReservedNodeOfferingId))
}

func testSlice5TagsRealClient(t *testing.T) {
	t.Helper()

	backend, client := newSlice5RedshiftBackendAndClient(t)
	ctx := t.Context()

	cluster, err := backend.CreateCluster("slice5-tagc", "dc2.large", "dev", "admin", nil, "")
	require.NoError(t, err)

	resourceArn := "arn:aws:redshift:" + rtTestRegion + ":000000000000:cluster:" + cluster.ClusterIdentifier

	_, err = client.CreateTags(ctx, &redshiftsdk.CreateTagsInput{
		ResourceName: aws.String(resourceArn),
		Tags:         []types.Tag{{Key: aws.String("env"), Value: aws.String("test")}},
	})
	require.NoError(t, err)

	tagsOut, err := client.DescribeTags(ctx, &redshiftsdk.DescribeTagsInput{ResourceName: aws.String(resourceArn)})
	require.NoError(t, err)
	require.Len(t, tagsOut.TaggedResources, 1)
	assert.Equal(t, "env", aws.ToString(tagsOut.TaggedResources[0].Tag.Key))

	_, err = client.DeleteTags(ctx, &redshiftsdk.DeleteTagsInput{
		ResourceName: aws.String(resourceArn),
		TagKeys:      []string{"env"},
	})
	require.NoError(t, err)

	afterDelete, err := client.DescribeTags(ctx, &redshiftsdk.DescribeTagsInput{ResourceName: aws.String(resourceArn)})
	require.NoError(t, err)
	assert.Empty(t, afterDelete.TaggedResources)
}

func testSlice5EndpointAccessRealClient(t *testing.T) {
	t.Helper()

	backend, client := newSlice5RedshiftBackendAndClient(t)
	ctx := t.Context()

	_, err := backend.CreateCluster("slice5-epc", "dc2.large", "dev", "admin", nil, "")
	require.NoError(t, err)

	_, err = backend.CreateEndpointAccess("slice5-epc", "slice5-endpoint", "", "", nil)
	require.NoError(t, err)

	_, err = client.AuthorizeEndpointAccess(ctx, &redshiftsdk.AuthorizeEndpointAccessInput{
		ClusterIdentifier: aws.String("slice5-epc"),
		Account:           aws.String("111111111111"),
	})
	require.NoError(t, err)

	modified, err := client.ModifyEndpointAccess(ctx, &redshiftsdk.ModifyEndpointAccessInput{
		EndpointName:        aws.String("slice5-endpoint"),
		VpcSecurityGroupIds: []string{"sg-1"},
	})
	require.NoError(t, err)
	require.Len(t, modified.VpcSecurityGroups, 1)
	assert.Equal(t, "sg-1", aws.ToString(modified.VpcSecurityGroups[0].VpcSecurityGroupId))

	_, err = client.RevokeEndpointAccess(ctx, &redshiftsdk.RevokeEndpointAccessInput{
		ClusterIdentifier: aws.String("slice5-epc"),
		Account:           aws.String("111111111111"),
	})
	require.NoError(t, err)

	_, err = client.DeleteEndpointAccess(ctx, &redshiftsdk.DeleteEndpointAccessInput{
		EndpointName: aws.String("slice5-endpoint"),
	})
	require.NoError(t, err)
}

func testSlice5DataSharesRealClient(t *testing.T) {
	t.Helper()

	backend, client := newSlice5RedshiftBackendAndClient(t)
	ctx := t.Context()

	arn := "arn:aws:redshift:us-east-1:000000000000:datashare:ns-1/slice5-share"

	backend.AddDataShareInternal(&redshift.DataShare{
		DataShareArn:  arn,
		ProducerArn:   "arn:aws:redshift:us-east-1:000000000000:namespace:ns-1",
		DataShareType: "INTERNAL",
	})

	authorized, err := client.AuthorizeDataShare(ctx, &redshiftsdk.AuthorizeDataShareInput{
		DataShareArn:       aws.String(arn),
		ConsumerIdentifier: aws.String("222222222222"),
	})
	require.NoError(t, err)
	require.NotNil(t, authorized.DataShareAssociations)
	require.Len(t, authorized.DataShareAssociations, 1)
	assert.Equal(t, "222222222222", aws.ToString(authorized.DataShareAssociations[0].ConsumerIdentifier))

	associated, err := client.AssociateDataShareConsumer(ctx, &redshiftsdk.AssociateDataShareConsumerInput{
		DataShareArn: aws.String(arn),
		ConsumerArn:  aws.String("arn:aws:redshift:us-east-1:222222222222:namespace:ns-2"),
	})
	require.NoError(t, err)
	require.Len(t, associated.DataShareAssociations, 2)

	forProducer, err := client.DescribeDataSharesForProducer(ctx, &redshiftsdk.DescribeDataSharesForProducerInput{})
	require.NoError(t, err)
	require.NotEmpty(t, forProducer.DataShares)

	forConsumer, err := client.DescribeDataSharesForConsumer(ctx, &redshiftsdk.DescribeDataSharesForConsumerInput{})
	require.NoError(t, err)
	require.NotEmpty(t, forConsumer.DataShares)

	_, err = client.DisassociateDataShareConsumer(ctx, &redshiftsdk.DisassociateDataShareConsumerInput{
		DataShareArn: aws.String(arn),
		ConsumerArn:  aws.String("arn:aws:redshift:us-east-1:222222222222:namespace:ns-2"),
	})
	require.NoError(t, err)

	_, err = client.DeauthorizeDataShare(ctx, &redshiftsdk.DeauthorizeDataShareInput{
		DataShareArn:       aws.String(arn),
		ConsumerIdentifier: aws.String("222222222222"),
	})
	require.NoError(t, err)

	backend.AddDataShareInternal(&redshift.DataShare{
		DataShareArn:  arn + "-reject",
		ProducerArn:   "arn:aws:redshift:us-east-1:000000000000:namespace:ns-1",
		DataShareType: "INTERNAL",
	})

	_, err = client.RejectDataShare(ctx, &redshiftsdk.RejectDataShareInput{
		DataShareArn: aws.String(arn + "-reject"),
	})
	require.NoError(t, err)
}

func testSlice5PartnersRealClient(t *testing.T) {
	t.Helper()

	backend, client := newSlice5RedshiftBackendAndClient(t)
	ctx := t.Context()

	_, err := backend.CreateCluster("slice5-partnerc", "dc2.large", "dev", "admin", nil, "")
	require.NoError(t, err)

	added, err := client.AddPartner(ctx, &redshiftsdk.AddPartnerInput{
		AccountId:         aws.String("000000000000"),
		ClusterIdentifier: aws.String("slice5-partnerc"),
		DatabaseName:      aws.String("dev"),
		PartnerName:       aws.String("slice5-partner"),
	})
	require.NoError(t, err)
	assert.Equal(t, "slice5-partner", aws.ToString(added.PartnerName))

	updated, err := client.UpdatePartnerStatus(ctx, &redshiftsdk.UpdatePartnerStatusInput{
		AccountId:         aws.String("000000000000"),
		ClusterIdentifier: aws.String("slice5-partnerc"),
		DatabaseName:      aws.String("dev"),
		PartnerName:       aws.String("slice5-partner"),
		Status:            types.PartnerIntegrationStatusRuntimeFailure,
		StatusMessage:     aws.String("test failure"),
	})
	require.NoError(t, err)
	assert.Equal(t, "slice5-partner", aws.ToString(updated.PartnerName))

	described, err := client.DescribePartners(ctx, &redshiftsdk.DescribePartnersInput{
		AccountId:         aws.String("000000000000"),
		ClusterIdentifier: aws.String("slice5-partnerc"),
	})
	require.NoError(t, err)
	require.Len(t, described.PartnerIntegrationInfoList, 1)
	assert.Equal(t, types.PartnerIntegrationStatusRuntimeFailure, described.PartnerIntegrationInfoList[0].Status)

	_, err = client.DeletePartner(ctx, &redshiftsdk.DeletePartnerInput{
		AccountId:         aws.String("000000000000"),
		ClusterIdentifier: aws.String("slice5-partnerc"),
		DatabaseName:      aws.String("dev"),
		PartnerName:       aws.String("slice5-partner"),
	})
	require.NoError(t, err)
}

func testSlice5IdcApplicationsRealClient(t *testing.T) {
	t.Helper()

	_, client := newSlice5RedshiftBackendAndClient(t)
	ctx := t.Context()

	qev2Created, err := client.CreateQev2IdcApplication(ctx, &redshiftsdk.CreateQev2IdcApplicationInput{
		Qev2IdcApplicationName: aws.String("slice5-qev2"),
		IdcInstanceArn:         aws.String("arn:aws:sso:::instance/ssoins-slice5"),
		IdcDisplayName:         aws.String("slice5 qev2 app"),
	})
	require.NoError(t, err)
	require.NotNil(t, qev2Created.Qev2IdcApplication)
	assert.Equal(t, "slice5-qev2", aws.ToString(qev2Created.Qev2IdcApplication.Qev2IdcApplicationName))

	qev2Arn := qev2Created.Qev2IdcApplication.Qev2IdcApplicationArn

	qev2Modified, err := client.ModifyQev2IdcApplication(ctx, &redshiftsdk.ModifyQev2IdcApplicationInput{
		Qev2IdcApplicationArn: qev2Arn,
		IdcDisplayName:        aws.String("slice5 qev2 app updated"),
	})
	require.NoError(t, err)
	require.NotNil(t, qev2Modified.Qev2IdcApplication)
	assert.Equal(t, "slice5 qev2 app updated", aws.ToString(qev2Modified.Qev2IdcApplication.IdcDisplayName))

	qev2Described, err := client.DescribeQev2IdcApplications(ctx, &redshiftsdk.DescribeQev2IdcApplicationsInput{
		Qev2IdcApplicationArn: qev2Arn,
	})
	require.NoError(t, err)
	require.Len(t, qev2Described.Qev2IdcApplications, 1)

	_, err = client.DeleteQev2IdcApplication(ctx, &redshiftsdk.DeleteQev2IdcApplicationInput{
		Qev2IdcApplicationArn: qev2Arn,
	})
	require.NoError(t, err)

	idcCreated, err := client.CreateRedshiftIdcApplication(ctx, &redshiftsdk.CreateRedshiftIdcApplicationInput{
		IdcInstanceArn:             aws.String("arn:aws:sso:::instance/ssoins-slice5"),
		RedshiftIdcApplicationName: aws.String("slice5-idc"),
		IdcDisplayName:             aws.String("slice5 idc app"),
		IamRoleArn:                 aws.String("arn:aws:iam::000000000000:role/redshift-idc-role"),
	})
	require.NoError(t, err)
	assert.Equal(t, "slice5-idc", aws.ToString(idcCreated.RedshiftIdcApplication.RedshiftIdcApplicationName))

	idcModified, err := client.ModifyRedshiftIdcApplication(ctx, &redshiftsdk.ModifyRedshiftIdcApplicationInput{
		RedshiftIdcApplicationArn: idcCreated.RedshiftIdcApplication.RedshiftIdcApplicationArn,
		IdcDisplayName:            aws.String("slice5 idc app updated"),
	})
	require.NoError(t, err)
	assert.Equal(t, "slice5 idc app updated", aws.ToString(idcModified.RedshiftIdcApplication.IdcDisplayName))

	idcDescribed, err := client.DescribeRedshiftIdcApplications(ctx, &redshiftsdk.DescribeRedshiftIdcApplicationsInput{
		RedshiftIdcApplicationArn: idcCreated.RedshiftIdcApplication.RedshiftIdcApplicationArn,
	})
	require.NoError(t, err)
	require.Len(t, idcDescribed.RedshiftIdcApplications, 1)

	_, err = client.DeleteRedshiftIdcApplication(ctx, &redshiftsdk.DeleteRedshiftIdcApplicationInput{
		RedshiftIdcApplicationArn: idcCreated.RedshiftIdcApplication.RedshiftIdcApplicationArn,
	})
	require.NoError(t, err)
}

func testSlice5IntegrationsRealClient(t *testing.T) {
	t.Helper()

	backend, client := newSlice5RedshiftBackendAndClient(t)
	ctx := t.Context()

	sourceArn := "arn:aws:rds:us-east-1:000000000000:db:src"
	targetArn := "arn:aws:redshift-serverless:us-east-1:000000000000:namespace/ns-1"

	ig, err := backend.CreateIntegration("slice5-integ", sourceArn, targetArn, "", "test integration", nil)
	require.NoError(t, err)

	modified, err := client.ModifyIntegration(ctx, &redshiftsdk.ModifyIntegrationInput{
		IntegrationArn: aws.String(ig.IntegrationArn),
		Description:    aws.String("updated integration"),
	})
	require.NoError(t, err)
	assert.Equal(t, "updated integration", aws.ToString(modified.Description))

	_, err = client.DeleteIntegration(ctx, &redshiftsdk.DeleteIntegrationInput{
		IntegrationArn: aws.String(ig.IntegrationArn),
	})
	require.NoError(t, err)
}

func testSlice5AuthProfilesRealClient(t *testing.T) {
	t.Helper()

	_, client := newSlice5RedshiftBackendAndClient(t)
	ctx := t.Context()

	created, err := client.CreateAuthenticationProfile(ctx, &redshiftsdk.CreateAuthenticationProfileInput{
		AuthenticationProfileName:    aws.String("slice5-authprofile"),
		AuthenticationProfileContent: aws.String(`{"AllowDBUserOverride":"1"}`),
	})
	require.NoError(t, err)
	assert.Equal(t, "slice5-authprofile", aws.ToString(created.AuthenticationProfileName))

	modified, err := client.ModifyAuthenticationProfile(ctx, &redshiftsdk.ModifyAuthenticationProfileInput{
		AuthenticationProfileName:    aws.String("slice5-authprofile"),
		AuthenticationProfileContent: aws.String(`{"AllowDBUserOverride":"0"}`),
	})
	require.NoError(t, err)
	assert.JSONEq(t, `{"AllowDBUserOverride":"0"}`, aws.ToString(modified.AuthenticationProfileContent))

	_, err = client.DeleteAuthenticationProfile(ctx, &redshiftsdk.DeleteAuthenticationProfileInput{
		AuthenticationProfileName: aws.String("slice5-authprofile"),
	})
	require.NoError(t, err)
}

func testSlice5ResourcePolicyCustomDomainRealClient(t *testing.T) {
	t.Helper()

	backend, client := newSlice5RedshiftBackendAndClient(t)
	ctx := t.Context()

	_, err := backend.CreateCluster("slice5-rpc", "dc2.large", "dev", "admin", nil, "")
	require.NoError(t, err)

	resourceArn := "arn:aws:redshift:" + rtTestRegion + ":000000000000:cluster:slice5-rpc"

	_, err = client.PutResourcePolicy(ctx, &redshiftsdk.PutResourcePolicyInput{
		ResourceArn: aws.String(resourceArn),
		Policy:      aws.String(`{"Version":"2012-10-17","Statement":[]}`),
	})
	require.NoError(t, err)

	got, err := client.GetResourcePolicy(ctx, &redshiftsdk.GetResourcePolicyInput{ResourceArn: aws.String(resourceArn)})
	require.NoError(t, err)
	require.NotNil(t, got.ResourcePolicy)
	assert.Equal(t, resourceArn, aws.ToString(got.ResourcePolicy.ResourceArn))

	_, err = client.DeleteResourcePolicy(
		ctx, &redshiftsdk.DeleteResourcePolicyInput{ResourceArn: aws.String(resourceArn)},
	)
	require.NoError(t, err)

	_, err = backend.CreateCustomDomainAssociation(
		"slice5-rpc", "slice5.example.com", "arn:aws:acm:us-east-1:000000000000:certificate/abc",
	)
	require.NoError(t, err)

	_, err = client.DeleteCustomDomainAssociation(ctx, &redshiftsdk.DeleteCustomDomainAssociationInput{
		ClusterIdentifier: aws.String("slice5-rpc"),
		CustomDomainName:  aws.String("slice5.example.com"),
	})
	require.NoError(t, err)
}

func testSlice5AccountClusterInfoRealClient(t *testing.T) {
	t.Helper()

	backend, client := newSlice5RedshiftBackendAndClient(t)
	ctx := t.Context()

	_, err := backend.CreateCluster("slice5-infoc", "dc2.large", "dev", "admin", nil, "")
	require.NoError(t, err)

	attrs, err := client.DescribeAccountAttributes(ctx, &redshiftsdk.DescribeAccountAttributesInput{})
	require.NoError(t, err)
	assert.NotNil(t, attrs)

	revisions, err := client.DescribeClusterDbRevisions(ctx, &redshiftsdk.DescribeClusterDbRevisionsInput{
		ClusterIdentifier: aws.String("slice5-infoc"),
	})
	require.NoError(t, err)
	assert.NotNil(t, revisions)

	tracks, err := client.DescribeClusterTracks(ctx, &redshiftsdk.DescribeClusterTracksInput{})
	require.NoError(t, err)
	assert.NotNil(t, tracks)

	versions, err := client.DescribeClusterVersions(ctx, &redshiftsdk.DescribeClusterVersionsInput{})
	require.NoError(t, err)
	assert.NotEmpty(t, versions.ClusterVersions)

	options, err := client.DescribeOrderableClusterOptions(ctx, &redshiftsdk.DescribeOrderableClusterOptionsInput{})
	require.NoError(t, err)
	assert.NotEmpty(t, options.OrderableClusterOptions)

	storage, err := client.DescribeStorage(ctx, &redshiftsdk.DescribeStorageInput{})
	require.NoError(t, err)
	assert.NotNil(t, storage)

	_, err = client.FailoverPrimaryCompute(ctx, &redshiftsdk.FailoverPrimaryComputeInput{
		ClusterIdentifier: aws.String("slice5-infoc"),
	})
	require.NoError(t, err)

	creds, err := client.GetClusterCredentials(ctx, &redshiftsdk.GetClusterCredentialsInput{
		ClusterIdentifier: aws.String("slice5-infoc"),
		DbUser:            aws.String("admin"),
	})
	require.NoError(t, err)
	assert.NotEmpty(t, aws.ToString(creds.DbUser))

	credsIAM, err := client.GetClusterCredentialsWithIAM(ctx, &redshiftsdk.GetClusterCredentialsWithIAMInput{
		ClusterIdentifier: aws.String("slice5-infoc"),
	})
	require.NoError(t, err)
	assert.NotEmpty(t, aws.ToString(credsIAM.DbUser))

	idcToken, err := client.GetIdentityCenterAuthToken(ctx, &redshiftsdk.GetIdentityCenterAuthTokenInput{
		ClusterIds: []string{"slice5-infoc"},
	})
	require.NoError(t, err)
	assert.NotEmpty(t, aws.ToString(idcToken.Token))
}

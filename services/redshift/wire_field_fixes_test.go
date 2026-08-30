package redshift_test

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	redshiftsdk "github.com/aws/aws-sdk-go-v2/service/redshift"
	"github.com/aws/aws-sdk-go-v2/service/redshift/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/redshift"
)

// TestDescribeReservedNodeExchangeStatus_StatusIsLegalEnumMember drives
// DescribeReservedNodeExchangeStatus through the real aws-sdk-go-v2 client.
// ReservedNodeExchangeStatus.Status is types.ReservedNodeExchangeStatusType
// (REQUESTED/PENDING/IN_PROGRESS/RETRYING/SUCCEEDED/FAILED --
// redshift@v1.65.4 types/enums.go:468); the backend previously returned the
// bare string "Active" (borrowed from an unrelated PartnerIntegrationStatus
// constant), which is not a member of ReservedNodeExchangeStatusType, so a
// real client's waiter for an exchange request would never match any case
// and poll until timeout.
func TestDescribeReservedNodeExchangeStatus_StatusIsLegalEnumMember(t *testing.T) {
	t.Parallel()

	backend := redshift.NewInMemoryBackend("000000000000", "us-east-1")
	backend.AddReservedNodeInternal(&redshift.ReservedNode{
		ReservedNodeID: "rn-exchange",
		State:          "active",
	})
	client := newTestRedshiftClient(t, redshift.NewHandler(backend))
	ctx := t.Context()

	out, err := client.DescribeReservedNodeExchangeStatus(ctx, &redshiftsdk.DescribeReservedNodeExchangeStatusInput{
		ReservedNodeId: aws.String("rn-exchange"),
	})
	require.NoError(t, err)
	require.Len(t, out.ReservedNodeExchangeStatusDetails, 1)
	assert.Equal(t, types.ReservedNodeExchangeStatusTypeSucceeded, out.ReservedNodeExchangeStatusDetails[0].Status)
}

// TestDescribeUsageLimits_FiltersByTagKeys drives DescribeUsageLimits through the
// real client with TagKeys set. DescribeUsageLimitsInput.TagKeys/TagValues are real,
// documented request fields (api_op_DescribeUsageLimits.go) that the handler
// previously never read at all, so any TagKeys/TagValues filter was silently
// ignored and every usage limit was returned regardless.
func TestDescribeUsageLimits_FiltersByTagKeys(t *testing.T) {
	t.Parallel()

	backend := redshift.NewInMemoryBackend("000000000000", "us-east-1")
	client := newTestRedshiftClient(t, redshift.NewHandler(backend))
	ctx := t.Context()

	_, err := client.CreateCluster(ctx, &redshiftsdk.CreateClusterInput{
		ClusterIdentifier:  aws.String("ul-cluster"),
		NodeType:           aws.String("dc2.large"),
		MasterUsername:     aws.String("admin"),
		MasterUserPassword: aws.String("Password1"),
	})
	require.NoError(t, err)

	_, err = client.CreateUsageLimit(ctx, &redshiftsdk.CreateUsageLimitInput{
		ClusterIdentifier: aws.String("ul-cluster"),
		FeatureType:       types.UsageLimitFeatureTypeConcurrencyScaling,
		LimitType:         types.UsageLimitLimitTypeTime,
		Amount:            aws.Int64(60),
		Tags:              []types.Tag{{Key: aws.String("env"), Value: aws.String("prod")}},
	})
	require.NoError(t, err)

	_, err = client.CreateUsageLimit(ctx, &redshiftsdk.CreateUsageLimitInput{
		ClusterIdentifier: aws.String("ul-cluster"),
		FeatureType:       types.UsageLimitFeatureTypeSpectrum,
		LimitType:         types.UsageLimitLimitTypeDataScanned,
		Amount:            aws.Int64(10),
		Tags:              []types.Tag{{Key: aws.String("env"), Value: aws.String("staging")}},
	})
	require.NoError(t, err)

	out, err := client.DescribeUsageLimits(ctx, &redshiftsdk.DescribeUsageLimitsInput{
		TagValues: []string{"prod"},
	})
	require.NoError(t, err)
	require.Len(t, out.UsageLimits, 1)
	assert.Equal(t, types.UsageLimitFeatureTypeConcurrencyScaling, out.UsageLimits[0].FeatureType)
}

// TestDescribeHsmClientCertificates_FiltersByTagKeys drives
// DescribeHsmClientCertificates through the real client with TagKeys set.
// DescribeHsmClientCertificatesInput.TagKeys/TagValues (api_op_DescribeHsmClientCertificates.go)
// were previously never read by the handler, so the filter was a silent no-op.
func TestDescribeHsmClientCertificates_FiltersByTagKeys(t *testing.T) {
	t.Parallel()

	backend := redshift.NewInMemoryBackend("000000000000", "us-east-1")
	client := newTestRedshiftClient(t, redshift.NewHandler(backend))
	ctx := t.Context()

	_, err := client.CreateHsmClientCertificate(ctx, &redshiftsdk.CreateHsmClientCertificateInput{
		HsmClientCertificateIdentifier: aws.String("cert-a"),
		Tags:                           []types.Tag{{Key: aws.String("team"), Value: aws.String("data")}},
	})
	require.NoError(t, err)

	_, err = client.CreateHsmClientCertificate(ctx, &redshiftsdk.CreateHsmClientCertificateInput{
		HsmClientCertificateIdentifier: aws.String("cert-b"),
		Tags:                           []types.Tag{{Key: aws.String("team"), Value: aws.String("platform")}},
	})
	require.NoError(t, err)

	out, err := client.DescribeHsmClientCertificates(ctx, &redshiftsdk.DescribeHsmClientCertificatesInput{
		TagKeys: []string{"nonexistent"},
	})
	require.NoError(t, err)
	assert.Empty(t, out.HsmClientCertificates)

	out, err = client.DescribeHsmClientCertificates(ctx, &redshiftsdk.DescribeHsmClientCertificatesInput{
		TagValues: []string{"data"},
	})
	require.NoError(t, err)
	require.Len(t, out.HsmClientCertificates, 1)
	assert.Equal(t, "cert-a", aws.ToString(out.HsmClientCertificates[0].HsmClientCertificateIdentifier))
}

// TestDescribeHsmConfigurations_FiltersByTagKeys mirrors the HsmClientCertificates
// case for DescribeHsmConfigurationsInput.TagKeys/TagValues.
func TestDescribeHsmConfigurations_FiltersByTagKeys(t *testing.T) {
	t.Parallel()

	backend := redshift.NewInMemoryBackend("000000000000", "us-east-1")
	client := newTestRedshiftClient(t, redshift.NewHandler(backend))
	ctx := t.Context()

	_, err := client.CreateHsmConfiguration(ctx, &redshiftsdk.CreateHsmConfigurationInput{
		HsmConfigurationIdentifier: aws.String("cfg-a"),
		Description:                aws.String("d"),
		HsmIpAddress:               aws.String("10.0.0.1"),
		HsmPartitionName:           aws.String("p1"),
		HsmPartitionPassword:       aws.String("pw"),
		HsmServerPublicCertificate: aws.String("cert"),
		Tags:                       []types.Tag{{Key: aws.String("env"), Value: aws.String("prod")}},
	})
	require.NoError(t, err)

	_, err = client.CreateHsmConfiguration(ctx, &redshiftsdk.CreateHsmConfigurationInput{
		HsmConfigurationIdentifier: aws.String("cfg-b"),
		Description:                aws.String("d"),
		HsmIpAddress:               aws.String("10.0.0.2"),
		HsmPartitionName:           aws.String("p2"),
		HsmPartitionPassword:       aws.String("pw"),
		HsmServerPublicCertificate: aws.String("cert"),
		Tags:                       []types.Tag{{Key: aws.String("env"), Value: aws.String("staging")}},
	})
	require.NoError(t, err)

	out, err := client.DescribeHsmConfigurations(ctx, &redshiftsdk.DescribeHsmConfigurationsInput{
		TagValues: []string{"prod"},
	})
	require.NoError(t, err)
	require.Len(t, out.HsmConfigurations, 1)
	assert.Equal(t, "cfg-a", aws.ToString(out.HsmConfigurations[0].HsmConfigurationIdentifier))
}

// TestDescribeEndpointAccess_FiltersByResourceOwner drives DescribeEndpointAccess
// through the real client with ResourceOwner set. DescribeEndpointAccessInput.
// ResourceOwner (api_op_DescribeEndpointAccess.go) was previously never read by
// the handler, even though EndpointAccess.ResourceOwner is real backend data
// populated directly from CreateEndpointAccessInput.ResourceOwner.
func TestDescribeEndpointAccess_FiltersByResourceOwner(t *testing.T) {
	t.Parallel()

	backend := redshift.NewInMemoryBackend("000000000000", "us-east-1")
	client := newTestRedshiftClient(t, redshift.NewHandler(backend))
	ctx := t.Context()

	_, err := client.CreateCluster(ctx, &redshiftsdk.CreateClusterInput{
		ClusterIdentifier:  aws.String("ep-cluster"),
		NodeType:           aws.String("dc2.large"),
		MasterUsername:     aws.String("admin"),
		MasterUserPassword: aws.String("Password1"),
	})
	require.NoError(t, err)

	_, err = client.CreateEndpointAccess(ctx, &redshiftsdk.CreateEndpointAccessInput{
		ClusterIdentifier: aws.String("ep-cluster"),
		EndpointName:      aws.String("ep-owner-a"),
		SubnetGroupName:   aws.String("default"),
		ResourceOwner:     aws.String("111111111111"),
	})
	require.NoError(t, err)

	_, err = client.CreateEndpointAccess(ctx, &redshiftsdk.CreateEndpointAccessInput{
		ClusterIdentifier: aws.String("ep-cluster"),
		EndpointName:      aws.String("ep-owner-b"),
		SubnetGroupName:   aws.String("default"),
		ResourceOwner:     aws.String("222222222222"),
	})
	require.NoError(t, err)

	out, err := client.DescribeEndpointAccess(ctx, &redshiftsdk.DescribeEndpointAccessInput{
		ResourceOwner: aws.String("111111111111"),
	})
	require.NoError(t, err)
	require.Len(t, out.EndpointAccessList, 1)
	assert.Equal(t, "ep-owner-a", aws.ToString(out.EndpointAccessList[0].EndpointName))
}

// TestDescribeScheduledActions_FiltersByActive drives DescribeScheduledActions
// through the real client with Active set. DescribeScheduledActionsInput.Active
// (api_op_DescribeScheduledActions.go) was previously never read, so it never
// excluded disabled scheduled actions from the response.
func TestDescribeScheduledActions_FiltersByActive(t *testing.T) {
	t.Parallel()

	backend := redshift.NewInMemoryBackend("000000000000", "us-east-1")
	client := newTestRedshiftClient(t, redshift.NewHandler(backend))
	ctx := t.Context()

	_, err := client.CreateScheduledAction(ctx, &redshiftsdk.CreateScheduledActionInput{
		ScheduledActionName: aws.String("active-action"),
		Schedule:            aws.String("rate(1 day)"),
		IamRole:             aws.String("arn:aws:iam::000000000000:role/r"),
		Enable:              aws.Bool(true),
		TargetAction: &types.ScheduledActionType{
			PauseCluster: &types.PauseClusterMessage{ClusterIdentifier: aws.String("c1")},
		},
	})
	require.NoError(t, err)

	_, err = client.CreateScheduledAction(ctx, &redshiftsdk.CreateScheduledActionInput{
		ScheduledActionName: aws.String("disabled-action"),
		Schedule:            aws.String("rate(1 day)"),
		IamRole:             aws.String("arn:aws:iam::000000000000:role/r"),
		Enable:              aws.Bool(false),
		TargetAction: &types.ScheduledActionType{
			PauseCluster: &types.PauseClusterMessage{ClusterIdentifier: aws.String("c2")},
		},
	})
	require.NoError(t, err)

	out, err := client.DescribeScheduledActions(ctx, &redshiftsdk.DescribeScheduledActionsInput{
		Active: aws.Bool(true),
	})
	require.NoError(t, err)
	require.Len(t, out.ScheduledActions, 1)
	assert.Equal(t, "active-action", aws.ToString(out.ScheduledActions[0].ScheduledActionName))

	out, err = client.DescribeScheduledActions(ctx, &redshiftsdk.DescribeScheduledActionsInput{
		Active: aws.Bool(false),
	})
	require.NoError(t, err)
	require.Len(t, out.ScheduledActions, 1)
	assert.Equal(t, "disabled-action", aws.ToString(out.ScheduledActions[0].ScheduledActionName))
}

// TestDescribeClusterSnapshots_FiltersByStartTime drives DescribeClusterSnapshots
// through the real client with StartTime set. DescribeClusterSnapshotsInput.
// StartTime/EndTime (api_op_DescribeClusterSnapshots.go) were previously never
// read, even though Snapshot.SnapshotCreateTime is real backend data set at
// CreateClusterSnapshot time.
func TestDescribeClusterSnapshots_FiltersByStartTime(t *testing.T) {
	t.Parallel()

	backend := redshift.NewInMemoryBackend("000000000000", "us-east-1")
	client := newTestRedshiftClient(t, redshift.NewHandler(backend))
	ctx := t.Context()

	_, err := client.CreateCluster(ctx, &redshiftsdk.CreateClusterInput{
		ClusterIdentifier:  aws.String("snap-cluster"),
		NodeType:           aws.String("dc2.large"),
		MasterUsername:     aws.String("admin"),
		MasterUserPassword: aws.String("Password1"),
	})
	require.NoError(t, err)

	before := time.Now().UTC()

	_, err = client.CreateClusterSnapshot(ctx, &redshiftsdk.CreateClusterSnapshotInput{
		SnapshotIdentifier: aws.String("snap-1"),
		ClusterIdentifier:  aws.String("snap-cluster"),
	})
	require.NoError(t, err)

	after := time.Now().UTC()

	out, err := client.DescribeClusterSnapshots(ctx, &redshiftsdk.DescribeClusterSnapshotsInput{
		StartTime: aws.Time(after.Add(time.Hour)),
	})
	require.NoError(t, err)
	assert.Empty(t, out.Snapshots, "StartTime after snapshot creation must exclude it")

	out, err = client.DescribeClusterSnapshots(ctx, &redshiftsdk.DescribeClusterSnapshotsInput{
		StartTime: aws.Time(before.Add(-time.Hour)),
	})
	require.NoError(t, err)
	require.Len(t, out.Snapshots, 1)
	assert.Equal(t, "snap-1", aws.ToString(out.Snapshots[0].SnapshotIdentifier))

	out, err = client.DescribeClusterSnapshots(ctx, &redshiftsdk.DescribeClusterSnapshotsInput{
		EndTime: aws.Time(before.Add(-time.Hour)),
	})
	require.NoError(t, err)
	assert.Empty(t, out.Snapshots, "EndTime before snapshot creation must exclude it")
}

package rds_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	rdssdk "github.com/aws/aws-sdk-go-v2/service/rds"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/rds"
)

// TestRealClient_DescribePagination proves the reqfielddiff tier-1 fix (gopherstack-xhu2t):
// 23 Describe ops never called handler_shared.go's paginateDescribe/parseDescribePagination,
// so MaxRecords/Marker were silently dropped -- every item was always returned in one page
// regardless of the caller's MaxRecords. Each subtest seeds enough items to force a second
// page, requests MaxRecords less than the seeded count through the real typed client, and
// asserts both that the first page respects the limit and that following the returned
// Marker yields the rest. A handful of ops (DescribeEngineDefaultParameters,
// DescribeEngineDefaultClusterParameters, DescribeOptionGroupOptions,
// DescribeDBProxyTargetGroups) never have more than one page of data available in this
// backend (documented gaps -- see PARITY.md), so those assert only that an invalid
// MaxRecords is now rejected instead of silently ignored.
func TestRealClient_DescribePagination(t *testing.T) {
	t.Parallel()

	cases := []struct {
		run  func(t *testing.T)
		name string
	}{
		{testCertificatesPaginationRealClient, "certificates"},
		{testSourceRegionsPaginationRealClient, "source_regions"},
		{testDBMajorEngineVersionsPaginationRealClient, "db_major_engine_versions"},
		{testDBEngineVersionsPaginationRealClient, "db_engine_versions"},
		{testOrderableDBInstanceOptionsPaginationRealClient, "orderable_db_instance_options"},
		{testDBClusterBacktracksPaginationRealClient, "db_cluster_backtracks"},
		{testDBClusterEndpointsPaginationRealClient, "db_cluster_endpoints"},
		{testDBClusterParametersPaginationAndSourceRealClient, "db_cluster_parameters"},
		{testDBProxiesPaginationRealClient, "db_proxies"},
		{testDBProxyEndpointsPaginationRealClient, "db_proxy_endpoints"},
		{testDBProxyTargetsPaginationRealClient, "db_proxy_targets"},
		{testDBSecurityGroupsPaginationRealClient, "db_security_groups"},
		{testEventSubscriptionsPaginationRealClient, "event_subscriptions"},
		{testExportTasksPaginationRealClient, "export_tasks"},
		{testGlobalClustersPaginationRealClient, "global_clusters"},
		{testPendingMaintenanceActionsPaginationRealClient, "pending_maintenance_actions"},
		{testReservedDBInstancesPaginationRealClient, "reserved_db_instances"},
		{testReservedDBInstancesOfferingsPaginationRealClient, "reserved_db_instances_offerings"},
		{testBlueGreenDeploymentsPaginationRealClient, "blue_green_deployments"},
		{testEngineDefaultParametersMaxRecordsValidatedRealClient, "engine_default_parameters"},
		{testEngineDefaultClusterParametersMaxRecordsValidatedRealClient, "engine_default_cluster_parameters"},
		{testOptionGroupOptionsMaxRecordsValidatedRealClient, "option_group_options"},
		{testDBProxyTargetGroupsMaxRecordsValidatedRealClient, "db_proxy_target_groups"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.run(t)
		})
	}
}

func testCertificatesPaginationRealClient(t *testing.T) {
	t.Helper()

	_, client := newRealClientBackendAndClient(t)
	ctx := t.Context()

	first, err := client.DescribeCertificates(ctx, &rdssdk.DescribeCertificatesInput{
		MaxRecords: aws.Int32(2),
	})
	require.NoError(t, err)
	assert.Len(t, first.Certificates, 2)
	require.NotEmpty(t, aws.ToString(first.Marker))

	second, err := client.DescribeCertificates(ctx, &rdssdk.DescribeCertificatesInput{
		Marker: first.Marker,
	})
	require.NoError(t, err)
	assert.NotEmpty(t, second.Certificates)
	assert.NotEqual(
		t,
		aws.ToString(first.Certificates[0].CertificateIdentifier),
		aws.ToString(second.Certificates[0].CertificateIdentifier),
	)
}

func testSourceRegionsPaginationRealClient(t *testing.T) {
	t.Helper()

	_, client := newRealClientBackendAndClient(t)
	ctx := t.Context()

	first, err := client.DescribeSourceRegions(ctx, &rdssdk.DescribeSourceRegionsInput{
		MaxRecords: aws.Int32(5),
	})
	require.NoError(t, err)
	assert.Len(t, first.SourceRegions, 5)
	require.NotEmpty(t, aws.ToString(first.Marker))

	second, err := client.DescribeSourceRegions(ctx, &rdssdk.DescribeSourceRegionsInput{
		Marker: first.Marker,
	})
	require.NoError(t, err)
	assert.NotEmpty(t, second.SourceRegions)
}

func testDBMajorEngineVersionsPaginationRealClient(t *testing.T) {
	t.Helper()

	_, client := newRealClientBackendAndClient(t)
	ctx := t.Context()

	first, err := client.DescribeDBMajorEngineVersions(ctx, &rdssdk.DescribeDBMajorEngineVersionsInput{
		MaxRecords: aws.Int32(5),
	})
	require.NoError(t, err)
	assert.Len(t, first.DBMajorEngineVersions, 5)
	require.NotEmpty(t, aws.ToString(first.Marker))

	second, err := client.DescribeDBMajorEngineVersions(ctx, &rdssdk.DescribeDBMajorEngineVersionsInput{
		Marker: first.Marker,
	})
	require.NoError(t, err)
	assert.NotEmpty(t, second.DBMajorEngineVersions)
}

func testDBEngineVersionsPaginationRealClient(t *testing.T) {
	t.Helper()

	_, client := newRealClientBackendAndClient(t)
	ctx := t.Context()

	first, err := client.DescribeDBEngineVersions(ctx, &rdssdk.DescribeDBEngineVersionsInput{
		MaxRecords: aws.Int32(3),
	})
	require.NoError(t, err)
	assert.Len(t, first.DBEngineVersions, 3)
	require.NotEmpty(t, aws.ToString(first.Marker))

	second, err := client.DescribeDBEngineVersions(ctx, &rdssdk.DescribeDBEngineVersionsInput{
		Marker: first.Marker,
	})
	require.NoError(t, err)
	assert.NotEmpty(t, second.DBEngineVersions)
}

func testOrderableDBInstanceOptionsPaginationRealClient(t *testing.T) {
	t.Helper()

	_, client := newRealClientBackendAndClient(t)
	ctx := t.Context()

	first, err := client.DescribeOrderableDBInstanceOptions(ctx, &rdssdk.DescribeOrderableDBInstanceOptionsInput{
		Engine:     aws.String("postgres"),
		MaxRecords: aws.Int32(4),
	})
	require.NoError(t, err)
	assert.Len(t, first.OrderableDBInstanceOptions, 4)
	require.NotEmpty(t, aws.ToString(first.Marker))

	second, err := client.DescribeOrderableDBInstanceOptions(ctx, &rdssdk.DescribeOrderableDBInstanceOptionsInput{
		Engine: aws.String("postgres"),
		Marker: first.Marker,
	})
	require.NoError(t, err)
	assert.NotEmpty(t, second.OrderableDBInstanceOptions)
}

func testDBClusterBacktracksPaginationRealClient(t *testing.T) {
	t.Helper()

	backend, client := newRealClientBackendAndClient(t)
	ctx := t.Context()

	_, err := backend.CreateDBCluster(
		"pg-backtrack-clu", "aurora-postgresql", "admin", "mydb", "", 5432, nil, rds.DBClusterOptions{},
	)
	require.NoError(t, err)
	for range 3 {
		_, err = backend.BacktrackDBCluster("pg-backtrack-clu", "2024-01-01T00:00:00Z")
		require.NoError(t, err)
	}

	first, err := client.DescribeDBClusterBacktracks(ctx, &rdssdk.DescribeDBClusterBacktracksInput{
		DBClusterIdentifier: aws.String("pg-backtrack-clu"),
		MaxRecords:          aws.Int32(2),
	})
	require.NoError(t, err)
	assert.Len(t, first.DBClusterBacktracks, 2)
	require.NotEmpty(t, aws.ToString(first.Marker))

	second, err := client.DescribeDBClusterBacktracks(ctx, &rdssdk.DescribeDBClusterBacktracksInput{
		DBClusterIdentifier: aws.String("pg-backtrack-clu"),
		Marker:              first.Marker,
	})
	require.NoError(t, err)
	assert.Len(t, second.DBClusterBacktracks, 1)
}

func testDBClusterEndpointsPaginationRealClient(t *testing.T) {
	t.Helper()

	backend, client := newRealClientBackendAndClient(t)
	ctx := t.Context()

	_, err := backend.CreateDBCluster(
		"pg-endpoint-clu", "aurora-postgresql", "admin", "mydb", "", 5432, nil, rds.DBClusterOptions{},
	)
	require.NoError(t, err)
	for _, id := range []string{"pg-ep-a", "pg-ep-b", "pg-ep-c"} {
		_, err = client.CreateDBClusterEndpoint(ctx, &rdssdk.CreateDBClusterEndpointInput{
			DBClusterEndpointIdentifier: aws.String(id),
			DBClusterIdentifier:         aws.String("pg-endpoint-clu"),
			EndpointType:                aws.String("READER"),
		})
		require.NoError(t, err)
	}

	first, err := client.DescribeDBClusterEndpoints(ctx, &rdssdk.DescribeDBClusterEndpointsInput{
		DBClusterIdentifier: aws.String("pg-endpoint-clu"),
		MaxRecords:          aws.Int32(2),
	})
	require.NoError(t, err)
	assert.Len(t, first.DBClusterEndpoints, 2)
	require.NotEmpty(t, aws.ToString(first.Marker))

	second, err := client.DescribeDBClusterEndpoints(ctx, &rdssdk.DescribeDBClusterEndpointsInput{
		DBClusterIdentifier: aws.String("pg-endpoint-clu"),
		Marker:              first.Marker,
	})
	require.NoError(t, err)
	assert.Len(t, second.DBClusterEndpoints, 1)
}

// testDBClusterParametersPaginationAndSourceRealClient also proves the
// DescribeDBClusterParameters.Source fix: real AWS narrows the returned
// parameter list to those matching the given source ("user"/"system"/
// "engine-default"), which maps to the already-modeled DBParameter.Source
// field.
func testDBClusterParametersPaginationAndSourceRealClient(t *testing.T) {
	t.Helper()

	backend, client := newRealClientBackendAndClient(t)
	ctx := t.Context()

	_, err := backend.CreateDBClusterParameterGroup("pg-param-grp", "aurora-postgresql14", "test")
	require.NoError(t, err)
	_, err = backend.ModifyDBClusterParameterGroup("pg-param-grp", []rds.DBParameter{
		{ParameterName: "param_a", ParameterValue: "1", Source: "user"},
		{ParameterName: "param_b", ParameterValue: "2", Source: "user"},
		{ParameterName: "param_c", ParameterValue: "3", Source: "system"},
	})
	require.NoError(t, err)

	first, err := client.DescribeDBClusterParameters(ctx, &rdssdk.DescribeDBClusterParametersInput{
		DBClusterParameterGroupName: aws.String("pg-param-grp"),
		Source:                      aws.String("user"),
		MaxRecords:                  aws.Int32(1),
	})
	require.NoError(t, err)
	require.Len(t, first.Parameters, 1)
	assert.Equal(t, "user", aws.ToString(first.Parameters[0].Source))
	require.NotEmpty(t, aws.ToString(first.Marker))

	second, err := client.DescribeDBClusterParameters(ctx, &rdssdk.DescribeDBClusterParametersInput{
		DBClusterParameterGroupName: aws.String("pg-param-grp"),
		Source:                      aws.String("user"),
		Marker:                      first.Marker,
	})
	require.NoError(t, err)
	require.Len(t, second.Parameters, 1)
	assert.Equal(t, "user", aws.ToString(second.Parameters[0].Source))

	systemOnly, err := client.DescribeDBClusterParameters(ctx, &rdssdk.DescribeDBClusterParametersInput{
		DBClusterParameterGroupName: aws.String("pg-param-grp"),
		Source:                      aws.String("system"),
	})
	require.NoError(t, err)
	require.Len(t, systemOnly.Parameters, 1)
	assert.Equal(t, "param_c", aws.ToString(systemOnly.Parameters[0].ParameterName))
}

func testDBProxiesPaginationRealClient(t *testing.T) {
	t.Helper()

	backend, client := newRealClientBackendAndClient(t)
	ctx := t.Context()

	for _, name := range []string{"pg-proxy-a", "pg-proxy-b", "pg-proxy-c"} {
		_, err := backend.CreateDBProxy(
			name, "POSTGRESQL", "arn:aws:iam::123456789012:role/proxy-role", nil, nil, nil, "", "", "")
		require.NoError(t, err)
	}

	first, err := client.DescribeDBProxies(ctx, &rdssdk.DescribeDBProxiesInput{MaxRecords: aws.Int32(2)})
	require.NoError(t, err)
	assert.Len(t, first.DBProxies, 2)
	require.NotEmpty(t, aws.ToString(first.Marker))

	second, err := client.DescribeDBProxies(ctx, &rdssdk.DescribeDBProxiesInput{Marker: first.Marker})
	require.NoError(t, err)
	assert.Len(t, second.DBProxies, 1)
}

func testDBProxyEndpointsPaginationRealClient(t *testing.T) {
	t.Helper()

	backend, client := newRealClientBackendAndClient(t)
	ctx := t.Context()

	_, err := backend.CreateDBProxy(
		"pg-proxy-ep", "POSTGRESQL", "arn:aws:iam::123456789012:role/proxy-role", nil, nil, nil, "", "", "")
	require.NoError(t, err)
	for _, name := range []string{"pg-proxy-ep-a", "pg-proxy-ep-b", "pg-proxy-ep-c"} {
		_, err = backend.CreateDBProxyEndpoint("pg-proxy-ep", name, "READ_WRITE", []string{"subnet-1"}, nil, "")
		require.NoError(t, err)
	}

	first, err := client.DescribeDBProxyEndpoints(ctx, &rdssdk.DescribeDBProxyEndpointsInput{
		DBProxyName: aws.String("pg-proxy-ep"),
		MaxRecords:  aws.Int32(2),
	})
	require.NoError(t, err)
	assert.Len(t, first.DBProxyEndpoints, 2)
	require.NotEmpty(t, aws.ToString(first.Marker))

	second, err := client.DescribeDBProxyEndpoints(ctx, &rdssdk.DescribeDBProxyEndpointsInput{
		DBProxyName: aws.String("pg-proxy-ep"),
		Marker:      first.Marker,
	})
	require.NoError(t, err)
	assert.Len(t, second.DBProxyEndpoints, 1)
}

func testDBProxyTargetsPaginationRealClient(t *testing.T) {
	t.Helper()

	backend, client := newRealClientBackendAndClient(t)
	ctx := t.Context()

	_, err := backend.CreateDBProxy(
		"pg-proxy-tgt", "POSTGRESQL", "arn:aws:iam::123456789012:role/proxy-role", nil, nil, nil, "", "", "")
	require.NoError(t, err)
	_, err = backend.RegisterDBProxyTargets(
		"pg-proxy-tgt", "", []string{"inst-a", "inst-b", "inst-c"}, nil,
	)
	require.NoError(t, err)

	first, err := client.DescribeDBProxyTargets(ctx, &rdssdk.DescribeDBProxyTargetsInput{
		DBProxyName: aws.String("pg-proxy-tgt"),
		MaxRecords:  aws.Int32(2),
	})
	require.NoError(t, err)
	assert.Len(t, first.Targets, 2)
	require.NotEmpty(t, aws.ToString(first.Marker))

	second, err := client.DescribeDBProxyTargets(ctx, &rdssdk.DescribeDBProxyTargetsInput{
		DBProxyName: aws.String("pg-proxy-tgt"),
		Marker:      first.Marker,
	})
	require.NoError(t, err)
	assert.Len(t, second.Targets, 1)
}

func testDBSecurityGroupsPaginationRealClient(t *testing.T) {
	t.Helper()

	_, client := newRealClientBackendAndClient(t)
	ctx := t.Context()

	for _, name := range []string{"pg-sg-a", "pg-sg-b", "pg-sg-c"} {
		_, err := client.CreateDBSecurityGroup(ctx, &rdssdk.CreateDBSecurityGroupInput{
			DBSecurityGroupName:        aws.String(name),
			DBSecurityGroupDescription: aws.String("test"),
		})
		require.NoError(t, err)
	}

	first, err := client.DescribeDBSecurityGroups(ctx, &rdssdk.DescribeDBSecurityGroupsInput{MaxRecords: aws.Int32(2)})
	require.NoError(t, err)
	assert.Len(t, first.DBSecurityGroups, 2)
	require.NotEmpty(t, aws.ToString(first.Marker))

	second, err := client.DescribeDBSecurityGroups(ctx, &rdssdk.DescribeDBSecurityGroupsInput{Marker: first.Marker})
	require.NoError(t, err)
	assert.Len(t, second.DBSecurityGroups, 1)
}

func testEventSubscriptionsPaginationRealClient(t *testing.T) {
	t.Helper()

	backend, client := newRealClientBackendAndClient(t)
	ctx := t.Context()

	for _, name := range []string{"pg-sub-a", "pg-sub-b", "pg-sub-c"} {
		_, err := backend.CreateEventSubscription(name, "arn:aws:sns:us-east-1:123456789012:topic", "", nil, nil)
		require.NoError(t, err)
	}

	first, err := client.DescribeEventSubscriptions(ctx, &rdssdk.DescribeEventSubscriptionsInput{
		MaxRecords: aws.Int32(2),
	})
	require.NoError(t, err)
	assert.Len(t, first.EventSubscriptionsList, 2)
	require.NotEmpty(t, aws.ToString(first.Marker))

	second, err := client.DescribeEventSubscriptions(ctx, &rdssdk.DescribeEventSubscriptionsInput{
		Marker: first.Marker,
	})
	require.NoError(t, err)
	assert.Len(t, second.EventSubscriptionsList, 1)
}

func testExportTasksPaginationRealClient(t *testing.T) {
	t.Helper()

	backend, client := newRealClientBackendAndClient(t)
	ctx := t.Context()

	for _, id := range []string{"pg-export-a", "pg-export-b", "pg-export-c"} {
		_, err := backend.StartExportTask(
			id, "arn:aws:rds:us-east-1:123456789012:snapshot:src", "my-bucket",
			"arn:aws:iam::123456789012:role/export-role", "arn:aws:kms:us-east-1:123456789012:key/abc",
		)
		require.NoError(t, err)
	}

	first, err := client.DescribeExportTasks(ctx, &rdssdk.DescribeExportTasksInput{MaxRecords: aws.Int32(2)})
	require.NoError(t, err)
	assert.Len(t, first.ExportTasks, 2)
	require.NotEmpty(t, aws.ToString(first.Marker))

	second, err := client.DescribeExportTasks(ctx, &rdssdk.DescribeExportTasksInput{Marker: first.Marker})
	require.NoError(t, err)
	assert.Len(t, second.ExportTasks, 1)
}

func testGlobalClustersPaginationRealClient(t *testing.T) {
	t.Helper()

	_, client := newRealClientBackendAndClient(t)
	ctx := t.Context()

	for _, id := range []string{"pg-global-a", "pg-global-b", "pg-global-c"} {
		_, err := client.CreateGlobalCluster(ctx, &rdssdk.CreateGlobalClusterInput{
			GlobalClusterIdentifier: aws.String(id),
			Engine:                  aws.String("aurora-postgresql"),
		})
		require.NoError(t, err)
	}

	first, err := client.DescribeGlobalClusters(ctx, &rdssdk.DescribeGlobalClustersInput{MaxRecords: aws.Int32(2)})
	require.NoError(t, err)
	assert.Len(t, first.GlobalClusters, 2)
	require.NotEmpty(t, aws.ToString(first.Marker))

	second, err := client.DescribeGlobalClusters(ctx, &rdssdk.DescribeGlobalClustersInput{Marker: first.Marker})
	require.NoError(t, err)
	assert.Len(t, second.GlobalClusters, 1)
}

func testPendingMaintenanceActionsPaginationRealClient(t *testing.T) {
	t.Helper()

	backend, client := newRealClientBackendAndClient(t)
	ctx := t.Context()

	// Create and wait for both instances first, then run both deferred
	// ModifyDBInstance calls back-to-back with no waiting in between: each
	// registered pending action is auto-cleared by the reconciler after
	// instanceTransitionDelay (250ms, lifecycle.go), so any wait between the
	// first Modify and the Describe call below risks a flaky clear.
	ids := []string{"pg-pma-a", "pg-pma-b"}
	for _, id := range ids {
		_, err := backend.CreateDBInstance(id, "mysql", "db.t3.micro", "mydb", "admin", "", 20, rds.DBInstanceOptions{
			EngineVersion: "8.0.30",
		})
		require.NoError(t, err)
		waitForInstanceStatus(t, backend, id, "available")
	}
	for _, id := range ids {
		_, err := backend.ModifyDBInstance(id, "", 0, rds.DBInstanceOptions{
			EngineVersion: "8.0.35",
		})
		require.NoError(t, err)
	}

	first, err := client.DescribePendingMaintenanceActions(ctx, &rdssdk.DescribePendingMaintenanceActionsInput{
		MaxRecords: aws.Int32(1),
	})
	require.NoError(t, err)
	assert.Len(t, first.PendingMaintenanceActions, 1)
	require.NotEmpty(t, aws.ToString(first.Marker))

	second, err := client.DescribePendingMaintenanceActions(ctx, &rdssdk.DescribePendingMaintenanceActionsInput{
		Marker: first.Marker,
	})
	require.NoError(t, err)
	assert.Len(t, second.PendingMaintenanceActions, 1)
}

func testReservedDBInstancesPaginationRealClient(t *testing.T) {
	t.Helper()

	_, client := newRealClientBackendAndClient(t)
	ctx := t.Context()

	offerings, err := client.DescribeReservedDBInstancesOfferings(
		ctx, &rdssdk.DescribeReservedDBInstancesOfferingsInput{},
	)
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(offerings.ReservedDBInstancesOfferings), 3)

	for i, id := range []string{"pg-reserved-a", "pg-reserved-b", "pg-reserved-c"} {
		_, err = client.PurchaseReservedDBInstancesOffering(ctx, &rdssdk.PurchaseReservedDBInstancesOfferingInput{
			ReservedDBInstancesOfferingId: offerings.ReservedDBInstancesOfferings[i].ReservedDBInstancesOfferingId,
			ReservedDBInstanceId:          aws.String(id),
			DBInstanceCount:               aws.Int32(1),
		})
		require.NoError(t, err)
	}

	first, err := client.DescribeReservedDBInstances(ctx, &rdssdk.DescribeReservedDBInstancesInput{
		MaxRecords: aws.Int32(2),
	})
	require.NoError(t, err)
	assert.Len(t, first.ReservedDBInstances, 2)
	require.NotEmpty(t, aws.ToString(first.Marker))

	second, err := client.DescribeReservedDBInstances(ctx, &rdssdk.DescribeReservedDBInstancesInput{
		Marker: first.Marker,
	})
	require.NoError(t, err)
	assert.Len(t, second.ReservedDBInstances, 1)
}

func testReservedDBInstancesOfferingsPaginationRealClient(t *testing.T) {
	t.Helper()

	_, client := newRealClientBackendAndClient(t)
	ctx := t.Context()

	first, err := client.DescribeReservedDBInstancesOfferings(ctx, &rdssdk.DescribeReservedDBInstancesOfferingsInput{
		MaxRecords: aws.Int32(3),
	})
	require.NoError(t, err)
	assert.Len(t, first.ReservedDBInstancesOfferings, 3)
	require.NotEmpty(t, aws.ToString(first.Marker))

	second, err := client.DescribeReservedDBInstancesOfferings(ctx, &rdssdk.DescribeReservedDBInstancesOfferingsInput{
		Marker: first.Marker,
	})
	require.NoError(t, err)
	assert.NotEmpty(t, second.ReservedDBInstancesOfferings)
}

func testBlueGreenDeploymentsPaginationRealClient(t *testing.T) {
	t.Helper()

	backend, client := newRealClientBackendAndClient(t)
	ctx := t.Context()

	for _, name := range []string{"pg-bg-a", "pg-bg-b", "pg-bg-c"} {
		_, err := backend.CreateBlueGreenDeployment(name, "arn:aws:rds:us-east-1:123456789012:db:"+name)
		require.NoError(t, err)
	}

	first, err := client.DescribeBlueGreenDeployments(ctx, &rdssdk.DescribeBlueGreenDeploymentsInput{
		MaxRecords: aws.Int32(2),
	})
	require.NoError(t, err)
	assert.Len(t, first.BlueGreenDeployments, 2)
	require.NotEmpty(t, aws.ToString(first.Marker))

	second, err := client.DescribeBlueGreenDeployments(ctx, &rdssdk.DescribeBlueGreenDeploymentsInput{
		Marker: first.Marker,
	})
	require.NoError(t, err)
	assert.Len(t, second.BlueGreenDeployments, 1)
}

// testEngineDefaultParametersMaxRecordsValidatedRealClient covers a
// permanently-empty catalog (see PARITY.md): DescribeEngineDefaultParameters
// never had backing data before this fix and still doesn't, so the
// observable effect of wiring parseDescribePagination in is that an
// out-of-range MaxRecords is now rejected instead of silently ignored.
func testEngineDefaultParametersMaxRecordsValidatedRealClient(t *testing.T) {
	t.Helper()

	_, client := newRealClientBackendAndClient(t)
	ctx := t.Context()

	_, err := client.DescribeEngineDefaultParameters(ctx, &rdssdk.DescribeEngineDefaultParametersInput{
		DBParameterGroupFamily: aws.String("mysql8.0"),
		MaxRecords:             aws.Int32(0),
	})
	require.Error(t, err)

	out, err := client.DescribeEngineDefaultParameters(ctx, &rdssdk.DescribeEngineDefaultParametersInput{
		DBParameterGroupFamily: aws.String("mysql8.0"),
		MaxRecords:             aws.Int32(20),
	})
	require.NoError(t, err)
	assert.Empty(t, aws.ToString(out.EngineDefaults.Marker))
}

func testEngineDefaultClusterParametersMaxRecordsValidatedRealClient(t *testing.T) {
	t.Helper()

	_, client := newRealClientBackendAndClient(t)
	ctx := t.Context()

	_, err := client.DescribeEngineDefaultClusterParameters(ctx, &rdssdk.DescribeEngineDefaultClusterParametersInput{
		DBParameterGroupFamily: aws.String("aurora-postgresql14"),
		MaxRecords:             aws.Int32(0),
	})
	require.Error(t, err)

	out, err := client.DescribeEngineDefaultClusterParameters(
		ctx, &rdssdk.DescribeEngineDefaultClusterParametersInput{
			DBParameterGroupFamily: aws.String("aurora-postgresql14"),
			MaxRecords:             aws.Int32(20),
		},
	)
	require.NoError(t, err)
	assert.Empty(t, aws.ToString(out.EngineDefaults.Marker))
}

func testOptionGroupOptionsMaxRecordsValidatedRealClient(t *testing.T) {
	t.Helper()

	_, client := newRealClientBackendAndClient(t)
	ctx := t.Context()

	_, err := client.DescribeOptionGroupOptions(ctx, &rdssdk.DescribeOptionGroupOptionsInput{
		EngineName: aws.String("mysql"),
		MaxRecords: aws.Int32(0),
	})
	require.Error(t, err)

	out, err := client.DescribeOptionGroupOptions(ctx, &rdssdk.DescribeOptionGroupOptionsInput{
		EngineName: aws.String("mysql"),
		MaxRecords: aws.Int32(20),
	})
	require.NoError(t, err)
	assert.Empty(t, out.OptionGroupOptions)
}

// testDBProxyTargetGroupsMaxRecordsValidatedRealClient: this backend gives
// every proxy exactly one ("default") target group, matching real AWS's own
// one-target-group-per-proxy model, so there's never a second page to prove
// the round trip against -- the observable effect is that an out-of-range
// MaxRecords is now rejected instead of ignored.
func testDBProxyTargetGroupsMaxRecordsValidatedRealClient(t *testing.T) {
	t.Helper()

	backend, client := newRealClientBackendAndClient(t)
	ctx := t.Context()

	_, err := backend.CreateDBProxy(
		"pg-proxy-tg-validate", "POSTGRESQL", "arn:aws:iam::123456789012:role/proxy-role", nil, nil, nil, "", "", "")
	require.NoError(t, err)

	_, err = client.DescribeDBProxyTargetGroups(ctx, &rdssdk.DescribeDBProxyTargetGroupsInput{
		DBProxyName: aws.String("pg-proxy-tg-validate"),
		MaxRecords:  aws.Int32(0),
	})
	require.Error(t, err)

	out, err := client.DescribeDBProxyTargetGroups(ctx, &rdssdk.DescribeDBProxyTargetGroupsInput{
		DBProxyName: aws.String("pg-proxy-tg-validate"),
		MaxRecords:  aws.Int32(20),
	})
	require.NoError(t, err)
	assert.Len(t, out.TargetGroups, 1)
	assert.Empty(t, aws.ToString(out.Marker))
}

package lightsail_test

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	lightsailsdk "github.com/aws/aws-sdk-go-v2/service/lightsail"
	lightsailtypes "github.com/aws/aws-sdk-go-v2/service/lightsail/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestRealClient_ResourceCoverage drives every gopherstack-n3zi previously
// uncovered lightsail op through the real aws-sdk-go-v2 client
// (newTestClient, shared with the other sdk_roundtrip_*_test.go files).
func TestRealClient_ResourceCoverage(t *testing.T) {
	t.Parallel()

	cases := []struct {
		fn   func(t *testing.T)
		name string
	}{
		{testInstancePortsAndSnapshotsRealClient, "instance_ports_and_snapshots"},
		{testInstanceSetupAndMetadataRealClient, "instance_setup_and_metadata"},
		{testImportKeyPairRealClient, "import_keypair"},
		{testGetDisksRealClient, "get_disks"},
		{testLoadBalancersRealClient, "load_balancers"},
		{testBucketsRealClient, "buckets"},
		{testDatabasesRealClient, "databases"},
		{testContainersRealClient, "containers"},
		{testDistributionsRealClient, "distributions"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.fn(t)
		})
	}
}

// testInstancePortsAndSnapshotsRealClient covers OpenInstancePublicPorts,
// CloseInstancePublicPorts, DeleteInstanceSnapshot, GetInstanceSnapshots,
// GetInstanceMetricData, DeleteAutoSnapshot, DisableAddOn.
func testInstancePortsAndSnapshotsRealClient(t *testing.T) {
	t.Helper()

	client := newTestClient(t)
	ctx := t.Context()

	_, err := client.CreateInstances(ctx, &lightsailsdk.CreateInstancesInput{
		InstanceNames:    []string{"slice33-instance"},
		AvailabilityZone: aws.String("us-east-1a"),
		BlueprintId:      aws.String("amazon_linux_2023"),
		BundleId:         aws.String("nano_3_0"),
	})
	require.NoError(t, err)

	_, err = client.OpenInstancePublicPorts(ctx, &lightsailsdk.OpenInstancePublicPortsInput{
		InstanceName: aws.String("slice33-instance"),
		PortInfo: &lightsailtypes.PortInfo{
			FromPort: 22, ToPort: 22, Protocol: lightsailtypes.NetworkProtocolTcp,
		},
	})
	require.NoError(t, err)

	portsOut, err := client.GetInstancePortStates(ctx, &lightsailsdk.GetInstancePortStatesInput{
		InstanceName: aws.String("slice33-instance"),
	})
	require.NoError(t, err)
	require.Len(t, portsOut.PortStates, 1)

	_, err = client.CloseInstancePublicPorts(ctx, &lightsailsdk.CloseInstancePublicPortsInput{
		InstanceName: aws.String("slice33-instance"),
		PortInfo: &lightsailtypes.PortInfo{
			FromPort: 22, ToPort: 22, Protocol: lightsailtypes.NetworkProtocolTcp,
		},
	})
	require.NoError(t, err)

	now := time.Now()

	metricOut, err := client.GetInstanceMetricData(ctx, &lightsailsdk.GetInstanceMetricDataInput{
		InstanceName: aws.String("slice33-instance"),
		MetricName:   lightsailtypes.InstanceMetricNameCPUUtilization,
		Period:       aws.Int32(300),
		StartTime:    aws.Time(now.Add(-time.Hour)),
		EndTime:      aws.Time(now),
		Unit:         lightsailtypes.MetricUnitPercent,
		Statistics:   []lightsailtypes.MetricStatistic{lightsailtypes.MetricStatisticAverage},
	})
	require.NoError(t, err)
	assert.NotNil(t, metricOut.MetricData)

	_, err = client.CreateInstanceSnapshot(ctx, &lightsailsdk.CreateInstanceSnapshotInput{
		InstanceName: aws.String("slice33-instance"), InstanceSnapshotName: aws.String("slice33-snap"),
	})
	require.NoError(t, err)

	listOut, err := client.GetInstanceSnapshots(ctx, &lightsailsdk.GetInstanceSnapshotsInput{})
	require.NoError(t, err)
	require.Len(t, listOut.InstanceSnapshots, 1)

	_, err = client.DeleteInstanceSnapshot(ctx, &lightsailsdk.DeleteInstanceSnapshotInput{
		InstanceSnapshotName: aws.String("slice33-snap"),
	})
	require.NoError(t, err)

	listOut2, err := client.GetInstanceSnapshots(ctx, &lightsailsdk.GetInstanceSnapshotsInput{})
	require.NoError(t, err)
	assert.Empty(t, listOut2.InstanceSnapshots)

	// DeleteAutoSnapshot is idempotent on a date with no matching entry
	// (deleteAutoSnapshotByDate filters, never errors on a miss) -- exercise
	// the real op end to end against an existing resource.
	_, err = client.DeleteAutoSnapshot(ctx, &lightsailsdk.DeleteAutoSnapshotInput{
		ResourceName: aws.String("slice33-instance"), Date: aws.String("2024-01-01"),
	})
	require.NoError(t, err)

	_, err = client.EnableAddOn(ctx, &lightsailsdk.EnableAddOnInput{
		ResourceName: aws.String("slice33-instance"),
		AddOnRequest: &lightsailtypes.AddOnRequest{
			AddOnType: lightsailtypes.AddOnTypeAutoSnapshot,
			AutoSnapshotAddOnRequest: &lightsailtypes.AutoSnapshotAddOnRequest{
				SnapshotTimeOfDay: aws.String("06:00"),
			},
		},
	})
	require.NoError(t, err)

	_, err = client.DisableAddOn(ctx, &lightsailsdk.DisableAddOnInput{
		ResourceName: aws.String("slice33-instance"), AddOnType: lightsailtypes.AddOnTypeAutoSnapshot,
	})
	require.NoError(t, err)

	addOnsOut, err := client.GetInstance(
		ctx,
		&lightsailsdk.GetInstanceInput{InstanceName: aws.String("slice33-instance")},
	)
	require.NoError(t, err)
	assert.Empty(t, addOnsOut.Instance.AddOns)
}

// testInstanceSetupAndMetadataRealClient covers SetupInstanceHttps,
// GetSetupHistory, DeleteKnownHostKeys, UpdateInstanceMetadataOptions,
// SetIpAddressType.
func testInstanceSetupAndMetadataRealClient(t *testing.T) {
	t.Helper()

	client := newTestClient(t)
	ctx := t.Context()

	_, err := client.CreateInstances(ctx, &lightsailsdk.CreateInstancesInput{
		InstanceNames:    []string{"slice33-setup-instance"},
		AvailabilityZone: aws.String("us-east-1a"),
		BlueprintId:      aws.String("amazon_linux_2023"),
		BundleId:         aws.String("nano_3_0"),
	})
	require.NoError(t, err)

	_, err = client.SetupInstanceHttps(ctx, &lightsailsdk.SetupInstanceHttpsInput{
		InstanceName:        aws.String("slice33-setup-instance"),
		CertificateProvider: lightsailtypes.CertificateProviderLetsEncrypt,
		DomainNames:         []string{"example.com"},
		EmailAddress:        aws.String("admin@example.com"),
	})
	require.NoError(t, err)

	historyOut, err := client.GetSetupHistory(ctx, &lightsailsdk.GetSetupHistoryInput{
		ResourceName: aws.String("slice33-setup-instance"),
	})
	require.NoError(t, err)
	require.Len(t, historyOut.SetupHistory, 1)
	require.NotNil(t, historyOut.SetupHistory[0].Request)
	assert.Equal(t, []string{"example.com"}, historyOut.SetupHistory[0].Request.DomainNames)

	_, err = client.DeleteKnownHostKeys(ctx, &lightsailsdk.DeleteKnownHostKeysInput{
		InstanceName: aws.String("slice33-setup-instance"),
	})
	require.NoError(t, err)

	updOut, err := client.UpdateInstanceMetadataOptions(ctx, &lightsailsdk.UpdateInstanceMetadataOptionsInput{
		InstanceName: aws.String("slice33-setup-instance"),
		HttpTokens:   lightsailtypes.HttpTokensRequired,
	})
	require.NoError(t, err)
	assert.NotNil(t, updOut.Operation)

	_, err = client.SetIpAddressType(ctx, &lightsailsdk.SetIpAddressTypeInput{
		ResourceName:  aws.String("slice33-setup-instance"),
		ResourceType:  lightsailtypes.ResourceTypeInstance,
		IpAddressType: lightsailtypes.IpAddressTypeDualstack,
	})
	require.NoError(t, err)

	getOut, err := client.GetInstance(ctx, &lightsailsdk.GetInstanceInput{
		InstanceName: aws.String("slice33-setup-instance"),
	})
	require.NoError(t, err)
	assert.Equal(t, lightsailtypes.IpAddressTypeDualstack, getOut.Instance.IpAddressType)
}

// testImportKeyPairRealClient covers ImportKeyPair.
func testImportKeyPairRealClient(t *testing.T) {
	t.Helper()

	client := newTestClient(t)
	ctx := t.Context()

	_, err := client.ImportKeyPair(ctx, &lightsailsdk.ImportKeyPairInput{
		KeyPairName:     aws.String("slice33-imported-kp"),
		PublicKeyBase64: aws.String("c3NoLXJzYSBBQUFBQjNOemFDMXljMkVBQUFBREFRQUJBQUFCQVFDCg=="),
	})
	require.NoError(t, err)

	getOut, err := client.GetKeyPair(ctx, &lightsailsdk.GetKeyPairInput{KeyPairName: aws.String("slice33-imported-kp")})
	require.NoError(t, err)
	assert.Equal(t, "slice33-imported-kp", aws.ToString(getOut.KeyPair.Name))
}

// testGetDisksRealClient covers GetDisks.
func testGetDisksRealClient(t *testing.T) {
	t.Helper()

	client := newTestClient(t)
	ctx := t.Context()

	_, err := client.CreateDisk(ctx, &lightsailsdk.CreateDiskInput{
		DiskName: aws.String("slice33-disk"), AvailabilityZone: aws.String("us-east-1a"), SizeInGb: aws.Int32(16),
	})
	require.NoError(t, err)

	listOut, err := client.GetDisks(ctx, &lightsailsdk.GetDisksInput{})
	require.NoError(t, err)
	require.Len(t, listOut.Disks, 1)
	assert.Equal(t, "slice33-disk", aws.ToString(listOut.Disks[0].Name))
}

// testLoadBalancersRealClient covers GetLoadBalancers,
// UpdateLoadBalancerAttribute, GetLoadBalancerMetricData.
func testLoadBalancersRealClient(t *testing.T) {
	t.Helper()

	client := newTestClient(t)
	ctx := t.Context()

	_, err := client.CreateLoadBalancer(ctx, &lightsailsdk.CreateLoadBalancerInput{
		LoadBalancerName: aws.String("slice33-lb"), InstancePort: 80,
	})
	require.NoError(t, err)

	listOut, err := client.GetLoadBalancers(ctx, &lightsailsdk.GetLoadBalancersInput{})
	require.NoError(t, err)
	require.Len(t, listOut.LoadBalancers, 1)

	_, err = client.UpdateLoadBalancerAttribute(ctx, &lightsailsdk.UpdateLoadBalancerAttributeInput{
		LoadBalancerName: aws.String("slice33-lb"),
		AttributeName:    lightsailtypes.LoadBalancerAttributeNameHealthCheckPath,
		AttributeValue:   aws.String("/healthz"),
	})
	require.NoError(t, err)

	getOut, err := client.GetLoadBalancer(ctx, &lightsailsdk.GetLoadBalancerInput{
		LoadBalancerName: aws.String("slice33-lb"),
	})
	require.NoError(t, err)
	assert.Equal(t, "/healthz", aws.ToString(getOut.LoadBalancer.HealthCheckPath))

	now := time.Now()

	metricOut, err := client.GetLoadBalancerMetricData(ctx, &lightsailsdk.GetLoadBalancerMetricDataInput{
		LoadBalancerName: aws.String("slice33-lb"),
		MetricName:       lightsailtypes.LoadBalancerMetricNameRequestCount,
		Period:           aws.Int32(300),
		StartTime:        aws.Time(now.Add(-time.Hour)),
		EndTime:          aws.Time(now),
		Unit:             lightsailtypes.MetricUnitCount,
		Statistics:       []lightsailtypes.MetricStatistic{lightsailtypes.MetricStatisticSum},
	})
	require.NoError(t, err)
	assert.NotNil(t, metricOut.MetricData)
}

// testBucketsRealClient covers UpdateBucket, SetResourceAccessForBucket,
// GetBucketMetricData.
func testBucketsRealClient(t *testing.T) {
	t.Helper()

	client := newTestClient(t)
	ctx := t.Context()

	_, err := client.CreateBucket(ctx, &lightsailsdk.CreateBucketInput{
		BucketName: aws.String("slice33-bucket"), BundleId: aws.String("small_1_0"),
	})
	require.NoError(t, err)

	_, err = client.CreateInstances(ctx, &lightsailsdk.CreateInstancesInput{
		InstanceNames:    []string{"slice33-bucket-instance"},
		AvailabilityZone: aws.String("us-east-1a"),
		BlueprintId:      aws.String("amazon_linux_2023"),
		BundleId:         aws.String("nano_3_0"),
	})
	require.NoError(t, err)

	updOut, err := client.UpdateBucket(ctx, &lightsailsdk.UpdateBucketInput{
		BucketName: aws.String("slice33-bucket"), Versioning: aws.String("Enabled"),
	})
	require.NoError(t, err)
	require.NotNil(t, updOut.Bucket)
	assert.Equal(t, "Enabled", aws.ToString(updOut.Bucket.ObjectVersioning))

	_, err = client.SetResourceAccessForBucket(ctx, &lightsailsdk.SetResourceAccessForBucketInput{
		ResourceName: aws.String("slice33-bucket-instance"), BucketName: aws.String("slice33-bucket"),
		Access: lightsailtypes.ResourceBucketAccessAllow,
	})
	require.NoError(t, err)

	getOut, err := client.GetBuckets(ctx, &lightsailsdk.GetBucketsInput{BucketName: aws.String("slice33-bucket")})
	require.NoError(t, err)
	require.Len(t, getOut.Buckets, 1)
	assert.Contains(t, getOut.Buckets[0].ReadonlyAccessAccounts, "slice33-bucket-instance")

	now := time.Now()

	metricOut, err := client.GetBucketMetricData(ctx, &lightsailsdk.GetBucketMetricDataInput{
		BucketName: aws.String("slice33-bucket"),
		MetricName: lightsailtypes.BucketMetricNameBucketSizeBytes,
		Period:     aws.Int32(86400),
		StartTime:  aws.Time(now.Add(-24 * time.Hour)),
		EndTime:    aws.Time(now),
		Unit:       lightsailtypes.MetricUnitBytes,
		Statistics: []lightsailtypes.MetricStatistic{lightsailtypes.MetricStatisticAverage},
	})
	require.NoError(t, err)
	assert.NotNil(t, metricOut.MetricData)
}

// testDatabasesRealClient covers UpdateRelationalDatabase,
// GetRelationalDatabaseLogEvents, GetRelationalDatabaseMetricData,
// GetRelationalDatabaseSnapshots.
func testDatabasesRealClient(t *testing.T) {
	t.Helper()

	client := newTestClient(t)
	ctx := t.Context()

	_, err := client.CreateRelationalDatabase(ctx, &lightsailsdk.CreateRelationalDatabaseInput{
		RelationalDatabaseName:        aws.String("slice33-db"),
		MasterDatabaseName:            aws.String("appdb"),
		MasterUsername:                aws.String("dbadmin"),
		RelationalDatabaseBlueprintId: aws.String("mysql_8_0"),
		RelationalDatabaseBundleId:    aws.String("micro_2_0"),
	})
	require.NoError(t, err)

	updOut, err := client.UpdateRelationalDatabase(ctx, &lightsailsdk.UpdateRelationalDatabaseInput{
		RelationalDatabaseName: aws.String("slice33-db"),
		PreferredBackupWindow:  aws.String("05:00-06:00"),
	})
	require.NoError(t, err)
	assert.NotEmpty(t, updOut.Operations)

	getOut, err := client.GetRelationalDatabase(ctx, &lightsailsdk.GetRelationalDatabaseInput{
		RelationalDatabaseName: aws.String("slice33-db"),
	})
	require.NoError(t, err)
	assert.Equal(t, "05:00-06:00", aws.ToString(getOut.RelationalDatabase.PreferredBackupWindow))

	logEventsOut, err := client.GetRelationalDatabaseLogEvents(ctx, &lightsailsdk.GetRelationalDatabaseLogEventsInput{
		RelationalDatabaseName: aws.String("slice33-db"),
		LogStreamName:          aws.String("error/mysqld.log"),
	})
	require.NoError(t, err)
	assert.Empty(t, logEventsOut.ResourceLogEvents)

	now := time.Now()

	metricOut, err := client.GetRelationalDatabaseMetricData(ctx, &lightsailsdk.GetRelationalDatabaseMetricDataInput{
		RelationalDatabaseName: aws.String("slice33-db"),
		MetricName:             lightsailtypes.RelationalDatabaseMetricNameCPUUtilization,
		Period:                 aws.Int32(300),
		StartTime:              aws.Time(now.Add(-time.Hour)),
		EndTime:                aws.Time(now),
		Unit:                   lightsailtypes.MetricUnitPercent,
		Statistics:             []lightsailtypes.MetricStatistic{lightsailtypes.MetricStatisticAverage},
	})
	require.NoError(t, err)
	assert.NotNil(t, metricOut.MetricData)

	_, err = client.CreateRelationalDatabaseSnapshot(ctx, &lightsailsdk.CreateRelationalDatabaseSnapshotInput{
		RelationalDatabaseName:         aws.String("slice33-db"),
		RelationalDatabaseSnapshotName: aws.String("slice33-db-snap"),
	})
	require.NoError(t, err)

	snapsOut, err := client.GetRelationalDatabaseSnapshots(ctx, &lightsailsdk.GetRelationalDatabaseSnapshotsInput{})
	require.NoError(t, err)
	require.Len(t, snapsOut.RelationalDatabaseSnapshots, 1)
	assert.Equal(t, "slice33-db-snap", aws.ToString(snapsOut.RelationalDatabaseSnapshots[0].Name))
}

// testContainersRealClient covers UpdateContainerService,
// GetContainerServiceMetricData, GetContainerLog.
func testContainersRealClient(t *testing.T) {
	t.Helper()

	client := newTestClient(t)
	ctx := t.Context()

	_, err := client.CreateContainerService(ctx, &lightsailsdk.CreateContainerServiceInput{
		ServiceName: aws.String(
			"slice33-svc",
		),
		Power: lightsailtypes.ContainerServicePowerNameNano,
		Scale: aws.Int32(1),
	})
	require.NoError(t, err)

	updOut, err := client.UpdateContainerService(ctx, &lightsailsdk.UpdateContainerServiceInput{
		ServiceName: aws.String("slice33-svc"), Scale: aws.Int32(2),
	})
	require.NoError(t, err)
	require.NotNil(t, updOut.ContainerService)
	assert.Equal(t, int32(2), aws.ToInt32(updOut.ContainerService.Scale))

	now := time.Now()

	metricOut, err := client.GetContainerServiceMetricData(ctx, &lightsailsdk.GetContainerServiceMetricDataInput{
		ServiceName: aws.String("slice33-svc"),
		MetricName:  lightsailtypes.ContainerServiceMetricNameCPUUtilization,
		Period:      aws.Int32(300),
		StartTime:   aws.Time(now.Add(-time.Hour)),
		EndTime:     aws.Time(now),
		Statistics:  []lightsailtypes.MetricStatistic{lightsailtypes.MetricStatisticAverage},
	})
	require.NoError(t, err)
	assert.NotNil(t, metricOut.MetricData)

	logOut, err := client.GetContainerLog(ctx, &lightsailsdk.GetContainerLogInput{
		ServiceName:   aws.String("slice33-svc"),
		ContainerName: aws.String("main"),
	})
	require.NoError(t, err)
	assert.Empty(t, logOut.LogEvents)
}

// testDistributionsRealClient covers UpdateDistributionBundle,
// GetDistributionMetricData.
func testDistributionsRealClient(t *testing.T) {
	t.Helper()

	client := newTestClient(t)
	ctx := t.Context()

	_, err := client.CreateBucket(ctx, &lightsailsdk.CreateBucketInput{
		BucketName: aws.String("slice33-dist-origin"), BundleId: aws.String("small_1_0"),
	})
	require.NoError(t, err)

	_, err = client.CreateDistribution(ctx, &lightsailsdk.CreateDistributionInput{
		DistributionName: aws.String("slice33-dist"), BundleId: aws.String("small_1_0"),
		Origin:               &lightsailtypes.InputOrigin{Name: aws.String("slice33-dist-origin")},
		DefaultCacheBehavior: &lightsailtypes.CacheBehavior{Behavior: lightsailtypes.BehaviorEnum("cache")},
	})
	require.NoError(t, err)

	_, err = client.UpdateDistributionBundle(ctx, &lightsailsdk.UpdateDistributionBundleInput{
		DistributionName: aws.String("slice33-dist"), BundleId: aws.String("medium_1_0"),
	})
	require.NoError(t, err)

	getOut, err := client.GetDistributions(ctx, &lightsailsdk.GetDistributionsInput{
		DistributionName: aws.String("slice33-dist"),
	})
	require.NoError(t, err)
	require.Len(t, getOut.Distributions, 1)

	now := time.Now()

	metricOut, err := client.GetDistributionMetricData(ctx, &lightsailsdk.GetDistributionMetricDataInput{
		DistributionName: aws.String("slice33-dist"),
		MetricName:       lightsailtypes.DistributionMetricNameRequests,
		Period:           aws.Int32(300),
		StartTime:        aws.Time(now.Add(-time.Hour)),
		EndTime:          aws.Time(now),
		Unit:             lightsailtypes.MetricUnitCount,
		Statistics:       []lightsailtypes.MetricStatistic{lightsailtypes.MetricStatisticSum},
	})
	require.NoError(t, err)
	assert.NotNil(t, metricOut.MetricData)
}

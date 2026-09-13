package dms_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	dmssdk "github.com/aws/aws-sdk-go-v2/service/databasemigrationservice"
	"github.com/aws/aws-sdk-go-v2/service/databasemigrationservice/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestDescribeFiltersORAndAND proves, against the real aws-sdk-go-v2 dms
// client, that Filters.Values genuinely OR-matches (a filter with two values
// returns resources matching either) and that two distinct Filters entries
// AND together (both must match the same resource) -- gopherstack-pulu9.
// Pre-fix, extractFilterValue (handler.go) read only filters[i].Values[0],
// so a two-value filter silently behaved like a one-value filter and a
// second Filters entry with a different name was ignored entirely.
func TestDescribeFiltersORAndAND(t *testing.T) {
	t.Parallel()

	cases := []struct {
		run  func(t *testing.T, client *dmssdk.Client)
		name string
	}{
		{name: "certificates", run: testCertificatesFilters},
		{name: "replication_instances", run: testReplicationInstancesFilters},
		{name: "endpoints", run: testEndpointsFilters},
		{name: "replication_tasks", run: testReplicationTasksFilters},
		{name: "connections", run: testConnectionsFilters},
		{name: "replications", run: testReplicationsFilters},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestDMSHandler()
			client := newTestDMSClient(t, h)
			tc.run(t, client)
		})
	}
}

func testCertificatesFilters(t *testing.T, client *dmssdk.Client) {
	t.Helper()

	a, err := client.ImportCertificate(t.Context(), &dmssdk.ImportCertificateInput{
		CertificateIdentifier: aws.String("cert-a"),
		CertificatePem:        aws.String("pem-a"),
	})
	require.NoError(t, err)

	b, err := client.ImportCertificate(t.Context(), &dmssdk.ImportCertificateInput{
		CertificateIdentifier: aws.String("cert-b"),
		CertificatePem:        aws.String("pem-b"),
	})
	require.NoError(t, err)

	_, err = client.ImportCertificate(t.Context(), &dmssdk.ImportCertificateInput{
		CertificateIdentifier: aws.String("cert-c"),
		CertificatePem:        aws.String("pem-c"),
	})
	require.NoError(t, err)

	// OR: a two-value certificate-id filter returns both matches.
	or, err := client.DescribeCertificates(t.Context(), &dmssdk.DescribeCertificatesInput{
		Filters: []types.Filter{{Name: aws.String("certificate-id"), Values: []string{"cert-a", "cert-b"}}},
	})
	require.NoError(t, err)
	assert.Len(t, or.Certificates, 2)

	// AND: certificate-id and certificate-arn naming DIFFERENT certificates
	// must match nothing.
	mismatch, err := client.DescribeCertificates(t.Context(), &dmssdk.DescribeCertificatesInput{
		Filters: []types.Filter{
			{Name: aws.String("certificate-id"), Values: []string{"cert-a"}},
			{Name: aws.String("certificate-arn"), Values: []string{aws.ToString(b.Certificate.CertificateArn)}},
		},
	})
	require.NoError(t, err)
	assert.Empty(t, mismatch.Certificates)

	// AND: certificate-id and certificate-arn naming the SAME certificate
	// must match it.
	match, err := client.DescribeCertificates(t.Context(), &dmssdk.DescribeCertificatesInput{
		Filters: []types.Filter{
			{Name: aws.String("certificate-id"), Values: []string{"cert-a"}},
			{Name: aws.String("certificate-arn"), Values: []string{aws.ToString(a.Certificate.CertificateArn)}},
		},
	})
	require.NoError(t, err)
	require.Len(t, match.Certificates, 1)
	assert.Equal(t, "cert-a", aws.ToString(match.Certificates[0].CertificateIdentifier))
}

func testReplicationInstancesFilters(t *testing.T, client *dmssdk.Client) {
	t.Helper()

	a, err := client.CreateReplicationInstance(t.Context(), &dmssdk.CreateReplicationInstanceInput{
		ReplicationInstanceIdentifier: aws.String("ri-a"),
		ReplicationInstanceClass:      aws.String("dms.t3.micro"),
	})
	require.NoError(t, err)

	b, err := client.CreateReplicationInstance(t.Context(), &dmssdk.CreateReplicationInstanceInput{
		ReplicationInstanceIdentifier: aws.String("ri-b"),
		ReplicationInstanceClass:      aws.String("dms.t3.small"),
	})
	require.NoError(t, err)

	_, err = client.CreateReplicationInstance(t.Context(), &dmssdk.CreateReplicationInstanceInput{
		ReplicationInstanceIdentifier: aws.String("ri-c"),
		ReplicationInstanceClass:      aws.String("dms.t3.large"),
	})
	require.NoError(t, err)

	or, err := client.DescribeReplicationInstances(t.Context(), &dmssdk.DescribeReplicationInstancesInput{
		Filters: []types.Filter{
			{Name: aws.String("replication-instance-id"), Values: []string{"ri-a", "ri-b"}},
		},
	})
	require.NoError(t, err)
	assert.Len(t, or.ReplicationInstances, 2)

	mismatch, err := client.DescribeReplicationInstances(t.Context(), &dmssdk.DescribeReplicationInstancesInput{
		Filters: []types.Filter{
			{Name: aws.String("replication-instance-id"), Values: []string{"ri-a"}},
			{
				Name:   aws.String("replication-instance-arn"),
				Values: []string{aws.ToString(b.ReplicationInstance.ReplicationInstanceArn)},
			},
		},
	})
	require.NoError(t, err)
	assert.Empty(t, mismatch.ReplicationInstances)

	match, err := client.DescribeReplicationInstances(t.Context(), &dmssdk.DescribeReplicationInstancesInput{
		Filters: []types.Filter{
			{Name: aws.String("replication-instance-id"), Values: []string{"ri-a"}},
			{
				Name:   aws.String("replication-instance-arn"),
				Values: []string{aws.ToString(a.ReplicationInstance.ReplicationInstanceArn)},
			},
		},
	})
	require.NoError(t, err)
	require.Len(t, match.ReplicationInstances, 1)
	assert.Equal(t, "ri-a", aws.ToString(match.ReplicationInstances[0].ReplicationInstanceIdentifier))
}

func testEndpointsFilters(t *testing.T, client *dmssdk.Client) {
	t.Helper()

	a, err := client.CreateEndpoint(t.Context(), &dmssdk.CreateEndpointInput{
		EndpointIdentifier: aws.String("ep-a"),
		EndpointType:       types.ReplicationEndpointTypeValueSource,
		EngineName:         aws.String("mysql"),
	})
	require.NoError(t, err)

	b, err := client.CreateEndpoint(t.Context(), &dmssdk.CreateEndpointInput{
		EndpointIdentifier: aws.String("ep-b"),
		EndpointType:       types.ReplicationEndpointTypeValueTarget,
		EngineName:         aws.String("postgres"),
	})
	require.NoError(t, err)

	_, err = client.CreateEndpoint(t.Context(), &dmssdk.CreateEndpointInput{
		EndpointIdentifier: aws.String("ep-c"),
		EndpointType:       types.ReplicationEndpointTypeValueTarget,
		EngineName:         aws.String("s3"),
	})
	require.NoError(t, err)

	or, err := client.DescribeEndpoints(t.Context(), &dmssdk.DescribeEndpointsInput{
		Filters: []types.Filter{{Name: aws.String("endpoint-id"), Values: []string{"ep-a", "ep-b"}}},
	})
	require.NoError(t, err)
	assert.Len(t, or.Endpoints, 2)

	mismatch, err := client.DescribeEndpoints(t.Context(), &dmssdk.DescribeEndpointsInput{
		Filters: []types.Filter{
			{Name: aws.String("endpoint-id"), Values: []string{"ep-a"}},
			{Name: aws.String("endpoint-arn"), Values: []string{aws.ToString(b.Endpoint.EndpointArn)}},
		},
	})
	require.NoError(t, err)
	assert.Empty(t, mismatch.Endpoints)

	match, err := client.DescribeEndpoints(t.Context(), &dmssdk.DescribeEndpointsInput{
		Filters: []types.Filter{
			{Name: aws.String("endpoint-id"), Values: []string{"ep-a"}},
			{Name: aws.String("endpoint-arn"), Values: []string{aws.ToString(a.Endpoint.EndpointArn)}},
		},
	})
	require.NoError(t, err)
	require.Len(t, match.Endpoints, 1)
	assert.Equal(t, "ep-a", aws.ToString(match.Endpoints[0].EndpointIdentifier))
}

func testReplicationTasksFilters(t *testing.T, client *dmssdk.Client) {
	t.Helper()

	src, err := client.CreateEndpoint(t.Context(), &dmssdk.CreateEndpointInput{
		EndpointIdentifier: aws.String("rtf-src"),
		EndpointType:       types.ReplicationEndpointTypeValueSource,
		EngineName:         aws.String("mysql"),
	})
	require.NoError(t, err)

	tgt, err := client.CreateEndpoint(t.Context(), &dmssdk.CreateEndpointInput{
		EndpointIdentifier: aws.String("rtf-tgt"),
		EndpointType:       types.ReplicationEndpointTypeValueTarget,
		EngineName:         aws.String("mysql"),
	})
	require.NoError(t, err)

	ri, err := client.CreateReplicationInstance(t.Context(), &dmssdk.CreateReplicationInstanceInput{
		ReplicationInstanceIdentifier: aws.String("rtf-ri"),
		ReplicationInstanceClass:      aws.String("dms.t3.micro"),
	})
	require.NoError(t, err)

	_, err = client.CreateReplicationTask(t.Context(), &dmssdk.CreateReplicationTaskInput{
		ReplicationTaskIdentifier: aws.String("rtf-a"),
		SourceEndpointArn:         src.Endpoint.EndpointArn,
		TargetEndpointArn:         tgt.Endpoint.EndpointArn,
		ReplicationInstanceArn:    ri.ReplicationInstance.ReplicationInstanceArn,
		MigrationType:             types.MigrationTypeValueFullLoad,
		TableMappings:             aws.String(`{"rules":[]}`),
	})
	require.NoError(t, err)

	_, err = client.CreateReplicationTask(t.Context(), &dmssdk.CreateReplicationTaskInput{
		ReplicationTaskIdentifier: aws.String("rtf-b"),
		SourceEndpointArn:         src.Endpoint.EndpointArn,
		TargetEndpointArn:         tgt.Endpoint.EndpointArn,
		ReplicationInstanceArn:    ri.ReplicationInstance.ReplicationInstanceArn,
		MigrationType:             types.MigrationTypeValueCdc,
		TableMappings:             aws.String(`{"rules":[]}`),
	})
	require.NoError(t, err)

	or, err := client.DescribeReplicationTasks(t.Context(), &dmssdk.DescribeReplicationTasksInput{
		Filters: []types.Filter{
			{Name: aws.String("replication-task-id"), Values: []string{"rtf-a", "rtf-b"}},
		},
	})
	require.NoError(t, err)
	assert.Len(t, or.ReplicationTasks, 2)

	mismatch, err := client.DescribeReplicationTasks(t.Context(), &dmssdk.DescribeReplicationTasksInput{
		Filters: []types.Filter{
			{Name: aws.String("replication-task-id"), Values: []string{"rtf-a"}},
			{Name: aws.String("migration-type"), Values: []string{"cdc"}},
		},
	})
	require.NoError(t, err)
	assert.Empty(t, mismatch.ReplicationTasks)

	match, err := client.DescribeReplicationTasks(t.Context(), &dmssdk.DescribeReplicationTasksInput{
		Filters: []types.Filter{
			{Name: aws.String("replication-task-id"), Values: []string{"rtf-a"}},
			{Name: aws.String("migration-type"), Values: []string{"full-load"}},
		},
	})
	require.NoError(t, err)
	require.Len(t, match.ReplicationTasks, 1)
	assert.Equal(t, "rtf-a", aws.ToString(match.ReplicationTasks[0].ReplicationTaskIdentifier))
}

func testConnectionsFilters(t *testing.T, client *dmssdk.Client) {
	t.Helper()

	ri1, err := client.CreateReplicationInstance(t.Context(), &dmssdk.CreateReplicationInstanceInput{
		ReplicationInstanceIdentifier: aws.String("cf-ri-1"),
		ReplicationInstanceClass:      aws.String("dms.t3.micro"),
	})
	require.NoError(t, err)

	ri2, err := client.CreateReplicationInstance(t.Context(), &dmssdk.CreateReplicationInstanceInput{
		ReplicationInstanceIdentifier: aws.String("cf-ri-2"),
		ReplicationInstanceClass:      aws.String("dms.t3.micro"),
	})
	require.NoError(t, err)

	ep1, err := client.CreateEndpoint(t.Context(), &dmssdk.CreateEndpointInput{
		EndpointIdentifier: aws.String("cf-ep-1"),
		EndpointType:       types.ReplicationEndpointTypeValueSource,
		EngineName:         aws.String("mysql"),
	})
	require.NoError(t, err)

	ep2, err := client.CreateEndpoint(t.Context(), &dmssdk.CreateEndpointInput{
		EndpointIdentifier: aws.String("cf-ep-2"),
		EndpointType:       types.ReplicationEndpointTypeValueSource,
		EngineName:         aws.String("mysql"),
	})
	require.NoError(t, err)

	_, err = client.TestConnection(t.Context(), &dmssdk.TestConnectionInput{
		ReplicationInstanceArn: ri1.ReplicationInstance.ReplicationInstanceArn,
		EndpointArn:            ep1.Endpoint.EndpointArn,
	})
	require.NoError(t, err)

	_, err = client.TestConnection(t.Context(), &dmssdk.TestConnectionInput{
		ReplicationInstanceArn: ri2.ReplicationInstance.ReplicationInstanceArn,
		EndpointArn:            ep2.Endpoint.EndpointArn,
	})
	require.NoError(t, err)

	or, err := client.DescribeConnections(t.Context(), &dmssdk.DescribeConnectionsInput{
		Filters: []types.Filter{
			{
				Name: aws.String("endpoint-arn"),
				Values: []string{
					aws.ToString(ep1.Endpoint.EndpointArn),
					aws.ToString(ep2.Endpoint.EndpointArn),
				},
			},
		},
	})
	require.NoError(t, err)
	assert.Len(t, or.Connections, 2)

	mismatch, err := client.DescribeConnections(t.Context(), &dmssdk.DescribeConnectionsInput{
		Filters: []types.Filter{
			{Name: aws.String("endpoint-arn"), Values: []string{aws.ToString(ep1.Endpoint.EndpointArn)}},
			{
				Name:   aws.String("replication-instance-arn"),
				Values: []string{aws.ToString(ri2.ReplicationInstance.ReplicationInstanceArn)},
			},
		},
	})
	require.NoError(t, err)
	assert.Empty(t, mismatch.Connections)

	match, err := client.DescribeConnections(t.Context(), &dmssdk.DescribeConnectionsInput{
		Filters: []types.Filter{
			{Name: aws.String("endpoint-arn"), Values: []string{aws.ToString(ep1.Endpoint.EndpointArn)}},
			{
				Name:   aws.String("replication-instance-arn"),
				Values: []string{aws.ToString(ri1.ReplicationInstance.ReplicationInstanceArn)},
			},
		},
	})
	require.NoError(t, err)
	require.Len(t, match.Connections, 1)
	assert.Equal(t, "cf-ep-1", aws.ToString(match.Connections[0].EndpointIdentifier))
}

func testReplicationsFilters(t *testing.T, client *dmssdk.Client) {
	t.Helper()

	src, err := client.CreateEndpoint(t.Context(), &dmssdk.CreateEndpointInput{
		EndpointIdentifier: aws.String("rf-src"),
		EndpointType:       types.ReplicationEndpointTypeValueSource,
		EngineName:         aws.String("mysql"),
	})
	require.NoError(t, err)

	tgt, err := client.CreateEndpoint(t.Context(), &dmssdk.CreateEndpointInput{
		EndpointIdentifier: aws.String("rf-tgt"),
		EndpointType:       types.ReplicationEndpointTypeValueTarget,
		EngineName:         aws.String("s3"),
	})
	require.NoError(t, err)

	rcA, err := client.CreateReplicationConfig(t.Context(), &dmssdk.CreateReplicationConfigInput{
		ReplicationConfigIdentifier: aws.String("rf-a"),
		ReplicationType:             types.MigrationTypeValueFullLoad,
		SourceEndpointArn:           src.Endpoint.EndpointArn,
		TargetEndpointArn:           tgt.Endpoint.EndpointArn,
		TableMappings:               aws.String(`{"rules":[]}`),
		ComputeConfig:               &types.ComputeConfig{MaxCapacityUnits: aws.Int32(2)},
	})
	require.NoError(t, err)

	rcB, err := client.CreateReplicationConfig(t.Context(), &dmssdk.CreateReplicationConfigInput{
		ReplicationConfigIdentifier: aws.String("rf-b"),
		ReplicationType:             types.MigrationTypeValueFullLoad,
		SourceEndpointArn:           src.Endpoint.EndpointArn,
		TargetEndpointArn:           tgt.Endpoint.EndpointArn,
		TableMappings:               aws.String(`{"rules":[]}`),
		ComputeConfig:               &types.ComputeConfig{MaxCapacityUnits: aws.Int32(2)},
	})
	require.NoError(t, err)

	or, err := client.DescribeReplications(t.Context(), &dmssdk.DescribeReplicationsInput{
		Filters: []types.Filter{
			{Name: aws.String("replication-config-id"), Values: []string{"rf-a", "rf-b"}},
		},
	})
	require.NoError(t, err)
	assert.Len(t, or.Replications, 2)

	mismatch, err := client.DescribeReplications(t.Context(), &dmssdk.DescribeReplicationsInput{
		Filters: []types.Filter{
			{Name: aws.String("replication-config-id"), Values: []string{"rf-a"}},
			{
				Name:   aws.String("replication-config-arn"),
				Values: []string{aws.ToString(rcB.ReplicationConfig.ReplicationConfigArn)},
			},
		},
	})
	require.NoError(t, err)
	assert.Empty(t, mismatch.Replications)

	match, err := client.DescribeReplications(t.Context(), &dmssdk.DescribeReplicationsInput{
		Filters: []types.Filter{
			{Name: aws.String("replication-config-id"), Values: []string{"rf-a"}},
			{
				Name:   aws.String("replication-config-arn"),
				Values: []string{aws.ToString(rcA.ReplicationConfig.ReplicationConfigArn)},
			},
		},
	})
	require.NoError(t, err)
	require.Len(t, match.Replications, 1)
	assert.Equal(t, "rf-a", aws.ToString(match.Replications[0].ReplicationConfigIdentifier))
}

package kafka_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/kafka"
)

func TestClusterTypeProvisioned(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		create  func(b *kafka.InMemoryBackend) *kafka.Cluster
		wantTyp string
	}{
		{
			name: "CreateCluster",
			create: func(b *kafka.InMemoryBackend) *kafka.Cluster {
				c, err := b.CreateCluster(context.Background(), "c1", "3.5.1", 3, kafka.BrokerNodeGroupInfo{
					InstanceType:  "kafka.m5.large",
					ClientSubnets: []string{"subnet-1"},
				}, nil, nil)
				require.NoError(t, err)

				return c
			},
			wantTyp: kafka.ClusterTypeProvisioned,
		},
		{
			name: "AddClusterInternal",
			create: func(b *kafka.InMemoryBackend) *kafka.Cluster {
				return b.AddClusterInternal("c2", "3.6.0")
			},
			wantTyp: kafka.ClusterTypeProvisioned,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := kafka.NewInMemoryBackend(testAccountID, testRegion)
			cl := tt.create(b)
			assert.Equal(t, tt.wantTyp, cl.ClusterType)

			// Round-trip via DescribeCluster.
			described, err := b.DescribeCluster(context.Background(), cl.ClusterArn)
			require.NoError(t, err)
			assert.Equal(t, tt.wantTyp, described.ClusterType)
		})
	}
}

// TestRefinement2_CreateServerlessCluster verifies serverless cluster creation.

func TestCreateServerlessCluster(t *testing.T) {
	t.Parallel()

	tests := []struct {
		serverless *kafka.ServerlessClusterInfo
		name       string
		clName     string
		wantErr    bool
	}{
		{
			name:   "basic_serverless",
			clName: "srv-cluster",
			serverless: &kafka.ServerlessClusterInfo{
				VpcConfigs: []kafka.ServerlessVpcConfig{
					{SubnetIDs: []string{"subnet-1", "subnet-2"}, SecurityGroupIDs: []string{"sg-1"}},
				},
			},
		},
		{
			name:   "serverless_with_iam",
			clName: "srv-iam",
			serverless: &kafka.ServerlessClusterInfo{
				ClientAuthentication: &kafka.ServerlessClientAuthentication{
					Sasl: &kafka.SaslSettings{Iam: &kafka.SaslIam{Enabled: true}},
				},
				VpcConfigs: []kafka.ServerlessVpcConfig{
					{SubnetIDs: []string{"subnet-3"}},
				},
			},
		},
		{
			name:       "empty_name_fails",
			clName:     "",
			serverless: &kafka.ServerlessClusterInfo{},
			wantErr:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := kafka.NewInMemoryBackend(testAccountID, testRegion)
			cl, err := b.CreateServerlessCluster(context.Background(), tt.clName, tt.serverless, nil)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			require.NotNil(t, cl)
			assert.Equal(t, kafka.ClusterTypeServerless, cl.ClusterType)
			assert.Equal(t, tt.clName, cl.ClusterName)
			assert.Equal(t, kafka.ClusterStateCreating, cl.State)
			assert.NotEmpty(t, cl.ClusterArn)
		})
	}
}

// TestRefinement2_CreateServerlessCluster_NoDuplicate verifies duplicate detection.

func TestCreateServerlessCluster_NoDuplicate(t *testing.T) {
	t.Parallel()

	b := kafka.NewInMemoryBackend(testAccountID, testRegion)
	_, err := b.CreateServerlessCluster(context.Background(), "srv", &kafka.ServerlessClusterInfo{}, nil)
	require.NoError(t, err)

	_, err = b.CreateServerlessCluster(context.Background(), "srv", &kafka.ServerlessClusterInfo{}, nil)
	require.ErrorIs(t, err, kafka.ErrAlreadyExists)
}

// TestRefinement2_ServerlessCluster_Roundtrip verifies serverless config survives DescribeCluster.

func TestServerlessCluster_Roundtrip(t *testing.T) {
	t.Parallel()

	srv := &kafka.ServerlessClusterInfo{
		VpcConfigs: []kafka.ServerlessVpcConfig{
			{
				SubnetIDs:        []string{"subnet-a", "subnet-b"},
				SecurityGroupIDs: []string{"sg-1"},
			},
		},
		ClientAuthentication: &kafka.ServerlessClientAuthentication{
			Sasl: &kafka.SaslSettings{Iam: &kafka.SaslIam{Enabled: true}},
		},
	}

	b := kafka.NewInMemoryBackend(testAccountID, testRegion)
	cl, err := b.CreateServerlessCluster(context.Background(), "srv-rt", srv, nil)
	require.NoError(t, err)

	described, err := b.DescribeCluster(context.Background(), cl.ClusterArn)
	require.NoError(t, err)
	require.NotNil(t, described.Serverless)
	require.Len(t, described.Serverless.VpcConfigs, 1)
	assert.Equal(t, []string{"subnet-a", "subnet-b"}, described.Serverless.VpcConfigs[0].SubnetIDs)
	assert.Equal(t, []string{"sg-1"}, described.Serverless.VpcConfigs[0].SecurityGroupIDs)
	require.NotNil(t, described.Serverless.ClientAuthentication)
	require.NotNil(t, described.Serverless.ClientAuthentication.Sasl)
	assert.True(t, described.Serverless.ClientAuthentication.Sasl.Iam.Enabled)
}

// TestRefinement2_ServerlessCluster_HTTP verifies HTTP endpoint for serverless V2 creation.

func TestServerlessCluster_HTTP(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       map[string]any
		name       string
		wantType   string
		wantStatus int
	}{
		{
			name: "serverless_ok",
			body: map[string]any{
				"clusterName": "srv-http",
				"serverless": map[string]any{
					"vpcConfigs": []map[string]any{
						{"subnetIds": []string{"subnet-1"}},
					},
				},
			},
			wantStatus: http.StatusOK,
			wantType:   kafka.ClusterTypeServerless,
		},
		{
			name: "both_provisioned_and_serverless_fails",
			body: map[string]any{
				"clusterName": "mixed",
				"provisioned": map[string]any{
					"kafkaVersion":        "3.5.1",
					"numberOfBrokerNodes": 3,
					"brokerNodeGroupInfo": map[string]any{
						"instanceType":  "kafka.m5.large",
						"clientSubnets": []string{"subnet-1"},
					},
				},
				"serverless": map[string]any{
					"vpcConfigs": []map[string]any{
						{"subnetIds": []string{"subnet-1"}},
					},
				},
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "provisioned_ok",
			body: map[string]any{
				"clusterName": "prov-http",
				"provisioned": map[string]any{
					"kafkaVersion":        "3.5.1",
					"numberOfBrokerNodes": 3,
					"brokerNodeGroupInfo": map[string]any{
						"instanceType":  "kafka.m5.large",
						"clientSubnets": []string{"subnet-1"},
					},
				},
			},
			wantStatus: http.StatusOK,
			wantType:   kafka.ClusterTypeProvisioned,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doKafkaRequest(t, h, http.MethodPost, "/api/v2/clusters", tt.body)

			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantType != "" {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.Equal(t, tt.wantType, resp["clusterType"])
			}
		})
	}
}

// TestRefinement2_DescribeClusterV2_ServerlessArm verifies DescribeClusterV2 emits the serverless arm.

func TestDescribeClusterV2_ServerlessArm(t *testing.T) {
	t.Parallel()

	h, backend := newTestHandlerWithBackend(t)

	srv := &kafka.ServerlessClusterInfo{
		VpcConfigs: []kafka.ServerlessVpcConfig{
			{SubnetIDs: []string{"subnet-x"}},
		},
	}
	cl, err := backend.CreateServerlessCluster(context.Background(), "srv-v2", srv, nil)
	require.NoError(t, err)

	rec := doKafkaRequest(t, h, http.MethodGet, "/api/v2/clusters/"+url.PathEscape(cl.ClusterArn), nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	clInfo, ok := resp["clusterInfo"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, kafka.ClusterTypeServerless, clInfo["clusterType"])
	assert.NotNil(t, clInfo["serverless"], "serverless arm should be present")
	assert.Nil(t, clInfo["provisioned"], "provisioned arm should be absent")
}

// TestRefinement2_DescribeClusterV2_ProvisionedArm verifies provisioned arm in V2 response.

func TestDescribeClusterV2_ProvisionedArm(t *testing.T) {
	t.Parallel()

	h, backend := newTestHandlerWithBackend(t)
	cl := backend.AddClusterInternal("prov-v2", "3.5.1")

	rec := doKafkaRequest(t, h, http.MethodGet, "/api/v2/clusters/"+url.PathEscape(cl.ClusterArn), nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	clInfo, ok := resp["clusterInfo"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, kafka.ClusterTypeProvisioned, clInfo["clusterType"])
	assert.NotNil(t, clInfo["provisioned"], "provisioned arm should be present")
	assert.Nil(t, clInfo["serverless"], "serverless arm should be absent")
}

// TestRefinement2_EncryptionInfo_Roundtrip verifies EncryptionInfo survives create/describe.

func TestEncryptionInfo_Roundtrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		encIn *kafka.EncryptionInfo
		name  string
	}{
		{
			name: "tls_only",
			encIn: &kafka.EncryptionInfo{
				EncryptionInTransit: &kafka.EncryptionInTransit{
					ClientBroker: kafka.EncryptionInTransitTLS,
					InCluster:    true,
				},
			},
		},
		{
			name: "kms_key_at_rest",
			encIn: &kafka.EncryptionInfo{
				EncryptionAtRest: &kafka.EncryptionAtRest{
					DataVolumeKMSKeyID: "arn:aws:kms:us-east-1:123:key/abc-123",
				},
				EncryptionInTransit: &kafka.EncryptionInTransit{
					ClientBroker: kafka.EncryptionInTransitTLSPlaintext,
					InCluster:    false,
				},
			},
		},
		{
			name:  "nil_encryption",
			encIn: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := kafka.NewInMemoryBackend(testAccountID, testRegion)
			cl := b.AddClusterInternal("enc-cl", "3.5.1")

			// Store EncryptionInfo via GetStoredCluster (internal access).
			stored := kafka.GetStoredCluster(b, cl.ClusterArn)
			stored.EncryptionInfo = tt.encIn

			described, err := b.DescribeCluster(context.Background(), cl.ClusterArn)
			require.NoError(t, err)

			if tt.encIn == nil {
				assert.Nil(t, described.EncryptionInfo)

				return
			}

			require.NotNil(t, described.EncryptionInfo)
			if tt.encIn.EncryptionAtRest != nil {
				require.NotNil(t, described.EncryptionInfo.EncryptionAtRest)
				assert.Equal(t, tt.encIn.EncryptionAtRest.DataVolumeKMSKeyID,
					described.EncryptionInfo.EncryptionAtRest.DataVolumeKMSKeyID)
			}
			if tt.encIn.EncryptionInTransit != nil {
				require.NotNil(t, described.EncryptionInfo.EncryptionInTransit)
				assert.Equal(t, tt.encIn.EncryptionInTransit.ClientBroker,
					described.EncryptionInfo.EncryptionInTransit.ClientBroker)
				assert.Equal(t, tt.encIn.EncryptionInTransit.InCluster,
					described.EncryptionInfo.EncryptionInTransit.InCluster)
			}
		})
	}
}

// TestRefinement2_EncryptionInfo_InV1Response verifies EncryptionInfo appears in HTTP V1 response.

func TestEncryptionInfo_InV1Response(t *testing.T) {
	t.Parallel()

	h, backend := newTestHandlerWithBackend(t)
	cl := backend.AddClusterInternal("enc-v1", "3.6.0")

	stored := kafka.GetStoredCluster(backend, cl.ClusterArn)
	stored.EncryptionInfo = &kafka.EncryptionInfo{
		EncryptionAtRest: &kafka.EncryptionAtRest{
			DataVolumeKMSKeyID: "arn:aws:kms:us-east-1:123:key/abc",
		},
		EncryptionInTransit: &kafka.EncryptionInTransit{
			ClientBroker: kafka.EncryptionInTransitTLS,
			InCluster:    true,
		},
	}

	rec := doKafkaRequest(t, h, http.MethodGet, "/v1/clusters/"+url.PathEscape(cl.ClusterArn), nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	clInfo := resp["clusterInfo"].(map[string]any)
	encInfo, ok := clInfo["encryptionInfo"].(map[string]any)
	require.True(t, ok, "encryptionInfo should be present in V1 response")

	ear, ok := encInfo["encryptionAtRest"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "arn:aws:kms:us-east-1:123:key/abc", ear["dataVolumeKMSKeyId"])

	eit, ok := encInfo["encryptionInTransit"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, kafka.EncryptionInTransitTLS, eit["clientBroker"])
	assert.True(t, eit["inCluster"].(bool))
}

// TestRefinement2_EncryptionInfo_InV2Response verifies EncryptionInfo appears in HTTP V2 response.

func TestEncryptionInfo_InV2Response(t *testing.T) {
	t.Parallel()

	h, backend := newTestHandlerWithBackend(t)
	cl := backend.AddClusterInternal("enc-v2", "3.5.1")

	stored := kafka.GetStoredCluster(backend, cl.ClusterArn)
	stored.EncryptionInfo = &kafka.EncryptionInfo{
		EncryptionInTransit: &kafka.EncryptionInTransit{
			ClientBroker: kafka.EncryptionInTransitTLSPlaintext,
			InCluster:    true,
		},
	}

	rec := doKafkaRequest(t, h, http.MethodGet, "/api/v2/clusters/"+url.PathEscape(cl.ClusterArn), nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	clInfo := resp["clusterInfo"].(map[string]any)
	provisioned, ok := clInfo["provisioned"].(map[string]any)
	require.True(t, ok)

	encInfo, ok := provisioned["encryptionInfo"].(map[string]any)
	require.True(t, ok, "encryptionInfo should be present in V2 provisioned response")

	eit := encInfo["encryptionInTransit"].(map[string]any)
	assert.Equal(t, kafka.EncryptionInTransitTLSPlaintext, eit["clientBroker"])
}

// TestRefinement2_OpenMonitoring_Roundtrip verifies OpenMonitoring survives create/describe.

func TestOpenMonitoring_Roundtrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		om   *kafka.OpenMonitoring
		name string
	}{
		{
			name: "jmx_and_node_enabled",
			om: &kafka.OpenMonitoring{
				Prometheus: &kafka.PrometheusInfo{
					JmxExporter:  &kafka.JmxExporter{EnabledInBroker: true},
					NodeExporter: &kafka.NodeExporter{EnabledInBroker: true},
				},
			},
		},
		{
			name: "jmx_only",
			om: &kafka.OpenMonitoring{
				Prometheus: &kafka.PrometheusInfo{
					JmxExporter: &kafka.JmxExporter{EnabledInBroker: true},
				},
			},
		},
		{
			name: "nil",
			om:   nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := kafka.NewInMemoryBackend(testAccountID, testRegion)
			cl := b.AddClusterInternal("om-cl", "3.5.1")

			stored := kafka.GetStoredCluster(b, cl.ClusterArn)
			stored.OpenMonitoring = tt.om

			described, err := b.DescribeCluster(context.Background(), cl.ClusterArn)
			require.NoError(t, err)

			if tt.om == nil {
				assert.Nil(t, described.OpenMonitoring)

				return
			}

			require.NotNil(t, described.OpenMonitoring)
			require.NotNil(t, described.OpenMonitoring.Prometheus)

			if tt.om.Prometheus.JmxExporter != nil {
				require.NotNil(t, described.OpenMonitoring.Prometheus.JmxExporter)
				assert.Equal(t, tt.om.Prometheus.JmxExporter.EnabledInBroker,
					described.OpenMonitoring.Prometheus.JmxExporter.EnabledInBroker)
			}

			if tt.om.Prometheus.NodeExporter != nil {
				require.NotNil(t, described.OpenMonitoring.Prometheus.NodeExporter)
				assert.Equal(t, tt.om.Prometheus.NodeExporter.EnabledInBroker,
					described.OpenMonitoring.Prometheus.NodeExporter.EnabledInBroker)
			}
		})
	}
}

// TestRefinement2_LoggingInfo_Roundtrip verifies LoggingInfo survives describe.

func TestLoggingInfo_Roundtrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		li   *kafka.LoggingInfo
		name string
	}{
		{
			name: "cloudwatch_only",
			li: &kafka.LoggingInfo{
				BrokerLogs: &kafka.BrokerLogs{
					CloudWatchLogs: &kafka.CloudWatchLogs{
						Enabled:  true,
						LogGroup: "/aws/msk/cluster/my-cluster",
					},
				},
			},
		},
		{
			name: "firehose_and_s3",
			li: &kafka.LoggingInfo{
				BrokerLogs: &kafka.BrokerLogs{
					Firehose: &kafka.Firehose{
						Enabled:        true,
						DeliveryStream: "my-firehose",
					},
					S3: &kafka.S3Logs{
						Enabled: true,
						Bucket:  "my-bucket",
						Prefix:  "kafka/",
					},
				},
			},
		},
		{
			name: "nil",
			li:   nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := kafka.NewInMemoryBackend(testAccountID, testRegion)
			cl := b.AddClusterInternal("log-cl", "3.5.1")

			stored := kafka.GetStoredCluster(b, cl.ClusterArn)
			stored.LoggingInfo = tt.li

			described, err := b.DescribeCluster(context.Background(), cl.ClusterArn)
			require.NoError(t, err)

			if tt.li == nil {
				assert.Nil(t, described.LoggingInfo)

				return
			}

			require.NotNil(t, described.LoggingInfo)
			require.NotNil(t, described.LoggingInfo.BrokerLogs)

			bl := described.LoggingInfo.BrokerLogs
			expectedBl := tt.li.BrokerLogs

			if expectedBl.CloudWatchLogs != nil {
				require.NotNil(t, bl.CloudWatchLogs)
				assert.Equal(t, expectedBl.CloudWatchLogs.Enabled, bl.CloudWatchLogs.Enabled)
				assert.Equal(t, expectedBl.CloudWatchLogs.LogGroup, bl.CloudWatchLogs.LogGroup)
			}

			if expectedBl.Firehose != nil {
				require.NotNil(t, bl.Firehose)
				assert.Equal(t, expectedBl.Firehose.DeliveryStream, bl.Firehose.DeliveryStream)
			}

			if expectedBl.S3 != nil {
				require.NotNil(t, bl.S3)
				assert.Equal(t, expectedBl.S3.Bucket, bl.S3.Bucket)
				assert.Equal(t, expectedBl.S3.Prefix, bl.S3.Prefix)
			}
		})
	}
}

// TestRefinement2_ClientAuthentication_Unauthenticated verifies Unauthenticated arm roundtrip.

func TestClientAuthentication_Unauthenticated(t *testing.T) {
	t.Parallel()

	b := kafka.NewInMemoryBackend(testAccountID, testRegion)
	auth := &kafka.ClientAuthentication{
		Unauthenticated: &kafka.UnauthenticatedSettings{Enabled: true},
	}
	cl, err := b.CreateCluster(context.Background(), "ua-cluster", "3.5.1", 3, kafka.BrokerNodeGroupInfo{
		InstanceType:  "kafka.m5.large",
		ClientSubnets: []string{"subnet-1"},
	}, auth, nil)
	require.NoError(t, err)

	described, err := b.DescribeCluster(context.Background(), cl.ClusterArn)
	require.NoError(t, err)
	require.NotNil(t, described.ClientAuthentication)
	require.NotNil(t, described.ClientAuthentication.Unauthenticated)
	assert.True(t, described.ClientAuthentication.Unauthenticated.Enabled)
}

// TestRefinement2_ClientAuthentication_TLSWithCAArns verifies TLS with CA ARNs roundtrip.

func TestClientAuthentication_TLSWithCAArns(t *testing.T) {
	t.Parallel()

	caArns := []string{
		"arn:aws:acm-pca:us-east-1:123:certificate-authority/abc",
		"arn:aws:acm-pca:us-east-1:123:certificate-authority/def",
	}

	b := kafka.NewInMemoryBackend(testAccountID, testRegion)
	auth := &kafka.ClientAuthentication{
		TLS: &kafka.TLSSettings{
			Enabled:                     true,
			CertificateAuthorityArnList: caArns,
		},
	}
	cl, err := b.CreateCluster(context.Background(), "tls-ca-cluster", "3.5.1", 3, kafka.BrokerNodeGroupInfo{
		InstanceType:  "kafka.m5.large",
		ClientSubnets: []string{"subnet-1"},
	}, auth, nil)
	require.NoError(t, err)

	described, err := b.DescribeCluster(context.Background(), cl.ClusterArn)
	require.NoError(t, err)
	require.NotNil(t, described.ClientAuthentication)
	require.NotNil(t, described.ClientAuthentication.TLS)
	assert.True(t, described.ClientAuthentication.TLS.Enabled)
	assert.Equal(t, caArns, described.ClientAuthentication.TLS.CertificateAuthorityArnList)
}

// TestRefinement2_ClientAuthentication_TLS_NoAlias verifies CertificateAuthorityArnList is deep-copied.

func TestClientAuthentication_TLS_NoAlias(t *testing.T) {
	t.Parallel()

	b := kafka.NewInMemoryBackend(testAccountID, testRegion)
	caArns := []string{"arn:aws:acm-pca:us-east-1:123:ca/x"}
	auth := &kafka.ClientAuthentication{
		TLS: &kafka.TLSSettings{
			Enabled:                     true,
			CertificateAuthorityArnList: caArns,
		},
	}
	cl, err := b.CreateCluster(context.Background(), "alias-test", "3.5.1", 3, kafka.BrokerNodeGroupInfo{
		InstanceType:  "kafka.m5.large",
		ClientSubnets: []string{"subnet-1"},
	}, auth, nil)
	require.NoError(t, err)

	// Mutate original slice — should not affect stored cluster.
	caArns[0] = "mutated"

	described, err := b.DescribeCluster(context.Background(), cl.ClusterArn)
	require.NoError(t, err)
	assert.Equal(t, "arn:aws:acm-pca:us-east-1:123:ca/x",
		described.ClientAuthentication.TLS.CertificateAuthorityArnList[0])
}

// TestRefinement2_BrokerNodeGroupInfo_ZoneIds verifies ZoneIDs roundtrip.

func TestBrokerNodeGroupInfo_ZoneIds(t *testing.T) {
	t.Parallel()

	b := kafka.NewInMemoryBackend(testAccountID, testRegion)
	cl, err := b.CreateCluster(context.Background(), "zone-cl", "3.5.1", 3, kafka.BrokerNodeGroupInfo{
		InstanceType:         "kafka.m5.large",
		ClientSubnets:        []string{"subnet-1", "subnet-2", "subnet-3"},
		ZoneIDs:              []string{"use1-az1", "use1-az2", "use1-az3"},
		BrokerAZDistribution: "DEFAULT",
	}, nil, nil)
	require.NoError(t, err)

	described, err := b.DescribeCluster(context.Background(), cl.ClusterArn)
	require.NoError(t, err)
	assert.Equal(t, []string{"use1-az1", "use1-az2", "use1-az3"},
		described.BrokerNodeGroupInfo.ZoneIDs)
}

// TestRefinement2_BrokerNodeGroupInfo_ProvisionedThroughput verifies ProvisionedThroughput roundtrip.

func TestBrokerNodeGroupInfo_ProvisionedThroughput(t *testing.T) {
	t.Parallel()

	b := kafka.NewInMemoryBackend(testAccountID, testRegion)
	cl, err := b.CreateCluster(context.Background(), "pt-cl", "3.5.1", 3, kafka.BrokerNodeGroupInfo{
		InstanceType:  "kafka.m5.large",
		ClientSubnets: []string{"subnet-1"},
		StorageInfo: &kafka.StorageInfo{
			EbsStorageInfo: &kafka.EBSStorageInfo{
				VolumeSize: 100,
				ProvisionedThroughput: &kafka.ProvisionedThroughput{
					Enabled:          true,
					VolumeThroughput: 250,
				},
			},
		},
	}, nil, nil)
	require.NoError(t, err)

	described, err := b.DescribeCluster(context.Background(), cl.ClusterArn)
	require.NoError(t, err)
	require.NotNil(t, described.BrokerNodeGroupInfo.StorageInfo)
	require.NotNil(t, described.BrokerNodeGroupInfo.StorageInfo.EbsStorageInfo)
	require.NotNil(t, described.BrokerNodeGroupInfo.StorageInfo.EbsStorageInfo.ProvisionedThroughput)
	pt := described.BrokerNodeGroupInfo.StorageInfo.EbsStorageInfo.ProvisionedThroughput
	assert.True(t, pt.Enabled)
	assert.Equal(t, int32(250), pt.VolumeThroughput)
}

// TestRefinement2_BrokerNodeGroupInfo_ConnectivityInfo verifies ConnectivityInfo roundtrip.

func TestBrokerNodeGroupInfo_ConnectivityInfo(t *testing.T) {
	t.Parallel()

	tests := []struct {
		ci   *kafka.ConnectivityInfo
		name string
	}{
		{
			name: "public_access_service_provided",
			ci: &kafka.ConnectivityInfo{
				PublicAccess: &kafka.PublicAccess{Type: "SERVICE_PROVIDED_EIPS"},
			},
		},
		{
			name: "public_access_disabled",
			ci: &kafka.ConnectivityInfo{
				PublicAccess: &kafka.PublicAccess{Type: "DISABLED"},
			},
		},
		{
			name: "vpc_connectivity_tls",
			ci: &kafka.ConnectivityInfo{
				VpcConnectivity: &kafka.VpcConnectivity{
					ClientAuthentication: &kafka.VpcConnectivityClientAuthentication{
						TLS: &kafka.VpcConnectivityTLS{Enabled: true},
					},
				},
			},
		},
		{
			name: "vpc_connectivity_sasl_iam",
			ci: &kafka.ConnectivityInfo{
				VpcConnectivity: &kafka.VpcConnectivity{
					ClientAuthentication: &kafka.VpcConnectivityClientAuthentication{
						Sasl: &kafka.VpcConnectivitySasl{
							Iam: &kafka.VpcConnectivitySaslIam{Enabled: true},
						},
					},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := kafka.NewInMemoryBackend(testAccountID, testRegion)
			cl, err := b.CreateCluster(context.Background(), "ci-cl", "3.5.1", 3, kafka.BrokerNodeGroupInfo{
				InstanceType:     "kafka.m5.large",
				ClientSubnets:    []string{"subnet-1"},
				ConnectivityInfo: tt.ci,
			}, nil, nil)
			require.NoError(t, err)

			described, err := b.DescribeCluster(context.Background(), cl.ClusterArn)
			require.NoError(t, err)
			require.NotNil(t, described.BrokerNodeGroupInfo.ConnectivityInfo)

			if tt.ci.PublicAccess != nil {
				require.NotNil(t, described.BrokerNodeGroupInfo.ConnectivityInfo.PublicAccess)
				assert.Equal(t, tt.ci.PublicAccess.Type,
					described.BrokerNodeGroupInfo.ConnectivityInfo.PublicAccess.Type)
			}

			if tt.ci.VpcConnectivity != nil {
				vc := described.BrokerNodeGroupInfo.ConnectivityInfo.VpcConnectivity
				require.NotNil(t, vc)
				require.NotNil(t, vc.ClientAuthentication)
			}
		})
	}
}

// TestRefinement2_GetClusterPolicy_NotFoundException verifies NotFoundException when no policy set.

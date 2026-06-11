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

// ----------------------------------------
// Refinement 2 tests: expanded AWS accuracy
// ----------------------------------------

// TestRefinement2_ClusterTypeProvisioned verifies ClusterType=PROVISIONED is stored and returned.
func TestRefinement2_ClusterTypeProvisioned(t *testing.T) {
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
func TestRefinement2_CreateServerlessCluster(t *testing.T) {
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
			assert.Equal(t, kafka.ClusterStateActive, cl.State)
			assert.NotEmpty(t, cl.ClusterArn)
		})
	}
}

// TestRefinement2_CreateServerlessCluster_NoDuplicate verifies duplicate detection.
func TestRefinement2_CreateServerlessCluster_NoDuplicate(t *testing.T) {
	t.Parallel()

	b := kafka.NewInMemoryBackend(testAccountID, testRegion)
	_, err := b.CreateServerlessCluster(context.Background(), "srv", &kafka.ServerlessClusterInfo{}, nil)
	require.NoError(t, err)

	_, err = b.CreateServerlessCluster(context.Background(), "srv", &kafka.ServerlessClusterInfo{}, nil)
	require.ErrorIs(t, err, kafka.ErrAlreadyExists)
}

// TestRefinement2_ServerlessCluster_Roundtrip verifies serverless config survives DescribeCluster.
func TestRefinement2_ServerlessCluster_Roundtrip(t *testing.T) {
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
func TestRefinement2_ServerlessCluster_HTTP(t *testing.T) {
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
func TestRefinement2_DescribeClusterV2_ServerlessArm(t *testing.T) {
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
func TestRefinement2_DescribeClusterV2_ProvisionedArm(t *testing.T) {
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
func TestRefinement2_EncryptionInfo_Roundtrip(t *testing.T) {
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
func TestRefinement2_EncryptionInfo_InV1Response(t *testing.T) {
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
func TestRefinement2_EncryptionInfo_InV2Response(t *testing.T) {
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
func TestRefinement2_OpenMonitoring_Roundtrip(t *testing.T) {
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
func TestRefinement2_LoggingInfo_Roundtrip(t *testing.T) {
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
func TestRefinement2_ClientAuthentication_Unauthenticated(t *testing.T) {
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
func TestRefinement2_ClientAuthentication_TLSWithCAArns(t *testing.T) {
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
func TestRefinement2_ClientAuthentication_TLS_NoAlias(t *testing.T) {
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
func TestRefinement2_BrokerNodeGroupInfo_ZoneIds(t *testing.T) {
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
func TestRefinement2_BrokerNodeGroupInfo_ProvisionedThroughput(t *testing.T) {
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
func TestRefinement2_BrokerNodeGroupInfo_ConnectivityInfo(t *testing.T) {
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
func TestRefinement2_GetClusterPolicy_NotFoundException(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup     func(b *kafka.InMemoryBackend, clusterArn string)
		name      string
		wantErr   bool
		wantFound bool
	}{
		{
			name:      "no_policy_set",
			setup:     func(_ *kafka.InMemoryBackend, _ string) {},
			wantErr:   true,
			wantFound: false,
		},
		{
			name: "policy_set",
			setup: func(b *kafka.InMemoryBackend, clusterArn string) {
				err := b.PutClusterPolicy(context.Background(), clusterArn, `{"Version":"2012-10-17"}`)
				require.NoError(t, err)
			},
			wantErr:   false,
			wantFound: true,
		},
		{
			name: "policy_put_then_deleted",
			setup: func(b *kafka.InMemoryBackend, clusterArn string) {
				_ = b.PutClusterPolicy(context.Background(), clusterArn, `{"Version":"2012-10-17"}`)
				_ = b.DeleteClusterPolicy(context.Background(), clusterArn)
			},
			wantErr:   true,
			wantFound: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := kafka.NewInMemoryBackend(testAccountID, testRegion)
			cl := b.AddClusterInternal("policy-cl", "3.5.1")

			tt.setup(b, cl.ClusterArn)

			_, err := b.GetClusterPolicy(context.Background(), cl.ClusterArn)

			if tt.wantErr {
				require.Error(t, err)
				require.ErrorIs(t, err, kafka.ErrNotFound)
			} else {
				require.NoError(t, err)
			}

			assert.Equal(t, tt.wantFound, kafka.HasClusterPolicy(b, cl.ClusterArn))
		})
	}
}

// TestRefinement2_GetClusterPolicy_HTTP verifies HTTP NotFoundException when no policy.
func TestRefinement2_GetClusterPolicy_HTTP(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(h *kafka.Handler, encoded string)
		name       string
		wantStatus int
	}{
		{
			name:       "no_policy_returns_404",
			setup:      func(_ *kafka.Handler, _ string) {},
			wantStatus: http.StatusNotFound,
		},
		{
			name: "policy_set_returns_200",
			setup: func(h *kafka.Handler, encoded string) {
				rec := doKafkaRequest(t, h, http.MethodPut, "/v1/clusters/"+encoded+"/policy",
					map[string]any{"policy": `{"Version":"2012-10-17"}`})
				require.Equal(t, http.StatusOK, rec.Code)
			},
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, backend := newTestHandlerWithBackend(t)
			cl := backend.AddClusterInternal("pol-http", "3.5.1")
			encoded := url.PathEscape(cl.ClusterArn)

			tt.setup(h, encoded)

			rec := doKafkaRequest(t, h, http.MethodGet, "/v1/clusters/"+encoded+"/policy", nil)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestRefinement2_UpdateClusterConfiguration_PersistsConfig verifies config is stored on cluster.
func TestRefinement2_UpdateClusterConfiguration_PersistsConfig(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		configArn  string
		revision   int64
		wantArnSet bool
	}{
		{
			name:       "with_valid_config",
			configArn:  "arn:aws:kafka:us-east-1:123:configuration/my-cfg/abc-123",
			revision:   5,
			wantArnSet: true,
		},
		{
			name:       "empty_config_arn",
			configArn:  "",
			revision:   1,
			wantArnSet: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := kafka.NewInMemoryBackend(testAccountID, testRegion)
			cl := b.AddClusterInternal("cfg-update-cl", "3.5.1")

			op, err := b.UpdateClusterConfiguration(context.Background(), cl.ClusterArn, tt.configArn, tt.revision)
			require.NoError(t, err)
			require.NotNil(t, op)
			assert.Equal(t, "UPDATE_CLUSTER_CONFIGURATION", op.OperationType)

			described, err := b.DescribeCluster(context.Background(), cl.ClusterArn)
			require.NoError(t, err)

			if tt.wantArnSet {
				require.NotNil(t, described.ConfigurationInfo,
					"ConfigurationInfo should be set after UpdateClusterConfiguration")
				assert.Equal(t, tt.configArn, described.ConfigurationInfo.Arn)
				assert.Equal(t, tt.revision, described.ConfigurationInfo.Revision)
			}
		})
	}
}

// TestRefinement2_ListKafkaVersions_IncludesKRaft verifies KRaft variants are in the list.
func TestRefinement2_ListKafkaVersions_IncludesKRaft(t *testing.T) {
	t.Parallel()

	b := kafka.NewInMemoryBackend(testAccountID, testRegion)
	versions := b.ListKafkaVersions(context.Background())

	versionMap := make(map[string]string, len(versions))
	for _, v := range versions {
		versionMap[v.Version] = v.Status
	}

	tests := []struct {
		version    string
		wantStatus string
	}{
		{"3.8.0.kraft", "ACTIVE"},
		{"3.7.x.kraft", "ACTIVE"},
		{"3.6.0", "ACTIVE"},
		{"3.5.1", "ACTIVE"},
		{"3.4.0", "ACTIVE"},
		{"3.3.2", "ACTIVE"},
		{"3.3.1", "ACTIVE"},
		{"2.8.2.tiered", "ACTIVE"},
		{"2.8.1", "ACTIVE"},
		{"2.8.0", "DEPRECATED"},
		{"2.6.0", "DEPRECATED"},
	}

	for _, tt := range tests {
		t.Run(tt.version, func(t *testing.T) {
			t.Parallel()

			status, ok := versionMap[tt.version]
			assert.True(t, ok, "version %q should be in ListKafkaVersions", tt.version)
			assert.Equal(t, tt.wantStatus, status, "version %q has unexpected status", tt.version)
		})
	}
}

// TestRefinement2_ListKafkaVersions_HTTP verifies the HTTP response.
func TestRefinement2_ListKafkaVersions_HTTP(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doKafkaRequest(t, h, http.MethodGet, "/v1/kafka-versions", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	versions, ok := resp["kafkaVersions"].([]any)
	require.True(t, ok)
	assert.Greater(t, len(versions), 5, "should return more than 5 versions")

	// Find KRaft variant.
	foundKRaft := false
	for _, v := range versions {
		ver := v.(map[string]any)
		if ver["version"] == "3.8.0.kraft" {
			foundKRaft = true
		}
	}

	assert.True(t, foundKRaft, "3.8.0.kraft should be in version list")
}

// TestRefinement2_GetCompatibleKafkaVersions_KRaftOnly verifies KRaft clusters get KRaft targets.
func TestRefinement2_GetCompatibleKafkaVersions_KRaftOnly(t *testing.T) {
	t.Parallel()

	b := kafka.NewInMemoryBackend(testAccountID, testRegion)
	cl := b.AddClusterInternal("kraft-cl", "3.7.x.kraft")

	versions, err := b.GetCompatibleKafkaVersions(context.Background(), cl.ClusterArn)
	require.NoError(t, err)

	versionStrs := make([]string, 0, len(versions))
	for _, v := range versions {
		versionStrs = append(versionStrs, v.Version)
	}

	// KRaft clusters should only get KRaft targets.
	for _, v := range versions {
		assert.True(t, hasSuffix(v.Version, ".kraft") || v.Version == "3.8.0.kraft",
			"KRaft cluster should only get KRaft-compatible versions, got %q", v.Version)
	}

	assert.Contains(t, versionStrs, "3.8.0.kraft")
}

// TestRefinement2_GetCompatibleKafkaVersions_ZooKeeperNoKRaft verifies ZK clusters don't get KRaft.
func TestRefinement2_GetCompatibleKafkaVersions_ZooKeeperNoKRaft(t *testing.T) {
	t.Parallel()

	b := kafka.NewInMemoryBackend(testAccountID, testRegion)
	cl := b.AddClusterInternal("zk-cl", "2.8.1")

	versions, err := b.GetCompatibleKafkaVersions(context.Background(), cl.ClusterArn)
	require.NoError(t, err)
	require.NotEmpty(t, versions)

	for _, v := range versions {
		assert.False(t, hasSuffix(v.Version, ".kraft"),
			"ZooKeeper cluster should not get KRaft versions, got %q", v.Version)
	}
}

// TestRefinement2_GetCompatibleKafkaVersions_NotFound verifies error on missing cluster.
func TestRefinement2_GetCompatibleKafkaVersions_NotFound(t *testing.T) {
	t.Parallel()

	b := kafka.NewInMemoryBackend(testAccountID, testRegion)
	_, err := b.GetCompatibleKafkaVersions(context.Background(), "arn:aws:kafka:us-east-1:123:cluster/nonexistent/abc")
	require.ErrorIs(t, err, kafka.ErrNotFound)
}

// TestRefinement2_GetBootstrapBrokers_Variants tests bootstrap broker string generation.
func TestRefinement2_GetBootstrapBrokers_Variants(t *testing.T) {
	t.Parallel()

	tests := []struct {
		auth          *kafka.ClientAuthentication
		connectivity  *kafka.ConnectivityInfo
		name          string
		wantSCRAM     bool
		wantIAM       bool
		wantPublicTLS bool
		wantVpcTLS    bool
	}{
		{
			name:          "no_auth",
			auth:          nil,
			wantSCRAM:     false,
			wantIAM:       false,
			wantPublicTLS: false,
		},
		{
			name: "scram_enabled",
			auth: &kafka.ClientAuthentication{
				Sasl: &kafka.SaslSettings{
					Scram: &kafka.SaslScram{Enabled: true},
				},
			},
			wantSCRAM: true,
			wantIAM:   false,
		},
		{
			name: "iam_enabled",
			auth: &kafka.ClientAuthentication{
				Sasl: &kafka.SaslSettings{
					Iam: &kafka.SaslIam{Enabled: true},
				},
			},
			wantSCRAM: false,
			wantIAM:   true,
		},
		{
			name: "scram_and_iam",
			auth: &kafka.ClientAuthentication{
				Sasl: &kafka.SaslSettings{
					Scram: &kafka.SaslScram{Enabled: true},
					Iam:   &kafka.SaslIam{Enabled: true},
				},
			},
			wantSCRAM: true,
			wantIAM:   true,
		},
		{
			name: "public_access_eips",
			auth: &kafka.ClientAuthentication{
				TLS: &kafka.TLSSettings{Enabled: true},
			},
			connectivity: &kafka.ConnectivityInfo{
				PublicAccess: &kafka.PublicAccess{Type: "SERVICE_PROVIDED_EIPS"},
			},
			wantPublicTLS: true,
		},
		{
			name: "vpc_connectivity_tls",
			auth: &kafka.ClientAuthentication{
				TLS: &kafka.TLSSettings{Enabled: true},
			},
			connectivity: &kafka.ConnectivityInfo{
				VpcConnectivity: &kafka.VpcConnectivity{
					ClientAuthentication: &kafka.VpcConnectivityClientAuthentication{
						TLS: &kafka.VpcConnectivityTLS{Enabled: true},
					},
				},
			},
			wantVpcTLS: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, backend := newTestHandlerWithBackend(t)
			cl, err := backend.CreateCluster(context.Background(), "bs-cl", "3.5.1", 3,
				kafka.BrokerNodeGroupInfo{
					InstanceType:     "kafka.m5.large",
					ClientSubnets:    []string{"subnet-1"},
					ConnectivityInfo: tt.connectivity,
				}, tt.auth, nil)
			require.NoError(t, err)

			rec := doKafkaRequest(t, h, http.MethodGet,
				"/v1/clusters/"+url.PathEscape(cl.ClusterArn)+"/bootstrap-brokers", nil)
			require.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

			assert.NotEmpty(t, resp["bootstrapBrokerString"], "plaintext broker string always present")
			assert.NotEmpty(t, resp["bootstrapBrokerStringTls"], "TLS broker string always present")

			if tt.wantSCRAM {
				assert.NotEmpty(t, resp["bootstrapBrokerStringSaslScram"], "SCRAM broker string should be present")
			} else {
				assert.Empty(t, resp["bootstrapBrokerStringSaslScram"], "SCRAM broker string should be absent")
			}

			if tt.wantIAM {
				assert.NotEmpty(t, resp["bootstrapBrokerStringSaslIam"], "IAM broker string should be present")
			} else {
				assert.Empty(t, resp["bootstrapBrokerStringSaslIam"], "IAM broker string should be absent")
			}

			if tt.wantPublicTLS {
				assert.NotEmpty(t, resp["bootstrapBrokerStringTlsPublic"], "public TLS broker string should be present")
			}

			if tt.wantVpcTLS {
				assert.NotEmpty(
					t,
					resp["bootstrapBrokerStringVpcConnectivityTLS"],
					"VPC TLS broker string should be present",
				)
			}
		})
	}
}

// TestRefinement2_ZookeeperConnectString verifies ZookeeperConnectString in V1 responses.
func TestRefinement2_ZookeeperConnectString(t *testing.T) {
	t.Parallel()

	h, backend := newTestHandlerWithBackend(t)
	cl := backend.AddClusterInternal("zk-conn", "3.5.1")

	rec := doKafkaRequest(t, h, http.MethodGet, "/v1/clusters/"+url.PathEscape(cl.ClusterArn), nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	clInfo := resp["clusterInfo"].(map[string]any)
	zkStr, ok := clInfo["zookeeperConnectString"].(string)
	require.True(t, ok, "zookeeperConnectString should be present in V1 response")
	assert.Contains(t, zkStr, ":2181")
	assert.Contains(t, zkStr, "z-1.")
}

// TestRefinement2_StateInfo_Roundtrip verifies StateInfo roundtrip.
func TestRefinement2_StateInfo_Roundtrip(t *testing.T) {
	t.Parallel()

	b := kafka.NewInMemoryBackend(testAccountID, testRegion)
	cl := b.AddClusterInternal("state-cl", "3.5.1")

	// Set state info via internal access.
	stored := kafka.GetStoredCluster(b, cl.ClusterArn)
	stored.State = kafka.ClusterStateFailed
	stored.StateInfo = &kafka.StateInfo{
		Code:    "BROKER_STORAGE_FAILURE",
		Message: "EBS volume ran out of space",
	}

	described, err := b.DescribeCluster(context.Background(), cl.ClusterArn)
	require.NoError(t, err)
	assert.Equal(t, kafka.ClusterStateFailed, described.State)
	require.NotNil(t, described.StateInfo)
	assert.Equal(t, "BROKER_STORAGE_FAILURE", described.StateInfo.Code)
	assert.Equal(t, "EBS volume ran out of space", described.StateInfo.Message)
}

// TestRefinement2_NewClusterStateConstants verifies all new state constants are defined.
func TestRefinement2_NewClusterStateConstants(t *testing.T) {
	t.Parallel()

	tests := []struct {
		constant string
		value    string
	}{
		{constant: "UPDATING", value: kafka.ClusterStateUpdating},
		{constant: "REBOOTING_BROKER", value: kafka.ClusterStateRebootingBroker},
		{constant: "MAINTENANCE", value: kafka.ClusterStateMaintenance},
		{constant: "HEALING", value: kafka.ClusterStateHealing},
	}

	for _, tt := range tests {
		t.Run(tt.constant, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.constant, tt.value)
		})
	}
}

// TestRefinement2_EnhancedMonitoring_Constants verifies monitoring constant values.
func TestRefinement2_EnhancedMonitoring_Constants(t *testing.T) {
	t.Parallel()

	assert.Equal(t, "DEFAULT", kafka.EnhancedMonitoringDefault)
	assert.Equal(t, "PER_BROKER", kafka.EnhancedMonitoringPerBroker)
	assert.Equal(t, "PER_TOPIC_PER_BROKER", kafka.EnhancedMonitoringPerTopicPerBroker)
	assert.Equal(t, "PER_TOPIC_PER_PARTITION", kafka.EnhancedMonitoringPerTopicPerPartition)
}

// TestRefinement2_StorageMode_Constants verifies storage mode constant values.
func TestRefinement2_StorageMode_Constants(t *testing.T) {
	t.Parallel()

	assert.Equal(t, "LOCAL", kafka.StorageModeLocal)
	assert.Equal(t, "TIERED", kafka.StorageModeTiered)
}

// TestRefinement2_EncryptionInTransit_Constants verifies encryption constant values.
func TestRefinement2_EncryptionInTransit_Constants(t *testing.T) {
	t.Parallel()

	assert.Equal(t, "TLS", kafka.EncryptionInTransitTLS)
	assert.Equal(t, "TLS_PLAINTEXT", kafka.EncryptionInTransitTLSPlaintext)
	assert.Equal(t, "PLAINTEXT", kafka.EncryptionInTransitPlaintext)
}

// TestRefinement2_ClusterType_Constants verifies cluster type constant values.
func TestRefinement2_ClusterType_Constants(t *testing.T) {
	t.Parallel()

	assert.Equal(t, "PROVISIONED", kafka.ClusterTypeProvisioned)
	assert.Equal(t, "SERVERLESS", kafka.ClusterTypeServerless)
}

// TestRefinement2_StorageMode_Roundtrip verifies StorageMode is stored and returned.
func TestRefinement2_StorageMode_Roundtrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		mode string
	}{
		{name: "tiered", mode: kafka.StorageModeTiered},
		{name: "local", mode: kafka.StorageModeLocal},
		{name: "empty", mode: ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := kafka.NewInMemoryBackend(testAccountID, testRegion)
			cl := b.AddClusterInternal("sm-cl", "3.5.1")

			stored := kafka.GetStoredCluster(b, cl.ClusterArn)
			stored.StorageMode = tt.mode

			described, err := b.DescribeCluster(context.Background(), cl.ClusterArn)
			require.NoError(t, err)
			assert.Equal(t, tt.mode, described.StorageMode)
		})
	}
}

// TestRefinement2_EnhancedMonitoring_Roundtrip verifies EnhancedMonitoring round-trip.
func TestRefinement2_EnhancedMonitoring_Roundtrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		level string
	}{
		{"default", kafka.EnhancedMonitoringDefault},
		{"per_broker", kafka.EnhancedMonitoringPerBroker},
		{"per_topic_per_broker", kafka.EnhancedMonitoringPerTopicPerBroker},
		{"per_topic_per_partition", kafka.EnhancedMonitoringPerTopicPerPartition},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := kafka.NewInMemoryBackend(testAccountID, testRegion)
			cl := b.AddClusterInternal("em-cl", "3.5.1")

			stored := kafka.GetStoredCluster(b, cl.ClusterArn)
			stored.EnhancedMonitoring = tt.level

			described, err := b.DescribeCluster(context.Background(), cl.ClusterArn)
			require.NoError(t, err)
			assert.Equal(t, tt.level, described.EnhancedMonitoring)
		})
	}
}

// TestRefinement2_ListClustersV2_ClusterType verifies ClusterType propagates in ListClustersV2.
func TestRefinement2_ListClustersV2_ClusterType(t *testing.T) {
	t.Parallel()

	h, backend := newTestHandlerWithBackend(t)

	// Create one provisioned and one serverless.
	_, err := backend.CreateCluster(context.Background(), "prov-list", "3.5.1", 3, kafka.BrokerNodeGroupInfo{
		InstanceType:  "kafka.m5.large",
		ClientSubnets: []string{"subnet-1"},
	}, nil, nil)
	require.NoError(t, err)

	_, err = backend.CreateServerlessCluster(context.Background(), "srv-list", &kafka.ServerlessClusterInfo{
		VpcConfigs: []kafka.ServerlessVpcConfig{{SubnetIDs: []string{"subnet-2"}}},
	}, nil)
	require.NoError(t, err)

	rec := doKafkaRequest(t, h, http.MethodGet, "/api/v2/clusters", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	clusterList := resp["clusterInfoList"].([]any)
	require.Len(t, clusterList, 2)

	typesByName := make(map[string]string)
	for _, ci := range clusterList {
		info := ci.(map[string]any)
		typesByName[info["clusterName"].(string)] = info["clusterType"].(string)
	}

	assert.Equal(t, kafka.ClusterTypeProvisioned, typesByName["prov-list"])
	assert.Equal(t, kafka.ClusterTypeServerless, typesByName["srv-list"])
}

// TestRefinement2_ListClustersV2_ServerlessHasNoProvisionedArm verifies V2 list shape.
func TestRefinement2_ListClustersV2_ServerlessHasNoProvisionedArm(t *testing.T) {
	t.Parallel()

	h, backend := newTestHandlerWithBackend(t)
	_, err := backend.CreateServerlessCluster(context.Background(), "srv-noarm", &kafka.ServerlessClusterInfo{
		VpcConfigs: []kafka.ServerlessVpcConfig{{SubnetIDs: []string{"subnet-1"}}},
	}, nil)
	require.NoError(t, err)

	rec := doKafkaRequest(t, h, http.MethodGet, "/api/v2/clusters", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	clusterList := resp["clusterInfoList"].([]any)
	require.Len(t, clusterList, 1)

	ci := clusterList[0].(map[string]any)
	assert.Nil(t, ci["provisioned"], "serverless cluster should not have provisioned arm")
	assert.NotNil(t, ci["serverless"], "serverless cluster should have serverless arm")
}

// TestRefinement2_Persistence_ServerlessCluster verifies serverless cluster survives snapshot/restore.
func TestRefinement2_Persistence_ServerlessCluster(t *testing.T) {
	t.Parallel()

	b := kafka.NewInMemoryBackend(testAccountID, testRegion)
	srv := &kafka.ServerlessClusterInfo{
		VpcConfigs: []kafka.ServerlessVpcConfig{
			{SubnetIDs: []string{"subnet-1"}, SecurityGroupIDs: []string{"sg-1"}},
		},
	}
	cl, err := b.CreateServerlessCluster(context.Background(), "srv-persist", srv, map[string]string{"env": "test"})
	require.NoError(t, err)

	snap := b.Snapshot()
	require.NotNil(t, snap)

	b2 := kafka.NewInMemoryBackend("other", "eu-west-1")
	require.NoError(t, b2.Restore(snap))

	described, err := b2.DescribeCluster(context.Background(), cl.ClusterArn)
	require.NoError(t, err)
	assert.Equal(t, kafka.ClusterTypeServerless, described.ClusterType)
	require.NotNil(t, described.Serverless)
	require.Len(t, described.Serverless.VpcConfigs, 1)
	assert.Equal(t, []string{"subnet-1"}, described.Serverless.VpcConfigs[0].SubnetIDs)
}

// TestRefinement2_Persistence_EncryptionInfo verifies EncryptionInfo survives snapshot/restore.
func TestRefinement2_Persistence_EncryptionInfo(t *testing.T) {
	t.Parallel()

	b := kafka.NewInMemoryBackend(testAccountID, testRegion)
	cl := b.AddClusterInternal("enc-persist", "3.5.1")

	stored := kafka.GetStoredCluster(b, cl.ClusterArn)
	stored.EncryptionInfo = &kafka.EncryptionInfo{
		EncryptionAtRest: &kafka.EncryptionAtRest{
			DataVolumeKMSKeyID: "arn:aws:kms:us-east-1:123:key/persist",
		},
		EncryptionInTransit: &kafka.EncryptionInTransit{
			ClientBroker: kafka.EncryptionInTransitTLS,
			InCluster:    true,
		},
	}

	snap := b.Snapshot()
	b2 := kafka.NewInMemoryBackend("other", "eu-west-1")
	require.NoError(t, b2.Restore(snap))

	described, err := b2.DescribeCluster(context.Background(), cl.ClusterArn)
	require.NoError(t, err)
	require.NotNil(t, described.EncryptionInfo)
	require.NotNil(t, described.EncryptionInfo.EncryptionAtRest)
	assert.Equal(t, "arn:aws:kms:us-east-1:123:key/persist",
		described.EncryptionInfo.EncryptionAtRest.DataVolumeKMSKeyID)
}

// TestRefinement2_Persistence_ConfigurationInfo verifies ConfigurationInfo survives snapshot/restore.
func TestRefinement2_Persistence_ConfigurationInfo(t *testing.T) {
	t.Parallel()

	b := kafka.NewInMemoryBackend(testAccountID, testRegion)
	cl := b.AddClusterInternal("cfginfo-persist", "3.5.1")

	_, err := b.UpdateClusterConfiguration(context.Background(), cl.ClusterArn,
		"arn:aws:kafka:us-east-1:123:configuration/my-cfg/xyz",
		3)
	require.NoError(t, err)

	snap := b.Snapshot()
	b2 := kafka.NewInMemoryBackend("other", "eu-west-1")
	require.NoError(t, b2.Restore(snap))

	described, err := b2.DescribeCluster(context.Background(), cl.ClusterArn)
	require.NoError(t, err)
	require.NotNil(t, described.ConfigurationInfo)
	assert.Equal(t, "arn:aws:kafka:us-east-1:123:configuration/my-cfg/xyz",
		described.ConfigurationInfo.Arn)
	assert.Equal(t, int64(3), described.ConfigurationInfo.Revision)
}

// TestRefinement2_DeepCopy_ProvisionedThroughput verifies no aliasing in ProvisionedThroughput.
func TestRefinement2_DeepCopy_ProvisionedThroughput(t *testing.T) {
	t.Parallel()

	b := kafka.NewInMemoryBackend(testAccountID, testRegion)
	cl, err := b.CreateCluster(context.Background(), "pt-alias", "3.5.1", 3, kafka.BrokerNodeGroupInfo{
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

	// Mutate returned cluster's ProvisionedThroughput — should not affect stored.
	cl.BrokerNodeGroupInfo.StorageInfo.EbsStorageInfo.ProvisionedThroughput.VolumeThroughput = 999

	described, err := b.DescribeCluster(context.Background(), cl.ClusterArn)
	require.NoError(t, err)
	assert.Equal(t, int32(250),
		described.BrokerNodeGroupInfo.StorageInfo.EbsStorageInfo.ProvisionedThroughput.VolumeThroughput)
}

// TestRefinement2_DeepCopy_ZoneIds verifies no aliasing in ZoneIDs.
func TestRefinement2_DeepCopy_ZoneIds(t *testing.T) {
	t.Parallel()

	b := kafka.NewInMemoryBackend(testAccountID, testRegion)
	zones := []string{"us-east-1a", "us-east-1b"}
	cl, err := b.CreateCluster(context.Background(), "zone-alias", "3.5.1", 3, kafka.BrokerNodeGroupInfo{
		InstanceType:  "kafka.m5.large",
		ClientSubnets: []string{"subnet-1"},
		ZoneIDs:       zones,
	}, nil, nil)
	require.NoError(t, err)

	// Mutate original zones slice — should not affect stored.
	zones[0] = "mutated"
	cl.BrokerNodeGroupInfo.ZoneIDs[0] = "also-mutated"

	described, err := b.DescribeCluster(context.Background(), cl.ClusterArn)
	require.NoError(t, err)
	assert.Equal(t, "us-east-1a", described.BrokerNodeGroupInfo.ZoneIDs[0])
}

// TestRefinement2_OpenMonitoring_InV2Response verifies OpenMonitoring in V2 provisioned arm.
func TestRefinement2_OpenMonitoring_InV2Response(t *testing.T) {
	t.Parallel()

	h, backend := newTestHandlerWithBackend(t)
	cl := backend.AddClusterInternal("om-v2", "3.5.1")

	stored := kafka.GetStoredCluster(backend, cl.ClusterArn)
	stored.OpenMonitoring = &kafka.OpenMonitoring{
		Prometheus: &kafka.PrometheusInfo{
			JmxExporter:  &kafka.JmxExporter{EnabledInBroker: true},
			NodeExporter: &kafka.NodeExporter{EnabledInBroker: false},
		},
	}

	rec := doKafkaRequest(t, h, http.MethodGet, "/api/v2/clusters/"+url.PathEscape(cl.ClusterArn), nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	clInfo := resp["clusterInfo"].(map[string]any)
	provisioned := clInfo["provisioned"].(map[string]any)

	om, ok := provisioned["openMonitoring"].(map[string]any)
	require.True(t, ok, "openMonitoring should be present in V2 provisioned")

	prometheus := om["prometheus"].(map[string]any)
	jmx := prometheus["jmxExporter"].(map[string]any)
	assert.True(t, jmx["enabledInBroker"].(bool))
}

// TestRefinement2_CreateCluster_AllAuthModes verifies various auth mode combinations.
func TestRefinement2_CreateCluster_AllAuthModes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		auth *kafka.ClientAuthentication
		name string
	}{
		{
			name: "no_auth",
			auth: nil,
		},
		{
			name: "sasl_scram",
			auth: &kafka.ClientAuthentication{
				Sasl: &kafka.SaslSettings{
					Scram: &kafka.SaslScram{Enabled: true},
				},
			},
		},
		{
			name: "sasl_iam",
			auth: &kafka.ClientAuthentication{
				Sasl: &kafka.SaslSettings{
					Iam: &kafka.SaslIam{Enabled: true},
				},
			},
		},
		{
			name: "tls_with_ca_arns",
			auth: &kafka.ClientAuthentication{
				TLS: &kafka.TLSSettings{
					Enabled: true,
					CertificateAuthorityArnList: []string{
						"arn:aws:acm-pca:us-east-1:123:certificate-authority/abc",
					},
				},
			},
		},
		{
			name: "unauthenticated",
			auth: &kafka.ClientAuthentication{
				Unauthenticated: &kafka.UnauthenticatedSettings{Enabled: true},
			},
		},
		{
			name: "combined_sasl_and_tls",
			auth: &kafka.ClientAuthentication{
				Sasl: &kafka.SaslSettings{
					Scram: &kafka.SaslScram{Enabled: true},
					Iam:   &kafka.SaslIam{Enabled: true},
				},
				TLS: &kafka.TLSSettings{
					Enabled: true,
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := kafka.NewInMemoryBackend(testAccountID, testRegion)
			cl, err := b.CreateCluster(context.Background(), "auth-cl", "3.5.1", 3, kafka.BrokerNodeGroupInfo{
				InstanceType:  "kafka.m5.large",
				ClientSubnets: []string{"subnet-1"},
			}, tt.auth, nil)
			require.NoError(t, err)
			require.NotNil(t, cl)

			// Verify round-trip.
			described, err := b.DescribeCluster(context.Background(), cl.ClusterArn)
			require.NoError(t, err)

			if tt.auth == nil {
				assert.Nil(t, described.ClientAuthentication)
			} else {
				require.NotNil(t, described.ClientAuthentication)
			}

			if tt.auth != nil && tt.auth.Unauthenticated != nil {
				require.NotNil(t, described.ClientAuthentication.Unauthenticated)
				assert.Equal(t, tt.auth.Unauthenticated.Enabled,
					described.ClientAuthentication.Unauthenticated.Enabled)
			}

			if tt.auth != nil && tt.auth.TLS != nil && len(tt.auth.TLS.CertificateAuthorityArnList) > 0 {
				require.NotNil(t, described.ClientAuthentication.TLS)
				assert.Equal(t, tt.auth.TLS.CertificateAuthorityArnList,
					described.ClientAuthentication.TLS.CertificateAuthorityArnList)
			}
		})
	}
}

// TestRefinement2_CreateCluster_V1_HTTP_AuthModes verifies HTTP CreateCluster with auth modes.
func TestRefinement2_CreateCluster_V1_HTTP_AuthModes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body map[string]any
		name string
	}{
		{
			name: "sasl_scram",
			body: map[string]any{
				"clusterName":         "scram-cl",
				"kafkaVersion":        "3.5.1",
				"numberOfBrokerNodes": 3,
				"brokerNodeGroupInfo": map[string]any{
					"instanceType":  "kafka.m5.large",
					"clientSubnets": []string{"subnet-1"},
				},
				"clientAuthentication": map[string]any{
					"sasl": map[string]any{
						"scram": map[string]any{"enabled": true},
					},
				},
			},
		},
		{
			name: "unauthenticated",
			body: map[string]any{
				"clusterName":         "ua-cl",
				"kafkaVersion":        "3.5.1",
				"numberOfBrokerNodes": 3,
				"brokerNodeGroupInfo": map[string]any{
					"instanceType":  "kafka.m5.large",
					"clientSubnets": []string{"subnet-1"},
				},
				"clientAuthentication": map[string]any{
					"unauthenticated": map[string]any{"enabled": true},
				},
			},
		},
		{
			name: "tls_with_ca_arns",
			body: map[string]any{
				"clusterName":         "tls-cl",
				"kafkaVersion":        "3.5.1",
				"numberOfBrokerNodes": 3,
				"brokerNodeGroupInfo": map[string]any{
					"instanceType":  "kafka.m5.large",
					"clientSubnets": []string{"subnet-1"},
				},
				"clientAuthentication": map[string]any{
					"tls": map[string]any{
						"enabled": true,
						"certificateAuthorityArnList": []string{
							"arn:aws:acm-pca:us-east-1:123:certificate-authority/abc",
						},
					},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doKafkaRequest(t, h, http.MethodPost, "/v1/clusters", tt.body)
			assert.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			assert.NotEmpty(t, resp["clusterArn"])
		})
	}
}

// TestRefinement2_UpdateClusterConfiguration_HTTP verifies the HTTP endpoint persists config.
func TestRefinement2_UpdateClusterConfiguration_HTTP(t *testing.T) {
	t.Parallel()

	h, backend := newTestHandlerWithBackend(t)
	cl := backend.AddClusterInternal("cfg-update-http", "3.5.1")
	encoded := url.PathEscape(cl.ClusterArn)

	configArn := "arn:aws:kafka:us-east-1:123:configuration/my-cfg/abc-123"

	rec := doKafkaRequest(t, h, http.MethodPut, "/api/v2/clusters/"+encoded+"/configuration",
		map[string]any{
			"currentVersion": cl.CurrentVersion,
			"configurationInfo": map[string]any{
				"arn":      configArn,
				"revision": 5,
			},
		})
	require.Equal(t, http.StatusOK, rec.Code)

	// Verify via DescribeCluster.
	described, err := backend.DescribeCluster(context.Background(), cl.ClusterArn)
	require.NoError(t, err)
	require.NotNil(t, described.ConfigurationInfo)
	assert.Equal(t, configArn, described.ConfigurationInfo.Arn)
	assert.Equal(t, int64(5), described.ConfigurationInfo.Revision)
}

// TestRefinement2_GetBootstrapBrokers_ScramePublic verifies public SCRAM broker string.
func TestRefinement2_GetBootstrapBrokers_ScramPublic(t *testing.T) {
	t.Parallel()

	h, backend := newTestHandlerWithBackend(t)
	cl, err := backend.CreateCluster(context.Background(), "scram-pub", "3.5.1", 3,
		kafka.BrokerNodeGroupInfo{
			InstanceType:  "kafka.m5.large",
			ClientSubnets: []string{"subnet-1"},
			ConnectivityInfo: &kafka.ConnectivityInfo{
				PublicAccess: &kafka.PublicAccess{Type: "SERVICE_PROVIDED_EIPS"},
			},
		},
		&kafka.ClientAuthentication{
			Sasl: &kafka.SaslSettings{
				Scram: &kafka.SaslScram{Enabled: true},
			},
		},
		nil)
	require.NoError(t, err)

	rec := doKafkaRequest(t, h, http.MethodGet,
		"/v1/clusters/"+url.PathEscape(cl.ClusterArn)+"/bootstrap-brokers", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	assert.NotEmpty(t, resp["bootstrapBrokerStringSaslScram"])
	assert.NotEmpty(t, resp["bootstrapBrokerStringTlsPublic"])
	assert.NotEmpty(t, resp["bootstrapBrokerStringSaslScramPublic"])
}

// TestRefinement2_DescribeCluster_V1_IncludesNewFields verifies new fields present in V1.
func TestRefinement2_DescribeCluster_V1_IncludesNewFields(t *testing.T) {
	t.Parallel()

	h, backend := newTestHandlerWithBackend(t)
	cl := backend.AddClusterInternal("new-fields-v1", "3.5.1")

	stored := kafka.GetStoredCluster(backend, cl.ClusterArn)
	stored.EnhancedMonitoring = kafka.EnhancedMonitoringPerBroker
	stored.OpenMonitoring = &kafka.OpenMonitoring{
		Prometheus: &kafka.PrometheusInfo{
			JmxExporter: &kafka.JmxExporter{EnabledInBroker: true},
		},
	}
	stored.LoggingInfo = &kafka.LoggingInfo{
		BrokerLogs: &kafka.BrokerLogs{
			CloudWatchLogs: &kafka.CloudWatchLogs{
				Enabled:  true,
				LogGroup: "/aws/msk/test",
			},
		},
	}

	rec := doKafkaRequest(t, h, http.MethodGet, "/v1/clusters/"+url.PathEscape(cl.ClusterArn), nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	clInfo := resp["clusterInfo"].(map[string]any)
	assert.Equal(t, kafka.EnhancedMonitoringPerBroker, clInfo["enhancedMonitoring"])
	assert.NotNil(t, clInfo["openMonitoring"])
	assert.NotNil(t, clInfo["loggingInfo"])
	assert.NotEmpty(t, clInfo["zookeeperConnectString"])
}

// hasSuffix is a helper for test assertions.
func hasSuffix(s, suffix string) bool {
	return len(s) >= len(suffix) && s[len(s)-len(suffix):] == suffix
}

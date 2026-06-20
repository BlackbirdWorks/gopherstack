package kafka_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestParity_CreateCluster_NumberOfBrokerNodesValidation verifies that
// CreateCluster rejects requests with numberOfBrokerNodes less than 1.
// Real AWS MSK returns BadRequestException for zero or negative values;
// the emulator previously accepted any value including 0.
func TestParity_CreateCluster_NumberOfBrokerNodesValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     map[string]any
		name     string
		wantCode int
	}{
		{
			name: "zero_brokers_rejected",
			body: map[string]any{
				"clusterName":         "zero-broker-cluster",
				"kafkaVersion":        "2.8.0",
				"numberOfBrokerNodes": 0,
				"brokerNodeGroupInfo": map[string]any{
					"instanceType":  "kafka.m5.large",
					"clientSubnets": []string{"subnet-1"},
				},
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name: "negative_brokers_rejected",
			body: map[string]any{
				"clusterName":         "neg-broker-cluster",
				"kafkaVersion":        "2.8.0",
				"numberOfBrokerNodes": -1,
				"brokerNodeGroupInfo": map[string]any{
					"instanceType":  "kafka.m5.large",
					"clientSubnets": []string{"subnet-1"},
				},
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name: "one_broker_accepted",
			body: map[string]any{
				"clusterName":         "one-broker-cluster",
				"kafkaVersion":        "2.8.0",
				"numberOfBrokerNodes": 1,
				"brokerNodeGroupInfo": map[string]any{
					"instanceType":  "kafka.m5.large",
					"clientSubnets": []string{"subnet-1"},
				},
			},
			wantCode: http.StatusOK,
		},
		{
			name: "three_brokers_accepted",
			body: map[string]any{
				"clusterName":         "three-broker-cluster",
				"kafkaVersion":        "2.8.0",
				"numberOfBrokerNodes": 3,
				"brokerNodeGroupInfo": map[string]any{
					"instanceType":  "kafka.m5.large",
					"clientSubnets": []string{"subnet-1"},
				},
			},
			wantCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doKafkaRequest(t, h, http.MethodPost, "/v1/clusters", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code,
				"numberOfBrokerNodes=%v", tt.body["numberOfBrokerNodes"])
		})
	}
}

package kafka_test

import (
	"context"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/kafka"
)

func TestErrValidationMapping(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		body    map[string]any
		path    string
		method  string
		wantMsg string
	}{
		{
			name:   "create_cluster_empty_name",
			path:   "/v1/clusters",
			method: http.MethodPost,
			body: map[string]any{
				"clusterName":         "",
				"kafkaVersion":        "2.8.0",
				"numberOfBrokerNodes": 3,
				"brokerNodeGroupInfo": map[string]any{
					"instanceType":  "kafka.m5.large",
					"clientSubnets": []string{"subnet-1"},
				},
			},
			wantMsg: "clusterName is required",
		},
		{
			name:   "create_replicator_empty_name",
			path:   "/replication/v1/replicators",
			method: http.MethodPost,
			body:   map[string]any{"replicatorName": ""},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doKafkaRequest(t, h, tt.method, tt.path, tt.body)

			assert.Equal(t, http.StatusBadRequest, rec.Code)

			if tt.wantMsg != "" {
				assert.Contains(t, rec.Body.String(), tt.wantMsg)
			}
		})
	}
}

func TestErrAlreadyExistsMapping(t *testing.T) {
	t.Parallel()

	tests := []struct {
		fn   func(*kafka.InMemoryBackend) error
		name string
	}{
		{
			name: "duplicate_cluster",
			fn: func(b *kafka.InMemoryBackend) error {
				b.AddClusterInternal("dup", "2.8.0")
				_, err := b.CreateCluster(
					context.Background(),
					"dup",
					"2.8.0",
					3,
					kafka.BrokerNodeGroupInfo{},
					nil,
					nil,
				)

				return err
			},
		},
		{
			name: "duplicate_configuration",
			fn: func(b *kafka.InMemoryBackend) error {
				b.AddConfigurationInternal("dup-cfg")
				_, err := b.CreateConfiguration(context.Background(), "dup-cfg", "", nil, "")

				return err
			},
		},
		{
			name: "duplicate_replicator",
			fn: func(b *kafka.InMemoryBackend) error {
				b.AddReplicatorInternal("dup-rep")
				_, err := b.CreateReplicator(context.Background(), "dup-rep", "", "", nil, nil, nil)

				return err
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := kafka.NewInMemoryBackend(testAccountID, testRegion)
			err := tt.fn(b)

			require.Error(t, err)
			require.ErrorIs(t, err, kafka.ErrAlreadyExists)
		})
	}
}

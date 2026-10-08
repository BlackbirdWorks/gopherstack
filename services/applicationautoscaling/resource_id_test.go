package applicationautoscaling_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/applicationautoscaling"
)

func TestRegisterScalableTarget_ResourceIDShape(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		namespace  string
		dimension  string
		resourceID string
		wantErr    bool
	}{
		{"ecs_ok", "ecs", "ecs:service:DesiredCount", "service/c/s", false},
		{"ecs_missing_service", "ecs", "ecs:service:DesiredCount", "service/c", true},
		{"ecs_wrong_prefix", "ecs", "ecs:service:DesiredCount", "table/c/s", true},
		{"dynamodb_table_ok", "dynamodb", "dynamodb:table:ReadCapacityUnits", "table/t", false},
		{"dynamodb_table_with_index", "dynamodb", "dynamodb:table:ReadCapacityUnits", "table/t/index/i", true},
		{"dynamodb_index_ok", "dynamodb", "dynamodb:index:ReadCapacityUnits", "table/t/index/i", false},
		{"dynamodb_index_bad_marker", "dynamodb", "dynamodb:index:ReadCapacityUnits", "table/t/gsi/i", true},
		{"rds_ok", "rds", "rds:cluster:ReadReplicaCount", "cluster:db", false},
		{"rds_slash", "rds", "rds:cluster:ReadReplicaCount", "cluster/db", true},
		{"lambda_alias_ok", "lambda", "lambda:function:ProvisionedConcurrency", "function:f:prod", false},
		{"lambda_latest", "lambda", "lambda:function:ProvisionedConcurrency", "function:f:$LATEST", true},
		{"lambda_no_qualifier", "lambda", "lambda:function:ProvisionedConcurrency", "function:f", true},
		{"sagemaker_ok", "sagemaker", "sagemaker:variant:DesiredInstanceCount", "endpoint/e/variant/v", false},
		{"sagemaker_bad", "sagemaker", "sagemaker:variant:DesiredInstanceCount", "endpoint/e", true},
		{"cassandra_ok", "cassandra", "cassandra:table:ReadCapacityUnits", "keyspace/k/table/t", false},
		{"kafka_arn_ok", "kafka", "kafka:broker-storage:VolumeSize", "arn:aws:kafka:us-east-1:1:cluster/c/u", false},
		{"kafka_not_arn", "kafka", "kafka:broker-storage:VolumeSize", "cluster/c", true},
		{"comprehend_arn_ok", "comprehend", "comprehend:document-classifier-endpoint:DesiredInferenceUnits",
			"arn:aws:comprehend:us-east-1:1:document-classifier-endpoint/e", false},
		{"comprehend_wrong_kind", "comprehend", "comprehend:entity-recognizer-endpoint:DesiredInferenceUnits",
			"arn:aws:comprehend:us-east-1:1:document-classifier-endpoint/e", true},
		{"custom_any", "custom-resource", "custom-resource:ResourceType:Property", "anything", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			bk := applicationautoscaling.NewInMemoryBackend("000000000000", "us-east-1")
			minCap, maxCap := int32(1), int32(2)

			_, err := bk.RegisterScalableTarget(
				tt.namespace, tt.resourceID, tt.dimension, &minCap, &maxCap, nil, "", nil)

			if tt.wantErr {
				require.ErrorIs(t, err, applicationautoscaling.ErrValidation)

				return
			}

			assert.NoError(t, err)
		})
	}
}

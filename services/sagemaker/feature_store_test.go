package sagemaker_test

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sagemakersdk "github.com/aws/aws-sdk-go-v2/service/sagemaker"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/sagemaker"
)

func TestBackend_FeatureStore_Direct(t *testing.T) {
	t.Parallel()

	b := sagemaker.NewInMemoryBackend("000000000000", "us-east-1")

	// Create feature group with an identifier field.
	_, err := b.CreateFeatureGroup(context.Background(), sagemaker.CreateFeatureGroupOptions{
		FeatureGroupName:            "direct-fg",
		RecordIdentifierFeatureName: "id",
		EventTimeFeatureName:        "event_time",
	})
	require.NoError(t, err)

	// PutRecord.
	err = b.PutRecord(context.Background(), "direct-fg", map[string]string{
		"id":         "rec-1",
		"event_time": "2024-01-01T00:00:00Z",
		"value":      "42",
	})
	require.NoError(t, err)

	// GetRecord.
	rec, err := b.GetRecord(context.Background(), "direct-fg", "rec-1", nil)
	require.NoError(t, err)
	assert.Equal(t, "rec-1", rec.Record["id"])

	// BatchGetRecord.
	results := b.BatchGetRecord(context.Background(), []struct {
		FeatureGroupName              string
		RecordIdentifierValueAsString string
		FeatureNames                  []string
	}{
		{FeatureGroupName: "direct-fg", RecordIdentifierValueAsString: "rec-1"},
	})
	assert.Len(t, results, 1)
	assert.Empty(t, results[0].ErrorCode)

	// DeleteRecord.
	err = b.DeleteRecord(context.Background(), "direct-fg", "rec-1")
	require.NoError(t, err)

	// Record should be gone.
	_, err = b.GetRecord(context.Background(), "direct-fg", "rec-1", nil)
	require.Error(t, err)
}

func TestDescribeFeatureGroup_OnlineStoreTotalSizeBytes(t *testing.T) {
	t.Parallel()

	enabled := true

	tests := []struct {
		name    string
		records []map[string]string
		want    int64
	}{
		{name: "empty", want: 0},
		{
			name:    "one_record",
			records: []map[string]string{{"id": "r1", "v": "abc"}},
			want:    int64(len("id") + len("r1") + len("v") + len("abc")),
		},
		{
			name: "overwrite_same_id",
			records: []map[string]string{
				{"id": "r1", "v": "abc"},
				{"id": "r1", "v": "abcdef"},
			},
			want: int64(len("id") + len("r1") + len("v") + len("abcdef")),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := sagemaker.NewInMemoryBackend("000000000000", "us-east-1")
			_, err := b.CreateFeatureGroup(t.Context(), sagemaker.CreateFeatureGroupOptions{
				FeatureGroupName:            "fg-size",
				RecordIdentifierFeatureName: "id",
				EventTimeFeatureName:        "event_time",
				OnlineStoreConfig:           &sagemaker.OnlineStoreConfig{EnableOnlineStore: &enabled},
			})
			require.NoError(t, err)

			for _, r := range tt.records {
				require.NoError(t, b.PutRecord(t.Context(), "fg-size", r))
			}

			client := newTestSageMakerClient(t, sagemaker.NewHandler(b))
			out, err := client.DescribeFeatureGroup(t.Context(), &sagemakersdk.DescribeFeatureGroupInput{
				FeatureGroupName: aws.String("fg-size"),
			})
			require.NoError(t, err)
			require.NotNil(t, out.OnlineStoreTotalSizeBytes)
			assert.Equal(t, tt.want, aws.ToInt64(out.OnlineStoreTotalSizeBytes))
		})
	}
}

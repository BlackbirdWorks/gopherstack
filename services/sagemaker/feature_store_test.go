package sagemaker_test

import (
	"context"
	"testing"

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

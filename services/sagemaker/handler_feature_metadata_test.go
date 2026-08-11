package sagemaker_test

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/sagemaker"
)

func TestHandler_UpdateAndDescribeFeatureMetadata(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateFeatureGroup", map[string]any{
		"FeatureGroupName":                  "meta-features",
		"RecordIdentifierFeatureDefinition": "id",
		"EventTimeFeatureName":              "event_time",
		"FeatureDefinitions": []map[string]any{
			{"FeatureName": "id", "FeatureType": "Integral"},
			{"FeatureName": "event_time", "FeatureType": "String"},
		},
	})

	recUpdate := doSageMakerRequest(t, h, "UpdateFeatureMetadata", map[string]any{
		"FeatureGroupName": "meta-features",
		"FeatureName":      "id",
		"Description":      "the record id",
	})
	assert.Equal(t, http.StatusOK, recUpdate.Code)

	recDescribe := doSageMakerRequest(t, h, "DescribeFeatureMetadata", map[string]any{
		"FeatureGroupName": "meta-features",
		"FeatureName":      "id",
	})
	assert.Equal(t, http.StatusOK, recDescribe.Code)

	var describeOut map[string]any
	require.NoError(t, json.Unmarshal(recDescribe.Body.Bytes(), &describeOut))
	assert.Equal(t, "the record id", describeOut["Description"])
	assert.Equal(t, "id", describeOut["FeatureName"])
	assert.Equal(t, "Integral", describeOut["FeatureType"])
	assert.NotEmpty(t, describeOut["FeatureGroupArn"])
}

func TestHandler_UpdateFeatureMetadata_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doSageMakerRequest(t, h, "UpdateFeatureMetadata", map[string]any{
		"FeatureGroupName": "no-such-group",
		"FeatureName":      "id",
		"Description":      "stub test",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestBackend_FeatureMetadata_Direct(t *testing.T) {
	t.Parallel()

	b := sagemaker.NewInMemoryBackend("000000000000", "us-east-1")

	// Create feature group.
	_, err := b.CreateFeatureGroup(context.Background(), sagemaker.CreateFeatureGroupOptions{
		FeatureGroupName:            "meta-fg",
		RecordIdentifierFeatureName: "id",
		EventTimeFeatureName:        "event_time",
		FeatureDefinitions: []sagemaker.FeatureDefinition{
			{FeatureName: "id", FeatureType: "Integral"},
			{FeatureName: "event_time", FeatureType: "String"},
		},
	})
	require.NoError(t, err)

	// UpdateFeatureMetadata.
	err = b.UpdateFeatureMetadata(context.Background(), "meta-fg", "id", "The record identifier", nil)
	require.NoError(t, err)

	// GetFeatureMetadata.
	meta, err := b.GetFeatureMetadata(context.Background(), "meta-fg", "id")
	require.NoError(t, err)
	assert.Equal(t, "The record identifier", meta.Description)
}

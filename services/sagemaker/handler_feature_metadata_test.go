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

	// RecordIdentifierFeatureName was previously misspelled
	// "RecordIdentifierFeatureDefinition" here -- the same typo found and
	// fixed in handler_feature_groups_test.go, hidden until this required
	// field was validated.
	doSageMakerRequest(t, h, "CreateFeatureGroup", map[string]any{
		"FeatureGroupName":            "meta-features",
		"RecordIdentifierFeatureName": "id",
		"EventTimeFeatureName":        "event_time",
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

func TestHandler_UpdateFeatureMetadata_ParameterRemovals(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateFeatureGroup", map[string]any{
		"FeatureGroupName":            "meta-features-removals",
		"RecordIdentifierFeatureName": "id",
		"EventTimeFeatureName":        "event_time",
		"FeatureDefinitions": []map[string]any{
			{"FeatureName": "id", "FeatureType": "Integral"},
			{"FeatureName": "event_time", "FeatureType": "String"},
		},
	})

	doSageMakerRequest(t, h, "UpdateFeatureMetadata", map[string]any{
		"FeatureGroupName": "meta-features-removals",
		"FeatureName":      "id",
		"ParameterAdditions": []map[string]any{
			{"Key": "owner", "Value": "team-a"},
			{"Key": "pii", "Value": "false"},
		},
	})

	rec := doSageMakerRequest(t, h, "UpdateFeatureMetadata", map[string]any{
		"FeatureGroupName":  "meta-features-removals",
		"FeatureName":       "id",
		"ParameterRemovals": []string{"pii"},
	})
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	descRec := doSageMakerRequest(t, h, "DescribeFeatureMetadata", map[string]any{
		"FeatureGroupName": "meta-features-removals",
		"FeatureName":      "id",
	})
	require.Equal(t, http.StatusOK, descRec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(descRec.Body.Bytes(), &out))

	params, ok := out["Parameters"].([]any)
	require.True(t, ok)
	require.Len(t, params, 1)

	kv, ok := params[0].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "owner", kv["Key"])
	assert.Equal(t, "team-a", kv["Value"])
}

func TestHandler_DescribeFeatureMetadata_LastModifiedTimeDefaultsToGroupCreation(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateFeatureGroup", map[string]any{
		"FeatureGroupName":            "meta-features-lmt",
		"RecordIdentifierFeatureName": "id",
		"EventTimeFeatureName":        "event_time",
		"FeatureDefinitions": []map[string]any{
			{"FeatureName": "id", "FeatureType": "Integral"},
			{"FeatureName": "event_time", "FeatureType": "String"},
		},
	})

	rec := doSageMakerRequest(t, h, "DescribeFeatureMetadata", map[string]any{
		"FeatureGroupName": "meta-features-lmt",
		"FeatureName":      "id",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

	assert.Equal(t, out["CreationTime"], out["LastModifiedTime"],
		"a never-updated feature's LastModifiedTime falls back to the group's CreationTime")
}

// TestBackend_FeatureMetadata_LastModifiedTimeAdvances asserts on the
// backend's time.Time field directly (nanosecond precision) rather than
// through the wire's epochSeconds truncation, since two calls in the same
// unit test can otherwise land in the same whole second.
func TestBackend_FeatureMetadata_LastModifiedTimeAdvances(t *testing.T) {
	t.Parallel()

	b := sagemaker.NewInMemoryBackend("000000000000", "us-east-1")
	ctx := context.Background()

	_, err := b.CreateFeatureGroup(ctx, sagemaker.CreateFeatureGroupOptions{
		FeatureGroupName:            "meta-lmt",
		RecordIdentifierFeatureName: "id",
		EventTimeFeatureName:        "event_time",
		FeatureDefinitions: []sagemaker.FeatureDefinition{
			{FeatureName: "id", FeatureType: "Integral"},
			{FeatureName: "event_time", FeatureType: "String"},
		},
	})
	require.NoError(t, err)

	before, err := b.GetFeatureMetadata(ctx, "meta-lmt", "id")
	require.NoError(t, err)
	assert.True(t, before.LastModifiedTime.IsZero(), "a never-updated feature has no LastModifiedTime of its own")

	require.NoError(t, b.UpdateFeatureMetadata(ctx, "meta-lmt", "id", "updated", nil, nil))

	after, err := b.GetFeatureMetadata(ctx, "meta-lmt", "id")
	require.NoError(t, err)
	assert.False(t, after.LastModifiedTime.IsZero(),
		"LastModifiedTime must be set once UpdateFeatureMetadata is called")
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
	err = b.UpdateFeatureMetadata(context.Background(), "meta-fg", "id", "The record identifier", nil, nil)
	require.NoError(t, err)

	// GetFeatureMetadata.
	meta, err := b.GetFeatureMetadata(context.Background(), "meta-fg", "id")
	require.NoError(t, err)
	assert.Equal(t, "The record identifier", meta.Description)
}

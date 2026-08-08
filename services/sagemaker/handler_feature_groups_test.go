package sagemaker_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandler_FeatureGroupLifecycle(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create feature group.
	recCreate := doSageMakerRequest(t, h, "CreateFeatureGroup", map[string]any{
		"FeatureGroupName":                  "my-features",
		"RecordIdentifierFeatureDefinition": "id",
		"EventTimeFeatureName":              "event_time",
		"FeatureDefinitions": []map[string]any{
			{"FeatureName": "id", "FeatureType": "Integral"},
			{"FeatureName": "event_time", "FeatureType": "String"},
		},
	})
	assert.Equal(t, http.StatusOK, recCreate.Code)

	var createOut map[string]any
	require.NoError(t, json.Unmarshal(recCreate.Body.Bytes(), &createOut))
	assert.NotEmpty(t, createOut["FeatureGroupArn"])

	// Describe feature group.
	recDesc := doSageMakerRequest(t, h, "DescribeFeatureGroup", map[string]any{
		"FeatureGroupName": "my-features",
	})
	assert.Equal(t, http.StatusOK, recDesc.Code)

	// List feature groups.
	recList := doSageMakerRequest(t, h, "ListFeatureGroups", map[string]any{})
	assert.Equal(t, http.StatusOK, recList.Code)

	var listOut map[string]any
	require.NoError(t, json.Unmarshal(recList.Body.Bytes(), &listOut))
	assert.Len(t, listOut["FeatureGroupSummaries"].([]any), 1)

	// Delete feature group.
	recDelete := doSageMakerRequest(t, h, "DeleteFeatureGroup", map[string]any{
		"FeatureGroupName": "my-features",
	})
	assert.Equal(t, http.StatusOK, recDelete.Code)

	// Should be gone.
	recDesc2 := doSageMakerRequest(t, h, "DescribeFeatureGroup", map[string]any{
		"FeatureGroupName": "my-features",
	})
	assert.Equal(t, http.StatusBadRequest, recDesc2.Code)
}

func TestHandler_FeatureGroup_Duplicate(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	body := map[string]any{"FeatureGroupName": "dup-features"}
	rec := doSageMakerRequest(t, h, "CreateFeatureGroup", body)
	require.Equal(t, http.StatusOK, rec.Code)

	rec2 := doSageMakerRequest(t, h, "CreateFeatureGroup", body)
	assert.Equal(t, http.StatusBadRequest, rec2.Code)
}

func TestHandler_FeatureGroup_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	for _, op := range []string{"DescribeFeatureGroup", "DeleteFeatureGroup"} {
		t.Run(op, func(t *testing.T) {
			t.Parallel()

			rec := doSageMakerRequest(t, h, op, map[string]any{"FeatureGroupName": "nonexistent"})
			assert.Equal(t, http.StatusBadRequest, rec.Code)
		})
	}
}

func TestHandler_UpdateFeatureGroup(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create feature group
	rec := doSageMakerRequest(t, h, "CreateFeatureGroup", map[string]any{
		"FeatureGroupName":            "my-features",
		"RecordIdentifierFeatureName": "id",
		"EventTimeFeatureName":        "ts",
		"FeatureDefinitions": []any{
			map[string]any{"FeatureName": "id", "FeatureType": "String"},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// Update: add a new feature definition
	rec = doSageMakerRequest(t, h, "UpdateFeatureGroup", map[string]any{
		"FeatureGroupName": "my-features",
		"FeatureAdditions": []any{
			map[string]any{"FeatureName": "score", "FeatureType": "Fractional"},
		},
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var updateResp map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &updateResp))
	assert.NotEmpty(t, updateResp["FeatureGroupArn"])

	// Describe should show 2 features now
	rec = doSageMakerRequest(t, h, "DescribeFeatureGroup", map[string]any{
		"FeatureGroupName": "my-features",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var descResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &descResp))
	defs := descResp["FeatureDefinitions"].([]any)
	assert.Len(t, defs, 2)
}

func TestHandler_UpdateFeatureGroup_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doSageMakerRequest(t, h, "UpdateFeatureGroup", map[string]any{
		"FeatureGroupName": "nonexistent",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_Tags_FeatureGroup(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doSageMakerRequest(t, h, "CreateFeatureGroup", map[string]any{
		"FeatureGroupName":            "tagged-fg",
		"RecordIdentifierFeatureName": "id",
		"EventTimeFeatureName":        "ts",
		"Tags": []any{
			map[string]any{"Key": "env", "Value": "test"},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var createResp map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
	fgARN := createResp["FeatureGroupArn"]
	require.NotEmpty(t, fgARN)

	// ListTags should find the tag via ARN
	rec = doSageMakerRequest(t, h, "ListTags", map[string]any{
		"ResourceArn": fgARN,
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var tagsResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &tagsResp))
	tags := tagsResp["Tags"].([]any)
	require.Len(t, tags, 1)
	assert.Equal(t, "env", tags[0].(map[string]any)["Key"])
}

func TestHandler_CreateFeatureGroup_RoleArnAndDescription(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doSageMakerRequest(t, h, "CreateFeatureGroup", map[string]any{
		"FeatureGroupName":            "fg-with-role",
		"RecordIdentifierFeatureName": "id",
		"EventTimeFeatureName":        "ts",
		"RoleArn":                     "arn:aws:iam::000000000000:role/FeatureStoreRole",
		"Description":                 "A feature group with a role",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doSageMakerRequest(t, h, "DescribeFeatureGroup", map[string]any{
		"FeatureGroupName": "fg-with-role",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var descResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &descResp))
	assert.Equal(t, "arn:aws:iam::000000000000:role/FeatureStoreRole", descResp["RoleArn"])
	assert.Equal(t, "A feature group with a role", descResp["Description"])
}

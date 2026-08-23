package sagemaker_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sagemakersdk "github.com/aws/aws-sdk-go-v2/service/sagemaker"
	smtypes "github.com/aws/aws-sdk-go-v2/service/sagemaker/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandler_FeatureGroupLifecycle(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create feature group.
	//
	// RecordIdentifierFeatureName was previously misspelled
	// "RecordIdentifierFeatureDefinition" here -- a typo that this test
	// never caught because the required field it names was never validated.
	recCreate := doSageMakerRequest(t, h, "CreateFeatureGroup", map[string]any{
		"FeatureGroupName":            "my-features",
		"RecordIdentifierFeatureName": "id",
		"EventTimeFeatureName":        "event_time",
		"FeatureDefinitions": []map[string]any{
			{"FeatureName": "id", "FeatureType": "Integral"},
			{"FeatureName": "event_time", "FeatureType": "String"},
		},
	})
	assert.Equal(t, http.StatusOK, recCreate.Code, recCreate.Body.String())

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

	body := map[string]any{
		"FeatureGroupName":            "dup-features",
		"RecordIdentifierFeatureName": "id",
		"EventTimeFeatureName":        "ts",
		"FeatureDefinitions": []any{
			map[string]any{"FeatureName": "id", "FeatureType": "String"},
		},
	}
	rec := doSageMakerRequest(t, h, "CreateFeatureGroup", body)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

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
		"FeatureDefinitions": []any{
			map[string]any{"FeatureName": "id", "FeatureType": "String"},
		},
		"Tags": []any{
			map[string]any{"Key": "env", "Value": "test"},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

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
		"FeatureDefinitions": []any{
			map[string]any{"FeatureName": "id", "FeatureType": "String"},
		},
		"RoleArn":     "arn:aws:iam::000000000000:role/FeatureStoreRole",
		"Description": "A feature group with a role",
	})
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	rec = doSageMakerRequest(t, h, "DescribeFeatureGroup", map[string]any{
		"FeatureGroupName": "fg-with-role",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var descResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &descResp))
	assert.Equal(t, "arn:aws:iam::000000000000:role/FeatureStoreRole", descResp["RoleArn"])
	assert.Equal(t, "A feature group with a role", descResp["Description"])
}

// TestHandler_CreateFeatureGroup_RequiredFieldsEnforced asserts
// EventTimeFeatureName/FeatureDefinitions/RecordIdentifierFeatureName (all
// "This member is required" per api_op_CreateFeatureGroup.go:29-119) are
// each independently rejected when absent -- previously none of the three
// were validated, so a request supplying only FeatureGroupName still
// succeeded.
func TestHandler_CreateFeatureGroup_RequiredFieldsEnforced(t *testing.T) {
	t.Parallel()

	base := map[string]any{
		"FeatureGroupName":            "fg-required",
		"RecordIdentifierFeatureName": "id",
		"EventTimeFeatureName":        "ts",
		"FeatureDefinitions": []any{
			map[string]any{"FeatureName": "id", "FeatureType": "String"},
		},
	}

	tests := []struct {
		name  string
		strip string
	}{
		{"missing record identifier feature name", "RecordIdentifierFeatureName"},
		{"missing event time feature name", "EventTimeFeatureName"},
		{"missing feature definitions", "FeatureDefinitions"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			body := map[string]any{}
			for k, v := range base {
				if k != tt.strip {
					body[k] = v
				}
			}

			rec := doSageMakerRequest(t, h, "CreateFeatureGroup", body)
			assert.Equal(t, http.StatusBadRequest, rec.Code, rec.Body.String())
		})
	}
}

// TestHandler_UpdateFeatureGroup_StoreConfigs_RealClient asserts
// OnlineStoreConfig.TtlDuration and ThroughputConfig -- previously entirely
// absent from decode, so a client updating either got a 200 and no effect
// at all -- now round-trip through DescribeFeatureGroup, and
// LastUpdateStatus (previously never emitted) reports Successful.
func TestHandler_UpdateFeatureGroup_StoreConfigs_RealClient(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestSageMakerClient(t, h)

	_, err := client.CreateFeatureGroup(t.Context(), &sagemakersdk.CreateFeatureGroupInput{
		FeatureGroupName:            aws.String("fg-update-stores"),
		RecordIdentifierFeatureName: aws.String("id"),
		EventTimeFeatureName:        aws.String("ts"),
		FeatureDefinitions: []smtypes.FeatureDefinition{
			{FeatureName: aws.String("id"), FeatureType: smtypes.FeatureTypeString},
		},
	})
	require.NoError(t, err)

	_, err = client.UpdateFeatureGroup(t.Context(), &sagemakersdk.UpdateFeatureGroupInput{
		FeatureGroupName: aws.String("fg-update-stores"),
		OnlineStoreConfig: &smtypes.OnlineStoreConfigUpdate{
			TtlDuration: &smtypes.TtlDuration{Unit: smtypes.TtlDurationUnitDays, Value: aws.Int32(7)},
		},
		ThroughputConfig: &smtypes.ThroughputConfigUpdate{
			ThroughputMode:                smtypes.ThroughputModeProvisioned,
			ProvisionedReadCapacityUnits:  aws.Int32(10),
			ProvisionedWriteCapacityUnits: aws.Int32(10),
		},
	})
	require.NoError(t, err)

	out, err := client.DescribeFeatureGroup(t.Context(), &sagemakersdk.DescribeFeatureGroupInput{
		FeatureGroupName: aws.String("fg-update-stores"),
	})
	require.NoError(t, err)

	require.NotNil(t, out.OnlineStoreConfig)
	require.NotNil(t, out.OnlineStoreConfig.TtlDuration)
	assert.Equal(t, smtypes.TtlDurationUnitDays, out.OnlineStoreConfig.TtlDuration.Unit)
	assert.Equal(t, int32(7), aws.ToInt32(out.OnlineStoreConfig.TtlDuration.Value))

	require.NotNil(t, out.ThroughputConfig)
	assert.Equal(t, smtypes.ThroughputModeProvisioned, out.ThroughputConfig.ThroughputMode)
	assert.Equal(t, int32(10), aws.ToInt32(out.ThroughputConfig.ProvisionedReadCapacityUnits))

	require.NotNil(t, out.LastUpdateStatus)
	assert.Equal(t, smtypes.LastUpdateStatusValueSuccessful, out.LastUpdateStatus.Status)

	// NextToken is "This member is required" on DescribeFeatureGroupOutput
	// (api_op_DescribeFeatureGroup.go:60-63) -- previously absent entirely.
	assert.NotNil(t, out.NextToken)
}

// TestHandler_ListFeatureGroups_FilterSort_RealClient asserts
// NameContains/FeatureGroupStatusEquals filters and SortBy -- previously
// this decoded only NextToken and dropped every filter/sort control.
func TestHandler_ListFeatureGroups_FilterSort_RealClient(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestSageMakerClient(t, h)

	for _, name := range []string{"alpha-fg", "beta-fg", "other-thing"} {
		_, err := client.CreateFeatureGroup(t.Context(), &sagemakersdk.CreateFeatureGroupInput{
			FeatureGroupName:            aws.String(name),
			RecordIdentifierFeatureName: aws.String("id"),
			EventTimeFeatureName:        aws.String("ts"),
			FeatureDefinitions: []smtypes.FeatureDefinition{
				{FeatureName: aws.String("id"), FeatureType: smtypes.FeatureTypeString},
			},
		})
		require.NoError(t, err)
	}

	out, err := client.ListFeatureGroups(t.Context(), &sagemakersdk.ListFeatureGroupsInput{
		NameContains: aws.String("-fg"),
		SortBy:       smtypes.FeatureGroupSortByName,
		SortOrder:    smtypes.FeatureGroupSortOrderDescending,
	})
	require.NoError(t, err)
	require.Len(t, out.FeatureGroupSummaries, 2)
	assert.Equal(t, "beta-fg", aws.ToString(out.FeatureGroupSummaries[0].FeatureGroupName))
	assert.Equal(t, "alpha-fg", aws.ToString(out.FeatureGroupSummaries[1].FeatureGroupName))
}

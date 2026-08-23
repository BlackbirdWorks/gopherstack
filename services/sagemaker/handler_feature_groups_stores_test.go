package sagemaker_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandler_CreateFeatureGroup_StoreConfigsRoundTrip(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doSageMakerRequest(t, h, "CreateFeatureGroup", map[string]any{
		"FeatureGroupName":            "fg-with-stores",
		"RecordIdentifierFeatureName": "id",
		"EventTimeFeatureName":        "ts",
		"FeatureDefinitions": []any{
			map[string]any{"FeatureName": "id", "FeatureType": "String"},
			map[string]any{"FeatureName": "ts", "FeatureType": "Fractional"},
		},
		"OnlineStoreConfig": map[string]any{
			"EnableOnlineStore": true,
			"StorageType":       "InMemory",
			"SecurityConfig": map[string]any{
				"KmsKeyId": "arn:aws:kms:us-east-1:000000000000:key/online",
			},
		},
		"OfflineStoreConfig": map[string]any{
			"S3StorageConfig": map[string]any{
				"S3Uri": "s3://bucket/offline/",
			},
			"TableFormat": "Iceberg",
		},
		"ThroughputConfig": map[string]any{
			"ThroughputMode":                "PROVISIONED",
			"ProvisionedReadCapacityUnits":  5,
			"ProvisionedWriteCapacityUnits": 5,
		},
	})
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	descRec := doSageMakerRequest(t, h, "DescribeFeatureGroup", map[string]any{
		"FeatureGroupName": "fg-with-stores",
	})
	require.Equal(t, http.StatusOK, descRec.Code, descRec.Body.String())

	var out map[string]any
	require.NoError(t, json.Unmarshal(descRec.Body.Bytes(), &out))

	onlineCfg, ok := out["OnlineStoreConfig"].(map[string]any)
	require.True(t, ok, "OnlineStoreConfig must round-trip")
	assert.Equal(t, true, onlineCfg["EnableOnlineStore"])
	assert.Equal(t, "InMemory", onlineCfg["StorageType"])

	security, ok := onlineCfg["SecurityConfig"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "arn:aws:kms:us-east-1:000000000000:key/online", security["KmsKeyId"])

	offlineCfg, ok := out["OfflineStoreConfig"].(map[string]any)
	require.True(t, ok, "OfflineStoreConfig must round-trip")
	assert.Equal(t, "Iceberg", offlineCfg["TableFormat"])

	s3Cfg, ok := offlineCfg["S3StorageConfig"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "s3://bucket/offline/", s3Cfg["S3Uri"])

	throughput, ok := out["ThroughputConfig"].(map[string]any)
	require.True(t, ok, "ThroughputConfig must round-trip")
	assert.Equal(t, "PROVISIONED", throughput["ThroughputMode"])
	assert.InEpsilon(t, float64(5), throughput["ProvisionedReadCapacityUnits"], 0)
}

func TestHandler_CreateFeatureGroup_NoStoreConfigsOmitsFields(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doSageMakerRequest(t, h, "CreateFeatureGroup", map[string]any{
		"FeatureGroupName":            "fg-no-stores",
		"RecordIdentifierFeatureName": "id",
		"EventTimeFeatureName":        "ts",
		"FeatureDefinitions": []any{
			map[string]any{"FeatureName": "id", "FeatureType": "String"},
			map[string]any{"FeatureName": "ts", "FeatureType": "Fractional"},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	descRec := doSageMakerRequest(t, h, "DescribeFeatureGroup", map[string]any{
		"FeatureGroupName": "fg-no-stores",
	})
	require.Equal(t, http.StatusOK, descRec.Code, descRec.Body.String())

	var out map[string]any
	require.NoError(t, json.Unmarshal(descRec.Body.Bytes(), &out))

	_, hasOnline := out["OnlineStoreConfig"]
	_, hasOffline := out["OfflineStoreConfig"]
	_, hasThroughput := out["ThroughputConfig"]
	assert.False(t, hasOnline)
	assert.False(t, hasOffline)
	assert.False(t, hasThroughput)
}

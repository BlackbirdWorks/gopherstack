package kms_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	kmssdk "github.com/aws/aws-sdk-go-v2/service/kms"
	"github.com/aws/aws-sdk-go-v2/service/kms/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandler_KeyMetadataOmitsUndeclaredMembers(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		body string
	}{
		{name: "single_region", body: `{}`},
		{name: "multi_region", body: `{"MultiRegion":true}`},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestKMSHandler()
			rec := sendKMSOp(t, h, "CreateKey", tc.body)
			require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

			var out map[string]map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
			assert.NotContains(t, out["KeyMetadata"], "PrimaryRegion")
			assert.NotContains(t, out["KeyMetadata"], "MultiRegionKeyType")

			id := out["KeyMetadata"]["KeyId"].(string)
			rec = sendKMSOp(t, h, "DescribeKey", `{"KeyId":"`+id+`"}`)
			require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
			assert.NotContains(t, out["KeyMetadata"], "PrimaryRegion")
			assert.NotContains(t, out["KeyMetadata"], "MultiRegionKeyType")
		})
	}
}

func TestSDK_MultiRegionConfigurationSurvives(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		replica string
	}{
		{name: "us_west_2", replica: "us-west-2"},
		{name: "eu_west_1", replica: "eu-west-1"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			client := newTestKMSClient(t, newTestKMSHandler())
			ctx := t.Context()

			created, err := client.CreateKey(ctx, &kmssdk.CreateKeyInput{MultiRegion: aws.Bool(true)})
			require.NoError(t, err)
			assert.True(t, aws.ToBool(created.KeyMetadata.MultiRegion))

			_, err = client.ReplicateKey(ctx, &kmssdk.ReplicateKeyInput{
				KeyId: created.KeyMetadata.KeyId, ReplicaRegion: aws.String(tc.replica),
			})
			require.NoError(t, err)

			desc, err := client.DescribeKey(ctx, &kmssdk.DescribeKeyInput{KeyId: created.KeyMetadata.KeyId})
			require.NoError(t, err)
			cfg := desc.KeyMetadata.MultiRegionConfiguration
			require.NotNil(t, cfg)
			assert.Equal(t, types.MultiRegionKeyTypePrimary, cfg.MultiRegionKeyType)
			require.Len(t, cfg.ReplicaKeys, 1)
			assert.Equal(t, tc.replica, aws.ToString(cfg.ReplicaKeys[0].Region))
		})
	}
}

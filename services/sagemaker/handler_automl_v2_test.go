package sagemaker_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandler_CreateAutoMLJobV2_RoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body            map[string]any
		wantProblemName string
		name            string
	}{
		{
			name: "full",
			body: map[string]any{
				"AutoMLJobName": "automl-v2-full",
				"RoleArn":       "arn:aws:iam::000000000000:role/test",
				"AutoMLJobInputDataConfig": []any{
					map[string]any{
						"ChannelType": "training",
						"DataSource": map[string]any{
							"S3DataSource": map[string]any{
								"S3DataType": "S3Prefix",
								"S3Uri":      "s3://bucket/train/",
							},
						},
					},
				},
				"AutoMLProblemTypeConfig": map[string]any{
					"TabularJobConfig": map[string]any{
						"TargetAttributeName": "label",
					},
				},
				"OutputDataConfig": map[string]any{
					"S3OutputPath": "s3://bucket/out/",
				},
			},
			wantProblemName: "Tabular",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			createRec := doSageMakerRequest(t, h, "CreateAutoMLJobV2", tt.body)
			require.Equal(t, http.StatusOK, createRec.Code, createRec.Body.String())

			descRec := doSageMakerRequest(t, h, "DescribeAutoMLJobV2", map[string]any{
				"AutoMLJobName": tt.body["AutoMLJobName"],
			})
			require.Equal(t, http.StatusOK, descRec.Code, descRec.Body.String())

			var out map[string]any
			require.NoError(t, json.Unmarshal(descRec.Body.Bytes(), &out))

			channels, ok := out["AutoMLJobInputDataConfig"].([]any)
			require.True(t, ok, "DescribeAutoMLJobV2 must always emit AutoMLJobInputDataConfig")

			if tt.body["AutoMLJobInputDataConfig"] != nil {
				require.Len(t, channels, 1)

				channel, channelOK := channels[0].(map[string]any)
				require.True(t, channelOK)
				dataSource, dataSourceOK := channel["DataSource"].(map[string]any)
				require.True(t, dataSourceOK)
				s3Source, s3SourceOK := dataSource["S3DataSource"].(map[string]any)
				require.True(t, s3SourceOK)
				assert.Equal(t, "s3://bucket/train/", s3Source["S3Uri"])
			} else {
				assert.Empty(t, channels)
			}

			_, hasV1Field := out["InputDataConfig"]
			assert.False(t, hasV1Field, "DescribeAutoMLJobV2 must not emit the V1-only InputDataConfig field")

			if tt.body["AutoMLProblemTypeConfig"] != nil {
				problemCfg, problemCfgOK := out["AutoMLProblemTypeConfig"].(map[string]any)
				require.True(t, problemCfgOK, "AutoMLProblemTypeConfig must round-trip opaquely")
				tabular, tabularOK := problemCfg["TabularJobConfig"].(map[string]any)
				require.True(t, tabularOK)
				assert.Equal(t, "label", tabular["TargetAttributeName"])
				assert.Equal(t, tt.wantProblemName, out["AutoMLProblemTypeConfigName"])
			} else {
				_, hasProblemCfg := out["AutoMLProblemTypeConfig"]
				assert.False(t, hasProblemCfg)
			}
		})
	}
}

func TestHandler_CreateAutoMLJobV2_RequiresAllRequiredMembers(t *testing.T) {
	t.Parallel()

	full := map[string]any{
		"AutoMLJobName": "automl-v2-reqs",
		"RoleArn":       "arn:aws:iam::000000000000:role/test",
		"AutoMLJobInputDataConfig": []any{
			map[string]any{"ChannelType": "training"},
		},
		"AutoMLProblemTypeConfig": map[string]any{
			"TabularJobConfig": map[string]any{"TargetAttributeName": "label"},
		},
		"OutputDataConfig": map[string]any{
			"S3OutputPath": "s3://bucket/out/",
		},
	}

	tests := []struct {
		name string
		omit string
	}{
		{name: "missing role arn", omit: "RoleArn"},
		{name: "missing input data config", omit: "AutoMLJobInputDataConfig"},
		{name: "missing problem type config", omit: "AutoMLProblemTypeConfig"},
		{name: "missing output data config", omit: "OutputDataConfig"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			body := make(map[string]any, len(full))
			for k, v := range full {
				if k != tt.omit {
					body[k] = v
				}
			}

			rec := doSageMakerRequest(t, h, "CreateAutoMLJobV2", body)
			assert.Equal(t, http.StatusBadRequest, rec.Code, rec.Body.String())
		})
	}
}

func TestHandler_DescribeAutoMLJobV1_OmitsV2Fields(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createRec := doSageMakerRequest(t, h, "CreateAutoMLJobV2", map[string]any{
		"AutoMLJobName": "automl-v1-view",
		"RoleArn":       "arn:aws:iam::000000000000:role/test",
		"AutoMLJobInputDataConfig": []any{
			map[string]any{"ChannelType": "training"},
		},
		"AutoMLProblemTypeConfig": map[string]any{
			"TabularJobConfig": map[string]any{"TargetAttributeName": "label"},
		},
		"OutputDataConfig": map[string]any{
			"S3OutputPath": "s3://bucket/out/",
		},
	})
	require.Equal(t, http.StatusOK, createRec.Code, createRec.Body.String())

	descRec := doSageMakerRequest(t, h, "DescribeAutoMLJob", map[string]any{
		"AutoMLJobName": "automl-v1-view",
	})
	require.Equal(t, http.StatusOK, descRec.Code, descRec.Body.String())

	var out map[string]any
	require.NoError(t, json.Unmarshal(descRec.Body.Bytes(), &out))

	_, hasV2Field := out["AutoMLJobInputDataConfig"]
	assert.False(t, hasV2Field, "DescribeAutoMLJob (V1) must not emit the V2-only AutoMLJobInputDataConfig field")

	_, hasProblemCfg := out["AutoMLProblemTypeConfig"]
	assert.False(t, hasProblemCfg, "DescribeAutoMLJob (V1) must not emit the V2-only AutoMLProblemTypeConfig field")
}

package stepfunctions_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sfnsdk "github.com/aws/aws-sdk-go-v2/service/sfn"
	sfntypes "github.com/aws/aws-sdk-go-v2/service/sfn/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/stepfunctions"
)

func TestTestState_InspectionLevel_RealClient(t *testing.T) {
	t.Parallel()

	const definition = `{"Type":"Pass","InputPath":"$.data","Parameters":{"v.$":"$.n"},` +
		`"Result":{"r":1},"ResultSelector":{"picked.$":"$.r"},"ResultPath":"$.out","End":true}`

	cases := []struct {
		wantFields map[string]string
		level      sfntypes.InspectionLevel
		name       string
		wantNone   bool
		wantErr    bool
	}{
		{name: "default_info", wantNone: true},
		{name: "explicit_info", level: sfntypes.InspectionLevelInfo, wantNone: true},
		{
			name:  "debug",
			level: sfntypes.InspectionLevelDebug,
			wantFields: map[string]string{
				"input":               `{"data":{"n":7}}`,
				"afterInputPath":      `{"n":7}`,
				"afterParameters":     `{"v":7}`,
				"result":              `{"r":1}`,
				"afterResultSelector": `{"picked":1}`,
				"afterResultPath":     `{"data":{"n":7},"out":{"picked":1}}`,
			},
		},
		{
			name:  "trace_without_http_task",
			level: sfntypes.InspectionLevelTrace,
			wantFields: map[string]string{
				"input":           `{"data":{"n":7}}`,
				"afterInputPath":  `{"n":7}`,
				"afterParameters": `{"v":7}`,
			},
		},
		{name: "unknown_level", level: sfntypes.InspectionLevel("VERBOSE"), wantErr: true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			client := newJSONataClient(t, stepfunctions.NewHandler(stepfunctions.NewInMemoryBackend()))

			out, err := client.TestState(t.Context(), &sfnsdk.TestStateInput{
				Definition:      aws.String(definition),
				Input:           aws.String(`{"data":{"n":7}}`),
				RoleArn:         aws.String(testRoleArn),
				InspectionLevel: tc.level,
			})
			if tc.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, sfntypes.TestExecutionStatusSucceeded, out.Status)

			if tc.wantNone {
				assert.Nil(t, out.InspectionData)

				return
			}

			require.NotNil(t, out.InspectionData)
			got := map[string]*string{
				"input":               out.InspectionData.Input,
				"afterInputPath":      out.InspectionData.AfterInputPath,
				"afterParameters":     out.InspectionData.AfterParameters,
				"result":              out.InspectionData.Result,
				"afterResultSelector": out.InspectionData.AfterResultSelector,
				"afterResultPath":     out.InspectionData.AfterResultPath,
			}

			for key, want := range tc.wantFields {
				require.NotNil(t, got[key], key)
				assert.JSONEq(t, want, aws.ToString(got[key]), key)
			}
		})
	}
}

package ssm_test

import (
	"fmt"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	ssmsdk "github.com/aws/aws-sdk-go-v2/service/ssm"
	"github.com/aws/aws-sdk-go-v2/service/ssm/types"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func putParam(t *testing.T, client *ssmsdk.Client, name string) {
	t.Helper()

	_, err := client.PutParameter(t.Context(), &ssmsdk.PutParameterInput{
		Name: aws.String(name), Value: aws.String("v"), Type: types.ParameterTypeString,
	})
	require.NoError(t, err)
}

func TestLabelParameterVersion_Realism_SDK(t *testing.T) {
	t.Parallel()

	many := make([]string, 11)
	for i := range many {
		many[i] = fmt.Sprintf("lab%d", i)
	}

	tests := []struct {
		version     *int64
		name        string
		wantErrCode string
		labels      []string
		wantInvalid []string
	}{
		{name: "valid", labels: []string{"prod", "v1.2-rc_1"}},
		{name: "reserved prefixes", labels: []string{"aws-x", "SSM1", "ok"}, wantInvalid: []string{"aws-x", "SSM1"}},
		{name: "leading digit", labels: []string{"1abc"}, wantInvalid: []string{"1abc"}},
		{name: "bad chars", labels: []string{"a b", "a/b"}, wantInvalid: []string{"a b", "a/b"}},
		{name: "too long", labels: []string{strings.Repeat("a", 101)}, wantInvalid: []string{strings.Repeat("a", 101)}},
		{name: "eleven labels", labels: many, wantErrCode: "ParameterVersionLabelLimitExceeded"},
		{
			name:        "missing version",
			labels:      []string{"x"},
			version:     aws.Int64(9),
			wantErrCode: "ParameterVersionNotFound",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			client := newRealClient(t)
			putParam(t, client, "/lbl")

			out, err := client.LabelParameterVersion(t.Context(), &ssmsdk.LabelParameterVersionInput{
				Name: aws.String("/lbl"), Labels: tc.labels, ParameterVersion: tc.version,
			})
			if tc.wantErrCode != "" {
				var apiErr smithy.APIError
				require.ErrorAs(t, err, &apiErr, err)
				assert.Equal(t, tc.wantErrCode, apiErr.ErrorCode())

				return
			}

			require.NoError(t, err)
			assert.ElementsMatch(t, tc.wantInvalid, out.InvalidLabels)
		})
	}
}

func TestGetParameters_NameCountLimit_SDK(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		count   int
		wantErr bool
	}{
		{name: "ten", count: 10},
		{name: "eleven", count: 11, wantErr: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			names := make([]string, tc.count)
			for i := range names {
				names[i] = fmt.Sprintf("/n%d", i)
			}

			_, err := newRealClient(t).GetParameters(t.Context(), &ssmsdk.GetParametersInput{Names: names})
			if tc.wantErr {
				var apiErr smithy.APIError
				require.ErrorAs(t, err, &apiErr, err)
				assert.Equal(t, "ValidationException", apiErr.ErrorCode())

				return
			}

			require.NoError(t, err)
		})
	}
}

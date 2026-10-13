package awsconfig_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/awsconfig"
)

func TestPutConformancePack_InputParametersSubstituted(t *testing.T) {
	t.Parallel()

	const jsonTpl = `{
		"Parameters": {"MaxAge": {"Type": "String", "Default": "90"}},
		"Resources": {"R": {"Type": "AWS::Config::ConfigRule", "Properties": {
			"ConfigRuleName": "rule-r",
			"InputParameters": {"maxAccessKeyAge": {"Ref": "MaxAge"}},
			"Source": {"Owner": "AWS", "SourceIdentifier": "ACCESS_KEYS_ROTATED"}}}}
	}`

	const yamlTpl = `
Parameters:
  MaxAge:
    Type: String
    Default: "90"
Resources:
  R:
    Type: AWS::Config::ConfigRule
    Properties:
      ConfigRuleName: rule-r
      InputParameters:
        maxAccessKeyAge: !Ref MaxAge
      Source:
        Owner: AWS
        SourceIdentifier: ACCESS_KEYS_ROTATED
`

	tests := []struct {
		name   string
		tpl    string
		want   string
		params []awsconfig.ConformancePackInputParameter
	}{
		{name: "json_default", tpl: jsonTpl, want: `{"maxAccessKeyAge":"90"}`},
		{
			name:   "json_override",
			tpl:    jsonTpl,
			params: []awsconfig.ConformancePackInputParameter{{ParameterName: "MaxAge", ParameterValue: "30"}},
			want:   `{"maxAccessKeyAge":"30"}`,
		},
		{
			name:   "yaml_short_ref_override",
			tpl:    yamlTpl,
			params: []awsconfig.ConformancePackInputParameter{{ParameterName: "MaxAge", ParameterValue: "45"}},
			want:   `{"maxAccessKeyAge":"45"}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := awsconfig.NewInMemoryBackend()
			_, err := b.PutConformancePackWithParams("p", "", "", tt.tpl, "", "", nil, tt.params)
			require.NoError(t, err)

			rules, err := b.DescribeConfigRules([]string{"rule-r"})
			require.NoError(t, err)
			require.Len(t, rules, 1)
			assert.JSONEq(t, tt.want, rules[0].InputParameters)
		})
	}
}

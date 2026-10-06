package cloudformation_test

import (
	"net/url"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	cfnsdk "github.com/aws/aws-sdk-go-v2/service/cloudformation"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const paramTemplate = `{
	"AWSTemplateFormatVersion": "2010-09-09",
	"Parameters": {
		"Env": {"Type": "String", "AllowedValues": ["dev", "prod"], "AllowedPattern": "[a-z]+",
			"ConstraintDescription": "lowercase"}
	},
	"Resources": {"B": {"Type": "AWS::S3::Bucket"}}
}`

func TestHandler_UndeclaredMembersAbsent(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		values url.Values
		absent []string
	}{
		{
			name:   "template_summary",
			values: url.Values{"Action": {"GetTemplateSummary"}, "TemplateBody": {paramTemplate}},
			absent: []string{"<AllowedPattern>", "<ConstraintDescription>", "<Parameters><member><AllowedValues>"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			resp := postFormValues(t, newHandler(), tc.values)
			resp.mustOK(t)

			for _, a := range tc.absent {
				assert.NotContains(t, resp.Body, a)
			}

			assert.Contains(t, resp.Body, "<ParameterConstraints><AllowedValues>")
		})
	}
}

func TestSDK_TemplateSummaryParameterConstraints(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		want []string
	}{
		{name: "allowed_values", want: []string{"dev", "prod"}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			client := newNestedStackCapableClient(t)

			out, err := client.GetTemplateSummary(t.Context(), &cfnsdk.GetTemplateSummaryInput{
				TemplateBody: aws.String(paramTemplate),
			})
			require.NoError(t, err)
			require.Len(t, out.Parameters, 1)
			assert.Equal(t, "Env", aws.ToString(out.Parameters[0].ParameterKey))
			require.NotNil(t, out.Parameters[0].ParameterConstraints)
			assert.Equal(t, tc.want, out.Parameters[0].ParameterConstraints.AllowedValues)
		})
	}
}

package awsconfig_test

import (
	"context"
	"io/fs"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	configservicesdk "github.com/aws/aws-sdk-go-v2/service/configservice"
	configtypes "github.com/aws/aws-sdk-go-v2/service/configservice/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type fakeTemplates struct {
	s3  map[string]string
	ssm map[string]string
}

func (f fakeTemplates) S3Template(_ context.Context, bucket, key string) ([]byte, error) {
	if v, ok := f.s3[bucket+"/"+key]; ok {
		return []byte(v), nil
	}

	return nil, fs.ErrNotExist
}

func (f fakeTemplates) SSMTemplate(_ context.Context, name, version string) (string, error) {
	if v, ok := f.ssm[name+"@"+version]; ok {
		return v, nil
	}

	return "", fs.ErrNotExist
}

func TestPutConformancePack_TemplateSources(t *testing.T) {
	t.Parallel()

	tests := []struct {
		in       configservicesdk.PutConformancePackInput
		name     string
		errCode  string
		wantRule string
	}{
		{
			name:     "s3_uri",
			in:       configservicesdk.PutConformancePackInput{TemplateS3Uri: aws.String("s3://tpl/pack.json")},
			wantRule: "rule-a",
		},
		{
			name: "ssm_document",
			in: configservicesdk.PutConformancePackInput{
				TemplateSSMDocumentDetails: &configtypes.TemplateSSMDocumentDetails{
					DocumentName: aws.String("my-doc"), DocumentVersion: aws.String("2"),
				},
			},
			wantRule: "rule-a",
		},
		{
			name:    "s3_missing_key",
			in:      configservicesdk.PutConformancePackInput{TemplateS3Uri: aws.String("s3://tpl/absent.json")},
			errCode: "ConformancePackTemplateValidationException",
		},
		{
			name:    "s3_malformed_uri",
			in:      configservicesdk.PutConformancePackInput{TemplateS3Uri: aws.String("https://tpl/pack.json")},
			errCode: "ConformancePackTemplateValidationException",
		},
		{
			name: "ssm_missing_document",
			in: configservicesdk.PutConformancePackInput{
				TemplateSSMDocumentDetails: &configtypes.TemplateSSMDocumentDetails{
					DocumentName: aws.String("absent"),
				},
			},
			errCode: "ConformancePackTemplateValidationException",
		},
		{
			name: "two_sources",
			in: configservicesdk.PutConformancePackInput{
				TemplateBody: aws.String("{}"), TemplateS3Uri: aws.String("s3://tpl/pack.json"),
			},
			errCode: "InvalidParameterValueException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b, client := newOpenItemsClient(t)
			b.SetTemplateSource(fakeTemplates{
				s3:  map[string]string{"tpl/pack.json": conformancePackComplianceTestTemplate},
				ssm: map[string]string{"my-doc@2": conformancePackComplianceTestTemplate},
			})

			in := tt.in
			in.ConformancePackName = aws.String("pack")

			_, err := client.PutConformancePack(t.Context(), &in)
			if tt.errCode != "" {
				require.ErrorContains(t, err, tt.errCode)

				return
			}

			require.NoError(t, err)

			rules, err := b.DescribeConfigRules(nil)
			require.NoError(t, err)
			require.Len(t, rules, 2)
			assert.Equal(t, tt.wantRule, rules[0].ConfigRuleName)
		})
	}
}

func TestPutConformancePack_NoTemplateSourceRejected(t *testing.T) {
	t.Parallel()

	_, client := newOpenItemsClient(t)

	_, err := client.PutConformancePack(t.Context(), &configservicesdk.PutConformancePackInput{
		ConformancePackName: aws.String("no-source"),
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "InvalidParameterValueException")
}

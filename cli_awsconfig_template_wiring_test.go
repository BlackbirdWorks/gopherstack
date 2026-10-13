package main

import (
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/configservice"
	configtypes "github.com/aws/aws-sdk-go-v2/service/configservice/types"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/ssm"
	ssmtypes "github.com/aws/aws-sdk-go-v2/service/ssm/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const conformanceTemplate = `{"Resources":{"RuleA":{"Type":"AWS::Config::ConfigRule","Properties":{
"ConfigRuleName":"tpl-rule","Source":{"Owner":"AWS","SourceIdentifier":"ENCRYPTED_VOLUMES"}}}}}`

func TestAWSConfigConformancePackTemplateWiring(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input func(t *testing.T, fx *sfnFixture) *configservice.PutConformancePackInput
		name  string
	}{
		{name: "s3", input: func(t *testing.T, fx *sfnFixture) *configservice.PutConformancePackInput {
			t.Helper()

			c := s3.NewFromConfig(fx.cfg, func(o *s3.Options) { o.UsePathStyle = true })
			_, err := c.CreateBucket(t.Context(), &s3.CreateBucketInput{Bucket: aws.String("tpl-bucket")})
			require.NoError(t, err)

			_, err = c.PutObject(t.Context(), &s3.PutObjectInput{
				Bucket: aws.String(
					"tpl-bucket",
				),
				Key:  aws.String("pack.json"),
				Body: strings.NewReader(conformanceTemplate),
			})
			require.NoError(t, err)

			return &configservice.PutConformancePackInput{TemplateS3Uri: aws.String("s3://tpl-bucket/pack.json")}
		}},
		{name: "ssm", input: func(t *testing.T, fx *sfnFixture) *configservice.PutConformancePackInput {
			t.Helper()

			_, err := ssm.NewFromConfig(fx.cfg).CreateDocument(t.Context(), &ssm.CreateDocumentInput{
				Name: aws.String("tpl-doc"), Content: aws.String(conformanceTemplate),
				DocumentType: ssmtypes.DocumentTypeConformancePackTemplate, DocumentFormat: ssmtypes.DocumentFormatJson,
			})
			require.NoError(t, err)

			return &configservice.PutConformancePackInput{
				TemplateSSMDocumentDetails: &configtypes.TemplateSSMDocumentDetails{
					DocumentName: aws.String("tpl-doc"),
				},
			}
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			fx := newSFNFixture(t)
			in := tt.input(t, fx)
			in.ConformancePackName = aws.String("pack")

			cfg := configservice.NewFromConfig(fx.cfg)
			_, err := cfg.PutConformancePack(t.Context(), in)
			require.NoError(t, err)

			rules, err := cfg.DescribeConfigRules(t.Context(), &configservice.DescribeConfigRulesInput{})
			require.NoError(t, err)
			require.Len(t, rules.ConfigRules, 1)
			assert.Equal(t, "tpl-rule", aws.ToString(rules.ConfigRules[0].ConfigRuleName))
		})
	}
}

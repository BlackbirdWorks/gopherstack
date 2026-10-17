package app

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/acm"
	"github.com/aws/aws-sdk-go-v2/service/amplify"
	"github.com/aws/aws-sdk-go-v2/service/codedeploy"
	"github.com/aws/aws-sdk-go-v2/service/codepipeline"
	"github.com/aws/aws-sdk-go-v2/service/eventbridge"
	"github.com/aws/aws-sdk-go-v2/service/iot"
	"github.com/aws/aws-sdk-go-v2/service/route53resolver"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/transcribe"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/require"
)

func TestSDKErrorMessageNonEmpty(t *testing.T) {
	t.Parallel()

	_, cfg := devEndpointsServer(t)

	tests := []struct {
		call func(ctx context.Context) error
		name string
	}{
		{name: "route53resolver", call: func(ctx context.Context) error {
			_, err := route53resolver.NewFromConfig(cfg).GetResolverEndpoint(ctx,
				&route53resolver.GetResolverEndpointInput{ResolverEndpointId: aws.String("rslvr-in-missing")})

			return err
		}},
		{name: "sqs", call: func(ctx context.Context) error {
			_, err := sqs.NewFromConfig(cfg).GetQueueUrl(ctx,
				&sqs.GetQueueUrlInput{QueueName: aws.String("missing-queue")})

			return err
		}},
		{name: "eventbridge", call: func(ctx context.Context) error {
			_, err := eventbridge.NewFromConfig(cfg).DescribeRule(ctx,
				&eventbridge.DescribeRuleInput{Name: aws.String("missing-rule")})

			return err
		}},
		{name: "acm", call: func(ctx context.Context) error {
			arn := "arn:aws:acm:us-east-1:000000000000:certificate/00000000-0000-0000-0000-000000000000"
			_, err := acm.NewFromConfig(cfg).DescribeCertificate(ctx, &acm.DescribeCertificateInput{
				CertificateArn: aws.String(arn),
			})

			return err
		}},
		{name: "codedeploy", call: func(ctx context.Context) error {
			_, err := codedeploy.NewFromConfig(cfg).GetApplication(ctx,
				&codedeploy.GetApplicationInput{ApplicationName: aws.String("missing-app")})

			return err
		}},
		{name: "codepipeline", call: func(ctx context.Context) error {
			_, err := codepipeline.NewFromConfig(cfg).GetPipeline(ctx,
				&codepipeline.GetPipelineInput{Name: aws.String("missing-pipeline")})

			return err
		}},
		{name: "transcribe", call: func(ctx context.Context) error {
			_, err := transcribe.NewFromConfig(cfg).GetTranscriptionJob(ctx,
				&transcribe.GetTranscriptionJobInput{TranscriptionJobName: aws.String("missing-job")})

			return err
		}},
		{name: "amplify", call: func(ctx context.Context) error {
			_, err := amplify.NewFromConfig(cfg).GetApp(ctx, &amplify.GetAppInput{AppId: aws.String("d1missing")})

			return err
		}},
		{name: "iot", call: func(ctx context.Context) error {
			_, err := iot.NewFromConfig(cfg).DescribeThing(ctx,
				&iot.DescribeThingInput{ThingName: aws.String("missing-thing")})

			return err
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := tt.call(t.Context())
			require.Error(t, err)

			var apiErr smithy.APIError
			require.ErrorAs(t, err, &apiErr)
			require.NotEmpty(t, apiErr.ErrorCode())
			require.NotEmpty(t, apiErr.ErrorMessage(), "error: %v", err)
		})
	}
}

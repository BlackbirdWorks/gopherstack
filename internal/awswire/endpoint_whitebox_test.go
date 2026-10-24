package awswire

import (
	"context"
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatch"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	"github.com/aws/aws-sdk-go-v2/service/eventbridge"
	"github.com/aws/aws-sdk-go-v2/service/iam"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kms"
	"github.com/aws/aws-sdk-go-v2/service/lambda"
	"github.com/aws/aws-sdk-go-v2/service/secretsmanager"
	"github.com/aws/aws-sdk-go-v2/service/sfn"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/ssm"
	smithyauth "github.com/aws/smithy-go/auth"
	smithyendpoints "github.com/aws/smithy-go/endpoints"
	smithyhttp "github.com/aws/smithy-go/transport/http"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type sdkResolver func(ctx context.Context, region string, fips, dual bool) (smithyendpoints.Endpoint, error)

func sdkResolvers() map[string]sdkResolver {
	return map[string]sdkResolver{
		"cloudwatch": func(ctx context.Context, region string, fips, dual bool) (smithyendpoints.Endpoint, error) {
			return cloudwatch.NewDefaultEndpointResolverV2().ResolveEndpoint(ctx, cloudwatch.EndpointParameters{
				Region: aws.String(region), UseFIPS: aws.Bool(fips), UseDualStack: aws.Bool(dual),
			})
		},
		"cloudwatchlogs": func(ctx context.Context, region string, fips, dual bool) (smithyendpoints.Endpoint, error) {
			return cloudwatchlogs.NewDefaultEndpointResolverV2().ResolveEndpoint(ctx, cloudwatchlogs.EndpointParameters{
				Region: aws.String(region), UseFIPS: aws.Bool(fips), UseDualStack: aws.Bool(dual),
			})
		},
		"eventbridge": func(ctx context.Context, region string, fips, dual bool) (smithyendpoints.Endpoint, error) {
			return eventbridge.NewDefaultEndpointResolverV2().ResolveEndpoint(ctx, eventbridge.EndpointParameters{
				Region: aws.String(region), UseFIPS: aws.Bool(fips), UseDualStack: aws.Bool(dual),
			})
		},
		"iam": func(ctx context.Context, region string, fips, dual bool) (smithyendpoints.Endpoint, error) {
			return iam.NewDefaultEndpointResolverV2().ResolveEndpoint(ctx, iam.EndpointParameters{
				Region: aws.String(region), UseFIPS: aws.Bool(fips), UseDualStack: aws.Bool(dual),
			})
		},
		"kinesis": func(ctx context.Context, region string, fips, dual bool) (smithyendpoints.Endpoint, error) {
			return kinesis.NewDefaultEndpointResolverV2().ResolveEndpoint(ctx, kinesis.EndpointParameters{
				Region: aws.String(region), UseFIPS: aws.Bool(fips), UseDualStack: aws.Bool(dual),
			})
		},
		"kms": func(ctx context.Context, region string, fips, dual bool) (smithyendpoints.Endpoint, error) {
			return kms.NewDefaultEndpointResolverV2().ResolveEndpoint(ctx, kms.EndpointParameters{
				Region: aws.String(region), UseFIPS: aws.Bool(fips), UseDualStack: aws.Bool(dual),
			})
		},
		"lambda": func(ctx context.Context, region string, fips, dual bool) (smithyendpoints.Endpoint, error) {
			return lambda.NewDefaultEndpointResolverV2().ResolveEndpoint(ctx, lambda.EndpointParameters{
				Region: aws.String(region), UseFIPS: aws.Bool(fips), UseDualStack: aws.Bool(dual),
			})
		},
		"secretsmanager": func(ctx context.Context, region string, fips, dual bool) (smithyendpoints.Endpoint, error) {
			return secretsmanager.NewDefaultEndpointResolverV2().ResolveEndpoint(ctx, secretsmanager.EndpointParameters{
				Region: aws.String(region), UseFIPS: aws.Bool(fips), UseDualStack: aws.Bool(dual),
			})
		},
		"sfn": func(ctx context.Context, region string, fips, dual bool) (smithyendpoints.Endpoint, error) {
			return sfn.NewDefaultEndpointResolverV2().ResolveEndpoint(ctx, sfn.EndpointParameters{
				Region: aws.String(region), UseFIPS: aws.Bool(fips), UseDualStack: aws.Bool(dual),
			})
		},
		"sns": func(ctx context.Context, region string, fips, dual bool) (smithyendpoints.Endpoint, error) {
			return sns.NewDefaultEndpointResolverV2().ResolveEndpoint(ctx, sns.EndpointParameters{
				Region: aws.String(region), UseFIPS: aws.Bool(fips), UseDualStack: aws.Bool(dual),
			})
		},
		"sqs": func(ctx context.Context, region string, fips, dual bool) (smithyendpoints.Endpoint, error) {
			return sqs.NewDefaultEndpointResolverV2().ResolveEndpoint(ctx, sqs.EndpointParameters{
				Region: aws.String(region), UseFIPS: aws.Bool(fips), UseDualStack: aws.Bool(dual),
			})
		},
		"ssm": func(ctx context.Context, region string, fips, dual bool) (smithyendpoints.Endpoint, error) {
			return ssm.NewDefaultEndpointResolverV2().ResolveEndpoint(ctx, ssm.EndpointParameters{
				Region: aws.String(region), UseFIPS: aws.Bool(fips), UseDualStack: aws.Bool(dual),
			})
		},
		"ec2": func(ctx context.Context, region string, fips, dual bool) (smithyendpoints.Endpoint, error) {
			return ec2.NewDefaultEndpointResolverV2().ResolveEndpoint(ctx, ec2.EndpointParameters{
				Region: aws.String(region), UseFIPS: aws.Bool(fips), UseDualStack: aws.Bool(dual),
			})
		},
	}
}

func sdkSigningRegion(ep smithyendpoints.Endpoint) string {
	opts, ok := smithyauth.GetAuthOptions(&ep.Properties)
	if !ok || len(opts) == 0 {
		return ""
	}

	r, _ := smithyhttp.GetSigV4SigningRegion(&opts[0].SignerProperties)

	return r
}

// TestResolveMatchesSDKRules checks every service against the endpoint rules of its real client.
func TestResolveMatchesSDKRules(t *testing.T) {
	t.Parallel()

	regions := []string{
		"us-east-1", "eu-west-1", "ap-southeast-2", "cn-north-1", "cn-northwest-1", "us-gov-west-1", "us-gov-east-1",
		"us-iso-east-1", "us-iso-west-1", "us-isob-east-1", "eu-isoe-west-1", "us-isof-south-1", "eusc-de-east-1",
		"mars-1", "aws-global", "fips-us-west-2", "us-west-2-fips", "fips-us-gov-west-1",
	}

	for key, sdk := range sdkResolvers() {
		for _, region := range regions {
			for _, fips := range []bool{false, true} {
				for _, dual := range []bool{false, true} {
					name := fmt.Sprintf("%s/%s/fips=%v/dual=%v", key, region, fips, dual)

					t.Run(name, func(t *testing.T) {
						t.Parallel()

						opts := []func(*config.LoadOptions) error{
							config.WithRegion(region),
							config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("k", "s", "")),
							config.WithSharedConfigFiles([]string{}),
							config.WithSharedCredentialsFiles([]string{}),
						}
						if fips {
							opts = append(opts, config.WithUseFIPSEndpoint(aws.FIPSEndpointStateEnabled))
						}

						if dual {
							opts = append(opts, config.WithUseDualStackEndpoint(aws.DualStackEndpointStateEnabled))
						}

						cfg, err := config.LoadDefaultConfig(t.Context(), opts...)
						require.NoError(t, err)

						sdkRegion, sdkFIPS := region, fips
						if r, legacy := FIPSRegion(region); legacy {
							sdkRegion, sdkFIPS = r, true
						}

						want, err := sdk(t.Context(), sdkRegion, sdkFIPS, dual)
						require.NoError(t, err)

						got := resolve(key, cfg, nil)
						assert.Equal(t, want.URI.String(), got.url, "differs from the SDK endpoint rules")

						wantSigning := sdkSigningRegion(want)
						if wantSigning == "" {
							wantSigning = sdkRegion
						}

						assert.Equal(t, wantSigning, got.signingRegion)
					})
				}
			}
		}
	}
}

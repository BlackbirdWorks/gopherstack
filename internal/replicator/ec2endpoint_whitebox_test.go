package replicator

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEC2EndpointPartitions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		region string
		want   string
		fips   bool
		dual   bool
	}{
		{name: "commercial", region: "eu-west-1", want: "https://ec2.eu-west-1.amazonaws.com"},
		{name: "china", region: "cn-north-1", want: "https://ec2.cn-north-1.amazonaws.com.cn"},
		{name: "govcloud", region: "us-gov-west-1", want: "https://ec2.us-gov-west-1.amazonaws.com"},
		{name: "iso", region: "us-iso-east-1", want: "https://ec2.us-iso-east-1.c2s.ic.gov"},
		{name: "iso-b", region: "us-isob-east-1", want: "https://ec2.us-isob-east-1.sc2s.sgov.gov"},
		{name: "iso-e", region: "eu-isoe-west-1", want: "https://ec2.eu-isoe-west-1.cloud.adc-e.uk"},
		{name: "iso-f", region: "us-isof-south-1", want: "https://ec2.us-isof-south-1.csp.hci.ic.gov"},
		{name: "eusc", region: "eusc-de-east-1", want: "https://ec2.eusc-de-east-1.amazonaws.eu"},
		{name: "unknown region", region: "mars-1", want: "https://ec2.mars-1.amazonaws.com"},
		{name: "fips", region: "us-east-1", fips: true, want: "https://ec2-fips.us-east-1.amazonaws.com"},
		{name: "fips govcloud", region: "us-gov-east-1", fips: true, want: "https://ec2.us-gov-east-1.amazonaws.com"},
		{name: "fips china", region: "cn-north-1", fips: true, want: "https://ec2-fips.cn-north-1.amazonaws.com.cn"},
		{name: "fips iso", region: "us-iso-east-1", fips: true, want: "https://ec2-fips.us-iso-east-1.c2s.ic.gov"},
		{name: "legacy fips region", region: "fips-us-west-2", want: "https://ec2-fips.us-west-2.amazonaws.com"},
		{name: "dual stack", region: "us-east-1", dual: true, want: "https://ec2.us-east-1.api.aws"},
		{
			name: "dual stack china", region: "cn-north-1", dual: true,
			want: "https://ec2.cn-north-1.api.amazonwebservices.com.cn",
		},
		{
			name: "dual stack iso", region: "us-iso-east-1", dual: true,
			want: "https://ec2.us-iso-east-1.api.aws.ic.gov",
		},
		{
			name: "fips dual stack", region: "eu-west-1", fips: true, dual: true,
			want: "https://ec2-fips.eu-west-1.api.aws",
		},
		{
			name: "fips dual stack iso-b", region: "us-isob-east-1", fips: true, dual: true,
			want: "https://ec2-fips.us-isob-east-1.api.aws.scloud",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			opts := []func(*config.LoadOptions) error{
				config.WithRegion(tt.region),
				config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("k", "s", "")),
				config.WithSharedConfigFiles([]string{}),
				config.WithSharedCredentialsFiles([]string{}),
			}
			if tt.fips {
				opts = append(opts, config.WithUseFIPSEndpoint(aws.FIPSEndpointStateEnabled))
			}

			if tt.dual {
				opts = append(opts, config.WithUseDualStackEndpoint(aws.DualStackEndpointStateEnabled))
			}

			cfg, err := config.LoadDefaultConfig(t.Context(), opts...)
			require.NoError(t, err)

			assert.Equal(t, tt.want, ec2Endpoint(cfg))

			params := ec2.EndpointParameters{
				Region:       aws.String(tt.region),
				UseFIPS:      aws.Bool(tt.fips),
				UseDualStack: aws.Bool(tt.dual),
			}
			if r, legacy := fipsRegion(tt.region); legacy {
				params.Region, params.UseFIPS = aws.String(r), aws.Bool(true)
			}

			ep, err := ec2.NewDefaultEndpointResolverV2().ResolveEndpoint(t.Context(), params)
			require.NoError(t, err)
			assert.Equal(t, ep.URI.String(), ec2Endpoint(cfg), "differs from the SDK endpoint rules")
		})
	}
}

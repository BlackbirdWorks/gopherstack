package cognitoidp_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	cognitoidpsdk "github.com/aws/aws-sdk-go-v2/service/cognitoidentityprovider"
	"github.com/aws/aws-sdk-go-v2/service/cognitoidentityprovider/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestUserPoolDomain_Routing(t *testing.T) {
	t.Parallel()

	failover := &types.RoutingType{Failover: &types.FailoverType{
		PrimaryRoute53HealthCheckId: aws.String("hc-1234"),
		SecondaryRegion:             aws.String("us-west-2"),
	}}

	tests := []struct {
		cert    *types.CustomDomainConfigType
		routing *types.RoutingType
		name    string
		domain  string
		wantErr bool
	}{
		{
			name:    "custom domain with failover",
			domain:  "auth.example.com",
			routing: failover,
			cert: &types.CustomDomainConfigType{
				CertificateArn: aws.String("arn:aws:acm:us-east-1:000000000000:certificate/abc"),
			},
		},
		{name: "prefix domain rejects routing", domain: "prefix-domain", routing: failover, wantErr: true},
		{name: "no routing", domain: "plain-domain"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestCognitoIDPClient(t, newTestHandler(t))
			ctx := t.Context()

			pool, err := client.CreateUserPool(ctx, &cognitoidpsdk.CreateUserPoolInput{PoolName: aws.String("routing")})
			require.NoError(t, err)

			created, err := client.CreateUserPoolDomain(ctx, &cognitoidpsdk.CreateUserPoolDomainInput{
				UserPoolId: pool.UserPool.Id, Domain: aws.String(tt.domain),
				CustomDomainConfig: tt.cert, Routing: tt.routing,
			})

			if tt.wantErr {
				var invalid *types.InvalidParameterException
				require.ErrorAs(t, err, &invalid)

				return
			}

			require.NoError(t, err)

			described, err := client.DescribeUserPoolDomain(ctx, &cognitoidpsdk.DescribeUserPoolDomainInput{
				Domain: aws.String(tt.domain),
			})
			require.NoError(t, err)

			if tt.routing == nil {
				assert.Nil(t, created.Routing)
				assert.Nil(t, described.DomainDescription.Routing)

				return
			}

			require.NotNil(t, created.Routing)
			assert.Equal(t, "hc-1234", aws.ToString(created.Routing.Failover.PrimaryRoute53HealthCheckId))
			require.NotNil(t, described.DomainDescription.Routing)
			assert.Equal(t, "us-west-2", aws.ToString(described.DomainDescription.Routing.Failover.SecondaryRegion))
		})
	}
}

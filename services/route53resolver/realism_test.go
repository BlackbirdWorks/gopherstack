package route53resolver_test

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	route53resolversdk "github.com/aws/aws-sdk-go-v2/service/route53resolver"
	"github.com/aws/aws-sdk-go-v2/service/route53resolver/types"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/route53resolver"
)

func createRealismEndpoint(
	t *testing.T,
	c *route53resolversdk.Client,
	ip string,
) (*route53resolversdk.CreateResolverEndpointOutput, error) {
	t.Helper()

	return c.CreateResolverEndpoint(t.Context(), &route53resolversdk.CreateResolverEndpointInput{
		CreatorRequestId: aws.String("req-" + ip),
		Name:             aws.String("ep"),
		Direction:        types.ResolverEndpointDirectionInbound,
		SecurityGroupIds: []string{"sg-1"},
		IpAddresses: []types.IpAddressRequest{
			{SubnetId: aws.String("subnet-1"), Ip: aws.String(ip)},
			{SubnetId: aws.String("subnet-2")},
		},
	})
}

func TestRealism_Lifecycle(t *testing.T) {
	t.Parallel()

	b := route53resolver.NewInMemoryBackend("000000000000", "us-east-1")
	b.SetLifecycleDelay(time.Minute)

	var offset atomic.Int64

	base := time.Now()
	b.SetClock(func() time.Time { return base.Add(time.Duration(offset.Load())) })

	c := newTestRoute53ResolverClient(t, route53resolver.NewHandler(b))
	ctx := t.Context()

	created, err := createRealismEndpoint(t, c, "10.0.0.5")
	require.NoError(t, err)
	assert.Equal(t, types.ResolverEndpointStatusCreating, created.ResolverEndpoint.Status)

	id := created.ResolverEndpoint.Id

	got, err := c.GetResolverEndpoint(ctx, &route53resolversdk.GetResolverEndpointInput{ResolverEndpointId: id})
	require.NoError(t, err)
	assert.Equal(t, types.ResolverEndpointStatusCreating, got.ResolverEndpoint.Status)

	rule, err := c.CreateResolverRule(ctx, &route53resolversdk.CreateResolverRuleInput{
		CreatorRequestId:   aws.String("rule-req"),
		DomainName:         aws.String("example.com"),
		RuleType:           types.RuleTypeOptionForward,
		ResolverEndpointId: id,
		TargetIps:          []types.TargetAddress{{Ip: aws.String("10.1.1.1")}},
	})
	require.NoError(t, err)

	assoc, err := c.AssociateResolverRule(ctx, &route53resolversdk.AssociateResolverRuleInput{
		ResolverRuleId: rule.ResolverRule.Id, VPCId: aws.String("vpc-1"),
	})
	require.NoError(t, err)
	assert.Equal(t, types.ResolverRuleAssociationStatusCreating, assoc.ResolverRuleAssociation.Status)

	offset.Store(int64(2 * time.Minute))

	got, err = c.GetResolverEndpoint(ctx, &route53resolversdk.GetResolverEndpointInput{ResolverEndpointId: id})
	require.NoError(t, err)
	assert.Equal(t, types.ResolverEndpointStatusOperational, got.ResolverEndpoint.Status)

	ga, err := c.GetResolverRuleAssociation(ctx, &route53resolversdk.GetResolverRuleAssociationInput{
		ResolverRuleAssociationId: assoc.ResolverRuleAssociation.Id,
	})
	require.NoError(t, err)
	assert.Equal(t, types.ResolverRuleAssociationStatusComplete, ga.ResolverRuleAssociation.Status)

	dis, err := c.DisassociateResolverRule(ctx, &route53resolversdk.DisassociateResolverRuleInput{
		ResolverRuleId: rule.ResolverRule.Id, VPCId: aws.String("vpc-1"),
	})
	require.NoError(t, err)
	assert.Equal(t, types.ResolverRuleAssociationStatusDeleting, dis.ResolverRuleAssociation.Status)

	delRule, err := c.DeleteResolverRule(
		ctx,
		&route53resolversdk.DeleteResolverRuleInput{ResolverRuleId: rule.ResolverRule.Id},
	)
	require.NoError(t, err)
	assert.Equal(t, types.ResolverRuleStatusDeleting, delRule.ResolverRule.Status)

	del, err := c.DeleteResolverEndpoint(ctx, &route53resolversdk.DeleteResolverEndpointInput{ResolverEndpointId: id})
	require.NoError(t, err)
	assert.Equal(t, types.ResolverEndpointStatusDeleting, del.ResolverEndpoint.Status)
}

func TestRealism_Errors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		call     func(t *testing.T, c *route53resolversdk.Client) error
		name     string
		wantCode string
		wantMsg  string
	}{
		{
			name: "endpoint not found",
			call: func(t *testing.T, c *route53resolversdk.Client) error {
				t.Helper()
				_, err := c.GetResolverEndpoint(t.Context(), &route53resolversdk.GetResolverEndpointInput{
					ResolverEndpointId: aws.String("rslvr-in-ghost"),
				})

				return err
			},
			wantCode: "ResourceNotFoundException",
			wantMsg:  "Resolver endpoint with ID 'rslvr-in-ghost' does not exist",
		},
		{
			name: "bad endpoint ip",
			call: func(t *testing.T, c *route53resolversdk.Client) error {
				t.Helper()
				_, err := createRealismEndpoint(t, c, "not-an-ip")

				return err
			},
			wantCode: "InvalidParameterException",
			wantMsg:  `Ip "not-an-ip" is not a valid IP address`,
		},
		{
			name: "bad target ip",
			call: func(t *testing.T, c *route53resolversdk.Client) error {
				t.Helper()
				_, err := c.CreateResolverRule(t.Context(), &route53resolversdk.CreateResolverRuleInput{
					CreatorRequestId: aws.String("r"),
					DomainName:       aws.String("example.com"),
					RuleType:         types.RuleTypeOptionForward,
					TargetIps:        []types.TargetAddress{{Ip: aws.String("300.1.1.1")}},
				})

				return err
			},
			wantCode: "InvalidParameterException",
			wantMsg:  `Ip "300.1.1.1" is not a valid IP address`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			c := newRealClient(t)

			var apiErr smithy.APIError
			require.ErrorAs(t, tt.call(t, c), &apiErr)
			assert.Equal(t, tt.wantCode, apiErr.ErrorCode())
			assert.Equal(t, tt.wantMsg, apiErr.ErrorMessage())
		})
	}
}

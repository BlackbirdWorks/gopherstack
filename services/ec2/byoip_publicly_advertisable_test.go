package ec2_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	ec2sdk "github.com/aws/aws-sdk-go-v2/service/ec2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ec2"
)

func TestProvisionByoipCidr_PubliclyAdvertisable_RealClient(t *testing.T) {
	t.Parallel()

	cases := []struct {
		advertisable *bool
		name         string
		cidr         string
		wantState    string
	}{
		{name: "ipv6_default", cidr: "2001:db8::/48", wantState: "pending-provision"},
		{name: "ipv6_true", cidr: "2001:db8::/48", advertisable: aws.Bool(true), wantState: "pending-provision"},
		{
			name: "ipv6_false", cidr: "2001:db8::/48", advertisable: aws.Bool(false),
			wantState: "provisioned-not-publicly-advertisable",
		},
		{
			name:         "ipv4_false_ignored",
			cidr:         "203.0.113.0/24",
			advertisable: aws.Bool(false),
			wantState:    "pending-provision",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			client := newTestEC2Client(t, ec2.NewHandler(ec2.NewInMemoryBackend("000000000000", "us-east-1")))

			_, err := client.ProvisionByoipCidr(t.Context(), &ec2sdk.ProvisionByoipCidrInput{
				Cidr:                 aws.String(tc.cidr),
				PubliclyAdvertisable: tc.advertisable,
			})
			require.NoError(t, err)

			out, err := client.DescribeByoipCidrs(
				t.Context(),
				&ec2sdk.DescribeByoipCidrsInput{MaxResults: aws.Int32(10)},
			)
			require.NoError(t, err)
			require.Len(t, out.ByoipCidrs, 1)
			assert.Equal(t, tc.wantState, string(out.ByoipCidrs[0].State))
		})
	}
}

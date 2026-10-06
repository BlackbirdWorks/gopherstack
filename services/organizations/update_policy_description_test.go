package organizations_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	organizationssdk "github.com/aws/aws-sdk-go-v2/service/organizations"
	organizationstypes "github.com/aws/aws-sdk-go-v2/service/organizations/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRealClient_UpdatePolicyDescription(t *testing.T) {
	t.Parallel()

	tests := []struct {
		desc *string
		name string
		want string
	}{
		{name: "omitted keeps", desc: nil, want: "initial"},
		{name: "empty clears", desc: aws.String(""), want: ""},
		{name: "new value", desc: aws.String("changed"), want: "changed"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client, _ := newRealClient(t)
			ctx := t.Context()

			created, err := client.CreatePolicy(ctx, &organizationssdk.CreatePolicyInput{
				Name:        aws.String("p"),
				Description: aws.String("initial"),
				Type:        organizationstypes.PolicyTypeServiceControlPolicy,
				Content: aws.String(
					`{"Version":"2012-10-17","Statement":[{"Effect":"Deny","Action":"*","Resource":"*"}]}`,
				),
			})
			require.NoError(t, err)

			id := created.Policy.PolicySummary.Id
			_, err = client.UpdatePolicy(ctx, &organizationssdk.UpdatePolicyInput{PolicyId: id, Description: tt.desc})
			require.NoError(t, err)

			got, err := client.DescribePolicy(ctx, &organizationssdk.DescribePolicyInput{PolicyId: id})
			require.NoError(t, err)
			assert.Equal(t, tt.want, aws.ToString(got.Policy.PolicySummary.Description))
		})
	}
}

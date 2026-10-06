package lambda_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	lambdasdk "github.com/aws/aws-sdk-go-v2/service/lambda"
	"github.com/aws/aws-sdk-go-v2/service/lambda/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/lambda"
)

func TestListCapacityProviders_PagingAndState(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		state     types.CapacityProviderState
		wantPages []int
		maxItems  int32
	}{
		{name: "all one page", wantPages: []int{3}},
		{name: "pages of two", maxItems: 2, wantPages: []int{2, 1}},
		{name: "active filter", state: types.CapacityProviderStateActive, wantPages: []int{3}},
		{name: "failed filter", state: types.CapacityProviderStateFailed, wantPages: []int{0}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := lambda.NewInMemoryBackend(
				nil,
				nil,
				lambda.DefaultSettings(),
				"000000000000",
				capacityProviderTestRegion,
			)
			client := newTestLambdaClient(t, lambda.NewHandler(backend))

			for _, name := range []string{"cp-c", "cp-a", "cp-b"} {
				_, err := client.CreateCapacityProvider(t.Context(), &lambdasdk.CreateCapacityProviderInput{
					CapacityProviderName: aws.String(name),
					PermissionsConfig: &types.CapacityProviderPermissionsConfig{
						CapacityProviderOperatorRoleArn: aws.String("arn:aws:iam::000000000000:role/cp-role"),
					},
					VpcConfig: &types.CapacityProviderVpcConfig{
						SubnetIds:        []string{"subnet-1"},
						SecurityGroupIds: []string{"sg-1"},
					},
				})
				require.NoError(t, err)
			}

			in := &lambdasdk.ListCapacityProvidersInput{State: tt.state}
			if tt.maxItems > 0 {
				in.MaxItems = aws.Int32(tt.maxItems)
			}

			var sizes []int
			var arns []string

			for {
				out, err := client.ListCapacityProviders(t.Context(), in)
				require.NoError(t, err)

				sizes = append(sizes, len(out.CapacityProviders))
				for _, cp := range out.CapacityProviders {
					arns = append(arns, aws.ToString(cp.CapacityProviderArn))
				}

				if out.NextMarker == nil {
					break
				}

				in.Marker = out.NextMarker
			}

			assert.Equal(t, tt.wantPages, sizes)
			assert.IsIncreasing(t, arns)
		})
	}
}

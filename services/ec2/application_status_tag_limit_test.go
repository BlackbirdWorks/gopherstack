package ec2_test

import (
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	ec2sdk "github.com/aws/aws-sdk-go-v2/service/ec2"
	"github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAssociateApplicationStatusCheck_TagAssociationLimit(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name             string
		tags             int
		wantSuccessful   int
		wantUnsuccessful int
	}{
		{name: "at limit", tags: 50, wantSuccessful: 50},
		{name: "over limit", tags: 52, wantSuccessful: 50, wantUnsuccessful: 2},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			_, client := newMiscClient(t)

			created, err := client.CreateApplicationStatusCheck(t.Context(), &ec2sdk.CreateApplicationStatusCheckInput{
				Protocol: types.NetworkProtocolEnumHttp,
				Port:     aws.Int32(80),
			})
			require.NoError(t, err)

			pairs := make([]types.CustomTagKeyValueRequestPair, 0, tc.tags)
			for i := range tc.tags {
				pairs = append(pairs, types.CustomTagKeyValueRequestPair{
					Key: aws.String(fmt.Sprintf("k%d", i)), Value: aws.String("v"),
				})
			}

			out, err := client.AssociateApplicationStatusCheck(
				t.Context(),
				&ec2sdk.AssociateApplicationStatusCheckInput{
					ApplicationStatusCheckId: created.ApplicationStatusCheck.ApplicationStatusCheckId,
					TargetTagAssociations:    pairs,
				},
			)
			require.NoError(t, err)
			assert.Len(t, out.SuccessfulResults, tc.wantSuccessful)
			assert.Len(t, out.UnsuccessfulResults, tc.wantUnsuccessful)
		})
	}
}

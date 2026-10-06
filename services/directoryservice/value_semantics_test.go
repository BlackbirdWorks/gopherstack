package directoryservice_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	dssdk "github.com/aws/aws-sdk-go-v2/service/directoryservice"
	"github.com/aws/aws-sdk-go-v2/service/directoryservice/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCreateMicrosoftAD_DefaultsAndUpdateTrustKeepsSelectiveAuth(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name        string
		selective   types.SelectiveAuth
		wantUpdated types.SelectiveAuth
	}{
		{name: "update without selective auth keeps disabled", wantUpdated: types.SelectiveAuthDisabled},
		{
			name:        "update enables selective auth",
			selective:   types.SelectiveAuthEnabled,
			wantUpdated: types.SelectiveAuthEnabled,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			c := newTestDirectoryServiceClient(t, newTestHandler(t))
			vc := &types.DirectoryVpcSettings{VpcId: aws.String("vpc-1"), SubnetIds: []string{"subnet-1", "subnet-2"}}

			m, err := c.CreateMicrosoftAD(t.Context(), &dssdk.CreateMicrosoftADInput{
				Name: aws.String("corp.example.com"), Password: aws.String("Passw0rd!x"), VpcSettings: vc,
			})
			require.NoError(t, err)

			d, err := c.DescribeDirectories(
				t.Context(),
				&dssdk.DescribeDirectoriesInput{DirectoryIds: []string{*m.DirectoryId}},
			)
			require.NoError(t, err)
			assert.Equal(t, types.DirectoryEditionEnterprise, d.DirectoryDescriptions[0].Edition)

			tr, err := c.CreateTrust(t.Context(), &dssdk.CreateTrustInput{
				DirectoryId: m.DirectoryId, RemoteDomainName: aws.String("r.example.com"),
				TrustPassword: aws.String("Passw0rd!x"), TrustDirection: types.TrustDirectionTwoWay,
			})
			require.NoError(t, err)

			_, err = c.UpdateTrust(
				t.Context(),
				&dssdk.UpdateTrustInput{TrustId: tr.TrustId, SelectiveAuth: tc.selective},
			)
			require.NoError(t, err)

			dt, err := c.DescribeTrusts(t.Context(), &dssdk.DescribeTrustsInput{TrustIds: []string{*tr.TrustId}})
			require.NoError(t, err)
			assert.Equal(t, tc.wantUpdated, dt.Trusts[0].SelectiveAuth)
			assert.Equal(t, types.TrustTypeForest, dt.Trusts[0].TrustType)
		})
	}
}

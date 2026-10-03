package s3control_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	s3csdk "github.com/aws/aws-sdk-go-v2/service/s3control"
	"github.com/aws/aws-sdk-go-v2/service/s3control/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/s3control"
)

func TestListCallerAccessGrants_GrantScopePrefix_RealClient(t *testing.T) {
	t.Parallel()

	tests := []struct {
		fragment func(locA string) *string
		name     string
		want     int
	}{
		{name: "no filter", fragment: func(string) *string { return nil }, want: 2},
		{name: "common fragment", fragment: func(string) *string { return aws.String("s3://") }, want: 2},
		{name: "one location prefix", fragment: func(loc string) *string { return aws.String("s3://" + loc) }, want: 1},
		{name: "no match", fragment: func(string) *string { return aws.String("s3://zzz") }, want: 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestS3ControlClient(t, s3control.NewHandler(
				s3control.NewInMemoryBackendWithConfig(createTagsTestAccountID, createTagsTestRegion),
			))

			var locA string

			for _, scope := range []string{"s3://alpha/", "s3://beta/"} {
				loc, err := client.CreateAccessGrantsLocation(t.Context(), &s3csdk.CreateAccessGrantsLocationInput{
					AccountId:     aws.String(createTagsTestAccountID),
					LocationScope: aws.String(scope),
					IAMRoleArn:    aws.String("arn:aws:iam::123456789012:role/access-grants"),
				})
				require.NoError(t, err)

				if locA == "" {
					locA = aws.ToString(loc.AccessGrantsLocationId)
				}

				_, err = client.CreateAccessGrant(t.Context(), &s3csdk.CreateAccessGrantInput{
					AccountId:              aws.String(createTagsTestAccountID),
					AccessGrantsLocationId: loc.AccessGrantsLocationId,
					Grantee: &types.Grantee{
						GranteeType:       types.GranteeTypeIam,
						GranteeIdentifier: aws.String("arn:aws:iam::123456789012:role/reader"),
					},
					Permission: types.PermissionRead,
				})
				require.NoError(t, err)
			}

			out, err := client.ListCallerAccessGrants(t.Context(), &s3csdk.ListCallerAccessGrantsInput{
				AccountId:  aws.String(createTagsTestAccountID),
				GrantScope: tt.fragment(locA),
			})
			require.NoError(t, err)
			assert.Len(t, out.CallerAccessGrantsList, tt.want)
		})
	}
}

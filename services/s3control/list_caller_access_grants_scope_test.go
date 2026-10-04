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
		{name: "one location prefix", fragment: func(string) *string { return aws.String("s3://alpha") }, want: 1},
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

func TestCreateAccessGrant_S3SubPrefix_RealClient(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		locationScope string
		subPrefix     *string
		wantScope     string
	}{
		{name: "no sub prefix", locationScope: "s3://bkt/", subPrefix: nil, wantScope: "s3://bkt/"},
		{name: "sub prefix", locationScope: "s3://bkt/", subPrefix: aws.String("data/*"), wantScope: "s3://bkt/data/*"},
		{name: "default location", locationScope: "s3://", subPrefix: aws.String("/*"), wantScope: "s3:///*"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestS3ControlClient(t, s3control.NewHandler(
				s3control.NewInMemoryBackendWithConfig(createTagsTestAccountID, createTagsTestRegion),
			))

			loc, err := client.CreateAccessGrantsLocation(t.Context(), &s3csdk.CreateAccessGrantsLocationInput{
				AccountId:     aws.String(createTagsTestAccountID),
				LocationScope: aws.String(tt.locationScope),
				IAMRoleArn:    aws.String("arn:aws:iam::123456789012:role/access-grants"),
			})
			require.NoError(t, err)

			in := &s3csdk.CreateAccessGrantInput{
				AccountId:              aws.String(createTagsTestAccountID),
				AccessGrantsLocationId: loc.AccessGrantsLocationId,
				Grantee: &types.Grantee{
					GranteeType:       types.GranteeTypeIam,
					GranteeIdentifier: aws.String("arn:aws:iam::123456789012:role/reader"),
				},
				Permission: types.PermissionRead,
			}
			if tt.subPrefix != nil {
				in.AccessGrantsLocationConfiguration = &types.AccessGrantsLocationConfiguration{
					S3SubPrefix: tt.subPrefix,
				}
			}

			created, err := client.CreateAccessGrant(t.Context(), in)
			require.NoError(t, err)
			assert.Equal(t, tt.wantScope, aws.ToString(created.GrantScope))

			listed, err := client.ListAccessGrants(t.Context(), &s3csdk.ListAccessGrantsInput{
				AccountId:  aws.String(createTagsTestAccountID),
				GrantScope: aws.String(tt.wantScope),
			})
			require.NoError(t, err)
			require.Len(t, listed.AccessGrantsList, 1)
			assert.Equal(t, tt.wantScope, aws.ToString(listed.AccessGrantsList[0].GrantScope))
		})
	}
}

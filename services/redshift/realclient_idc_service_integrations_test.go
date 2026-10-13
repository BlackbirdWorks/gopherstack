package redshift_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	redshiftsdk "github.com/aws/aws-sdk-go-v2/service/redshift"
	"github.com/aws/aws-sdk-go-v2/service/redshift/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/redshift"
)

func TestRealClient_IdcApplicationServiceIntegrations(t *testing.T) {
	t.Parallel()

	connect := &types.ServiceIntegrationsUnionMemberRedshift{Value: []types.RedshiftScopeUnion{
		&types.RedshiftScopeUnionMemberConnect{Value: types.Connect{Authorization: types.ServiceAuthorizationEnabled}},
	}}
	lf := &types.ServiceIntegrationsUnionMemberLakeFormation{Value: []types.LakeFormationScopeUnion{
		&types.LakeFormationScopeUnionMemberLakeFormationQuery{
			Value: types.LakeFormationQuery{Authorization: types.ServiceAuthorizationDisabled},
		},
	}}
	s3 := &types.ServiceIntegrationsUnionMemberS3AccessGrants{Value: []types.S3AccessGrantsScopeUnion{
		&types.S3AccessGrantsScopeUnionMemberReadWriteAccess{
			Value: types.ReadWriteAccess{Authorization: types.ServiceAuthorizationEnabled},
		},
	}}

	tests := []struct {
		name     string
		create   []types.ServiceIntegrationsUnion
		modify   []types.ServiceIntegrationsUnion
		wantType []string
		wantMod  []string
	}{
		{
			name:     "create_only",
			create:   []types.ServiceIntegrationsUnion{connect, lf},
			wantType: []string{"Redshift", "LakeFormation"},
			wantMod:  []string{"Redshift", "LakeFormation"},
		},
		{
			name:     "modify_replaces",
			create:   []types.ServiceIntegrationsUnion{connect},
			modify:   []types.ServiceIntegrationsUnion{s3},
			wantType: []string{"Redshift"},
			wantMod:  []string{"S3AccessGrants"},
		},
		{name: "none", wantType: nil, wantMod: nil},
	}

	kind := func(u types.ServiceIntegrationsUnion) string {
		switch u.(type) {
		case *types.ServiceIntegrationsUnionMemberRedshift:
			return "Redshift"
		case *types.ServiceIntegrationsUnionMemberLakeFormation:
			return "LakeFormation"
		case *types.ServiceIntegrationsUnionMemberS3AccessGrants:
			return "S3AccessGrants"
		default:
			return "unknown"
		}
	}

	kinds := func(in []types.ServiceIntegrationsUnion) []string {
		if len(in) == 0 {
			return nil
		}

		out := make([]string, 0, len(in))
		for _, u := range in {
			out = append(out, kind(u))
		}

		return out
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestRedshiftClient(
				t,
				redshift.NewHandler(redshift.NewInMemoryBackend("000000000000", rtTestRegion)),
			)

			created, err := client.CreateRedshiftIdcApplication(
				t.Context(),
				&redshiftsdk.CreateRedshiftIdcApplicationInput{
					RedshiftIdcApplicationName: aws.String("app"),
					IdcInstanceArn:             aws.String("arn:aws:sso:::instance/ssoins-1"),
					IdcDisplayName:             aws.String("App"),
					IamRoleArn:                 aws.String("arn:aws:iam::000000000000:role/r"),
					ServiceIntegrations:        tt.create,
				},
			)
			require.NoError(t, err)
			assert.Equal(t, tt.wantType, kinds(created.RedshiftIdcApplication.ServiceIntegrations))

			if tt.name == "create_only" {
				rs, ok := created.RedshiftIdcApplication.ServiceIntegrations[0].(*types.ServiceIntegrationsUnionMemberRedshift)
				require.True(t, ok)
				c, ok := rs.Value[0].(*types.RedshiftScopeUnionMemberConnect)
				require.True(t, ok)
				assert.Equal(t, types.ServiceAuthorizationEnabled, c.Value.Authorization)
			}

			if tt.modify == nil {
				return
			}

			modified, err := client.ModifyRedshiftIdcApplication(
				t.Context(),
				&redshiftsdk.ModifyRedshiftIdcApplicationInput{
					RedshiftIdcApplicationArn: created.RedshiftIdcApplication.RedshiftIdcApplicationArn,
					ServiceIntegrations:       tt.modify,
				},
			)
			require.NoError(t, err)
			assert.Equal(t, tt.wantMod, kinds(modified.RedshiftIdcApplication.ServiceIntegrations))

			desc, err := client.DescribeRedshiftIdcApplications(
				t.Context(),
				&redshiftsdk.DescribeRedshiftIdcApplicationsInput{},
			)
			require.NoError(t, err)
			require.Len(t, desc.RedshiftIdcApplications, 1)
			assert.Equal(t, tt.wantMod, kinds(desc.RedshiftIdcApplications[0].ServiceIntegrations))
		})
	}
}

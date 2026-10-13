package appsync_test

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	appsyncsdk "github.com/aws/aws-sdk-go-v2/service/appsync"
	appsynctypes "github.com/aws/aws-sdk-go-v2/service/appsync/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/appsync"
)

type fakeWebACLs map[string]string

func (f fakeWebACLs) WebACLARN(_ context.Context, resourceARN string) string { return f[resourceARN] }

func TestWafWebACLArn(t *testing.T) {
	t.Parallel()

	const aclARN = "arn:aws:wafv2:us-east-1:000000000000:regional/webacl/acl/1"

	tests := []struct {
		name       string
		want       string
		associated bool
	}{
		{name: "associated", associated: true, want: aclARN},
		{name: "unassociated"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			bk := appsync.NewInMemoryBackend("000000000000", "us-east-1", "")
			h := appsync.NewHandler(bk)
			client := newTestAppsyncClient(t, h)

			gql, err := client.CreateGraphqlApi(t.Context(), &appsyncsdk.CreateGraphqlApiInput{
				Name: aws.String("g"), AuthenticationType: appsynctypes.AuthenticationTypeApiKey,
			})
			require.NoError(t, err)

			evt, err := client.CreateApi(t.Context(), &appsyncsdk.CreateApiInput{
				Name: aws.String("e"),
				EventConfig: &appsynctypes.EventConfig{
					AuthProviders: []appsynctypes.AuthProvider{
						{AuthType: appsynctypes.AuthenticationTypeApiKey},
					},
					ConnectionAuthModes: []appsynctypes.AuthMode{
						{AuthType: appsynctypes.AuthenticationTypeApiKey},
					},
					DefaultPublishAuthModes: []appsynctypes.AuthMode{
						{AuthType: appsynctypes.AuthenticationTypeApiKey},
					},
					DefaultSubscribeAuthModes: []appsynctypes.AuthMode{
						{AuthType: appsynctypes.AuthenticationTypeApiKey},
					},
				},
			})
			require.NoError(t, err)

			acls := fakeWebACLs{}
			if tt.associated {
				acls[aws.ToString(gql.GraphqlApi.Arn)] = aclARN
				acls[aws.ToString(evt.Api.ApiArn)] = aclARN
			}

			h.SetWebACLResolver(acls)

			gotGQL, err := client.GetGraphqlApi(
				t.Context(),
				&appsyncsdk.GetGraphqlApiInput{ApiId: gql.GraphqlApi.ApiId},
			)
			require.NoError(t, err)
			assert.Equal(t, tt.want, aws.ToString(gotGQL.GraphqlApi.WafWebAclArn))

			listed, err := client.ListGraphqlApis(t.Context(), &appsyncsdk.ListGraphqlApisInput{})
			require.NoError(t, err)
			require.Len(t, listed.GraphqlApis, 1)
			assert.Equal(t, tt.want, aws.ToString(listed.GraphqlApis[0].WafWebAclArn))

			gotEvt, err := client.GetApi(t.Context(), &appsyncsdk.GetApiInput{ApiId: evt.Api.ApiId})
			require.NoError(t, err)
			assert.Equal(t, tt.want, aws.ToString(gotEvt.Api.WafWebAclArn))
		})
	}
}

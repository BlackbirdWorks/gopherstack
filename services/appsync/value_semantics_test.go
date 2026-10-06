package appsync_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	appsyncsdk "github.com/aws/aws-sdk-go-v2/service/appsync"
	"github.com/aws/aws-sdk-go-v2/service/appsync/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGraphqlAPI_DefaultsAndPartialUpdate(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name     string
		xray     bool
		wantXray bool
	}{
		{name: "xray kept when omitted", xray: true, wantXray: true},
		{name: "xray off", xray: false, wantXray: false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newTestHandler()
			c := newTestAppsyncClient(t, h)
			created, err := c.CreateGraphqlApi(t.Context(), &appsyncsdk.CreateGraphqlApiInput{
				Name: aws.String("api"), AuthenticationType: types.AuthenticationTypeApiKey, XrayEnabled: tc.xray,
			})
			require.NoError(t, err)

			_, err = c.UpdateGraphqlApi(t.Context(), &appsyncsdk.UpdateGraphqlApiInput{
				ApiId:              created.GraphqlApi.ApiId,
				Name:               aws.String("renamed"),
				AuthenticationType: types.AuthenticationTypeApiKey,
			})
			require.NoError(t, err)

			got, err := c.GetGraphqlApi(t.Context(), &appsyncsdk.GetGraphqlApiInput{ApiId: created.GraphqlApi.ApiId})
			require.NoError(t, err)
			assert.Equal(t, "renamed", aws.ToString(got.GraphqlApi.Name))
			assert.Equal(t, types.GraphQLApiVisibilityGlobal, got.GraphqlApi.Visibility)
			assert.Equal(t, types.GraphQLApiIntrospectionConfigEnabled, got.GraphqlApi.IntrospectionConfig)
			assert.Equal(t, tc.wantXray, got.GraphqlApi.XrayEnabled)
		})
	}
}

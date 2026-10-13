package appsync_test

import (
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	appsyncsdk "github.com/aws/aws-sdk-go-v2/service/appsync"
	appsynctypes "github.com/aws/aws-sdk-go-v2/service/appsync/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSDKCreateDataSourceNameValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		dsName  string
		wantErr bool
	}{
		{name: "underscore", dsName: "_my_ds1"},
		{name: "hyphen", dsName: "bad-name", wantErr: true},
		{name: "leading_digit", dsName: "1ds", wantErr: true},
		{name: "too_long", dsName: strings.Repeat("a", 66), wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newTestHandler()
			client := newTestAppsyncClient(t, h)

			api, err := client.CreateGraphqlApi(t.Context(), &appsyncsdk.CreateGraphqlApiInput{
				Name:               aws.String("a"),
				AuthenticationType: appsynctypes.AuthenticationTypeApiKey,
			})
			require.NoError(t, err)

			_, err = client.CreateDataSource(t.Context(), &appsyncsdk.CreateDataSourceInput{
				ApiId: api.GraphqlApi.ApiId,
				Name:  aws.String(tt.dsName),
				Type:  appsynctypes.DataSourceTypeNone,
			})

			if !tt.wantErr {
				require.NoError(t, err)

				return
			}

			var bad *appsynctypes.BadRequestException
			require.ErrorAs(t, err, &bad)
			assert.False(t, strings.HasPrefix(aws.ToString(bad.Message), "BadRequestException"))
		})
	}
}

func TestSDKNotFoundMessageHasNoCodePrefix(t *testing.T) {
	t.Parallel()

	h, _ := newTestHandler()
	client := newTestAppsyncClient(t, h)

	_, err := client.GetGraphqlApi(t.Context(), &appsyncsdk.GetGraphqlApiInput{ApiId: aws.String("missing")})

	var nf *appsynctypes.NotFoundException
	require.ErrorAs(t, err, &nf)
	assert.False(t, strings.HasPrefix(aws.ToString(nf.Message), "NotFoundException"))
	assert.Contains(t, aws.ToString(nf.Message), "missing")
}

package appsync_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	appsyncsdk "github.com/aws/aws-sdk-go-v2/service/appsync"
	appsynctypes "github.com/aws/aws-sdk-go-v2/service/appsync/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAppSyncListOps_NarrowSummaryParity(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T, client *appsyncsdk.Client)
		name string
	}{
		{
			name: "ListSourceApiAssociations_ReturnsNarrowSummaries",
			run: func(t *testing.T, client *appsyncsdk.Client) {
				t.Helper()
				ctx := t.Context()

				src, err := client.CreateGraphqlApi(ctx, &appsyncsdk.CreateGraphqlApiInput{
					Name:               aws.String("source-api-narrow"),
					AuthenticationType: appsynctypes.AuthenticationTypeApiKey,
				})
				require.NoError(t, err)

				merged, err := client.CreateGraphqlApi(ctx, &appsyncsdk.CreateGraphqlApiInput{
					Name:               aws.String("merged-api-narrow"),
					AuthenticationType: appsynctypes.AuthenticationTypeApiKey,
					ApiType:            appsynctypes.GraphQLApiTypeMerged,
				})
				require.NoError(t, err)

				assocOut, err := client.AssociateSourceGraphqlApi(ctx, &appsyncsdk.AssociateSourceGraphqlApiInput{
					MergedApiIdentifier: merged.GraphqlApi.ApiId,
					SourceApiIdentifier: src.GraphqlApi.ApiId,
					Description:         aws.String("narrow test association"),
				})
				require.NoError(t, err)
				require.NotNil(t, assocOut.SourceApiAssociation)

				listOut, err := client.ListSourceApiAssociations(ctx, &appsyncsdk.ListSourceApiAssociationsInput{
					ApiId: merged.GraphqlApi.ApiId,
				})
				require.NoError(t, err)
				require.Len(t, listOut.SourceApiAssociationSummaries, 1)

				summary := listOut.SourceApiAssociationSummaries[0]
				assert.Equal(t, assocOut.SourceApiAssociation.AssociationId, summary.AssociationId)
				assert.Equal(t, assocOut.SourceApiAssociation.AssociationArn, summary.AssociationArn)
				assert.Equal(t, src.GraphqlApi.ApiId, summary.SourceApiId)
				assert.Equal(t, merged.GraphqlApi.ApiId, summary.MergedApiId)
				assert.Equal(t, aws.String("narrow test association"), summary.Description)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newTestHandler()
			client := newTestAppsyncClient(t, h)
			tt.run(t, client)
		})
	}
}

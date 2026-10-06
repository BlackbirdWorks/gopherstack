package appsync_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	appsyncsdk "github.com/aws/aws-sdk-go-v2/service/appsync"
	appsynctypes "github.com/aws/aws-sdk-go-v2/service/appsync/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/appsync"
)

func TestElasticsearchDataSourceConfigRoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		endpoint string
		update   bool
	}{
		{name: "create", endpoint: "https://es.example.com"},
		{name: "update", endpoint: "https://es2.example.com", update: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := appsync.NewInMemoryBackend("000000000000", tagsRTRegion, "")
			client := newTestAppsyncClient(t, appsync.NewHandler(backend))
			ctx := t.Context()

			api, err := client.CreateGraphqlApi(ctx, &appsyncsdk.CreateGraphqlApiInput{
				Name:               aws.String("es-api"),
				AuthenticationType: appsynctypes.AuthenticationTypeApiKey,
			})
			require.NoError(t, err)
			apiID := api.GraphqlApi.ApiId

			cfg := &appsynctypes.ElasticsearchDataSourceConfig{
				Endpoint: aws.String("https://es.example.com"), AwsRegion: aws.String("us-east-1"),
			}
			_, err = client.CreateDataSource(ctx, &appsyncsdk.CreateDataSourceInput{
				ApiId:               apiID,
				Name:                aws.String("es"),
				Type:                appsynctypes.DataSourceTypeAmazonElasticsearch,
				ElasticsearchConfig: cfg,
			})
			require.NoError(t, err)

			if tt.update {
				cfg.Endpoint = aws.String(tt.endpoint)
				_, err = client.UpdateDataSource(ctx, &appsyncsdk.UpdateDataSourceInput{
					ApiId:               apiID,
					Name:                aws.String("es"),
					Type:                appsynctypes.DataSourceTypeAmazonElasticsearch,
					ElasticsearchConfig: cfg,
				})
				require.NoError(t, err)
			}

			got, err := client.GetDataSource(ctx, &appsyncsdk.GetDataSourceInput{ApiId: apiID, Name: aws.String("es")})
			require.NoError(t, err)
			require.NotNil(t, got.DataSource.ElasticsearchConfig)
			assert.Equal(t, tt.endpoint, aws.ToString(got.DataSource.ElasticsearchConfig.Endpoint))
			assert.Equal(t, "us-east-1", aws.ToString(got.DataSource.ElasticsearchConfig.AwsRegion))
		})
	}
}

func TestUpdateSourceApiAssociationAppliesMergeType(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		mergeType appsynctypes.MergeType
		wantErr   bool
	}{
		{name: "auto merge", mergeType: appsynctypes.MergeTypeAutoMerge},
		{name: "invalid", mergeType: appsynctypes.MergeType("BOGUS"), wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := appsync.NewInMemoryBackend("000000000000", tagsRTRegion, "")
			client := newTestAppsyncClient(t, appsync.NewHandler(backend))
			ctx := t.Context()

			src, err := client.CreateGraphqlApi(ctx, &appsyncsdk.CreateGraphqlApiInput{
				Name: aws.String("src"), AuthenticationType: appsynctypes.AuthenticationTypeApiKey,
			})
			require.NoError(t, err)
			merged, err := client.CreateGraphqlApi(ctx, &appsyncsdk.CreateGraphqlApiInput{
				Name: aws.String("merged"), AuthenticationType: appsynctypes.AuthenticationTypeApiKey,
				ApiType:                   appsynctypes.GraphQLApiTypeMerged,
				MergedApiExecutionRoleArn: aws.String("arn:aws:iam::000000000000:role/r"),
			})
			require.NoError(t, err)

			assoc, err := client.AssociateMergedGraphqlApi(ctx, &appsyncsdk.AssociateMergedGraphqlApiInput{
				SourceApiIdentifier: src.GraphqlApi.ApiId, MergedApiIdentifier: merged.GraphqlApi.ApiId,
			})
			require.NoError(t, err)
			require.NotNil(t, assoc.SourceApiAssociation.SourceApiAssociationConfig)
			assert.Equal(
				t,
				appsynctypes.MergeTypeManualMerge,
				assoc.SourceApiAssociation.SourceApiAssociationConfig.MergeType,
			)

			out, err := client.UpdateSourceApiAssociation(ctx, &appsyncsdk.UpdateSourceApiAssociationInput{
				MergedApiIdentifier:        merged.GraphqlApi.ApiId,
				AssociationId:              assoc.SourceApiAssociation.AssociationId,
				SourceApiAssociationConfig: &appsynctypes.SourceApiAssociationConfig{MergeType: tt.mergeType},
			})

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.mergeType, out.SourceApiAssociation.SourceApiAssociationConfig.MergeType)

			got, err := client.GetSourceApiAssociation(ctx, &appsyncsdk.GetSourceApiAssociationInput{
				MergedApiIdentifier: merged.GraphqlApi.ApiId, AssociationId: assoc.SourceApiAssociation.AssociationId,
			})
			require.NoError(t, err)
			assert.Equal(t, tt.mergeType, got.SourceApiAssociation.SourceApiAssociationConfig.MergeType)
		})
	}
}

package glue_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	gluesdk "github.com/aws/aws-sdk-go-v2/service/glue"
	"github.com/aws/aws-sdk-go-v2/service/glue/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/glue"
)

// Real AWS: glue's TagResourceInput.TagsToAdd is map[string]string, a plain
// JSON object body field (aws-sdk-go-v2/service/glue@v1.152.0
// serializers.go:37549-37564, awsAwsjson11_serializeOpDocumentTagResourceInput),
// matching this emulator's map-shaped TagsToAdd exactly.
func Test_SDKRoundTrip_Glue_TagResource_UntagResource_GetTags(t *testing.T) {
	t.Parallel()

	backend := glue.NewInMemoryBackend(testAccountID, testRegion)
	client := newTestGlueClient(t, glue.NewHandler(backend))
	ctx := t.Context()

	_, err := client.CreateDatabase(ctx, &gluesdk.CreateDatabaseInput{
		DatabaseInput: &types.DatabaseInput{Name: aws.String("tag-rt-db")},
	})
	require.NoError(t, err)

	dbARN := "arn:aws:glue:" + testRegion + ":" + testAccountID + ":database/tag-rt-db"

	_, err = client.TagResource(ctx, &gluesdk.TagResourceInput{
		ResourceArn: aws.String(dbARN),
		TagsToAdd:   map[string]string{"env": "prod", "team": "infra"},
	})
	require.NoError(t, err)

	got, err := client.GetTags(ctx, &gluesdk.GetTagsInput{ResourceArn: aws.String(dbARN)})
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"env": "prod", "team": "infra"}, got.Tags)

	_, err = client.UntagResource(ctx, &gluesdk.UntagResourceInput{
		ResourceArn:  aws.String(dbARN),
		TagsToRemove: []string{"team"},
	})
	require.NoError(t, err)

	afterUntag, err := client.GetTags(ctx, &gluesdk.GetTagsInput{ResourceArn: aws.String(dbARN)})
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"env": "prod"}, afterUntag.Tags)
}

// A terraform-provider-aws Read for aws_glue_registry/aws_glue_schema calls
// GetTags right after Create to populate tags_all; both were previously
// absent from the tag lookup chain entirely, so a tagged registry or schema
// could never be read back and TagResource/GetTags against either ARN
// returned a spurious EntityNotFoundException.
func Test_SDKRoundTrip_Glue_TagResource_GetTags_RegistryAndSchema(t *testing.T) {
	t.Parallel()

	backend := glue.NewInMemoryBackend(testAccountID, testRegion)
	client := newTestGlueClient(t, glue.NewHandler(backend))
	ctx := t.Context()

	createRegOut, err := client.CreateRegistry(ctx, &gluesdk.CreateRegistryInput{
		RegistryName: aws.String("tag-rt-registry"),
	})
	require.NoError(t, err)

	registryARN := aws.ToString(createRegOut.RegistryArn)

	_, err = client.TagResource(ctx, &gluesdk.TagResourceInput{
		ResourceArn: aws.String(registryARN),
		TagsToAdd:   map[string]string{"env": "prod"},
	})
	require.NoError(t, err)

	gotRegTags, err := client.GetTags(ctx, &gluesdk.GetTagsInput{ResourceArn: aws.String(registryARN)})
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"env": "prod"}, gotRegTags.Tags)

	createSchemaOut, err := client.CreateSchema(ctx, &gluesdk.CreateSchemaInput{
		SchemaName:    aws.String("tag-rt-schema"),
		RegistryId:    &types.RegistryId{RegistryName: aws.String("tag-rt-registry")},
		DataFormat:    types.DataFormatJson,
		Compatibility: types.CompatibilityNone,
	})
	require.NoError(t, err)

	schemaARN := aws.ToString(createSchemaOut.SchemaArn)

	_, err = client.TagResource(ctx, &gluesdk.TagResourceInput{
		ResourceArn: aws.String(schemaARN),
		TagsToAdd:   map[string]string{"team": "data"},
	})
	require.NoError(t, err)

	gotSchemaTags, err := client.GetTags(ctx, &gluesdk.GetTagsInput{ResourceArn: aws.String(schemaARN)})
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"team": "data"}, gotSchemaTags.Tags)
}

// A terraform-provider-aws Read for aws_glue_registry/aws_glue_schema keys
// GetRegistry/GetSchema by the ARN returned at create time (not by name), so
// both must resolve identity from RegistryId.RegistryArn / SchemaId.SchemaArn
// alone -- previously only the *Name fields were consulted.
func Test_SDKRoundTrip_Glue_GetRegistry_GetSchema_ByARN(t *testing.T) {
	t.Parallel()

	backend := glue.NewInMemoryBackend(testAccountID, testRegion)
	client := newTestGlueClient(t, glue.NewHandler(backend))
	ctx := t.Context()

	createRegOut, err := client.CreateRegistry(ctx, &gluesdk.CreateRegistryInput{
		RegistryName: aws.String("byarn-registry"),
	})
	require.NoError(t, err)

	registryARN := aws.ToString(createRegOut.RegistryArn)

	getRegOut, err := client.GetRegistry(ctx, &gluesdk.GetRegistryInput{
		RegistryId: &types.RegistryId{RegistryArn: aws.String(registryARN)},
	})
	require.NoError(t, err)
	assert.Equal(t, "byarn-registry", aws.ToString(getRegOut.RegistryName))

	createSchemaOut, err := client.CreateSchema(ctx, &gluesdk.CreateSchemaInput{
		SchemaName:    aws.String("byarn-schema"),
		RegistryId:    &types.RegistryId{RegistryName: aws.String("byarn-registry")},
		DataFormat:    types.DataFormatJson,
		Compatibility: types.CompatibilityNone,
	})
	require.NoError(t, err)

	schemaARN := aws.ToString(createSchemaOut.SchemaArn)

	getSchemaOut, err := client.GetSchema(ctx, &gluesdk.GetSchemaInput{
		SchemaId: &types.SchemaId{SchemaArn: aws.String(schemaARN)},
	})
	require.NoError(t, err)
	assert.Equal(t, "byarn-schema", aws.ToString(getSchemaOut.SchemaName))
}

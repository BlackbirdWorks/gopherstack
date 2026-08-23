package glue_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	gluesdk "github.com/aws/aws-sdk-go-v2/service/glue"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestSDKRoundTrip_QuerySchemaVersionMetadata_MetadataInfoMap drives
// QuerySchemaVersionMetadata through the real aws-sdk-go-v2 client and proves
// MetadataInfoMap decodes non-nil and correct. The two raw-body tests in
// handler_schemas_test.go decode through their own locally declared struct
// tagged json:"MetadataInfoMap", so they pass whether the handler's tag
// matches the real deserializer or not -- only a real client, whose
// deserializer switches on the SDK's own fixed case list
// (awsAwsjson11_deserializeOpDocumentQuerySchemaVersionMetadataOutput),
// proves the wire key is actually right. Fixed in c3aa73e59; this is the
// real-client assertion that commit could not carry. Refs: gopherstack-v4a4.
func TestSDKRoundTrip_QuerySchemaVersionMetadata_MetadataInfoMap(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	v1ID := setupTestSchema(t, h)

	putRec := doGlueRequest(t, h, "PutSchemaVersionMetadata", map[string]any{
		"SchemaVersionId":  v1ID,
		"MetadataKeyValue": map[string]any{"MetadataKey": "owner", "MetadataValue": "team-a"},
	})
	require.Equal(t, 200, putRec.Code)

	client := newTestGlueClient(t, h)

	out, err := client.QuerySchemaVersionMetadata(t.Context(), &gluesdk.QuerySchemaVersionMetadataInput{
		SchemaVersionId: aws.String(v1ID),
	})
	require.NoError(t, err)
	require.NotNil(t, out.MetadataInfoMap, "MetadataInfoMap must decode non-nil against the real SDK deserializer")
	require.Contains(t, out.MetadataInfoMap, "owner")
	assert.Equal(t, "team-a", aws.ToString(out.MetadataInfoMap["owner"].MetadataValue))
	assert.Equal(t, v1ID, aws.ToString(out.SchemaVersionId))
}

package opensearch_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/opensearchserverless"
	"github.com/aws/aws-sdk-go-v2/service/opensearchserverless/document"
	aosstypes "github.com/aws/aws-sdk-go-v2/service/opensearchserverless/types"
	smithy "github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestServerless_RealSDKClient_Index_Lifecycle drives CreateIndex ->
// GetIndex -> UpdateIndex -> DeleteIndex -> GetIndex (not found) through the
// real opensearchserverless client, and confirms CreateIndex on an unknown
// collection ID reports ResourceNotFoundException.
func TestServerless_RealSDKClient_Index_Lifecycle(t *testing.T) {
	t.Parallel()

	h := testServerlessHandler(t)
	client := newTestServerlessClient(t, h)

	coll, err := client.CreateCollection(t.Context(), &opensearchserverless.CreateCollectionInput{
		Name: aws.String("idx-coll"),
	})
	require.NoError(t, err)
	collID := aws.ToString(coll.CreateCollectionDetail.Id)

	_, err = client.CreateIndex(t.Context(), &opensearchserverless.CreateIndexInput{
		Id:        aws.String("ghost-collection"),
		IndexName: aws.String("logs"),
	})
	require.Error(t, err)

	var apiErr smithy.APIError
	require.ErrorAs(t, err, &apiErr)
	assert.Equal(t, "ResourceNotFoundException", apiErr.ErrorCode())

	initialSchema := map[string]any{"mappings": map[string]any{"properties": map[string]any{}}}
	_, err = client.CreateIndex(t.Context(), &opensearchserverless.CreateIndexInput{
		Id:          aws.String(collID),
		IndexName:   aws.String("logs"),
		IndexSchema: document.NewLazyDocument(initialSchema),
	})
	require.NoError(t, err)

	_, err = client.CreateIndex(t.Context(), &opensearchserverless.CreateIndexInput{
		Id:        aws.String(collID),
		IndexName: aws.String("logs"),
	})
	require.Error(t, err)
	require.ErrorAs(t, err, &apiErr)
	assert.Equal(t, "ConflictException", apiErr.ErrorCode())

	got, err := client.GetIndex(t.Context(), &opensearchserverless.GetIndexInput{
		Id:        aws.String(collID),
		IndexName: aws.String("logs"),
	})
	require.NoError(t, err)
	var schema map[string]any
	require.NoError(t, got.IndexSchema.UnmarshalSmithyDocument(&schema))
	assert.Contains(t, schema, "mappings")

	updatedSchemaIn := map[string]any{"mappings": map[string]any{"properties": map[string]any{"field": "text"}}}
	_, err = client.UpdateIndex(t.Context(), &opensearchserverless.UpdateIndexInput{
		Id:          aws.String(collID),
		IndexName:   aws.String("logs"),
		IndexSchema: document.NewLazyDocument(updatedSchemaIn),
	})
	require.NoError(t, err)

	updated, err := client.GetIndex(t.Context(), &opensearchserverless.GetIndexInput{
		Id:        aws.String(collID),
		IndexName: aws.String("logs"),
	})
	require.NoError(t, err)
	var updatedSchema map[string]any
	require.NoError(t, updated.IndexSchema.UnmarshalSmithyDocument(&updatedSchema))
	mappings, _ := updatedSchema["mappings"].(map[string]any)
	properties, _ := mappings["properties"].(map[string]any)
	assert.Equal(t, "text", properties["field"])

	_, err = client.DeleteIndex(t.Context(), &opensearchserverless.DeleteIndexInput{
		Id:        aws.String(collID),
		IndexName: aws.String("logs"),
	})
	require.NoError(t, err)

	_, err = client.GetIndex(t.Context(), &opensearchserverless.GetIndexInput{
		Id:        aws.String(collID),
		IndexName: aws.String("logs"),
	})
	require.Error(t, err)
	require.ErrorAs(t, err, &apiErr)
	assert.Equal(t, "ResourceNotFoundException", apiErr.ErrorCode())
}

// TestServerless_RealSDKClient_Index_InvalidSchema confirms a non-object
// IndexSchema is rejected as ValidationException rather than silently
// accepted.
func TestServerless_RealSDKClient_Index_InvalidSchema(t *testing.T) {
	t.Parallel()

	h := testServerlessHandler(t)
	client := newTestServerlessClient(t, h)

	coll, err := client.CreateCollection(t.Context(), &opensearchserverless.CreateCollectionInput{
		Name: aws.String("idx-coll-2"),
	})
	require.NoError(t, err)
	collID := aws.ToString(coll.CreateCollectionDetail.Id)

	_, err = client.CreateIndex(t.Context(), &opensearchserverless.CreateIndexInput{
		Id:          aws.String(collID),
		IndexName:   aws.String("bad"),
		IndexSchema: document.NewLazyDocument("not-an-object"),
	})
	require.Error(t, err)

	var apiErr smithy.APIError
	require.ErrorAs(t, err, &apiErr)
	assert.Equal(t, "ValidationException", apiErr.ErrorCode())
}

// TestServerless_RealSDKClient_VpcEndpoint_Lifecycle drives CreateVpcEndpoint
// -> ListVpcEndpoints -> UpdateVpcEndpoint -> DeleteVpcEndpoint through the
// real opensearchserverless client, and confirms BatchGetVpcEndpoint prefers
// this AOSS-native endpoint over a same-named classic one.
func TestServerless_RealSDKClient_VpcEndpoint_Lifecycle(t *testing.T) {
	t.Parallel()

	h := testServerlessHandler(t)
	client := newTestServerlessClient(t, h)

	_, err := client.CreateVpcEndpoint(t.Context(), &opensearchserverless.CreateVpcEndpointInput{
		Name:      aws.String("dup"),
		VpcId:     aws.String("vpc-1"),
		SubnetIds: []string{"subnet-1"},
	})
	require.NoError(t, err)

	_, err = client.CreateVpcEndpoint(t.Context(), &opensearchserverless.CreateVpcEndpointInput{
		Name:      aws.String("dup"),
		VpcId:     aws.String("vpc-1"),
		SubnetIds: []string{"subnet-1"},
	})
	require.Error(t, err)

	var apiErr smithy.APIError
	require.ErrorAs(t, err, &apiErr)
	assert.Equal(t, "ConflictException", apiErr.ErrorCode())

	created, err := client.CreateVpcEndpoint(t.Context(), &opensearchserverless.CreateVpcEndpointInput{
		Name:             aws.String("ep-1"),
		VpcId:            aws.String("vpc-1"),
		SubnetIds:        []string{"subnet-1"},
		SecurityGroupIds: []string{"sg-1"},
	})
	require.NoError(t, err)
	id := aws.ToString(created.CreateVpcEndpointDetail.Id)
	require.NotEmpty(t, id)
	assert.Equal(t, "ep-1", aws.ToString(created.CreateVpcEndpointDetail.Name))

	listed, err := client.ListVpcEndpoints(t.Context(), &opensearchserverless.ListVpcEndpointsInput{})
	require.NoError(t, err)
	require.Len(t, listed.VpcEndpointSummaries, 2)

	updated, err := client.UpdateVpcEndpoint(t.Context(), &opensearchserverless.UpdateVpcEndpointInput{
		Id:                  aws.String(id),
		AddSecurityGroupIds: []string{"sg-2"},
		RemoveSubnetIds:     []string{"subnet-1"},
		AddSubnetIds:        []string{"subnet-2"},
	})
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"subnet-2"}, updated.UpdateVpcEndpointDetail.SubnetIds)
	assert.ElementsMatch(t, []string{"sg-1", "sg-2"}, updated.UpdateVpcEndpointDetail.SecurityGroupIds)

	_, err = client.UpdateVpcEndpoint(t.Context(), &opensearchserverless.UpdateVpcEndpointInput{
		Id: aws.String("vpce-aoss-ghost"),
	})
	require.Error(t, err)
	require.ErrorAs(t, err, &apiErr)
	assert.Equal(t, "ValidationException", apiErr.ErrorCode())

	got, err := client.BatchGetVpcEndpoint(t.Context(), &opensearchserverless.BatchGetVpcEndpointInput{
		Ids: []string{id},
	})
	require.NoError(t, err)
	require.Len(t, got.VpcEndpointDetails, 1)
	assert.Equal(t, "ep-1", aws.ToString(got.VpcEndpointDetails[0].Name))

	_, err = client.DeleteVpcEndpoint(t.Context(), &opensearchserverless.DeleteVpcEndpointInput{
		Id: aws.String(id),
	})
	require.NoError(t, err)

	afterDelete, err := client.ListVpcEndpoints(t.Context(), &opensearchserverless.ListVpcEndpointsInput{})
	require.NoError(t, err)
	assert.Len(t, afterDelete.VpcEndpointSummaries, 1)
}

// TestServerless_RealSDKClient_UpdateCollection_And_CollectionGroupCount
// creates a collection group, creates a collection that joins it at
// creation time (CreateCollectionInput.CollectionGroupName -- the real
// UpdateCollectionInput has no such member, see UpdateServerlessCollection's
// doc comment), calls UpdateCollection to prove its own real fields
// round-trip, and confirms BatchGetCollectionGroup reports
// NumberOfCollections 1.
func TestServerless_RealSDKClient_UpdateCollection_And_CollectionGroupCount(t *testing.T) {
	t.Parallel()

	h := testServerlessHandler(t)
	client := newTestServerlessClient(t, h)

	group, err := client.CreateCollectionGroup(t.Context(), &opensearchserverless.CreateCollectionGroupInput{
		Name:            aws.String("grp-1"),
		StandbyReplicas: aosstypes.StandbyReplicasDisabled,
	})
	require.NoError(t, err)
	groupID := aws.ToString(group.CreateCollectionGroupDetail.Id)

	_, err = client.CreateCollection(t.Context(), &opensearchserverless.CreateCollectionInput{
		Name:                aws.String("member-coll"),
		CollectionGroupName: aws.String("ghost-group"),
	})
	require.Error(t, err)

	var apiErr smithy.APIError
	require.ErrorAs(t, err, &apiErr)
	assert.Equal(t, "ValidationException", apiErr.ErrorCode())

	created, err := client.CreateCollection(t.Context(), &opensearchserverless.CreateCollectionInput{
		Name:                aws.String("member-coll"),
		CollectionGroupName: aws.String("grp-1"),
	})
	require.NoError(t, err)
	collID := aws.ToString(created.CreateCollectionDetail.Id)
	assert.Equal(t, "grp-1", aws.ToString(created.CreateCollectionDetail.CollectionGroupName))

	updated, err := client.UpdateCollection(t.Context(), &opensearchserverless.UpdateCollectionInput{
		Id:          aws.String(collID),
		Description: aws.String("now with a description"),
	})
	require.NoError(t, err)
	assert.Equal(t, "now with a description", aws.ToString(updated.UpdateCollectionDetail.Description))

	_, err = client.UpdateCollection(t.Context(), &opensearchserverless.UpdateCollectionInput{
		Id: aws.String("ghost-collection"),
	})
	require.Error(t, err)
	require.ErrorAs(t, err, &apiErr)
	assert.Equal(t, "ValidationException", apiErr.ErrorCode())

	got, err := client.BatchGetCollectionGroup(t.Context(), &opensearchserverless.BatchGetCollectionGroupInput{
		Ids: []string{groupID},
	})
	require.NoError(t, err)
	require.Len(t, got.CollectionGroupDetails, 1)
	assert.EqualValues(t, 1, aws.ToInt32(got.CollectionGroupDetails[0].NumberOfCollections))
}

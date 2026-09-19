package eventbridge_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/schemas"
	schemastypes "github.com/aws/aws-sdk-go-v2/service/schemas/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestSchemasDiscoverer_RealSDKClient drives the full Discoverer
// create->describe/list->update->start/stop->delete->not-found lifecycle
// through the real schemas client.
func TestSchemasDiscoverer_RealSDKClient(t *testing.T) {
	t.Parallel()

	h := newTestSchemasHandler(t)
	client := newTestSchemasClient(t, h)

	created, err := client.CreateDiscoverer(t.Context(), &schemas.CreateDiscovererInput{
		SourceArn:   aws.String("arn:aws:events:us-east-1:000000000000:event-bus/sdk-disc-bus"),
		Description: aws.String("created via real sdk"),
		Tags:        map[string]string{"env": "test"},
	})
	require.NoError(t, err)
	assert.Equal(t, schemastypes.DiscovererStateStarted, created.State)
	assert.True(t, aws.ToBool(created.CrossAccount))
	assert.NotEmpty(t, aws.ToString(created.DiscovererId))
	assert.NotEmpty(t, aws.ToString(created.DiscovererArn))
	id := aws.ToString(created.DiscovererId)

	described, err := client.DescribeDiscoverer(t.Context(), &schemas.DescribeDiscovererInput{
		DiscovererId: aws.String(id),
	})
	require.NoError(t, err)
	assert.Equal(t, "created via real sdk", aws.ToString(described.Description))

	listed, err := client.ListDiscoverers(t.Context(), &schemas.ListDiscoverersInput{
		DiscovererIdPrefix: aws.String(id),
	})
	require.NoError(t, err)
	require.Len(t, listed.Discoverers, 1)
	assert.Equal(t, id, aws.ToString(listed.Discoverers[0].DiscovererId))

	updated, err := client.UpdateDiscoverer(t.Context(), &schemas.UpdateDiscovererInput{
		DiscovererId: aws.String(id),
		Description:  aws.String("updated via real sdk"),
		CrossAccount: aws.Bool(false),
	})
	require.NoError(t, err)
	assert.Equal(t, "updated via real sdk", aws.ToString(updated.Description))
	assert.False(t, aws.ToBool(updated.CrossAccount))

	stopped, err := client.StopDiscoverer(t.Context(), &schemas.StopDiscovererInput{
		DiscovererId: aws.String(id),
	})
	require.NoError(t, err)
	assert.Equal(t, schemastypes.DiscovererStateStopped, stopped.State)

	started, err := client.StartDiscoverer(t.Context(), &schemas.StartDiscovererInput{
		DiscovererId: aws.String(id),
	})
	require.NoError(t, err)
	assert.Equal(t, schemastypes.DiscovererStateStarted, started.State)

	_, err = client.DeleteDiscoverer(t.Context(), &schemas.DeleteDiscovererInput{
		DiscovererId: aws.String(id),
	})
	require.NoError(t, err)

	_, err = client.DescribeDiscoverer(t.Context(), &schemas.DescribeDiscovererInput{
		DiscovererId: aws.String(id),
	})
	require.Error(t, err)

	var nf *schemastypes.NotFoundException
	require.ErrorAs(t, err, &nf)
}

// TestSchemasDiscoverer_DuplicateSourceArn asserts CreateDiscoverer's
// declared ConflictException for a second discoverer on the same event bus.
func TestSchemasDiscoverer_DuplicateSourceArn(t *testing.T) {
	t.Parallel()

	h := newTestSchemasHandler(t)
	client := newTestSchemasClient(t, h)

	sourceArn := aws.String("arn:aws:events:us-east-1:000000000000:event-bus/dup-disc-bus")

	_, err := client.CreateDiscoverer(t.Context(), &schemas.CreateDiscovererInput{SourceArn: sourceArn})
	require.NoError(t, err)

	_, err = client.CreateDiscoverer(t.Context(), &schemas.CreateDiscovererInput{SourceArn: sourceArn})
	require.Error(t, err)

	var conflict *schemastypes.ConflictException
	require.ErrorAs(t, err, &conflict)
}

// TestSchemasResourcePolicy_RealSDKClient drives Put->Get->Delete->not-found
// through the real schemas client, and asserts PutResourcePolicy's declared
// PreconditionFailedException for a stale RevisionId.
func TestSchemasResourcePolicy_RealSDKClient(t *testing.T) {
	t.Parallel()

	h := newTestSchemasHandler(t)
	client := newTestSchemasClient(t, h)

	_, err := client.GetResourcePolicy(t.Context(), &schemas.GetResourcePolicyInput{
		RegistryName: aws.String("sdk-policy-registry"),
	})
	require.Error(t, err)

	var nf *schemastypes.NotFoundException
	require.ErrorAs(t, err, &nf)

	put, err := client.PutResourcePolicy(t.Context(), &schemas.PutResourcePolicyInput{
		RegistryName: aws.String("sdk-policy-registry"),
		Policy:       aws.String(`{"Version":"2012-10-17","Statement":[]}`),
	})
	require.NoError(t, err)
	firstRevision := aws.ToString(put.RevisionId)
	assert.NotEmpty(t, firstRevision)

	got, err := client.GetResourcePolicy(t.Context(), &schemas.GetResourcePolicyInput{
		RegistryName: aws.String("sdk-policy-registry"),
	})
	require.NoError(t, err)
	assert.Equal(t, firstRevision, aws.ToString(got.RevisionId))

	_, err = client.PutResourcePolicy(t.Context(), &schemas.PutResourcePolicyInput{
		RegistryName: aws.String("sdk-policy-registry"),
		Policy:       aws.String(`{"Version":"2012-10-17","Statement":[]}`),
		RevisionId:   aws.String("stale-revision"),
	})
	require.Error(t, err)

	var precond *schemastypes.PreconditionFailedException
	require.ErrorAs(t, err, &precond)

	_, err = client.DeleteResourcePolicy(t.Context(), &schemas.DeleteResourcePolicyInput{
		RegistryName: aws.String("sdk-policy-registry"),
	})
	require.NoError(t, err)

	_, err = client.GetResourcePolicy(t.Context(), &schemas.GetResourcePolicyInput{
		RegistryName: aws.String("sdk-policy-registry"),
	})
	require.Error(t, err)
	require.ErrorAs(t, err, &nf)
}

// TestSchemasExportSchema_RealSDKClient asserts ExportSchema returns the
// stored schema content, and rejects a Type other than JSONSchemaDraft4.
func TestSchemasExportSchema_RealSDKClient(t *testing.T) {
	t.Parallel()

	h := newTestSchemasHandler(t)
	client := newTestSchemasClient(t, h)

	_, err := client.CreateRegistry(t.Context(), &schemas.CreateRegistryInput{
		RegistryName: aws.String("sdk-export-registry"),
	})
	require.NoError(t, err)

	_, err = client.CreateSchema(t.Context(), &schemas.CreateSchemaInput{
		RegistryName: aws.String("sdk-export-registry"),
		SchemaName:   aws.String("sdk-export-schema"),
		Type:         schemastypes.TypeJSONSchemaDraft4,
		Content:      aws.String(`{"$schema":"http://json-schema.org/draft-04/schema#"}`),
	})
	require.NoError(t, err)

	exported, err := client.ExportSchema(t.Context(), &schemas.ExportSchemaInput{
		RegistryName: aws.String("sdk-export-registry"),
		SchemaName:   aws.String("sdk-export-schema"),
		Type:         aws.String("JSONSchemaDraft4"),
	})
	require.NoError(t, err)
	assert.JSONEq(t, `{"$schema":"http://json-schema.org/draft-04/schema#"}`, aws.ToString(exported.Content))
	assert.Equal(t, "1", aws.ToString(exported.SchemaVersion))

	_, err = client.ExportSchema(t.Context(), &schemas.ExportSchemaInput{
		RegistryName: aws.String("sdk-export-registry"),
		SchemaName:   aws.String("sdk-export-schema"),
		Type:         aws.String("OpenApi3"),
	})
	require.Error(t, err)

	var badReq *schemastypes.BadRequestException
	require.ErrorAs(t, err, &badReq)
}

package glue_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	gluesdk "github.com/aws/aws-sdk-go-v2/service/glue"
	gluesdktypes "github.com/aws/aws-sdk-go-v2/service/glue/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/glue"
)

// TestDeleteRegistry_ReturnsRegistryArn proves DeleteRegistryOutput carries
// RegistryArn (glue@v1.152.0 api_op_DeleteRegistry.go), a real member backed
// by state this backend already tracks (Registry.ARN, used by GetRegistry)
// but never surfaced because DeleteRegistry discarded the record before
// returning.
func TestDeleteRegistry_ReturnsRegistryArn(t *testing.T) {
	t.Parallel()

	backend := glue.NewInMemoryBackend(testAccountID, testRegion)
	client := newTestGlueClient(t, glue.NewHandler(backend))

	created, err := client.CreateRegistry(t.Context(), &gluesdk.CreateRegistryInput{
		RegistryName: aws.String("reg1"),
	})
	require.NoError(t, err)
	require.NotEmpty(t, aws.ToString(created.RegistryArn))

	out, err := client.DeleteRegistry(t.Context(), &gluesdk.DeleteRegistryInput{
		RegistryId: &gluesdktypes.RegistryId{RegistryName: aws.String("reg1")},
	})
	require.NoError(t, err)
	assert.Equal(t, aws.ToString(created.RegistryArn), aws.ToString(out.RegistryArn))
}

// TestUpdateRegistry_ReturnsRegistryArn proves UpdateRegistryOutput carries
// RegistryArn (glue@v1.152.0 api_op_UpdateRegistry.go), the same class of gap
// as DeleteRegistry above.
func TestUpdateRegistry_ReturnsRegistryArn(t *testing.T) {
	t.Parallel()

	backend := glue.NewInMemoryBackend(testAccountID, testRegion)
	client := newTestGlueClient(t, glue.NewHandler(backend))

	created, err := client.CreateRegistry(t.Context(), &gluesdk.CreateRegistryInput{
		RegistryName: aws.String("reg1"),
	})
	require.NoError(t, err)

	out, err := client.UpdateRegistry(t.Context(), &gluesdk.UpdateRegistryInput{
		RegistryId:  &gluesdktypes.RegistryId{RegistryName: aws.String("reg1")},
		Description: aws.String("updated"),
	})
	require.NoError(t, err)
	assert.Equal(t, aws.ToString(created.RegistryArn), aws.ToString(out.RegistryArn))
}

// TestDeleteSchema_ReturnsSchemaArn proves DeleteSchemaOutput carries
// SchemaArn (glue@v1.152.0 api_op_DeleteSchema.go), backed by state this
// backend already tracks (Schema.SchemaARN) but discarded before DeleteSchema
// could return it.
func TestDeleteSchema_ReturnsSchemaArn(t *testing.T) {
	t.Parallel()

	backend := glue.NewInMemoryBackend(testAccountID, testRegion)
	client := newTestGlueClient(t, glue.NewHandler(backend))

	reg, err := client.CreateRegistry(t.Context(), &gluesdk.CreateRegistryInput{RegistryName: aws.String("reg1")})
	require.NoError(t, err)

	created, err := client.CreateSchema(t.Context(), &gluesdk.CreateSchemaInput{
		SchemaName:       aws.String("schema1"),
		RegistryId:       &gluesdktypes.RegistryId{RegistryName: reg.RegistryName},
		DataFormat:       gluesdktypes.DataFormatJson,
		SchemaDefinition: aws.String(`{"type":"object"}`),
	})
	require.NoError(t, err)
	require.NotEmpty(t, aws.ToString(created.SchemaArn))

	out, err := client.DeleteSchema(t.Context(), &gluesdk.DeleteSchemaInput{
		SchemaId: &gluesdktypes.SchemaId{RegistryName: reg.RegistryName, SchemaName: aws.String("schema1")},
	})
	require.NoError(t, err)
	assert.Equal(t, aws.ToString(created.SchemaArn), aws.ToString(out.SchemaArn))
}

// TestUpdateSchema_ReturnsSchemaArn proves UpdateSchemaOutput carries
// SchemaArn (glue@v1.152.0 api_op_UpdateSchema.go). UpdateSchema was not
// itself in the queued 96-op set (it was already graded elsewhere), but it
// shares deleteSchemaInput/updateRegistryInput's schemaIDInput/registryIDInput
// plumbing and the identical never-populated-ARN bug, found while checking
// DeleteSchema/UpdateRegistry's sibling ops per the "check every op sharing
// that type" rule.
func TestUpdateSchema_ReturnsSchemaArn(t *testing.T) {
	t.Parallel()

	backend := glue.NewInMemoryBackend(testAccountID, testRegion)
	client := newTestGlueClient(t, glue.NewHandler(backend))

	reg, err := client.CreateRegistry(t.Context(), &gluesdk.CreateRegistryInput{RegistryName: aws.String("reg1")})
	require.NoError(t, err)

	created, err := client.CreateSchema(t.Context(), &gluesdk.CreateSchemaInput{
		SchemaName:       aws.String("schema1"),
		RegistryId:       &gluesdktypes.RegistryId{RegistryName: reg.RegistryName},
		DataFormat:       gluesdktypes.DataFormatJson,
		SchemaDefinition: aws.String(`{"type":"object"}`),
	})
	require.NoError(t, err)

	out, err := client.UpdateSchema(t.Context(), &gluesdk.UpdateSchemaInput{
		SchemaId:      &gluesdktypes.SchemaId{RegistryName: reg.RegistryName, SchemaName: aws.String("schema1")},
		Compatibility: gluesdktypes.CompatibilityBackward,
	})
	require.NoError(t, err)
	assert.Equal(t, aws.ToString(created.SchemaArn), aws.ToString(out.SchemaArn))
}

// TestUpdateUsageProfile_DoesNotClobberDescription proves UpdateUsageProfile
// does not wipe a profile's Description to empty on every real call. The
// previous handler always called Backend.UpdateUsageProfile(name, "")
// regardless of what the client sent, and the backend applied that empty
// string unconditionally -- a destructive clobber on every real
// UpdateUsageProfile call, the same bug class as
// UpdateGlueIdentityCenterConfiguration's InstanceArn clobber found in the
// prior pass.
func TestUpdateUsageProfile_DoesNotClobberDescription(t *testing.T) {
	t.Parallel()

	backend := glue.NewInMemoryBackend(testAccountID, testRegion)
	client := newTestGlueClient(t, glue.NewHandler(backend))

	_, err := client.CreateUsageProfile(t.Context(), &gluesdk.CreateUsageProfileInput{
		Name:          aws.String("profile1"),
		Description:   aws.String("original description"),
		Configuration: &gluesdktypes.ProfileConfiguration{},
	})
	require.NoError(t, err)

	_, err = client.UpdateUsageProfile(t.Context(), &gluesdk.UpdateUsageProfileInput{
		Name:          aws.String("profile1"),
		Configuration: &gluesdktypes.ProfileConfiguration{},
	})
	require.NoError(t, err)

	got, err := client.GetUsageProfile(t.Context(), &gluesdk.GetUsageProfileInput{Name: aws.String("profile1")})
	require.NoError(t, err)
	assert.Equal(
		t, "original description", aws.ToString(got.Description),
		"UpdateUsageProfile must not clobber Description when the client didn't set it",
	)

	_, err = client.UpdateUsageProfile(t.Context(), &gluesdk.UpdateUsageProfileInput{
		Name:          aws.String("profile1"),
		Description:   aws.String("new description"),
		Configuration: &gluesdktypes.ProfileConfiguration{},
	})
	require.NoError(t, err)

	got2, err := client.GetUsageProfile(t.Context(), &gluesdk.GetUsageProfileInput{Name: aws.String("profile1")})
	require.NoError(t, err)
	assert.Equal(t, "new description", aws.ToString(got2.Description))
}

// TestListDataQualityStatisticAnnotations_Paginates proves
// ListDataQualityStatisticAnnotations honors MaxResults/NextToken
// (glue@v1.152.0 api_op_ListDataQualityStatisticAnnotations.go), previously
// declared on neither the input nor output and always returning every
// annotation in one page.
func TestListDataQualityStatisticAnnotations_Paginates(t *testing.T) {
	t.Parallel()

	backend := glue.NewInMemoryBackend(testAccountID, testRegion)
	client := newTestGlueClient(t, glue.NewHandler(backend))

	for i := range 3 {
		_, err := client.BatchPutDataQualityStatisticAnnotation(
			t.Context(), &gluesdk.BatchPutDataQualityStatisticAnnotationInput{
				InclusionAnnotations: []gluesdktypes.DatapointInclusionAnnotation{
					{
						ProfileId:           aws.String("profile1"),
						StatisticId:         aws.String(string(rune('a' + i))),
						InclusionAnnotation: gluesdktypes.InclusionAnnotationValueInclude,
					},
				},
			},
		)
		require.NoError(t, err)
	}

	page1, err := client.ListDataQualityStatisticAnnotations(
		t.Context(), &gluesdk.ListDataQualityStatisticAnnotationsInput{
			ProfileId:  aws.String("profile1"),
			MaxResults: aws.Int32(2),
		},
	)
	require.NoError(t, err)
	require.Len(t, page1.Annotations, 2)
	require.NotEmpty(t, aws.ToString(page1.NextToken), "a truncated result must carry a NextToken")

	page2, err := client.ListDataQualityStatisticAnnotations(
		t.Context(), &gluesdk.ListDataQualityStatisticAnnotationsInput{
			ProfileId:  aws.String("profile1"),
			MaxResults: aws.Int32(2),
			NextToken:  page1.NextToken,
		},
	)
	require.NoError(t, err)
	assert.Len(t, page2.Annotations, 1)
	assert.Empty(t, aws.ToString(page2.NextToken))
}

// TestStartBlueprintRun_RequiresRoleArn proves StartBlueprintRun requires
// RoleArn (glue@v1.152.0 api_op_StartBlueprintRun.go), a real required member
// previously dropped entirely, and that the started run's RoleArn is
// surfaced back on GetBlueprintRun -- both the request-side drop and the
// response-side BlueprintRun.RoleArn gap in the same family.
func TestStartBlueprintRun_RequiresRoleArn(t *testing.T) {
	t.Parallel()

	backend := glue.NewInMemoryBackend(testAccountID, testRegion)
	client := newTestGlueClient(t, glue.NewHandler(backend))

	_, err := client.CreateBlueprint(t.Context(), &gluesdk.CreateBlueprintInput{
		Name:              aws.String("bp1"),
		BlueprintLocation: aws.String("s3://bucket/bp1"),
	})
	require.NoError(t, err)

	_, err = client.StartBlueprintRun(t.Context(), &gluesdk.StartBlueprintRunInput{
		BlueprintName: aws.String("bp1"),
	})
	require.Error(t, err, "a real client must reject a StartBlueprintRun call missing RoleArn")

	const roleArn = "arn:aws:iam::000000000000:role/GlueRole"

	started, err := client.StartBlueprintRun(t.Context(), &gluesdk.StartBlueprintRunInput{
		BlueprintName: aws.String("bp1"),
		RoleArn:       aws.String(roleArn),
	})
	require.NoError(t, err)

	got, err := client.GetBlueprintRun(t.Context(), &gluesdk.GetBlueprintRunInput{
		BlueprintName: aws.String("bp1"),
		RunId:         started.RunId,
	})
	require.NoError(t, err)
	require.NotNil(t, got.BlueprintRun, "response must wrap the run under BlueprintRun")
	assert.Equal(t, roleArn, aws.ToString(got.BlueprintRun.RoleArn))
}

// TestGetBlueprintRuns_WrapsUnderBlueprintRuns proves GetBlueprintRunsOutput
// carries BlueprintRuns (glue@v1.152.0 api_op_GetBlueprintRuns.go), not Runs
// -- previously a wrong wire key that decodes to a silently empty slice for
// a real client -- and that MaxResults/NextToken are honored.
func TestGetBlueprintRuns_WrapsUnderBlueprintRuns(t *testing.T) {
	t.Parallel()

	backend := glue.NewInMemoryBackend(testAccountID, testRegion)
	client := newTestGlueClient(t, glue.NewHandler(backend))

	_, err := client.CreateBlueprint(t.Context(), &gluesdk.CreateBlueprintInput{
		Name:              aws.String("bp1"),
		BlueprintLocation: aws.String("s3://bucket/bp1"),
	})
	require.NoError(t, err)

	for range 3 {
		_, startErr := client.StartBlueprintRun(t.Context(), &gluesdk.StartBlueprintRunInput{
			BlueprintName: aws.String("bp1"),
			RoleArn:       aws.String("arn:aws:iam::000000000000:role/GlueRole"),
		})
		require.NoError(t, startErr)
	}

	page1, err := client.GetBlueprintRuns(t.Context(), &gluesdk.GetBlueprintRunsInput{
		BlueprintName: aws.String("bp1"),
		MaxResults:    aws.Int32(2),
	})
	require.NoError(t, err)
	require.Len(t, page1.BlueprintRuns, 2, "a real client must decode a non-empty BlueprintRuns list")
	require.NotEmpty(t, aws.ToString(page1.NextToken))

	page2, err := client.GetBlueprintRuns(t.Context(), &gluesdk.GetBlueprintRunsInput{
		BlueprintName: aws.String("bp1"),
		MaxResults:    aws.Int32(2),
		NextToken:     page1.NextToken,
	})
	require.NoError(t, err)
	assert.Len(t, page2.BlueprintRuns, 1)
}

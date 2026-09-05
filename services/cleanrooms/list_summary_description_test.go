package cleanrooms_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	cleanroomssdk "github.com/aws/aws-sdk-go-v2/service/cleanrooms"
	crtypes "github.com/aws/aws-sdk-go-v2/service/cleanrooms/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestListAnalysisTemplates_SummaryHasDescription proves ListAnalysisTemplates'
// AnalysisTemplateSummary decodes the real, optional "description" key
// (types.AnalysisTemplateSummary, confirmed against
// awsRestjson1_deserializeDocumentAnalysisTemplateSummary in
// cleanrooms@v1.49.4's deserializers.go). The backend already tracks
// Description (set at CreateAnalysisTemplate and correctly surfaced by the
// singular GetAnalysisTemplate) but never copied it into the list summary --
// a real client's list arrived with every description silently blank
// regardless of what was stored.
func TestListAnalysisTemplates_SummaryHasDescription(t *testing.T) {
	t.Parallel()

	client := newRoundTripTestClient(t)
	ctx := t.Context()
	_, memID := createCollaborationAndMembership(t, client)

	const wantDescription = "distinguishable-analysis-template-description"

	_, err := client.CreateAnalysisTemplate(ctx, &cleanroomssdk.CreateAnalysisTemplateInput{
		MembershipIdentifier: aws.String(memID),
		Name:                 aws.String("tmpl"),
		Description:          aws.String(wantDescription),
		Format:               crtypes.AnalysisFormatSql,
		Source:               &crtypes.AnalysisSourceMemberText{Value: "SELECT 1"},
	})
	require.NoError(t, err)

	listOut, err := client.ListAnalysisTemplates(ctx, &cleanroomssdk.ListAnalysisTemplatesInput{
		MembershipIdentifier: aws.String(memID),
	})
	require.NoError(t, err)
	require.Len(t, listOut.AnalysisTemplateSummaries, 1)
	assert.Equal(t, wantDescription, aws.ToString(listOut.AnalysisTemplateSummaries[0].Description),
		"description must decode from the list summary, not just the singular Get")
}

// TestListIdMappingTables_SummaryHasDescription is the same shape for
// IdMappingTableSummary (types.go: Description is a real, optional field).
func TestListIdMappingTables_SummaryHasDescription(t *testing.T) {
	t.Parallel()

	client := newRoundTripTestClient(t)
	ctx := t.Context()
	_, memID := createCollaborationAndMembership(t, client)

	const wantDescription = "distinguishable-id-mapping-table-description"

	_, err := client.CreateIdMappingTable(ctx, &cleanroomssdk.CreateIdMappingTableInput{
		MembershipIdentifier: aws.String(memID),
		Name:                 aws.String("mapping-table"),
		Description:          aws.String(wantDescription),
		InputReferenceConfig: &crtypes.IdMappingTableInputReferenceConfig{
			InputReferenceArn: aws.String(
				"arn:aws:entityresolution:us-east-1:123456789012:idmappingworkflow/fixture",
			),
			ManageResourcePolicies: aws.Bool(true),
		},
	})
	require.NoError(t, err)

	listOut, err := client.ListIdMappingTables(ctx, &cleanroomssdk.ListIdMappingTablesInput{
		MembershipIdentifier: aws.String(memID),
	})
	require.NoError(t, err)
	require.Len(t, listOut.IdMappingTableSummaries, 1)
	assert.Equal(t, wantDescription, aws.ToString(listOut.IdMappingTableSummaries[0].Description),
		"description must decode from the list summary, not just the singular Get")
}

// TestListIdNamespaceAssociations_SummaryHasDescription is the same shape
// for IdNamespaceAssociationSummary (types.go: Description is a real,
// optional field).
func TestListIdNamespaceAssociations_SummaryHasDescription(t *testing.T) {
	t.Parallel()

	client := newRoundTripTestClient(t)
	ctx := t.Context()
	_, memID := createCollaborationAndMembership(t, client)

	const wantDescription = "distinguishable-id-namespace-association-description"

	_, err := client.CreateIdNamespaceAssociation(ctx, &cleanroomssdk.CreateIdNamespaceAssociationInput{
		MembershipIdentifier: aws.String(memID),
		Name:                 aws.String("ns"),
		Description:          aws.String(wantDescription),
		InputReferenceConfig: &crtypes.IdNamespaceAssociationInputReferenceConfig{
			InputReferenceArn:      aws.String("arn:aws:cleanrooms:us-east-1:123456789012:membership/" + memID),
			ManageResourcePolicies: aws.Bool(true),
		},
	})
	require.NoError(t, err)

	listOut, err := client.ListIdNamespaceAssociations(ctx, &cleanroomssdk.ListIdNamespaceAssociationsInput{
		MembershipIdentifier: aws.String(memID),
	})
	require.NoError(t, err)
	require.Len(t, listOut.IdNamespaceAssociationSummaries, 1)
	assert.Equal(t, wantDescription, aws.ToString(listOut.IdNamespaceAssociationSummaries[0].Description),
		"description must decode from the list summary, not just the singular Get")
}

package cleanrooms_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	cleanroomssdk "github.com/aws/aws-sdk-go-v2/service/cleanrooms"
	crtypes "github.com/aws/aws-sdk-go-v2/service/cleanrooms/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestListCollaborationScopedSummaries_MissingMembers proves the three
// collaboration-scoped List summaries (CollaborationAnalysisTemplateSummary,
// CollaborationConfiguredAudienceModelAssociationSummary,
// CollaborationIDNamespaceAssociationSummary) decode the real, optional
// "description" key (types.go) that each backend struct already tracks but
// never copied into these specific collaboration-scoped summaries. A typed
// decoder silently drops an absent key, so only asserting the decoded value
// (not just "no error") proves the fix.
func TestListCollaborationScopedSummaries_MissingMembers(t *testing.T) {
	t.Parallel()

	t.Run("analysis template description", func(t *testing.T) {
		t.Parallel()

		client := newRoundTripTestClient(t)
		ctx := t.Context()
		collabID, memID := createCollaborationAndMembership(t, client)

		const wantDescription = "distinguishable-collab-analysis-template-description"

		_, err := client.CreateAnalysisTemplate(ctx, &cleanroomssdk.CreateAnalysisTemplateInput{
			MembershipIdentifier: aws.String(memID),
			Name:                 aws.String("tmpl"),
			Description:          aws.String(wantDescription),
			Format:               crtypes.AnalysisFormatSql,
			Source:               &crtypes.AnalysisSourceMemberText{Value: "SELECT 1"},
		})
		require.NoError(t, err)

		out, err := client.ListCollaborationAnalysisTemplates(
			ctx, &cleanroomssdk.ListCollaborationAnalysisTemplatesInput{
				CollaborationIdentifier: aws.String(collabID),
			},
		)
		require.NoError(t, err)
		require.Len(t, out.CollaborationAnalysisTemplateSummaries, 1)
		assert.Equal(t, wantDescription,
			aws.ToString(out.CollaborationAnalysisTemplateSummaries[0].Description))
	})

	t.Run("configured audience model association description", func(t *testing.T) {
		t.Parallel()

		client := newRoundTripTestClient(t)
		ctx := t.Context()
		collabID, memID := createCollaborationAndMembership(t, client)

		const wantDescription = "distinguishable-collab-cama-description"

		_, err := client.CreateConfiguredAudienceModelAssociation(
			ctx, &cleanroomssdk.CreateConfiguredAudienceModelAssociationInput{
				MembershipIdentifier: aws.String(memID),
				ConfiguredAudienceModelArn: aws.String(
					"arn:aws:cleanrooms-ml::123456789012:configured-audience-model/fixture",
				),
				ConfiguredAudienceModelAssociationName: aws.String("cama"),
				ManageResourcePolicies:                 aws.Bool(true),
				Description:                            aws.String(wantDescription),
			},
		)
		require.NoError(t, err)

		out, err := client.ListCollaborationConfiguredAudienceModelAssociations(
			ctx, &cleanroomssdk.ListCollaborationConfiguredAudienceModelAssociationsInput{
				CollaborationIdentifier: aws.String(collabID),
			},
		)
		require.NoError(t, err)
		require.Len(t, out.CollaborationConfiguredAudienceModelAssociationSummaries, 1)
		got := out.CollaborationConfiguredAudienceModelAssociationSummaries[0]
		assert.Equal(t, wantDescription, aws.ToString(got.Description))
	})

	t.Run("membership scoped configured audience model association description", func(t *testing.T) {
		t.Parallel()

		client := newRoundTripTestClient(t)
		ctx := t.Context()
		_, memID := createCollaborationAndMembership(t, client)

		const wantDescription = "distinguishable-cama-description"

		_, err := client.CreateConfiguredAudienceModelAssociation(
			ctx, &cleanroomssdk.CreateConfiguredAudienceModelAssociationInput{
				MembershipIdentifier: aws.String(memID),
				ConfiguredAudienceModelArn: aws.String(
					"arn:aws:cleanrooms-ml::123456789012:configured-audience-model/fixture",
				),
				ConfiguredAudienceModelAssociationName: aws.String("cama"),
				ManageResourcePolicies:                 aws.Bool(true),
				Description:                            aws.String(wantDescription),
			},
		)
		require.NoError(t, err)

		out, err := client.ListConfiguredAudienceModelAssociations(
			ctx, &cleanroomssdk.ListConfiguredAudienceModelAssociationsInput{
				MembershipIdentifier: aws.String(memID),
			},
		)
		require.NoError(t, err)
		require.Len(t, out.ConfiguredAudienceModelAssociationSummaries, 1)
		assert.Equal(t, wantDescription,
			aws.ToString(out.ConfiguredAudienceModelAssociationSummaries[0].Description))
	})

	t.Run("id namespace association description", func(t *testing.T) {
		t.Parallel()

		client := newRoundTripTestClient(t)
		ctx := t.Context()
		collabID, memID := createCollaborationAndMembership(t, client)

		const wantDescription = "distinguishable-collab-idns-description"

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

		out, err := client.ListCollaborationIdNamespaceAssociations(
			ctx, &cleanroomssdk.ListCollaborationIdNamespaceAssociationsInput{
				CollaborationIdentifier: aws.String(collabID),
			},
		)
		require.NoError(t, err)
		require.Len(t, out.CollaborationIdNamespaceAssociationSummaries, 1)
		assert.Equal(t, wantDescription,
			aws.ToString(out.CollaborationIdNamespaceAssociationSummaries[0].Description))
	})
}

package cleanrooms_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	cleanroomssdk "github.com/aws/aws-sdk-go-v2/service/cleanrooms"
	crtypes "github.com/aws/aws-sdk-go-v2/service/cleanrooms/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// createCollaborationAndMembership bootstraps a collaboration + membership
// through the real SDK client, for tests that need a collaboration-scoped
// fixture.
func createCollaborationAndMembership(t *testing.T, client *cleanroomssdk.Client) (string, string) {
	t.Helper()
	ctx := t.Context()

	colOut, colErr := client.CreateCollaboration(ctx, &cleanroomssdk.CreateCollaborationInput{
		Name:                   aws.String("collab"),
		CreatorDisplayName:     aws.String("creator"),
		CreatorMemberAbilities: []crtypes.MemberAbility{crtypes.MemberAbilityCanQuery},
		Members:                []crtypes.MemberSpecification{},
		QueryLogStatus:         crtypes.CollaborationQueryLogStatusDisabled,
	})
	require.NoError(t, colErr)
	collabID := aws.ToString(colOut.Collaboration.Id)

	memOut, memErr := client.CreateMembership(ctx, &cleanroomssdk.CreateMembershipInput{
		CollaborationIdentifier: aws.String(collabID),
		QueryLogStatus:          crtypes.MembershipQueryLogStatusDisabled,
	})
	require.NoError(t, memErr)
	memID := aws.ToString(memOut.Membership.Id)

	return collabID, memID
}

// TestCollaborationScopedAnalysisTemplates drives GetCollaborationAnalysisTemplate,
// BatchGetCollaborationAnalysisTemplate, and ListCollaborationAnalysisTemplates
// through the real SDK client and asserts on decoded field content, not on
// raw JSON: the SDK decodes nothing from a response key it doesn't
// recognize, so an assertion against an empty result would pass whether or
// not gopherstack-bv5d's wrong-key bugs are fixed. Also re-drives the
// unscoped GetAnalysisTemplate sibling, since it previously shared
// keyAnalysisTemplate with the collaboration-scoped Get and must keep
// working after the two were split into separate constants.
func TestCollaborationScopedAnalysisTemplates(t *testing.T) {
	t.Parallel()

	client := newRoundTripTestClient(t)
	ctx := t.Context()
	collabID, memID := createCollaborationAndMembership(t, client)

	createOut, createErr := client.CreateAnalysisTemplate(ctx, &cleanroomssdk.CreateAnalysisTemplateInput{
		MembershipIdentifier: aws.String(memID),
		Name:                 aws.String("tmpl"),
		Format:               crtypes.AnalysisFormatSql,
		Source:               &crtypes.AnalysisSourceMemberText{Value: "SELECT 1"},
	})
	require.NoError(t, createErr)
	templateArn := aws.ToString(createOut.AnalysisTemplate.Arn)
	require.NotEmpty(t, templateArn)

	t.Run("get collaboration scoped", func(t *testing.T) {
		t.Parallel()

		out, err := client.GetCollaborationAnalysisTemplate(ctx, &cleanroomssdk.GetCollaborationAnalysisTemplateInput{
			CollaborationIdentifier: aws.String(collabID),
			AnalysisTemplateArn:     aws.String(templateArn),
		})
		require.NoError(t, err)
		require.NotNil(t, out.CollaborationAnalysisTemplate,
			"SDK decodes nothing from an unrecognized key; a nil pointer means the wrong key is still in play")
		assert.Equal(t, templateArn, aws.ToString(out.CollaborationAnalysisTemplate.Arn))
	})

	t.Run("batch get collaboration scoped", func(t *testing.T) {
		t.Parallel()

		out, err := client.BatchGetCollaborationAnalysisTemplate(
			ctx,
			&cleanroomssdk.BatchGetCollaborationAnalysisTemplateInput{
				CollaborationIdentifier: aws.String(collabID),
				AnalysisTemplateArns:    []string{templateArn},
			},
		)
		require.NoError(t, err)
		require.Len(t, out.CollaborationAnalysisTemplates, 1)
		assert.Equal(t, templateArn, aws.ToString(out.CollaborationAnalysisTemplates[0].Arn))
	})

	t.Run("list collaboration scoped", func(t *testing.T) {
		t.Parallel()

		out, err := client.ListCollaborationAnalysisTemplates(
			ctx,
			&cleanroomssdk.ListCollaborationAnalysisTemplatesInput{
				CollaborationIdentifier: aws.String(collabID),
			},
		)
		require.NoError(t, err)
		require.Len(t, out.CollaborationAnalysisTemplateSummaries, 1)
		assert.Equal(t, templateArn, aws.ToString(out.CollaborationAnalysisTemplateSummaries[0].Arn))
	})

	t.Run("unscoped get still works", func(t *testing.T) {
		t.Parallel()

		templateID := aws.ToString(createOut.AnalysisTemplate.Id)
		out, err := client.GetAnalysisTemplate(ctx, &cleanroomssdk.GetAnalysisTemplateInput{
			MembershipIdentifier:       aws.String(memID),
			AnalysisTemplateIdentifier: aws.String(templateID),
		})
		require.NoError(t, err)
		require.NotNil(t, out.AnalysisTemplate)
		assert.Equal(t, templateArn, aws.ToString(out.AnalysisTemplate.Arn))
	})
}

// TestCollaborationScopedConfiguredAudienceModelAssociations drives
// GetCollaborationConfiguredAudienceModelAssociation and
// ListCollaborationConfiguredAudienceModelAssociations, plus a regression
// check that the unscoped GetConfiguredAudienceModelAssociation sibling
// (formerly sharing keyCAMAAssociation with the scoped Get) still works.
func TestCollaborationScopedConfiguredAudienceModelAssociations(t *testing.T) {
	t.Parallel()

	client := newRoundTripTestClient(t)
	ctx := t.Context()
	collabID, memID := createCollaborationAndMembership(t, client)

	camaArn := "arn:aws:cleanrooms-ml::123456789012:configured-audience-model/fixture"
	createOut, createErr := client.CreateConfiguredAudienceModelAssociation(
		ctx,
		&cleanroomssdk.CreateConfiguredAudienceModelAssociationInput{
			MembershipIdentifier:                   aws.String(memID),
			ConfiguredAudienceModelArn:             aws.String(camaArn),
			ConfiguredAudienceModelAssociationName: aws.String("cama"),
			ManageResourcePolicies:                 aws.Bool(true),
		},
	)
	require.NoError(t, createErr)
	camaID := aws.ToString(createOut.ConfiguredAudienceModelAssociation.Id)
	require.NotEmpty(t, camaID)

	t.Run("get collaboration scoped", func(t *testing.T) {
		t.Parallel()

		in := &cleanroomssdk.GetCollaborationConfiguredAudienceModelAssociationInput{
			CollaborationIdentifier:                      aws.String(collabID),
			ConfiguredAudienceModelAssociationIdentifier: aws.String(camaID),
		}
		out, err := client.GetCollaborationConfiguredAudienceModelAssociation(ctx, in)
		require.NoError(t, err)
		require.NotNil(t, out.CollaborationConfiguredAudienceModelAssociation)
		assert.Equal(t, camaID, aws.ToString(out.CollaborationConfiguredAudienceModelAssociation.Id))
	})

	t.Run("list collaboration scoped", func(t *testing.T) {
		t.Parallel()

		out, err := client.ListCollaborationConfiguredAudienceModelAssociations(
			ctx,
			&cleanroomssdk.ListCollaborationConfiguredAudienceModelAssociationsInput{
				CollaborationIdentifier: aws.String(collabID),
			},
		)
		require.NoError(t, err)
		require.Len(t, out.CollaborationConfiguredAudienceModelAssociationSummaries, 1)
		got := out.CollaborationConfiguredAudienceModelAssociationSummaries[0]
		assert.Equal(t, camaID, aws.ToString(got.Id))
	})

	t.Run("unscoped get still works", func(t *testing.T) {
		t.Parallel()

		in := &cleanroomssdk.GetConfiguredAudienceModelAssociationInput{
			MembershipIdentifier:                         aws.String(memID),
			ConfiguredAudienceModelAssociationIdentifier: aws.String(camaID),
		}
		out, err := client.GetConfiguredAudienceModelAssociation(ctx, in)
		require.NoError(t, err)
		require.NotNil(t, out.ConfiguredAudienceModelAssociation)
		assert.Equal(t, camaID, aws.ToString(out.ConfiguredAudienceModelAssociation.Id))
	})
}

// TestCollaborationScopedIdNamespaceAssociations drives
// GetCollaborationIdNamespaceAssociation and
// ListCollaborationIdNamespaceAssociations, plus a regression check that
// the unscoped GetIdNamespaceAssociation sibling still works.
func TestCollaborationScopedIdNamespaceAssociations(t *testing.T) {
	t.Parallel()

	client := newRoundTripTestClient(t)
	ctx := t.Context()
	collabID, memID := createCollaborationAndMembership(t, client)

	createOut, createErr := client.CreateIdNamespaceAssociation(ctx, &cleanroomssdk.CreateIdNamespaceAssociationInput{
		MembershipIdentifier: aws.String(memID),
		Name:                 aws.String("ns"),
		InputReferenceConfig: &crtypes.IdNamespaceAssociationInputReferenceConfig{
			InputReferenceArn:      aws.String("arn:aws:cleanrooms:us-east-1:123456789012:membership/" + memID),
			ManageResourcePolicies: aws.Bool(true),
		},
	})
	require.NoError(t, createErr)
	nsID := aws.ToString(createOut.IdNamespaceAssociation.Id)
	require.NotEmpty(t, nsID)

	t.Run("get collaboration scoped", func(t *testing.T) {
		t.Parallel()

		out, err := client.GetCollaborationIdNamespaceAssociation(
			ctx,
			&cleanroomssdk.GetCollaborationIdNamespaceAssociationInput{
				CollaborationIdentifier:          aws.String(collabID),
				IdNamespaceAssociationIdentifier: aws.String(nsID),
			},
		)
		require.NoError(t, err)
		require.NotNil(t, out.CollaborationIdNamespaceAssociation)
		assert.Equal(t, nsID, aws.ToString(out.CollaborationIdNamespaceAssociation.Id))
	})

	t.Run("list collaboration scoped", func(t *testing.T) {
		t.Parallel()

		out, err := client.ListCollaborationIdNamespaceAssociations(
			ctx,
			&cleanroomssdk.ListCollaborationIdNamespaceAssociationsInput{
				CollaborationIdentifier: aws.String(collabID),
			},
		)
		require.NoError(t, err)
		require.Len(t, out.CollaborationIdNamespaceAssociationSummaries, 1)
		got := out.CollaborationIdNamespaceAssociationSummaries[0]
		assert.Equal(t, nsID, aws.ToString(got.Id))
	})

	t.Run("unscoped get still works", func(t *testing.T) {
		t.Parallel()

		out, err := client.GetIdNamespaceAssociation(ctx, &cleanroomssdk.GetIdNamespaceAssociationInput{
			MembershipIdentifier:             aws.String(memID),
			IdNamespaceAssociationIdentifier: aws.String(nsID),
		})
		require.NoError(t, err)
		require.NotNil(t, out.IdNamespaceAssociation)
		assert.Equal(t, nsID, aws.ToString(out.IdNamespaceAssociation.Id))
	})
}

// TestCollaborationScopedPrivacyBudgets drives
// GetCollaborationPrivacyBudgetTemplate, ListCollaborationPrivacyBudgetTemplates,
// and ListCollaborationPrivacyBudgets, plus a regression check that the
// unscoped GetPrivacyBudgetTemplate sibling still works.
func TestCollaborationScopedPrivacyBudgets(t *testing.T) {
	t.Parallel()

	client := newRoundTripTestClient(t)
	ctx := t.Context()
	collabID, memID := createCollaborationAndMembership(t, client)

	createOut, createErr := client.CreatePrivacyBudgetTemplate(ctx, &cleanroomssdk.CreatePrivacyBudgetTemplateInput{
		MembershipIdentifier: aws.String(memID),
		PrivacyBudgetType:    crtypes.PrivacyBudgetTypeDifferentialPrivacy,
		AutoRefresh:          crtypes.PrivacyBudgetTemplateAutoRefreshCalendarMonth,
		Parameters: &crtypes.PrivacyBudgetTemplateParametersInputMemberDifferentialPrivacy{
			Value: crtypes.DifferentialPrivacyTemplateParametersInput{
				Epsilon:            aws.Int32(10),
				UsersNoisePerQuery: aws.Int32(100),
			},
		},
	})
	require.NoError(t, createErr)
	tmplID := aws.ToString(createOut.PrivacyBudgetTemplate.Id)
	require.NotEmpty(t, tmplID)

	t.Run("get collaboration scoped", func(t *testing.T) {
		t.Parallel()

		out, err := client.GetCollaborationPrivacyBudgetTemplate(
			ctx,
			&cleanroomssdk.GetCollaborationPrivacyBudgetTemplateInput{
				CollaborationIdentifier:         aws.String(collabID),
				PrivacyBudgetTemplateIdentifier: aws.String(tmplID),
			},
		)
		require.NoError(t, err)
		require.NotNil(t, out.CollaborationPrivacyBudgetTemplate)
		assert.Equal(t, tmplID, aws.ToString(out.CollaborationPrivacyBudgetTemplate.Id))
	})

	t.Run("list templates collaboration scoped", func(t *testing.T) {
		t.Parallel()

		out, err := client.ListCollaborationPrivacyBudgetTemplates(
			ctx,
			&cleanroomssdk.ListCollaborationPrivacyBudgetTemplatesInput{
				CollaborationIdentifier: aws.String(collabID),
			},
		)
		require.NoError(t, err)
		require.Len(t, out.CollaborationPrivacyBudgetTemplateSummaries, 1)
		got := out.CollaborationPrivacyBudgetTemplateSummaries[0]
		assert.Equal(t, tmplID, aws.ToString(got.Id))
	})

	t.Run("list budgets collaboration scoped", func(t *testing.T) {
		t.Parallel()

		out, err := client.ListCollaborationPrivacyBudgets(ctx, &cleanroomssdk.ListCollaborationPrivacyBudgetsInput{
			CollaborationIdentifier: aws.String(collabID),
			PrivacyBudgetType:       crtypes.PrivacyBudgetTypeDifferentialPrivacy,
		})
		require.NoError(t, err)
		const emptyListMsg = "budgets exist once the template does; empty here means the wrong response key"
		require.NotEmpty(t, out.CollaborationPrivacyBudgetSummaries, emptyListMsg)
	})

	t.Run("unscoped get still works", func(t *testing.T) {
		t.Parallel()

		out, err := client.GetPrivacyBudgetTemplate(ctx, &cleanroomssdk.GetPrivacyBudgetTemplateInput{
			MembershipIdentifier:            aws.String(memID),
			PrivacyBudgetTemplateIdentifier: aws.String(tmplID),
		})
		require.NoError(t, err)
		require.NotNil(t, out.PrivacyBudgetTemplate)
		assert.Equal(t, tmplID, aws.ToString(out.PrivacyBudgetTemplate.Id))
	})
}

// TestListCollaborationChangeRequests drives CreateCollaborationChangeRequest
// then ListCollaborationChangeRequests through the real SDK client.
func TestListCollaborationChangeRequests(t *testing.T) {
	t.Parallel()

	client := newRoundTripTestClient(t)
	ctx := t.Context()
	collabID, _ := createCollaborationAndMembership(t, client)

	createIn := &cleanroomssdk.CreateCollaborationChangeRequestInput{
		CollaborationIdentifier: aws.String(collabID),
		Changes: []crtypes.ChangeInput{
			{
				SpecificationType: crtypes.ChangeSpecificationTypeMember,
				Specification: &crtypes.ChangeSpecificationMemberMember{
					Value: crtypes.MemberChangeSpecification{
						AccountId:       aws.String("111111111111"),
						MemberAbilities: []crtypes.MemberAbility{},
					},
				},
			},
		},
	}
	createOut, createErr := client.CreateCollaborationChangeRequest(ctx, createIn)
	require.NoError(t, createErr)
	changeRequestID := aws.ToString(createOut.CollaborationChangeRequest.Id)
	require.NotEmpty(t, changeRequestID)

	out, err := client.ListCollaborationChangeRequests(ctx, &cleanroomssdk.ListCollaborationChangeRequestsInput{
		CollaborationIdentifier: aws.String(collabID),
	})
	require.NoError(t, err)
	require.Len(t, out.CollaborationChangeRequestSummaries, 1)
	assert.Equal(t, changeRequestID, aws.ToString(out.CollaborationChangeRequestSummaries[0].Id))
}

// TestPopulateIdMappingTable drives CreateIdMappingTable then
// PopulateIdMappingTable through the real SDK client and asserts the real
// idMappingJobId key decodes, not the previously fabricated
// mappedJobIdentifier key.
func TestPopulateIdMappingTable(t *testing.T) {
	t.Parallel()

	client := newRoundTripTestClient(t)
	ctx := t.Context()
	_, memID := createCollaborationAndMembership(t, client)

	workflowArn := "arn:aws:entityresolution:us-east-1:123456789012:idmappingworkflow/fixture"
	createOut, createErr := client.CreateIdMappingTable(ctx, &cleanroomssdk.CreateIdMappingTableInput{
		MembershipIdentifier: aws.String(memID),
		Name:                 aws.String("mapping-table"),
		InputReferenceConfig: &crtypes.IdMappingTableInputReferenceConfig{
			InputReferenceArn:      aws.String(workflowArn),
			ManageResourcePolicies: aws.Bool(true),
		},
	})
	require.NoError(t, createErr)
	tableID := aws.ToString(createOut.IdMappingTable.Id)
	require.NotEmpty(t, tableID)

	out, err := client.PopulateIdMappingTable(ctx, &cleanroomssdk.PopulateIdMappingTableInput{
		MembershipIdentifier:     aws.String(memID),
		IdMappingTableIdentifier: aws.String(tableID),
	})
	require.NoError(t, err)
	const emptyJobIDMsg = "empty IdMappingJobId means the handler is still emitting mappedJobIdentifier"
	assert.NotEmpty(t, aws.ToString(out.IdMappingJobId), emptyJobIDMsg)
}

// TestListMembers_MemberSummaries proves ListMembers decodes through the
// real SDK client. The handler wrapped its list under "memberList" -- the
// real key (cleanrooms@v1.49.4 deserializers.go
// awsRestjson1_deserializeOpDocumentListMembersOutput) is "memberSummaries";
// "memberList" is an unrelated wire key from ProtectedQuery.Participants
// (UpdateProtectedQueryOutput). The SDK deserializer's switch never matched
// "memberList", so MemberSummaries silently decoded nil with err == nil.
func TestListMembers_MemberSummaries(t *testing.T) {
	t.Parallel()

	client := newRoundTripTestClient(t)
	ctx := t.Context()
	collabID, _ := createCollaborationAndMembership(t, client)

	listOut, listErr := client.ListMembers(ctx, &cleanroomssdk.ListMembersInput{
		CollaborationIdentifier: aws.String(collabID),
	})
	require.NoError(t, listErr)
	require.NotEmpty(t, listOut.MemberSummaries, "ListMembersOutput.MemberSummaries must decode a non-empty slice")
	assert.Equal(t, "creator", aws.ToString(listOut.MemberSummaries[0].DisplayName))
}

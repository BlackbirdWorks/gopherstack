package cleanrooms_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	cleanroomssdk "github.com/aws/aws-sdk-go-v2/service/cleanrooms"
	crtypes "github.com/aws/aws-sdk-go-v2/service/cleanrooms/types"
	"github.com/stretchr/testify/require"
)

// Test_SDKRoundTrip_MemberAbilities_RequiredNotOmitted proves Membership's
// required "memberAbilities" (types.Membership, deserializers.go's
// awsRestjson1_deserializeDocumentMembership) decodes as a non-nil slice --
// not silently dropped -- when a real client creates a collaboration with
// an empty creatorMemberAbilities list, a valid (Smithy "required" on a list
// only means "must be present", not "non-empty") and reachable state.
// Before the fix, MemberAbilities was tagged `omitempty`: encoding/json
// omits a zero-length slice regardless of nilness, so the key vanished from
// the wire and the real SDK client's deserializer -- which only assigns the
// field when it sees the key at all -- left Membership.MemberAbilities nil,
// indistinguishable from the key never having existed.
func Test_SDKRoundTrip_MemberAbilities_RequiredNotOmitted(t *testing.T) {
	t.Parallel()

	client := newRoundTripTestClient(t)
	ctx := t.Context()

	out, err := client.CreateCollaboration(ctx, &cleanroomssdk.CreateCollaborationInput{
		Name:                   aws.String("r80d-collab"),
		CreatorDisplayName:     aws.String("creator"),
		CreatorMemberAbilities: []crtypes.MemberAbility{},
		Members:                []crtypes.MemberSpecification{},
		QueryLogStatus:         crtypes.CollaborationQueryLogStatusDisabled,
	})
	require.NoError(t, err)
	require.NotNil(
		t, out.Collaboration.MembershipId,
		"CreateCollaboration must auto-create the creator's membership",
	)

	memOut, err := client.GetMembership(ctx, &cleanroomssdk.GetMembershipInput{
		MembershipIdentifier: out.Collaboration.MembershipId,
	})
	require.NoError(t, err)
	require.NotNil(
		t, memOut.Membership.MemberAbilities,
		"memberAbilities must decode as [] (present), never as a dropped/null required field",
	)
	require.Empty(t, memOut.Membership.MemberAbilities)

	listOut, err := client.ListMemberships(ctx, &cleanroomssdk.ListMembershipsInput{})
	require.NoError(t, err)
	require.Len(t, listOut.MembershipSummaries, 1)
	require.NotNil(
		t, listOut.MembershipSummaries[0].MemberAbilities,
		"MembershipSummary.memberAbilities must decode as [] too, not be dropped",
	)
}

// Test_SDKRoundTrip_ConfiguredTable_AllowedColumnsAndAnalysisRuleTypes
// proves ConfiguredTable's required "allowedColumns" and "analysisRuleTypes"
// (types.ConfiguredTable) decode as non-nil slices for a freshly created
// table with no columns allowed yet and no analysis rule attached yet --
// both real, reachable states (allowedColumns is required-but-can-be-empty
// on the input per Smithy list semantics; analysisRuleTypes only grows once
// CreateConfiguredTableAnalysisRule is called, so a table between those two
// calls legitimately has zero analysis rule types).
func Test_SDKRoundTrip_ConfiguredTable_AllowedColumnsAndAnalysisRuleTypes(t *testing.T) {
	t.Parallel()

	client := newRoundTripTestClient(t)
	ctx := t.Context()

	out, err := client.CreateConfiguredTable(ctx, &cleanroomssdk.CreateConfiguredTableInput{
		Name: aws.String("r80d-table"),
		TableReference: &crtypes.TableReferenceMemberGlue{
			Value: crtypes.GlueTableReference{
				DatabaseName: aws.String("db"),
				TableName:    aws.String("table"),
			},
		},
		AllowedColumns: []string{},
		AnalysisMethod: crtypes.AnalysisMethodDirectQuery,
	})
	require.NoError(t, err)
	require.NotNil(
		t, out.ConfiguredTable.AllowedColumns,
		"allowedColumns must decode as [] (present), never as a dropped/null required field",
	)
	require.Empty(t, out.ConfiguredTable.AllowedColumns)
	require.NotNil(
		t, out.ConfiguredTable.AnalysisRuleTypes,
		"analysisRuleTypes must decode as [] before any analysis rule is attached",
	)
	require.Empty(t, out.ConfiguredTable.AnalysisRuleTypes)

	listOut, err := client.ListConfiguredTables(ctx, &cleanroomssdk.ListConfiguredTablesInput{})
	require.NoError(t, err)
	require.Len(t, listOut.ConfiguredTableSummaries, 1)
	require.NotNil(
		t, listOut.ConfiguredTableSummaries[0].AnalysisRuleTypes,
		"ConfiguredTableSummary.analysisRuleTypes must decode as [] too, not be dropped",
	)
}

// Test_SDKRoundTrip_ConfiguredTableAssociation_AnalysisRuleTypes proves
// ConfiguredTableAssociation's required "analysisRuleTypes"
// (types.ConfiguredTableAssociation) decodes as a non-nil slice for a
// freshly created association with no analysis rule attached yet.
func Test_SDKRoundTrip_ConfiguredTableAssociation_AnalysisRuleTypes(t *testing.T) {
	t.Parallel()

	client := newRoundTripTestClient(t)
	ctx := t.Context()

	_, memID := createCollaborationAndMembership(t, client)

	ctOut, err := client.CreateConfiguredTable(ctx, &cleanroomssdk.CreateConfiguredTableInput{
		Name: aws.String("r80d-assoc-table"),
		TableReference: &crtypes.TableReferenceMemberGlue{
			Value: crtypes.GlueTableReference{
				DatabaseName: aws.String("db"),
				TableName:    aws.String("table"),
			},
		},
		AllowedColumns: []string{"col1"},
		AnalysisMethod: crtypes.AnalysisMethodDirectQuery,
	})
	require.NoError(t, err)

	assocOut, err := client.CreateConfiguredTableAssociation(ctx, &cleanroomssdk.CreateConfiguredTableAssociationInput{
		Name:                      aws.String("r80d-assoc"),
		MembershipIdentifier:      aws.String(memID),
		ConfiguredTableIdentifier: ctOut.ConfiguredTable.Id,
		RoleArn:                   aws.String("arn:aws:iam::123456789012:role/AssocRole"),
	})
	require.NoError(t, err)
	require.NotNil(
		t, assocOut.ConfiguredTableAssociation.AnalysisRuleTypes,
		"analysisRuleTypes must decode as [] before any association analysis rule is attached",
	)
	require.Empty(t, assocOut.ConfiguredTableAssociation.AnalysisRuleTypes)
}

// Test_SDKRoundTrip_ConfiguredAudienceModelAssociationSummary_Arn proves
// ListConfiguredAudienceModelAssociations' required
// "configuredAudienceModelArn" (types.ConfiguredAudienceModelAssociationSummary)
// decodes -- flagged and left unfixed by gopherstack-dv4s's 2026-08-14 pass
// ("A real missing-field gap, recorded rather than folded into this pass's
// leak fix to keep the two bug classes separate").
func Test_SDKRoundTrip_ConfiguredAudienceModelAssociationSummary_Arn(t *testing.T) {
	t.Parallel()

	client := newRoundTripTestClient(t)
	ctx := t.Context()

	_, memID := createCollaborationAndMembership(t, client)

	const modelArn = "arn:aws:cleanrooms-ml:us-east-1:123456789012:configured-audience-model/abc"
	createOut, err := client.CreateConfiguredAudienceModelAssociation(
		ctx, &cleanroomssdk.CreateConfiguredAudienceModelAssociationInput{
			MembershipIdentifier:                   aws.String(memID),
			ConfiguredAudienceModelArn:             aws.String(modelArn),
			ConfiguredAudienceModelAssociationName: aws.String("r80d-cama"),
			ManageResourcePolicies:                 aws.Bool(false),
		},
	)
	require.NoError(t, err)
	require.Equal(t, modelArn, aws.ToString(createOut.ConfiguredAudienceModelAssociation.ConfiguredAudienceModelArn))

	listOut, err := client.ListConfiguredAudienceModelAssociations(
		ctx, &cleanroomssdk.ListConfiguredAudienceModelAssociationsInput{MembershipIdentifier: aws.String(memID)},
	)
	require.NoError(t, err)
	require.Len(t, listOut.ConfiguredAudienceModelAssociationSummaries, 1)
	require.NotNil(
		t, listOut.ConfiguredAudienceModelAssociationSummaries[0].ConfiguredAudienceModelArn,
		"configuredAudienceModelArn must decode on the List summary, not just the full resource",
	)
	require.Equal(
		t, modelArn,
		aws.ToString(listOut.ConfiguredAudienceModelAssociationSummaries[0].ConfiguredAudienceModelArn),
	)
}

// Test_SDKRoundTrip_PrivacyBudgetTemplate_AutoRefreshDefaultsToNone proves
// PrivacyBudgetTemplate's required "autoRefresh" (types.PrivacyBudgetTemplate)
// decodes as a real enum value ("NONE"), not the zero value, when a real
// client creates a template without specifying it -- autoRefresh is
// optional on CreatePrivacyBudgetTemplateInput but required on the output.
func Test_SDKRoundTrip_PrivacyBudgetTemplate_AutoRefreshDefaultsToNone(t *testing.T) {
	t.Parallel()

	client := newRoundTripTestClient(t)
	ctx := t.Context()

	_, memID := createCollaborationAndMembership(t, client)

	out, err := client.CreatePrivacyBudgetTemplate(ctx, &cleanroomssdk.CreatePrivacyBudgetTemplateInput{
		MembershipIdentifier: aws.String(memID),
		PrivacyBudgetType:    crtypes.PrivacyBudgetTypeDifferentialPrivacy,
		Parameters: &crtypes.PrivacyBudgetTemplateParametersInputMemberDifferentialPrivacy{
			Value: crtypes.DifferentialPrivacyTemplateParametersInput{
				Epsilon:            aws.Int32(10),
				UsersNoisePerQuery: aws.Int32(30),
			},
		},
	})
	require.NoError(t, err)
	require.Equal(
		t, crtypes.PrivacyBudgetTemplateAutoRefreshNone, out.PrivacyBudgetTemplate.AutoRefresh,
		"autoRefresh must default to a real enum value, not decode as the empty zero value",
	)
}

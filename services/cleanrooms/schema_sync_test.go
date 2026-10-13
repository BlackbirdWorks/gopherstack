package cleanrooms_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	cleanroomssdk "github.com/aws/aws-sdk-go-v2/service/cleanrooms"
	crtypes "github.com/aws/aws-sdk-go-v2/service/cleanrooms/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cleanrooms"
)

type fakeGlue struct{}

func (fakeGlue) TableColumns(_, database, table string) ([]cleanrooms.SchemaColumn, []cleanrooms.SchemaColumn, bool) {
	if database != "db" || table != "people" {
		return nil, nil, false
	}

	columns := []cleanrooms.SchemaColumn{
		{Name: "id", Type: "bigint"},
		{Name: "email", Type: "string"},
		{Name: "ssn", Type: "string"},
	}

	return columns, []cleanrooms.SchemaColumn{{Name: "dt", Type: "string"}}, true
}

func TestSchemasFromConfiguredTableAssociations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		table      string
		wantSchema bool
	}{
		{name: "glue_table", table: "people", wantSchema: true},
		{name: "missing_glue_table", table: "absent"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := cleanrooms.NewInMemoryBackend(rtTestAccountID, rtTestRegion)
			backend.SetGlueTableReader(fakeGlue{})
			client := newRoundTripClient(t, cleanrooms.NewHandler(backend))
			ctx := t.Context()
			collabID, memID := createCollaborationAndMembership(t, client)

			ct, err := client.CreateConfiguredTable(ctx, &cleanroomssdk.CreateConfiguredTableInput{
				Name: aws.String("ct"),
				TableReference: &crtypes.TableReferenceMemberGlue{Value: crtypes.GlueTableReference{
					DatabaseName: aws.String(
						"db",
					),
					TableName: aws.String(tt.table),
					Region:    crtypes.CommercialRegionUsEast1,
				}},
				AllowedColumns: []string{"id", "email", "dt"},
				AnalysisMethod: crtypes.AnalysisMethodDirectQuery,
			})
			require.NoError(t, err)

			assoc, err := client.CreateConfiguredTableAssociation(
				ctx,
				&cleanroomssdk.CreateConfiguredTableAssociationInput{
					Name:                      aws.String("people-assoc"),
					MembershipIdentifier:      aws.String(memID),
					ConfiguredTableIdentifier: ct.ConfiguredTable.Id,
					RoleArn:                   aws.String("arn:aws:iam::000000000000:role/r"),
				},
			)
			require.NoError(t, err)

			_, err = client.CreateConfiguredTableAnalysisRule(
				ctx,
				&cleanroomssdk.CreateConfiguredTableAnalysisRuleInput{
					ConfiguredTableIdentifier: ct.ConfiguredTable.Id,
					AnalysisRuleType:          crtypes.ConfiguredTableAnalysisRuleTypeList,
					AnalysisRulePolicy: &crtypes.ConfiguredTableAnalysisRulePolicyMemberV1{
						Value: &crtypes.ConfiguredTableAnalysisRulePolicyV1MemberList{Value: crtypes.AnalysisRuleList{
							JoinColumns: []string{"id"}, ListColumns: []string{"email"},
						}},
					},
				},
			)
			require.NoError(t, err)

			listed, err := client.ListSchemas(
				ctx,
				&cleanroomssdk.ListSchemasInput{CollaborationIdentifier: aws.String(collabID)},
			)
			require.NoError(t, err)

			if !tt.wantSchema {
				assert.Empty(t, listed.SchemaSummaries)

				return
			}

			require.Len(t, listed.SchemaSummaries, 1)

			got, err := client.GetSchema(ctx, &cleanroomssdk.GetSchemaInput{
				CollaborationIdentifier: aws.String(collabID), Name: assoc.ConfiguredTableAssociation.Name,
			})
			require.NoError(t, err)
			assert.Equal(t, crtypes.SchemaTypeTable, got.Schema.Type)
			require.Len(t, got.Schema.Columns, 2)
			assert.Equal(t, "id", aws.ToString(got.Schema.Columns[0].Name))
			assert.Equal(t, "bigint", aws.ToString(got.Schema.Columns[0].Type))
			require.Len(t, got.Schema.PartitionKeys, 1)
			assert.Equal(t, "dt", aws.ToString(got.Schema.PartitionKeys[0].Name))
			assert.Equal(t, []crtypes.AnalysisRuleType{crtypes.AnalysisRuleTypeList}, got.Schema.AnalysisRuleTypes)

			rule, err := client.GetSchemaAnalysisRule(ctx, &cleanroomssdk.GetSchemaAnalysisRuleInput{
				CollaborationIdentifier: aws.String(collabID), Name: assoc.ConfiguredTableAssociation.Name,
				Type: crtypes.AnalysisRuleTypeList,
			})
			require.NoError(t, err)
			assert.Equal(t, crtypes.AnalysisRuleTypeList, rule.AnalysisRule.Type)

			_, err = client.DeleteConfiguredTableAssociation(ctx, &cleanroomssdk.DeleteConfiguredTableAssociationInput{
				MembershipIdentifier: aws.String(
					memID,
				),
				ConfiguredTableAssociationIdentifier: assoc.ConfiguredTableAssociation.Id,
			})
			require.NoError(t, err)

			listed, err = client.ListSchemas(
				ctx,
				&cleanroomssdk.ListSchemasInput{CollaborationIdentifier: aws.String(collabID)},
			)
			require.NoError(t, err)
			assert.Empty(t, listed.SchemaSummaries)
		})
	}
}

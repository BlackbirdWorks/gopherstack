package cleanrooms_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	cleanroomssdk "github.com/aws/aws-sdk-go-v2/service/cleanrooms"
	crtypes "github.com/aws/aws-sdk-go-v2/service/cleanrooms/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRealClient_TableAndCollaborationLifecycle(t *testing.T) {
	t.Parallel()
	cases := []struct {
		run  func(t *testing.T)
		name string
	}{
		{name: "schema_ops_unpopulated", run: func(t *testing.T) {
			t.Helper()

			client := newRoundTripTestClient(t)
			ctx := t.Context()

			collabID, _ := createCollaborationAndMembership(t, client)

			listOut, err := client.ListSchemas(ctx, &cleanroomssdk.ListSchemasInput{
				CollaborationIdentifier: aws.String(collabID),
			})
			require.NoError(t, err)
			assert.Empty(t, listOut.SchemaSummaries)

			_, err = client.GetSchema(ctx, &cleanroomssdk.GetSchemaInput{
				CollaborationIdentifier: aws.String(collabID),
				Name:                    aws.String("nonexistent"),
			})
			require.Error(t, err)

			_, err = client.GetSchemaAnalysisRule(ctx, &cleanroomssdk.GetSchemaAnalysisRuleInput{
				CollaborationIdentifier: aws.String(collabID),
				Name:                    aws.String("nonexistent"),
				Type:                    crtypes.AnalysisRuleTypeAggregation,
			})
			require.Error(t, err)

			batchOut, err := client.BatchGetSchema(ctx, &cleanroomssdk.BatchGetSchemaInput{
				CollaborationIdentifier: aws.String(collabID),
				Names:                   []string{"nonexistent"},
			})
			require.NoError(t, err)
			assert.Empty(t, batchOut.Schemas)
			require.Len(t, batchOut.Errors, 1)
			assert.Equal(t, "nonexistent", aws.ToString(batchOut.Errors[0].Name))

			batchRuleOut, err := client.BatchGetSchemaAnalysisRule(
				ctx, &cleanroomssdk.BatchGetSchemaAnalysisRuleInput{
					CollaborationIdentifier: aws.String(collabID),
					SchemaAnalysisRuleRequests: []crtypes.SchemaAnalysisRuleRequest{
						{Name: aws.String("nonexistent"), Type: crtypes.AnalysisRuleTypeAggregation},
					},
				},
			)
			require.NoError(t, err)
			assert.Empty(t, batchRuleOut.AnalysisRules)
			require.Len(t, batchRuleOut.Errors, 1)
		}},
		{name: "configured_table_lifecycle", run: func(t *testing.T) {
			t.Helper()

			client := newRoundTripTestClient(t)
			ctx := t.Context()

			createOut, err := client.CreateConfiguredTable(ctx, &cleanroomssdk.CreateConfiguredTableInput{
				Name: aws.String("slice12-table"),
				TableReference: &crtypes.TableReferenceMemberGlue{
					Value: crtypes.GlueTableReference{
						DatabaseName: aws.String("db"),
						TableName:    aws.String("table"),
					},
				},
				AllowedColumns: []string{"col1", "col2"},
				AnalysisMethod: crtypes.AnalysisMethodDirectQuery,
			})
			require.NoError(t, err)
			tableID := createOut.ConfiguredTable.Id

			getOut, err := client.GetConfiguredTable(ctx, &cleanroomssdk.GetConfiguredTableInput{
				ConfiguredTableIdentifier: tableID,
			})
			require.NoError(t, err)
			assert.Equal(t, "slice12-table", aws.ToString(getOut.ConfiguredTable.Name))

			updOut, err := client.UpdateConfiguredTable(ctx, &cleanroomssdk.UpdateConfiguredTableInput{
				ConfiguredTableIdentifier: tableID,
				Description:               aws.String("updated description"),
			})
			require.NoError(t, err)
			assert.Equal(t, "updated description", aws.ToString(updOut.ConfiguredTable.Description))

			ruleOut, err := client.CreateConfiguredTableAnalysisRule(
				ctx, &cleanroomssdk.CreateConfiguredTableAnalysisRuleInput{
					ConfiguredTableIdentifier: tableID,
					AnalysisRuleType:          crtypes.ConfiguredTableAnalysisRuleTypeAggregation,
					AnalysisRulePolicy: &crtypes.ConfiguredTableAnalysisRulePolicyMemberV1{
						Value: &crtypes.ConfiguredTableAnalysisRulePolicyV1MemberAggregation{
							Value: crtypes.AnalysisRuleAggregation{
								AggregateColumns: []crtypes.AggregateColumn{
									{ColumnNames: []string{"col1"}, Function: crtypes.AggregateFunctionNameSum},
								},
								JoinColumns:          []string{"col2"},
								OutputConstraints:    []crtypes.AggregationConstraint{},
								DimensionColumns:     []string{},
								ScalarFunctions:      []crtypes.ScalarFunctions{},
								AllowedJoinOperators: []crtypes.JoinOperator{},
							},
						},
					},
				},
			)
			require.NoError(t, err)
			require.NotNil(t, ruleOut.AnalysisRule)

			getRuleOut, err := client.GetConfiguredTableAnalysisRule(
				ctx, &cleanroomssdk.GetConfiguredTableAnalysisRuleInput{
					ConfiguredTableIdentifier: tableID,
					AnalysisRuleType:          crtypes.ConfiguredTableAnalysisRuleTypeAggregation,
				},
			)
			require.NoError(t, err)
			require.NotNil(t, getRuleOut.AnalysisRule)

			updRuleOut, err := client.UpdateConfiguredTableAnalysisRule(
				ctx, &cleanroomssdk.UpdateConfiguredTableAnalysisRuleInput{
					ConfiguredTableIdentifier: tableID,
					AnalysisRuleType:          crtypes.ConfiguredTableAnalysisRuleTypeAggregation,
					AnalysisRulePolicy: &crtypes.ConfiguredTableAnalysisRulePolicyMemberV1{
						Value: &crtypes.ConfiguredTableAnalysisRulePolicyV1MemberAggregation{
							Value: crtypes.AnalysisRuleAggregation{
								AggregateColumns: []crtypes.AggregateColumn{
									{ColumnNames: []string{"col2"}, Function: crtypes.AggregateFunctionNameSum},
								},
								JoinColumns:          []string{"col1"},
								OutputConstraints:    []crtypes.AggregationConstraint{},
								DimensionColumns:     []string{},
								ScalarFunctions:      []crtypes.ScalarFunctions{},
								AllowedJoinOperators: []crtypes.JoinOperator{},
							},
						},
					},
				},
			)
			require.NoError(t, err)
			require.NotNil(t, updRuleOut.AnalysisRule)

			_, err = client.DeleteConfiguredTableAnalysisRule(
				ctx, &cleanroomssdk.DeleteConfiguredTableAnalysisRuleInput{
					ConfiguredTableIdentifier: tableID,
					AnalysisRuleType:          crtypes.ConfiguredTableAnalysisRuleTypeAggregation,
				},
			)
			require.NoError(t, err)

			_, err = client.DeleteConfiguredTable(ctx, &cleanroomssdk.DeleteConfiguredTableInput{
				ConfiguredTableIdentifier: tableID,
			})
			require.NoError(t, err)

			_, err = client.GetConfiguredTable(ctx, &cleanroomssdk.GetConfiguredTableInput{
				ConfiguredTableIdentifier: tableID,
			})
			require.Error(t, err)
		}},
		{name: "configured_table_association_lifecycle", run: func(t *testing.T) {
			t.Helper()

			client := newRoundTripTestClient(t)
			ctx := t.Context()

			_, memID := createCollaborationAndMembership(t, client)

			ctOut, err := client.CreateConfiguredTable(ctx, &cleanroomssdk.CreateConfiguredTableInput{
				Name: aws.String("assoc-table"),
				TableReference: &crtypes.TableReferenceMemberGlue{
					Value: crtypes.GlueTableReference{DatabaseName: aws.String("db"), TableName: aws.String("t")},
				},
				AllowedColumns: []string{"col1"},
				AnalysisMethod: crtypes.AnalysisMethodDirectQuery,
			})
			require.NoError(t, err)

			assocOut, err := client.CreateConfiguredTableAssociation(
				ctx, &cleanroomssdk.CreateConfiguredTableAssociationInput{
					Name:                      aws.String("slice12-assoc"),
					MembershipIdentifier:      aws.String(memID),
					ConfiguredTableIdentifier: ctOut.ConfiguredTable.Id,
					RoleArn:                   aws.String("arn:aws:iam::123456789012:role/AssocRole"),
				},
			)
			require.NoError(t, err)
			assocID := assocOut.ConfiguredTableAssociation.Id

			getOut, err := client.GetConfiguredTableAssociation(
				ctx, &cleanroomssdk.GetConfiguredTableAssociationInput{
					MembershipIdentifier:                 aws.String(memID),
					ConfiguredTableAssociationIdentifier: assocID,
				},
			)
			require.NoError(t, err)
			assert.Equal(t, "slice12-assoc", aws.ToString(getOut.ConfiguredTableAssociation.Name))

			listOut, err := client.ListConfiguredTableAssociations(
				ctx, &cleanroomssdk.ListConfiguredTableAssociationsInput{
					MembershipIdentifier: aws.String(memID),
				},
			)
			require.NoError(t, err)
			require.Len(t, listOut.ConfiguredTableAssociationSummaries, 1)

			updOut, err := client.UpdateConfiguredTableAssociation(
				ctx, &cleanroomssdk.UpdateConfiguredTableAssociationInput{
					MembershipIdentifier:                 aws.String(memID),
					ConfiguredTableAssociationIdentifier: assocID,
					Description:                          aws.String("updated"),
				},
			)
			require.NoError(t, err)
			assert.Equal(t, "updated", aws.ToString(updOut.ConfiguredTableAssociation.Description))

			ruleOut, err := client.CreateConfiguredTableAssociationAnalysisRule(
				ctx, &cleanroomssdk.CreateConfiguredTableAssociationAnalysisRuleInput{
					MembershipIdentifier:                 aws.String(memID),
					ConfiguredTableAssociationIdentifier: assocID,
					AnalysisRuleType:                     crtypes.ConfiguredTableAssociationAnalysisRuleTypeAggregation,
					AnalysisRulePolicy: &crtypes.ConfiguredTableAssociationAnalysisRulePolicyMemberV1{
						Value: &crtypes.ConfiguredTableAssociationAnalysisRulePolicyV1MemberAggregation{
							Value: crtypes.ConfiguredTableAssociationAnalysisRuleAggregation{
								AllowedResultReceivers: []string{},
							},
						},
					},
				},
			)
			require.NoError(t, err)
			require.NotNil(t, ruleOut.AnalysisRule)

			getRuleOut, err := client.GetConfiguredTableAssociationAnalysisRule(
				ctx, &cleanroomssdk.GetConfiguredTableAssociationAnalysisRuleInput{
					MembershipIdentifier:                 aws.String(memID),
					ConfiguredTableAssociationIdentifier: assocID,
					AnalysisRuleType:                     crtypes.ConfiguredTableAssociationAnalysisRuleTypeAggregation,
				},
			)
			require.NoError(t, err)
			require.NotNil(t, getRuleOut.AnalysisRule)

			updRuleOut, err := client.UpdateConfiguredTableAssociationAnalysisRule(
				ctx, &cleanroomssdk.UpdateConfiguredTableAssociationAnalysisRuleInput{
					MembershipIdentifier:                 aws.String(memID),
					ConfiguredTableAssociationIdentifier: assocID,
					AnalysisRuleType:                     crtypes.ConfiguredTableAssociationAnalysisRuleTypeAggregation,
					AnalysisRulePolicy: &crtypes.ConfiguredTableAssociationAnalysisRulePolicyMemberV1{
						Value: &crtypes.ConfiguredTableAssociationAnalysisRulePolicyV1MemberAggregation{
							Value: crtypes.ConfiguredTableAssociationAnalysisRuleAggregation{
								AllowedResultReceivers: []string{},
							},
						},
					},
				},
			)
			require.NoError(t, err)
			require.NotNil(t, updRuleOut.AnalysisRule)

			_, err = client.DeleteConfiguredTableAssociationAnalysisRule(
				ctx, &cleanroomssdk.DeleteConfiguredTableAssociationAnalysisRuleInput{
					MembershipIdentifier:                 aws.String(memID),
					ConfiguredTableAssociationIdentifier: assocID,
					AnalysisRuleType:                     crtypes.ConfiguredTableAssociationAnalysisRuleTypeAggregation,
				},
			)
			require.NoError(t, err)

			_, err = client.DeleteConfiguredTableAssociation(
				ctx, &cleanroomssdk.DeleteConfiguredTableAssociationInput{
					MembershipIdentifier:                 aws.String(memID),
					ConfiguredTableAssociationIdentifier: assocID,
				},
			)
			require.NoError(t, err)

			_, err = client.GetConfiguredTableAssociation(
				ctx, &cleanroomssdk.GetConfiguredTableAssociationInput{
					MembershipIdentifier:                 aws.String(memID),
					ConfiguredTableAssociationIdentifier: assocID,
				},
			)
			require.Error(t, err)
		}},
		{name: "intermediate_table_lifecycle", run: func(t *testing.T) {
			t.Helper()

			client := newRoundTripTestClient(t)
			ctx := t.Context()

			_, memID := createCollaborationAndMembership(t, client)

			createOut, err := client.CreateIntermediateTable(ctx, &cleanroomssdk.CreateIntermediateTableInput{
				MembershipIdentifier: aws.String(memID),
				Name:                 aws.String("slice12-intermediate"),
				KmsKeyArn:            aws.String("arn:aws:kms:us-east-1:123456789012:key/it-key"),
				PopulationAnalysisConfiguration: &crtypes.PopulationAnalysisConfigurationMemberSqlParameters{
					Value: crtypes.PopulationAnalysisSqlParameters{
						QueryString: aws.String("SELECT 1"),
					},
				},
				RetentionInDays: aws.Int32(30),
			})
			require.NoError(t, err)
			tableID := createOut.IntermediateTable.Id

			getOut, err := client.GetIntermediateTable(ctx, &cleanroomssdk.GetIntermediateTableInput{
				MembershipIdentifier:        aws.String(memID),
				IntermediateTableIdentifier: tableID,
			})
			require.NoError(t, err)
			assert.Equal(t, "slice12-intermediate", aws.ToString(getOut.IntermediateTable.Name))

			listOut, err := client.ListIntermediateTables(ctx, &cleanroomssdk.ListIntermediateTablesInput{
				MembershipIdentifier: aws.String(memID),
			})
			require.NoError(t, err)
			require.Len(t, listOut.IntermediateTableSummaries, 1)

			versionsOut, err := client.ListIntermediateTableVersions(
				ctx, &cleanroomssdk.ListIntermediateTableVersionsInput{
					MembershipIdentifier:        aws.String(memID),
					IntermediateTableIdentifier: tableID,
				},
			)
			require.NoError(t, err)
			assert.NotNil(
				t, versionsOut.IntermediateTableVersionSummaries,
				"required list member must decode as [] before any version exists, not null",
			)
			assert.Empty(t, versionsOut.IntermediateTableVersionSummaries)

			updOut, err := client.UpdateIntermediateTable(ctx, &cleanroomssdk.UpdateIntermediateTableInput{
				MembershipIdentifier:        aws.String(memID),
				IntermediateTableIdentifier: tableID,
				Description:                 aws.String("updated"),
			})
			require.NoError(t, err)
			assert.Equal(t, "updated", aws.ToString(updOut.IntermediateTable.Description))

			popOut, err := client.PopulateIntermediateTable(ctx, &cleanroomssdk.PopulateIntermediateTableInput{
				MembershipIdentifier:        aws.String(memID),
				IntermediateTableIdentifier: tableID,
			})
			require.NoError(t, err)
			require.NotNil(t, popOut.AnalysisId)

			ruleOut, err := client.CreateIntermediateTableAnalysisRule(
				ctx, &cleanroomssdk.CreateIntermediateTableAnalysisRuleInput{
					MembershipIdentifier:        aws.String(memID),
					IntermediateTableIdentifier: tableID,
					AnalysisRuleType:            crtypes.IntermediateTableAnalysisRuleTypeCustom,
					AnalysisRulePolicy: &crtypes.IntermediateTableAnalysisRulePolicyMemberV1{
						Value: &crtypes.IntermediateTableAnalysisRulePolicyV1MemberCustom{
							Value: crtypes.IntermediateTableAnalysisRuleCustom{
								AllowedAnalyses: []string{"ANY_QUERY"},
							},
						},
					},
				},
			)
			require.NoError(t, err)
			require.NotNil(t, ruleOut.AnalysisRule)

			getRuleOut, err := client.GetIntermediateTableAnalysisRule(
				ctx, &cleanroomssdk.GetIntermediateTableAnalysisRuleInput{
					MembershipIdentifier:        aws.String(memID),
					IntermediateTableIdentifier: tableID,
					AnalysisRuleType:            crtypes.IntermediateTableAnalysisRuleTypeCustom,
				},
			)
			require.NoError(t, err)
			require.NotNil(t, getRuleOut.AnalysisRule)

			updRuleOut, err := client.UpdateIntermediateTableAnalysisRule(
				ctx, &cleanroomssdk.UpdateIntermediateTableAnalysisRuleInput{
					MembershipIdentifier:        aws.String(memID),
					IntermediateTableIdentifier: tableID,
					AnalysisRuleType:            crtypes.IntermediateTableAnalysisRuleTypeCustom,
					AnalysisRulePolicy: &crtypes.IntermediateTableAnalysisRulePolicyMemberV1{
						Value: &crtypes.IntermediateTableAnalysisRulePolicyV1MemberCustom{
							Value: crtypes.IntermediateTableAnalysisRuleCustom{
								AllowedAnalyses:          []string{"ANY_QUERY"},
								AllowedAnalysisProviders: []string{"111111111111"},
							},
						},
					},
				},
			)
			require.NoError(t, err)
			require.NotNil(t, updRuleOut.AnalysisRule)

			_, err = client.DeleteIntermediateTableAnalysisRule(
				ctx, &cleanroomssdk.DeleteIntermediateTableAnalysisRuleInput{
					MembershipIdentifier:        aws.String(memID),
					IntermediateTableIdentifier: tableID,
					AnalysisRuleType:            crtypes.IntermediateTableAnalysisRuleTypeCustom,
				},
			)
			require.NoError(t, err)

			_, err = client.DisallowIntermediateTable(ctx, &cleanroomssdk.DisallowIntermediateTableInput{
				MembershipIdentifier:  aws.String(memID),
				IntermediateTableName: aws.String("slice12-intermediate"),
			})
			require.NoError(t, err)

			_, err = client.DeleteIntermediateTable(ctx, &cleanroomssdk.DeleteIntermediateTableInput{
				MembershipIdentifier:        aws.String(memID),
				IntermediateTableIdentifier: tableID,
			})
			require.NoError(t, err)

			_, err = client.GetIntermediateTable(ctx, &cleanroomssdk.GetIntermediateTableInput{
				MembershipIdentifier:        aws.String(memID),
				IntermediateTableIdentifier: tableID,
			})
			require.Error(t, err)
		}},
		{name: "analysis_template_update_delete", run: func(t *testing.T) {
			t.Helper()

			client := newRoundTripTestClient(t)
			ctx := t.Context()

			_, memID := createCollaborationAndMembership(t, client)

			createOut, err := client.CreateAnalysisTemplate(ctx, &cleanroomssdk.CreateAnalysisTemplateInput{
				MembershipIdentifier: aws.String(memID),
				Name:                 aws.String("slice12-template"),
				Format:               crtypes.AnalysisFormatSql,
				Source:               &crtypes.AnalysisSourceMemberText{Value: "SELECT 1"},
			})
			require.NoError(t, err)
			templateID := createOut.AnalysisTemplate.Id

			updOut, err := client.UpdateAnalysisTemplate(ctx, &cleanroomssdk.UpdateAnalysisTemplateInput{
				MembershipIdentifier:       aws.String(memID),
				AnalysisTemplateIdentifier: templateID,
				Description:                aws.String("updated description"),
			})
			require.NoError(t, err)
			assert.Equal(t, "updated description", aws.ToString(updOut.AnalysisTemplate.Description))

			_, err = client.DeleteAnalysisTemplate(ctx, &cleanroomssdk.DeleteAnalysisTemplateInput{
				MembershipIdentifier:       aws.String(memID),
				AnalysisTemplateIdentifier: templateID,
			})
			require.NoError(t, err)

			_, err = client.GetAnalysisTemplate(ctx, &cleanroomssdk.GetAnalysisTemplateInput{
				MembershipIdentifier:       aws.String(memID),
				AnalysisTemplateIdentifier: templateID,
			})
			require.Error(t, err)
		}},
		{name: "id_mapping_table_lifecycle", run: func(t *testing.T) {
			t.Helper()

			client := newRoundTripTestClient(t)
			ctx := t.Context()

			_, memID := createCollaborationAndMembership(t, client)

			workflowArn := "arn:aws:entityresolution:us-east-1:123456789012:idmappingworkflow/slice12"
			createOut, err := client.CreateIdMappingTable(ctx, &cleanroomssdk.CreateIdMappingTableInput{
				MembershipIdentifier: aws.String(memID),
				Name:                 aws.String("slice12-mapping-table"),
				InputReferenceConfig: &crtypes.IdMappingTableInputReferenceConfig{
					InputReferenceArn:      aws.String(workflowArn),
					ManageResourcePolicies: aws.Bool(true),
				},
			})
			require.NoError(t, err)
			tableID := createOut.IdMappingTable.Id

			getOut, err := client.GetIdMappingTable(ctx, &cleanroomssdk.GetIdMappingTableInput{
				MembershipIdentifier:     aws.String(memID),
				IdMappingTableIdentifier: tableID,
			})
			require.NoError(t, err)
			assert.Equal(t, "slice12-mapping-table", aws.ToString(getOut.IdMappingTable.Name))

			updOut, err := client.UpdateIdMappingTable(ctx, &cleanroomssdk.UpdateIdMappingTableInput{
				MembershipIdentifier:     aws.String(memID),
				IdMappingTableIdentifier: tableID,
				Description:              aws.String("updated"),
			})
			require.NoError(t, err)
			assert.Equal(t, "updated", aws.ToString(updOut.IdMappingTable.Description))

			_, err = client.DeleteIdMappingTable(ctx, &cleanroomssdk.DeleteIdMappingTableInput{
				MembershipIdentifier:     aws.String(memID),
				IdMappingTableIdentifier: tableID,
			})
			require.NoError(t, err)

			_, err = client.GetIdMappingTable(ctx, &cleanroomssdk.GetIdMappingTableInput{
				MembershipIdentifier:     aws.String(memID),
				IdMappingTableIdentifier: tableID,
			})
			require.Error(t, err)
		}},
		{name: "id_namespace_association_lifecycle", run: func(t *testing.T) {
			t.Helper()

			client := newRoundTripTestClient(t)
			ctx := t.Context()

			_, memID := createCollaborationAndMembership(t, client)

			createOut, err := client.CreateIdNamespaceAssociation(
				ctx, &cleanroomssdk.CreateIdNamespaceAssociationInput{
					MembershipIdentifier: aws.String(memID),
					Name:                 aws.String("slice12-ns"),
					InputReferenceConfig: &crtypes.IdNamespaceAssociationInputReferenceConfig{
						InputReferenceArn: aws.String(
							"arn:aws:cleanrooms:us-east-1:123456789012:membership/" + memID,
						),
						ManageResourcePolicies: aws.Bool(true),
					},
				},
			)
			require.NoError(t, err)
			nsID := createOut.IdNamespaceAssociation.Id

			updOut, err := client.UpdateIdNamespaceAssociation(
				ctx, &cleanroomssdk.UpdateIdNamespaceAssociationInput{
					MembershipIdentifier:             aws.String(memID),
					IdNamespaceAssociationIdentifier: nsID,
					Description:                      aws.String("updated"),
				},
			)
			require.NoError(t, err)
			assert.Equal(t, "updated", aws.ToString(updOut.IdNamespaceAssociation.Description))

			_, err = client.DeleteIdNamespaceAssociation(
				ctx, &cleanroomssdk.DeleteIdNamespaceAssociationInput{
					MembershipIdentifier:             aws.String(memID),
					IdNamespaceAssociationIdentifier: nsID,
				},
			)
			require.NoError(t, err)

			_, err = client.GetIdNamespaceAssociation(ctx, &cleanroomssdk.GetIdNamespaceAssociationInput{
				MembershipIdentifier:             aws.String(memID),
				IdNamespaceAssociationIdentifier: nsID,
			})
			require.Error(t, err)
		}},
		{name: "configured_audience_model_association_lifecycle", run: func(t *testing.T) {
			t.Helper()

			client := newRoundTripTestClient(t)
			ctx := t.Context()

			_, memID := createCollaborationAndMembership(t, client)

			camaArn := "arn:aws:cleanrooms-ml::123456789012:configured-audience-model/slice12"
			createOut, err := client.CreateConfiguredAudienceModelAssociation(
				ctx, &cleanroomssdk.CreateConfiguredAudienceModelAssociationInput{
					MembershipIdentifier:                   aws.String(memID),
					ConfiguredAudienceModelArn:             aws.String(camaArn),
					ConfiguredAudienceModelAssociationName: aws.String("slice12-cama"),
					ManageResourcePolicies:                 aws.Bool(true),
				},
			)
			require.NoError(t, err)
			camaID := createOut.ConfiguredAudienceModelAssociation.Id

			updOut, err := client.UpdateConfiguredAudienceModelAssociation(
				ctx, &cleanroomssdk.UpdateConfiguredAudienceModelAssociationInput{
					MembershipIdentifier:                         aws.String(memID),
					ConfiguredAudienceModelAssociationIdentifier: camaID,
					Description: aws.String("updated"),
				},
			)
			require.NoError(t, err)
			assert.Equal(t, "updated", aws.ToString(updOut.ConfiguredAudienceModelAssociation.Description))

			_, err = client.DeleteConfiguredAudienceModelAssociation(
				ctx, &cleanroomssdk.DeleteConfiguredAudienceModelAssociationInput{
					MembershipIdentifier:                         aws.String(memID),
					ConfiguredAudienceModelAssociationIdentifier: camaID,
				},
			)
			require.NoError(t, err)
		}},
		{name: "membership_and_member_lifecycle", run: func(t *testing.T) {
			t.Helper()

			client := newRoundTripTestClient(t)
			ctx := t.Context()

			collabID, memID := createCollaborationAndMembership(t, client)

			updOut, err := client.UpdateMembership(ctx, &cleanroomssdk.UpdateMembershipInput{
				MembershipIdentifier: aws.String(memID),
				QueryLogStatus:       crtypes.MembershipQueryLogStatusEnabled,
			})
			require.NoError(t, err)
			assert.Equal(t, crtypes.MembershipQueryLogStatusEnabled, updOut.Membership.QueryLogStatus)

			_, err = client.DeleteMember(ctx, &cleanroomssdk.DeleteMemberInput{
				CollaborationIdentifier: aws.String(collabID),
				AccountId:               aws.String("999999999999"),
			})
			require.Error(t, err, "deleting a member not in the collaboration must fail, not silently no-op")

			_, err = client.DeleteMembership(ctx, &cleanroomssdk.DeleteMembershipInput{
				MembershipIdentifier: aws.String(memID),
			})
			require.NoError(t, err)

			_, err = client.GetMembership(ctx, &cleanroomssdk.GetMembershipInput{
				MembershipIdentifier: aws.String(memID),
			})
			require.Error(t, err)
		}},
		{name: "privacy_budget_lifecycle", run: func(t *testing.T) {
			t.Helper()

			client := newRoundTripTestClient(t)
			ctx := t.Context()

			_, memID := createCollaborationAndMembership(t, client)

			createOut, err := client.CreatePrivacyBudgetTemplate(ctx, &cleanroomssdk.CreatePrivacyBudgetTemplateInput{
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
			require.NoError(t, err)
			tmplID := createOut.PrivacyBudgetTemplate.Id

			updOut, err := client.UpdatePrivacyBudgetTemplate(ctx, &cleanroomssdk.UpdatePrivacyBudgetTemplateInput{
				MembershipIdentifier:            aws.String(memID),
				PrivacyBudgetTemplateIdentifier: tmplID,
				PrivacyBudgetType:               crtypes.PrivacyBudgetTypeDifferentialPrivacy,
				Parameters: &crtypes.PrivacyBudgetTemplateUpdateParametersMemberDifferentialPrivacy{
					Value: crtypes.DifferentialPrivacyTemplateUpdateParameters{
						Epsilon: aws.Int32(12),
					},
				},
			})
			require.NoError(t, err)
			require.NotNil(t, updOut.PrivacyBudgetTemplate)

			listTmplOut, err := client.ListPrivacyBudgetTemplates(ctx, &cleanroomssdk.ListPrivacyBudgetTemplatesInput{
				MembershipIdentifier: aws.String(memID),
			})
			require.NoError(t, err)
			require.Len(t, listTmplOut.PrivacyBudgetTemplateSummaries, 1)

			listBudgetsOut, err := client.ListPrivacyBudgets(ctx, &cleanroomssdk.ListPrivacyBudgetsInput{
				MembershipIdentifier: aws.String(memID),
				PrivacyBudgetType:    crtypes.PrivacyBudgetTypeDifferentialPrivacy,
			})
			require.NoError(t, err)
			assert.NotNil(t, listBudgetsOut.PrivacyBudgetSummaries)

			previewOut, err := client.PreviewPrivacyImpact(ctx, &cleanroomssdk.PreviewPrivacyImpactInput{
				MembershipIdentifier: aws.String(memID),
				Parameters: &crtypes.PreviewPrivacyImpactParametersInputMemberDifferentialPrivacy{
					Value: crtypes.DifferentialPrivacyPreviewParametersInput{
						Epsilon:            aws.Int32(8),
						UsersNoisePerQuery: aws.Int32(50),
					},
				},
			})
			require.NoError(t, err)
			require.NotNil(t, previewOut.PrivacyImpact)

			_, err = client.DeletePrivacyBudgetTemplate(ctx, &cleanroomssdk.DeletePrivacyBudgetTemplateInput{
				MembershipIdentifier:            aws.String(memID),
				PrivacyBudgetTemplateIdentifier: tmplID,
			})
			require.NoError(t, err)

			_, err = client.GetPrivacyBudgetTemplate(ctx, &cleanroomssdk.GetPrivacyBudgetTemplateInput{
				MembershipIdentifier:            aws.String(memID),
				PrivacyBudgetTemplateIdentifier: tmplID,
			})
			require.Error(t, err)
		}},
		{name: "protected_job_and_query_lifecycle", run: func(t *testing.T) {
			t.Helper()

			client := newRoundTripTestClient(t)
			ctx := t.Context()

			_, memID := createCollaborationAndMembership(t, client)

			jobOut, err := client.StartProtectedJob(ctx, &cleanroomssdk.StartProtectedJobInput{
				MembershipIdentifier: aws.String(memID),
				Type:                 crtypes.ProtectedJobTypePyspark,
				JobParameters: &crtypes.ProtectedJobParameters{
					AnalysisTemplateArn: aws.String(
						"arn:aws:cleanrooms:us-east-1:123456789012:membership/" + memID + "/analysistemplate/slice12",
					),
				},
			})
			require.NoError(t, err)
			jobID := jobOut.ProtectedJob.Id

			updJobOut, err := client.UpdateProtectedJob(ctx, &cleanroomssdk.UpdateProtectedJobInput{
				MembershipIdentifier:   aws.String(memID),
				ProtectedJobIdentifier: jobID,
				TargetStatus:           crtypes.TargetProtectedJobStatusCancelled,
			})
			require.NoError(t, err)
			assert.Equal(t, crtypes.ProtectedJobStatusCancelled, updJobOut.ProtectedJob.Status)

			getJobOut, err := client.GetProtectedJob(ctx, &cleanroomssdk.GetProtectedJobInput{
				MembershipIdentifier:   aws.String(memID),
				ProtectedJobIdentifier: jobID,
			})
			require.NoError(t, err)
			assert.Equal(t, crtypes.ProtectedJobStatusCancelled, getJobOut.ProtectedJob.Status)

			listJobsOut, err := client.ListProtectedJobs(ctx, &cleanroomssdk.ListProtectedJobsInput{
				MembershipIdentifier: aws.String(memID),
			})
			require.NoError(t, err)
			require.Len(t, listJobsOut.ProtectedJobs, 1)

			queryOut, err := client.StartProtectedQuery(ctx, &cleanroomssdk.StartProtectedQueryInput{
				MembershipIdentifier: aws.String(memID),
				Type:                 crtypes.ProtectedQueryTypeSql,
				SqlParameters: &crtypes.ProtectedQuerySQLParameters{
					QueryString: aws.String("SELECT 1"),
				},
			})
			require.NoError(t, err)
			queryID := queryOut.ProtectedQuery.Id

			updQueryOut, err := client.UpdateProtectedQuery(ctx, &cleanroomssdk.UpdateProtectedQueryInput{
				MembershipIdentifier:     aws.String(memID),
				ProtectedQueryIdentifier: queryID,
				TargetStatus:             crtypes.TargetProtectedQueryStatusCancelled,
			})
			require.NoError(t, err)
			assert.Equal(t, crtypes.ProtectedQueryStatusCancelled, updQueryOut.ProtectedQuery.Status)

			getQueryOut, err := client.GetProtectedQuery(ctx, &cleanroomssdk.GetProtectedQueryInput{
				MembershipIdentifier:     aws.String(memID),
				ProtectedQueryIdentifier: queryID,
			})
			require.NoError(t, err)
			assert.Equal(t, crtypes.ProtectedQueryStatusCancelled, getQueryOut.ProtectedQuery.Status)

			listQueriesOut, err := client.ListProtectedQueries(ctx, &cleanroomssdk.ListProtectedQueriesInput{
				MembershipIdentifier: aws.String(memID),
			})
			require.NoError(t, err)
			require.Len(t, listQueriesOut.ProtectedQueries, 1)
		}},
		{name: "collaboration_change_request_lifecycle", run: func(t *testing.T) {
			t.Helper()

			client := newRoundTripTestClient(t)
			ctx := t.Context()

			collabID, _ := createCollaborationAndMembership(t, client)

			createOut, err := client.CreateCollaborationChangeRequest(
				ctx, &cleanroomssdk.CreateCollaborationChangeRequestInput{
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
				},
			)
			require.NoError(t, err)
			changeRequestID := createOut.CollaborationChangeRequest.Id

			getOut, err := client.GetCollaborationChangeRequest(
				ctx, &cleanroomssdk.GetCollaborationChangeRequestInput{
					CollaborationIdentifier: aws.String(collabID),
					ChangeRequestIdentifier: changeRequestID,
				},
			)
			require.NoError(t, err)
			assert.Equal(t, crtypes.ChangeRequestStatusPending, getOut.CollaborationChangeRequest.Status)

			updOut, err := client.UpdateCollaborationChangeRequest(
				ctx, &cleanroomssdk.UpdateCollaborationChangeRequestInput{
					CollaborationIdentifier: aws.String(collabID),
					ChangeRequestIdentifier: changeRequestID,
					Action:                  crtypes.ChangeRequestActionApprove,
				},
			)
			require.NoError(t, err)
			assert.Equal(t, crtypes.ChangeRequestStatusApproved, updOut.CollaborationChangeRequest.Status)
		}}}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.run(t)
		})
	}
}

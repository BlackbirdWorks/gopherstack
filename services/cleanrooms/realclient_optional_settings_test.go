package cleanrooms_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	cleanroomssdk "github.com/aws/aws-sdk-go-v2/service/cleanrooms"
	crtypes "github.com/aws/aws-sdk-go-v2/service/cleanrooms/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRealClient_CollaborationOptionalSettings(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		engine  crtypes.AnalyticsEngine
		regions []crtypes.SupportedS3Region
		wantErr bool
	}{
		{
			name:    "round_trip",
			engine:  crtypes.AnalyticsEngineSpark,
			regions: []crtypes.SupportedS3Region{crtypes.SupportedS3RegionUsEast1},
		},
		{name: "bad_engine", engine: "BOGUS", wantErr: true},
		{name: "bad_region", regions: []crtypes.SupportedS3Region{"mars-1"}, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newRoundTripTestClient(t)
			ctx := t.Context()
			out, err := client.CreateCollaboration(ctx, &cleanroomssdk.CreateCollaborationInput{
				Name:                   aws.String("c"),
				Description:            aws.String("d"),
				CreatorDisplayName:     aws.String("creator"),
				CreatorMemberAbilities: []crtypes.MemberAbility{crtypes.MemberAbilityCanQuery},
				Members:                []crtypes.MemberSpecification{},
				QueryLogStatus:         crtypes.CollaborationQueryLogStatusDisabled,
				AnalyticsEngine:        tt.engine,
				AllowedResultRegions:   tt.regions,
				DataEncryptionMetadata: &crtypes.DataEncryptionMetadata{
					AllowCleartext:                        aws.Bool(true),
					AllowDuplicates:                       aws.Bool(false),
					AllowJoinsOnColumnsWithDifferentNames: aws.Bool(true),
					PreserveNulls:                         aws.Bool(false),
				},
			})
			if tt.wantErr {
				require.Error(t, err)

				return
			}
			require.NoError(t, err)
			id := out.Collaboration.Id

			got, err := client.GetCollaboration(ctx, &cleanroomssdk.GetCollaborationInput{CollaborationIdentifier: id})
			require.NoError(t, err)
			assert.Equal(t, tt.engine, got.Collaboration.AnalyticsEngine)
			assert.Equal(t, tt.regions, got.Collaboration.AllowedResultRegions)
			require.NotNil(t, got.Collaboration.DataEncryptionMetadata)
			assert.True(t, aws.ToBool(got.Collaboration.DataEncryptionMetadata.AllowCleartext))
			assert.False(t, aws.ToBool(got.Collaboration.DataEncryptionMetadata.AllowDuplicates))
			assert.True(t, aws.ToBool(got.Collaboration.DataEncryptionMetadata.AllowJoinsOnColumnsWithDifferentNames))
			assert.False(t, aws.ToBool(got.Collaboration.DataEncryptionMetadata.PreserveNulls))

			upd, err := client.UpdateCollaboration(ctx, &cleanroomssdk.UpdateCollaborationInput{
				CollaborationIdentifier: id,
				AnalyticsEngine:         crtypes.AnalyticsEngineCleanRoomsSql,
			})
			require.NoError(t, err)
			assert.Equal(t, crtypes.AnalyticsEngineCleanRoomsSql, upd.Collaboration.AnalyticsEngine)

			_, err = client.UpdateCollaboration(ctx, &cleanroomssdk.UpdateCollaborationInput{
				CollaborationIdentifier: id,
				AnalyticsEngine:         "BOGUS",
			})
			require.Error(t, err)

			list, err := client.ListCollaborations(ctx, &cleanroomssdk.ListCollaborationsInput{})
			require.NoError(t, err)
			require.Len(t, list.CollaborationList, 1)
			assert.Equal(t, crtypes.AnalyticsEngineCleanRoomsSql, list.CollaborationList[0].AnalyticsEngine)
		})
	}
}

func TestRealClient_ConfiguredTableSelectedAnalysisMethods(t *testing.T) {
	t.Parallel()

	both := []crtypes.SelectedAnalysisMethod{
		crtypes.SelectedAnalysisMethodDirectQuery, crtypes.SelectedAnalysisMethodDirectJob,
	}
	tests := []struct {
		name     string
		selected []crtypes.SelectedAnalysisMethod
		wantErr  bool
	}{
		{name: "round_trip", selected: both},
		{name: "bad_method", selected: []crtypes.SelectedAnalysisMethod{"BOGUS"}, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newRoundTripTestClient(t)
			ctx := t.Context()
			out, err := client.CreateConfiguredTable(ctx, &cleanroomssdk.CreateConfiguredTableInput{
				Name: aws.String("t"),
				TableReference: &crtypes.TableReferenceMemberGlue{
					Value: crtypes.GlueTableReference{DatabaseName: aws.String("db"), TableName: aws.String("tbl")},
				},
				AllowedColumns:          []string{"a"},
				AnalysisMethod:          crtypes.AnalysisMethodMultiple,
				SelectedAnalysisMethods: tt.selected,
			})
			if tt.wantErr {
				require.Error(t, err)

				return
			}
			require.NoError(t, err)
			id := out.ConfiguredTable.Id

			got, err := client.GetConfiguredTable(
				ctx,
				&cleanroomssdk.GetConfiguredTableInput{ConfiguredTableIdentifier: id},
			)
			require.NoError(t, err)
			assert.Equal(t, both, got.ConfiguredTable.SelectedAnalysisMethods)

			list, err := client.ListConfiguredTables(ctx, &cleanroomssdk.ListConfiguredTablesInput{})
			require.NoError(t, err)
			require.Len(t, list.ConfiguredTableSummaries, 1)
			assert.Equal(t, both, list.ConfiguredTableSummaries[0].SelectedAnalysisMethods)

			upd, err := client.UpdateConfiguredTable(ctx, &cleanroomssdk.UpdateConfiguredTableInput{
				ConfiguredTableIdentifier: id,
				SelectedAnalysisMethods:   []crtypes.SelectedAnalysisMethod{crtypes.SelectedAnalysisMethodDirectJob},
			})
			require.NoError(t, err)
			assert.Equal(t,
				[]crtypes.SelectedAnalysisMethod{crtypes.SelectedAnalysisMethodDirectJob},
				upd.ConfiguredTable.SelectedAnalysisMethods)

			_, err = client.UpdateConfiguredTable(ctx, &cleanroomssdk.UpdateConfiguredTableInput{
				ConfiguredTableIdentifier: id,
				SelectedAnalysisMethods:   []crtypes.SelectedAnalysisMethod{"BOGUS"},
			})
			require.Error(t, err)
		})
	}
}

func TestRealClient_AnalysisTemplateErrorMessageConfiguration(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		typ     crtypes.ErrorMessageType
		wantErr bool
	}{
		{name: "detailed", typ: crtypes.ErrorMessageTypeDetailed},
		{name: "bad_type", typ: "BOGUS", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newRoundTripTestClient(t)
			ctx := t.Context()
			collabID, memID := createCollaborationAndMembership(t, client)
			out, err := client.CreateAnalysisTemplate(ctx, &cleanroomssdk.CreateAnalysisTemplateInput{
				MembershipIdentifier:      aws.String(memID),
				Name:                      aws.String("tmpl"),
				Format:                    crtypes.AnalysisFormatSql,
				Source:                    &crtypes.AnalysisSourceMemberText{Value: "SELECT 1"},
				ErrorMessageConfiguration: &crtypes.ErrorMessageConfiguration{Type: tt.typ},
			})
			if tt.wantErr {
				require.Error(t, err)

				return
			}
			require.NoError(t, err)
			require.NotNil(t, out.AnalysisTemplate.ErrorMessageConfiguration)
			assert.Equal(t, tt.typ, out.AnalysisTemplate.ErrorMessageConfiguration.Type)

			got, err := client.GetAnalysisTemplate(ctx, &cleanroomssdk.GetAnalysisTemplateInput{
				MembershipIdentifier:       aws.String(memID),
				AnalysisTemplateIdentifier: out.AnalysisTemplate.Id,
			})
			require.NoError(t, err)
			require.NotNil(t, got.AnalysisTemplate.ErrorMessageConfiguration)
			assert.Equal(t, tt.typ, got.AnalysisTemplate.ErrorMessageConfiguration.Type)

			cgot, err := client.GetCollaborationAnalysisTemplate(
				ctx,
				&cleanroomssdk.GetCollaborationAnalysisTemplateInput{
					CollaborationIdentifier: aws.String(collabID),
					AnalysisTemplateArn:     out.AnalysisTemplate.Arn,
				},
			)
			require.NoError(t, err)
			require.NotNil(t, cgot.CollaborationAnalysisTemplate.ErrorMessageConfiguration)
			assert.Equal(t, tt.typ, cgot.CollaborationAnalysisTemplate.ErrorMessageConfiguration.Type)
		})
	}
}

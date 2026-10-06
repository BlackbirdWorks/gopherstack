package cleanrooms_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	cleanroomssdk "github.com/aws/aws-sdk-go-v2/service/cleanrooms"
	crtypes "github.com/aws/aws-sdk-go-v2/service/cleanrooms/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRealClient_AnalysisTemplateSyntheticDataAndSchema(t *testing.T) {
	t.Parallel()

	ml := func(eps *float64) crtypes.SyntheticDataParameters {
		return &crtypes.SyntheticDataParametersMemberMlSyntheticDataParameters{
			Value: crtypes.MLSyntheticDataParameters{
				Epsilon:                           eps,
				MaxMembershipInferenceAttackScore: aws.Float64(0.5),
				ColumnClassification: &crtypes.ColumnClassificationDetails{
					ColumnMapping: []crtypes.SyntheticDataColumnProperties{{
						ColumnName:        aws.String("age"),
						ColumnType:        crtypes.SyntheticDataColumnTypeNumerical,
						IsPredictiveValue: aws.Bool(true),
					}},
				},
			},
		}
	}

	tests := []struct {
		synthetic crtypes.SyntheticDataParameters
		name      string
		tables    []string
		wantErr   bool
	}{
		{name: "synthetic and schema", synthetic: ml(aws.Float64(1.5)), tables: []string{"t1", "t2"}},
		{name: "neither"},
		{name: "missing epsilon", synthetic: ml(nil), wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newRoundTripTestClient(t)
			ctx := t.Context()
			collabID, memID := createCollaborationAndMembership(t, client)

			in := &cleanroomssdk.CreateAnalysisTemplateInput{
				MembershipIdentifier:    aws.String(memID),
				Name:                    aws.String("tmpl"),
				Format:                  crtypes.AnalysisFormatSql,
				Source:                  &crtypes.AnalysisSourceMemberText{Value: "SELECT 1"},
				SyntheticDataParameters: tt.synthetic,
			}
			if tt.tables != nil {
				in.Schema = &crtypes.AnalysisSchema{ReferencedTables: tt.tables}
			}

			out, err := client.CreateAnalysisTemplate(ctx, in)
			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)

			got, err := client.GetAnalysisTemplate(ctx, &cleanroomssdk.GetAnalysisTemplateInput{
				MembershipIdentifier:       aws.String(memID),
				AnalysisTemplateIdentifier: out.AnalysisTemplate.Id,
			})
			require.NoError(t, err)

			if tt.tables != nil {
				require.NotNil(t, got.AnalysisTemplate.Schema)
				assert.Equal(t, tt.tables, got.AnalysisTemplate.Schema.ReferencedTables)
			}

			if tt.synthetic == nil {
				assert.Nil(t, got.AnalysisTemplate.SyntheticDataParameters)
			} else {
				member, ok := got.AnalysisTemplate.SyntheticDataParameters.(*crtypes.
					SyntheticDataParametersMemberMlSyntheticDataParameters)
				require.True(t, ok)
				assert.InDelta(t, 1.5, aws.ToFloat64(member.Value.Epsilon), 0)
				require.Len(t, member.Value.ColumnClassification.ColumnMapping, 1)
				assert.Equal(t, "age", aws.ToString(member.Value.ColumnClassification.ColumnMapping[0].ColumnName))
			}

			list, err := client.ListAnalysisTemplates(ctx, &cleanroomssdk.ListAnalysisTemplatesInput{
				MembershipIdentifier: aws.String(memID),
			})
			require.NoError(t, err)
			require.Len(t, list.AnalysisTemplateSummaries, 1)
			assert.Equal(t, tt.synthetic != nil, aws.ToBool(list.AnalysisTemplateSummaries[0].IsSyntheticData))

			clist, err := client.ListCollaborationAnalysisTemplates(
				ctx,
				&cleanroomssdk.ListCollaborationAnalysisTemplatesInput{CollaborationIdentifier: aws.String(collabID)},
			)
			require.NoError(t, err)
			require.Len(t, clist.CollaborationAnalysisTemplateSummaries, 1)
			assert.Equal(
				t,
				tt.synthetic != nil,
				aws.ToBool(clist.CollaborationAnalysisTemplateSummaries[0].IsSyntheticData),
			)
		})
	}
}

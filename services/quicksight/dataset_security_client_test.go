package quicksight_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	quicksightsdk "github.com/aws/aws-sdk-go-v2/service/quicksight"
	"github.com/aws/aws-sdk-go-v2/service/quicksight/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func securityTestTables() map[string]types.PhysicalTable {
	return map[string]types.PhysicalTable{
		"pt1": &types.PhysicalTableMemberRelationalTable{
			Value: types.RelationalTable{
				DataSourceArn: aws.String("arn:aws:quicksight:us-east-1:000000000000:datasource/sec-src"),
				Name:          aws.String("orders"),
				InputColumns:  []types.InputColumn{{Name: aws.String("id"), Type: types.InputColumnDataTypeInteger}},
			},
		},
	}
}

func TestDataSetSecurity_RoundTrip(t *testing.T) {
	t.Parallel()

	rlsArn := "arn:aws:quicksight:us-east-1:000000000000:dataset/rules"

	tests := []struct {
		name         string
		wantUseAs    types.DataSetUseAs
		input        quicksightsdk.CreateDataSetInput
		wantColRules int
		wantRLS      bool
		wantTag      bool
		wantErr      bool
	}{
		{
			name:  "none",
			input: quicksightsdk.CreateDataSetInput{},
		},
		{
			name: "rls and column rules and tags",
			input: quicksightsdk.CreateDataSetInput{
				RowLevelPermissionDataSet: &types.RowLevelPermissionDataSet{ //nolint:staticcheck // deprecated
					Arn:              aws.String(rlsArn),
					PermissionPolicy: types.RowLevelPermissionPolicyGrantAccess,
					FormatVersion:    types.RowLevelPermissionFormatVersionVersion1,
				},
				RowLevelPermissionTagConfiguration: &types.RowLevelPermissionTagConfiguration{ //nolint:staticcheck // deprecated
					TagRules: []types.RowLevelPermissionTagRule{
						{TagKey: aws.String("dept"), ColumnName: aws.String("id")},
					},
				},
				ColumnLevelPermissionRules: []types.ColumnLevelPermissionRule{
					{
						ColumnNames: []string{"id"},
						Principals:  []string{"arn:aws:quicksight:us-east-1:000000000000:user/default/a"},
					},
				},
			},
			wantRLS:      true,
			wantTag:      true,
			wantColRules: 1,
		},
		{
			name:      "use as rls rules",
			input:     quicksightsdk.CreateDataSetInput{UseAs: types.DataSetUseAsRlsRules},
			wantUseAs: types.DataSetUseAsRlsRules,
		},
		{
			name: "rls missing arn rejected",
			input: quicksightsdk.CreateDataSetInput{
				RowLevelPermissionDataSet: &types.RowLevelPermissionDataSet{ //nolint:staticcheck // deprecated
					PermissionPolicy: types.RowLevelPermissionPolicyGrantAccess,
				},
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newQuickSightTestClient(t)
			ctx := t.Context()

			in := tt.input
			in.AwsAccountId = aws.String(qsTestAccountID)
			in.DataSetId = aws.String("sec-ds")
			in.Name = aws.String("sec-ds")
			in.ImportMode = types.DataSetImportModeDirectQuery
			in.PhysicalTableMap = securityTestTables()

			_, err := client.CreateDataSet(ctx, &in)
			if tt.wantErr {
				require.Error(t, err)

				return
			}
			require.NoError(t, err)

			desc, err := client.DescribeDataSet(ctx, &quicksightsdk.DescribeDataSetInput{
				AwsAccountId: aws.String(qsTestAccountID), DataSetId: aws.String("sec-ds"),
			})
			require.NoError(t, err)
			assert.Equal(t, tt.wantRLS, desc.DataSet.RowLevelPermissionDataSet != nil)
			assert.Equal(t, tt.wantTag, desc.DataSet.RowLevelPermissionTagConfiguration != nil)
			assert.Len(t, desc.DataSet.ColumnLevelPermissionRules, tt.wantColRules)
			assert.Equal(t, tt.wantUseAs, desc.DataSet.UseAs)

			list, err := client.ListDataSets(ctx, &quicksightsdk.ListDataSetsInput{
				AwsAccountId: aws.String(qsTestAccountID),
			})
			require.NoError(t, err)
			require.Len(t, list.DataSetSummaries, 1)

			sum := list.DataSetSummaries[0]
			assert.Equal(t, tt.wantColRules > 0, sum.ColumnLevelPermissionRulesApplied)
			assert.Equal(t, tt.wantTag, sum.RowLevelPermissionTagConfigurationApplied)
			assert.Equal(t, tt.wantUseAs, sum.UseAs)

			if tt.wantRLS {
				require.NotNil(t, sum.RowLevelPermissionDataSet)
				assert.Equal(t, rlsArn, aws.ToString(sum.RowLevelPermissionDataSet.Arn))
				assert.Equal(
					t,
					types.RowLevelPermissionPolicyGrantAccess,
					sum.RowLevelPermissionDataSet.PermissionPolicy,
				)
			}
		})
	}
}

func TestDataSetSecurity_UpdateReplaces(t *testing.T) {
	t.Parallel()

	client := newQuickSightTestClient(t)
	ctx := t.Context()

	_, err := client.CreateDataSet(ctx, &quicksightsdk.CreateDataSetInput{
		AwsAccountId:     aws.String(qsTestAccountID),
		DataSetId:        aws.String("sec-upd"),
		Name:             aws.String("sec-upd"),
		ImportMode:       types.DataSetImportModeDirectQuery,
		PhysicalTableMap: securityTestTables(),
		UseAs:            types.DataSetUseAsRlsRules,
		ColumnLevelPermissionRules: []types.ColumnLevelPermissionRule{
			{ColumnNames: []string{"id"}, Principals: []string{"p"}},
		},
	})
	require.NoError(t, err)

	_, err = client.UpdateDataSet(ctx, &quicksightsdk.UpdateDataSetInput{
		AwsAccountId:     aws.String(qsTestAccountID),
		DataSetId:        aws.String("sec-upd"),
		Name:             aws.String("sec-upd"),
		ImportMode:       types.DataSetImportModeDirectQuery,
		PhysicalTableMap: securityTestTables(),
	})
	require.NoError(t, err)

	desc, err := client.DescribeDataSet(ctx, &quicksightsdk.DescribeDataSetInput{
		AwsAccountId: aws.String(qsTestAccountID), DataSetId: aws.String("sec-upd"),
	})
	require.NoError(t, err)
	assert.Empty(t, desc.DataSet.ColumnLevelPermissionRules)
	assert.Equal(t, types.DataSetUseAsRlsRules, desc.DataSet.UseAs)
}

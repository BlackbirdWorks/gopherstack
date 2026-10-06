package glue_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	gluesdk "github.com/aws/aws-sdk-go-v2/service/glue"
	"github.com/aws/aws-sdk-go-v2/service/glue/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/glue"
)

func dqdlValid() map[string]string {
	return map[string]string{
		"basic":             `Rules = [ IsComplete "order-id", IsUnique "order-id" ]`,
		"multiline_comment": "Rules = [\n  # note\n  RowCount > 0,\n  ColumnExists \"a\"\n]",
		"between":           `Rules = [ Mean "colA" between 80 and 100 ]`,
		"not_between":       `Rules = [ ColumnValues "d" not between "2022-05-31" and "2022-06-30" ]`,
		"in_list":           `Rules = [ ColumnValues "Country" in [ "US", "CA", NULL, EMPTY, WHITESPACES_ONLY ] ]`,
		"not_in_threshold":  `Rules = [ ColumnValues "colA" not in ["A", "B"] with threshold > 0.8 ]`,
		"matches":           `Rules = [ ColumnValues "First_Name" matches "[a-zA-Z]*" ]`,
		"matches_regex":     `Rules = [ ColumnValues "n" matches /ab+c/i ]`,
		"composite":         `Rules = [ (IsComplete "id") and (IsUnique "id") ]`,
		"nested_composite":  `Rules = [ (Mean "a" > 3) and ((Mean "b" > 500) or (IsComplete "c")) ]`,
		"where":             `Rules = [ Completeness "colA" > 0.5 where "colB = 10" ]`,
		"where_threshold":   `Rules = [ ColumnValues "colB" in ["A", "B"] where "colC is not null" with threshold > 0.9 ]`,
		"freshness": `Rules = [ DataFreshness "Order_Date" <= 24 hours, ` +
			`DataFreshness "d" between 2 days and 5 days ]`,
		"dynamic": `Rules = [ RowCount > avg(last(10)) * 0.8, ` +
			`DistinctValuesCount "c" between min(last(10))-1 and max(last(10))+1 ]`,
		"now":         `Rules = [ ColumnValues "load_date" > (now() - 3 days) ]`,
		"custom_sql":  `Rules = [ CustomSql "select count(*) from primary" between 10 and 20 ]`,
		"referential": `Rules = [ ReferentialIntegrity "zipcode" "reference.zipcode" >= 0.9 ]`,
		"correlation": `Rules = [ ColumnCorrelation "height" "weight" > 0.8 ]`,
		"datatype":    `Rules = [ ColumnDataType "colA" = "INTEGER" with threshold > 0.9 ]`,
		"primary_key": `Rules = [ IsPrimaryKey colA "colB" "colC" ]`,
		"constants":   "mySql = \"select count(*) from primary\"\nRules = [ CustomSql $mySql between 0 and 100 ]",
		"labels": `DefaultLabels=["frequency"="monthly"] ` +
			`Rules = [ RowCount > 0 with threshold > 0.8 labels=["foo"="bar"] ]`,
		"analyzers": `Rules = [ RowCount > avg(last(3)) ] ` +
			`Analyzers = [ DistinctValuesCount "Name", Distribution "Age" with bins = 10 ]`,
		"file_rules":  `Rules = [ FileSize "s3://b/" > 2 MB, FileFreshness "s3://b/" > (now() - 24 hours) ]`,
		"empty_rules": `Rules = []`,
	}
}

func dqdlInvalid() map[string]string {
	return map[string]string{
		"no_rules_list":         `IsComplete "id"`,
		"lowercase_rules":       `rules = [ IsComplete "id" ]`,
		"unterminated_list":     `Rules = [ IsComplete "id"`,
		"missing_comma":         `Rules = [ IsComplete "a" IsUnique "a" ]`,
		"unknown_rule_type":     `Rules = [ IsAwesome "id" ]`,
		"wrong_case_rule":       `Rules = [ iscomplete "id" ]`,
		"missing_column":        `Rules = [ IsComplete ]`,
		"extra_column":          `Rules = [ IsComplete "a" "b" ]`,
		"expression_on_boolean": `Rules = [ IsComplete "a" > 1 ]`,
		"missing_expression":    `Rules = [ Mean "a" ]`,
		"dangling_operator":     `Rules = [ RowCount > ]`,
		"between_missing_and":   `Rules = [ RowCount between 1 10 ]`,
		"empty_in_list":         `Rules = [ ColumnValues "a" in [] ]`,
		"unterminated_string":   `Rules = [ IsComplete "a ]`,
		"bare_composite":        `Rules = [ IsComplete "a" and IsUnique "a" ]`,
		"unbalanced_paren":      `Rules = [ (IsComplete "a" ]`,
		"unbalanced_where":      `Rules = [ IsComplete "a" where "(b = 1" ]`,
		"threshold_no_expr":     `Rules = [ IsComplete "a" with threshold > 0.5 ]`,
		"last_without_agg":      `Rules = [ RowCount > last(5) ]`,
		"undefined_constant":    `Rules = [ CustomSql $nope > 1 ]`,
		"label_on_group_member": `Rules = [ (IsComplete "a" labels=["k"="v"]) and (RowCount > 0) ]`,
		"label_after_composite": `Rules = [ (IsComplete "a") and (RowCount > 0) labels=["k"="v"] ]`,
		"too_many_labels": `Rules = [ RowCount > 0 labels=["1"="a","2"="a","3"="a","4"="a","5"="a","6"="a",` +
			`"7"="a","8"="a","9"="a","10"="a","11"="a"] ]`,
		"analyzer_with_expr": `Analyzers = [ RowCount > 1 ]`,
		"unknown_analyzer":   `Analyzers = [ IsComplete "a" ]`,
		"duplicate_rules":    `Rules = [ RowCount > 0 ] Rules = [ RowCount > 1 ]`,
		"trailing_garbage":   `Rules = [ RowCount > 0 ] }`,
	}
}

func TestValidateDQDL(t *testing.T) {
	t.Parallel()

	tests := make([]struct {
		name    string
		text    string
		wantErr bool
	}, 0, len(dqdlValid())+len(dqdlInvalid()))

	for name, text := range dqdlValid() {
		tests = append(tests, struct {
			name    string
			text    string
			wantErr bool
		}{"valid_" + name, text, false})
	}

	for name, text := range dqdlInvalid() {
		tests = append(tests, struct {
			name    string
			text    string
			wantErr bool
		}{"invalid_" + name, text, true})
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			err := glue.ValidateDQDL(tc.text)
			if tc.wantErr {
				require.ErrorIs(t, err, glue.ErrValidation)

				return
			}

			require.NoError(t, err)
		})
	}
}

func TestSDKDataQualityRuleset_DQDLValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		ruleset string
		wantErr bool
	}{
		{name: "valid", ruleset: dqdlValid()["composite"]},
		{name: "invalid_rule_type", ruleset: dqdlInvalid()["unknown_rule_type"], wantErr: true},
		{name: "invalid_structure", ruleset: dqdlInvalid()["no_rules_list"], wantErr: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			client := newTestGlueClient(t, newTestHandler(t))

			_, err := client.CreateDataQualityRuleset(t.Context(), &gluesdk.CreateDataQualityRulesetInput{
				Name:    aws.String("rs"),
				Ruleset: aws.String(tc.ruleset),
			})
			assertInvalidInputIf(t, err, tc.wantErr)

			if tc.wantErr {
				return
			}

			_, err = client.UpdateDataQualityRuleset(t.Context(), &gluesdk.UpdateDataQualityRulesetInput{
				Name:    aws.String("rs"),
				Ruleset: aws.String(dqdlInvalid()["unknown_rule_type"]),
			})
			assertInvalidInputIf(t, err, true)

			got, err := client.GetDataQualityRuleset(
				t.Context(),
				&gluesdk.GetDataQualityRulesetInput{Name: aws.String("rs")},
			)
			require.NoError(t, err)
			assert.Equal(t, tc.ruleset, aws.ToString(got.Ruleset), "rejected update must not change the stored ruleset")

			_, err = client.UpdateDataQualityRuleset(t.Context(), &gluesdk.UpdateDataQualityRulesetInput{
				Name:    aws.String("rs"),
				Ruleset: aws.String(dqdlValid()["between"]),
			})
			require.NoError(t, err)
		})
	}
}

func assertInvalidInputIf(t *testing.T, err error, want bool) {
	t.Helper()

	if !want {
		require.NoError(t, err)

		return
	}

	var invalid *types.InvalidInputException

	require.ErrorAs(t, err, &invalid)
}

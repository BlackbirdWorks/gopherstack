package stepfunctions_test

import (
	"fmt"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sfnsdk "github.com/aws/aws-sdk-go-v2/service/sfn"
	sfntypes "github.com/aws/aws-sdk-go-v2/service/sfn/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/stepfunctions"
)

func bigInput(n int) string {
	return `{"big":"` + strings.Repeat("x", n) + `"}`
}

func chainedAssigns(count int) string {
	var states []string

	for i := range count {
		next := `"End":true`
		if i < count-1 {
			next = fmt.Sprintf(`"Next":"S%d"`, i+1)
		}

		states = append(states, fmt.Sprintf(`"S%d":{"Type":"Pass","Assign":{"v%d.$":"$.big"},%s}`, i, i, next))
	}

	return `{"StartAt":"S0","States":{` + strings.Join(states, ",") + `}}`
}

func TestVariables_SDK_SizeLimits(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		definition string
		input      string
		wantStatus string
		wantError  string
	}{
		{
			name:       "single_variable_within_limit",
			definition: `{"StartAt":"A","States":{"A":{"Type":"Pass","Assign":{"x.$":"$.big"},"End":true}}}`,
			input:      bigInput(250 * 1024),
			wantStatus: "SUCCEEDED",
		},
		{
			name: "single_variable_over_256kib",
			definition: `{"StartAt":"A","States":{"A":{"Type":"Pass","Assign":{
				"x.$":"States.Format('{}{}', $.big, $.big)"},"End":true}}}`,
			input:      bigInput(150 * 1024),
			wantStatus: "FAILED",
			wantError:  "States.DataLimitExceeded",
		},
		{
			name: "combined_assign_over_256kib",
			definition: `{"StartAt":"A","States":{"A":{"Type":"Pass","Assign":{
				"x.$":"$.big","y.$":"$.big"},"End":true}}}`,
			input:      bigInput(150 * 1024),
			wantStatus: "FAILED",
			wantError:  "States.DataLimitExceeded",
		},
		{
			name: "jsonata_single_variable_over_256kib",
			definition: `{"QueryLanguage":"JSONata","StartAt":"A","States":{"A":{"Type":"Pass","Assign":{
				"x":"{% $states.input.big & $states.input.big %}"},"End":true}}}`,
			input:      bigInput(150 * 1024),
			wantStatus: "FAILED",
			wantError:  "States.DataLimitExceeded",
		},
		{
			name:       "execution_total_within_10mib",
			definition: chainedAssigns(40),
			input:      bigInput(200 * 1024),
			wantStatus: "SUCCEEDED",
		},
		{
			name:       "execution_total_over_10mib",
			definition: chainedAssigns(60),
			input:      bigInput(200 * 1024),
			wantStatus: "FAILED",
			wantError:  "States.DataLimitExceeded",
		},
		{
			name: "reassigning_same_variable_does_not_accumulate",
			definition: `{"StartAt":"A","States":{
				"A":{"Type":"Pass","Assign":{"x.$":"$.big"},"Next":"B"},
				"B":{"Type":"Pass","Assign":{"x.$":"$.big"},"Next":"C"},
				"C":{"Type":"Pass","Assign":{"x.$":"$.big"},"End":true}}}`,
			input:      bigInput(250 * 1024),
			wantStatus: "SUCCEEDED",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := runJSONataExpress(t, tt.definition, tt.input)
			assert.Equal(t, tt.wantStatus, got.status, "cause=%.200s", got.cause)
			assert.Equal(t, tt.wantError, got.errMsg)
		})
	}
}

func TestVariables_SDK_JSONPathReferences(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		definition string
		input      string
		wantOutput string
		wantStatus string
		wantError  string
	}{
		{
			name: "input_path",
			definition: `{"StartAt":"A","States":{
				"A":{"Type":"Pass","Assign":{"cfg":{"k":"v"}},"Next":"B"},
				"B":{"Type":"Pass","InputPath":"$cfg","End":true}}}`,
			input:      `{"a":1}`,
			wantOutput: `{"k":"v"}`,
		},
		{
			name: "input_path_nested",
			definition: `{"StartAt":"A","States":{
				"A":{"Type":"Pass","Assign":{"cfg":{"k":{"z":9}}},"Next":"B"},
				"B":{"Type":"Pass","InputPath":"$cfg.k","End":true}}}`,
			input:      `{}`,
			wantOutput: `{"z":9}`,
		},
		{
			name: "output_path",
			definition: `{"StartAt":"A","States":{
				"A":{"Type":"Pass","Assign":{"cfg":{"k":"v"}},"Next":"B"},
				"B":{"Type":"Pass","OutputPath":"$cfg.k","End":true}}}`,
			input:      `{"a":1}`,
			wantOutput: `"v"`,
		},
		{
			name: "choice_variable_and_path_comparison",
			definition: `{"StartAt":"A","States":{
				"A":{"Type":"Pass","Assign":{"mode":"fast","limit":5},"Next":"C"},
				"C":{"Type":"Choice","Choices":[
					{"Variable":"$mode","StringEquals":"fast","Next":"D"}],"Default":"E"},
				"D":{"Type":"Choice","Choices":[
					{"Variable":"$.n","NumericLessThanPath":"$limit","Next":"Y"}],"Default":"N"},
				"Y":{"Type":"Pass","Result":"yes","End":true},
				"N":{"Type":"Pass","Result":"no","End":true},
				"E":{"Type":"Fail","Error":"Wrong"}}}`,
			input:      `{"n":3}`,
			wantOutput: `"yes"`,
		},
		{
			name: "choice_variable_undefined_takes_default",
			definition: `{"StartAt":"C","States":{
				"C":{"Type":"Choice","Choices":[
					{"Variable":"$nope","StringEquals":"x","Next":"Y"}],"Default":"N"},
				"Y":{"Type":"Pass","Result":"yes","End":true},
				"N":{"Type":"Pass","Result":"no","End":true}}}`,
			input:      `{}`,
			wantOutput: `"no"`,
		},
		{
			name: "map_items_path",
			definition: `{"StartAt":"A","States":{
				"A":{"Type":"Pass","Assign":{"list":[1,2,3]},"Next":"M"},
				"M":{"Type":"Map","ItemsPath":"$list","ItemProcessor":{"StartAt":"P","States":{
					"P":{"Type":"Pass","End":true}}},"End":true}}}`,
			input:      `{}`,
			wantOutput: `[1,2,3]`,
		},
		{
			name:       "input_path_undefined_variable_fails",
			definition: `{"StartAt":"B","States":{"B":{"Type":"Pass","InputPath":"$nope","End":true}}}`,
			input:      `{}`,
			wantStatus: "FAILED",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := runJSONataExpress(t, tt.definition, tt.input)

			wantStatus := tt.wantStatus
			if wantStatus == "" {
				wantStatus = "SUCCEEDED"
			}

			assert.Equal(t, wantStatus, got.status, "cause=%s", got.cause)

			if tt.wantError != "" {
				assert.Equal(t, tt.wantError, got.errMsg)
			}

			if tt.wantOutput != "" {
				assert.JSONEq(t, tt.wantOutput, got.output)
			}
		})
	}
}

func TestVariables_SDK_DistributedMapOuterScope(t *testing.T) {
	t.Parallel()

	const mapTmpl = `{"StartAt":"A","States":{
		"A":{"Type":"Pass","Assign":{"outer":"o"},"Next":"M"},
		"M":{"Type":"Map","ItemsPath":"$.items",%s"ItemProcessor":{
			"ProcessorConfig":{"Mode":"%s","ExecutionType":"STANDARD"},
			"StartAt":"P","States":{"P":%s}},"End":true}}}`

	tests := []struct {
		name    string
		mode    string
		extra   string
		inner   string
		wantErr bool
	}{
		{"distributed_parameters_reads_outer", "DISTRIBUTED", "",
			`{"Type":"Pass","Parameters":{"v.$":"$outer"},"End":true}`, true},
		{"distributed_jsonata_reads_outer", "DISTRIBUTED", "",
			`{"Type":"Pass","QueryLanguage":"JSONata","Output":"{% $outer %}","End":true}`, true},
		{"distributed_input_path_reads_outer", "DISTRIBUTED", "",
			`{"Type":"Pass","InputPath":"$outer","End":true}`, true},
		{"distributed_own_variable_ok", "DISTRIBUTED", "",
			`{"Type":"Pass","Assign":{"mine":1},"Parameters":{"v.$":"$mine"},"End":true}`, false},
		{"distributed_without_variable_use_ok", "DISTRIBUTED", "",
			`{"Type":"Pass","End":true}`, false},
		{"distributed_item_selector_may_read_outer", "DISTRIBUTED",
			`"ItemSelector":{"o.$":"$outer"},`, `{"Type":"Pass","End":true}`, false},
		{"inline_reads_outer_ok", "INLINE", "",
			`{"Type":"Pass","Parameters":{"v.$":"$outer"},"End":true}`, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			def := fmt.Sprintf(mapTmpl, tt.extra, tt.mode, tt.inner)
			client := newJSONataClient(t, stepfunctions.NewHandler(stepfunctions.NewInMemoryBackend()))

			val, err := client.ValidateStateMachineDefinition(t.Context(), &sfnsdk.ValidateStateMachineDefinitionInput{
				Definition: aws.String(def),
			})
			require.NoError(t, err)

			if tt.wantErr {
				assert.Equal(t, sfntypes.ValidateStateMachineDefinitionResultCodeFail, val.Result)
			} else {
				assert.Equal(t, sfntypes.ValidateStateMachineDefinitionResultCodeOk, val.Result, "%v", val.Diagnostics)
			}

			_, err = client.CreateStateMachine(t.Context(), &sfnsdk.CreateStateMachineInput{
				Name:       aws.String("dm-sm"),
				Definition: aws.String(def),
				RoleArn:    aws.String(testRoleArn),
			})

			if tt.wantErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), "InvalidDefinition")

				return
			}

			require.NoError(t, err)
		})
	}
}

package stepfunctions_test

import (
	"context"
	"errors"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	sfnsdk "github.com/aws/aws-sdk-go-v2/service/sfn"
	sfntypes "github.com/aws/aws-sdk-go-v2/service/sfn/types"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/stepfunctions"
)

var errJSONataLambdaBoom = errors.New("lambda exploded")

type jsonataLambda struct{}

func (jsonataLambda) InvokeFunction(_ context.Context, name, _ string, payload []byte) ([]byte, int, error) {
	if strings.HasSuffix(name, "fn-fail") {
		return nil, 500, errJSONataLambdaBoom
	}

	if strings.HasSuffix(name, "fn-echo") {
		return payload, 200, nil
	}

	return []byte(`{"v": 42, "name": "widget"}`), 200, nil
}

// newJSONataClient is newSFNSDKClient plus a dialer that reaches the test
// server through the SDK's "sync-" host-prefix rewrite.
func newJSONataClient(t *testing.T, h *stepfunctions.Handler) *sfnsdk.Client {
	t.Helper()

	e := echo.New()
	registry := service.NewRegistry()
	require.NoError(t, registry.Register(h))
	e.Use(service.NewServiceRouter(registry).RouteHandler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	cfg, err := awscfg.LoadDefaultConfig(
		t.Context(),
		awscfg.WithRegion("us-east-1"),
		awscfg.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
		awscfg.WithHTTPClient(dialToRealAddr(srv.Listener.Addr().String())),
	)
	require.NoError(t, err)

	return sfnsdk.NewFromConfig(cfg, func(o *sfnsdk.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
	})
}

type jsonataRun struct {
	status string
	output string
	errMsg string
	cause  string
}

func runJSONataExpress(t *testing.T, definition, input string) jsonataRun {
	t.Helper()

	backend := stepfunctions.NewInMemoryBackend()
	backend.SetLambdaInvoker(jsonataLambda{})
	client := newJSONataClient(t, stepfunctions.NewHandler(backend))

	created, err := client.CreateStateMachine(t.Context(), &sfnsdk.CreateStateMachineInput{
		Name:       aws.String("jsonata-sm"),
		Definition: aws.String(definition),
		RoleArn:    aws.String(testRoleArn),
		Type:       sfntypes.StateMachineTypeExpress,
	})
	require.NoError(t, err)

	out, err := client.StartSyncExecution(t.Context(), &sfnsdk.StartSyncExecutionInput{
		StateMachineArn: created.StateMachineArn,
		Input:           aws.String(input),
	})
	require.NoError(t, err)

	return jsonataRun{
		status: string(out.Status),
		output: aws.ToString(out.Output),
		errMsg: aws.ToString(out.Error),
		cause:  aws.ToString(out.Cause),
	}
}

func TestJSONata_SDK_Executions(t *testing.T) {
	t.Parallel()

	const lambdaArn = "arn:aws:lambda:us-east-1:000000000000:function:fn-ok"

	tests := []struct {
		name       string
		definition string
		input      string
		wantOutput string
		wantStatus string
		wantError  string
	}{
		{
			name: "succeed_output_object",
			definition: `{"QueryLanguage":"JSONata","StartAt":"S","States":{"S":{"Type":"Succeed","Output":{
				"lastName":"{% 'Last=>' & $states.input.customer.lastName %}",
				"orderValue":"{% $states.input.order.total %}"}}}}`,
			input:      `{"customer":{"lastName":"Rivera"},"order":{"total":27.91}}`,
			wantOutput: `{"lastName":"Last=>Rivera","orderValue":27.91}`,
		},
		{
			name: "pass_filter_path_operator",
			definition: `{"QueryLanguage":"JSONata","StartAt":"F","States":{"F":{"Type":"Pass","End":true,
				"Output":{"dietProducts":"{% $states.input.products[calories=0] %}"}}}}`,
			input:      `{"products":[{"calories":140,"name":"a"},{"calories":0,"name":"b"}]}`,
			wantOutput: `{"dietProducts":{"calories":0,"name":"b"}}`,
		},
		{
			name: "pass_default_output_is_input",
			definition: `{"QueryLanguage":"JSONata","StartAt":"P","States":{"P":{"Type":"Pass","End":true,
				"Assign":{"unused":1}}}}`,
			input:      `{"a":1}`,
			wantOutput: `{"a":1}`,
		},
		{
			name: "output_literals_and_array",
			definition: `{"QueryLanguage":"JSONata","StartAt":"P","States":{"P":{"Type":"Pass","End":true,
				"Output":[1,"{% $states.input.n * 2 %}",true,null,"plain"]}}}`,
			input:      `{"n":4}`,
			wantOutput: `[1,8,true,null,"plain"]`,
		},
		{
			name: "assign_evaluation_order_and_visibility",
			definition: `{"QueryLanguage":"JSONata","StartAt":"A","States":{
				"A":{"Type":"Pass","Assign":{"x":3,"a":6},"Next":"B"},
				"B":{"Type":"Pass","Assign":{"x":"{% $a %}","nextX":"{% $x %}"},
					"Output":{"seenX":"{% $x %}"},"Next":"C"},
				"C":{"Type":"Succeed","Output":{"x":"{% $x %}","nextX":"{% $nextX %}","seen":"{% $states.input.seenX %}"}}}}`,
			input:      `{}`,
			wantOutput: `{"x":6,"nextX":3,"seen":3}`,
		},
		{
			name: "choice_condition_and_rule_assign",
			definition: `{"QueryLanguage":"JSONata","StartAt":"C","States":{
				"C":{"Type":"Choice","Choices":[
					{"Condition":"{% $states.input.n > 10 %}","Assign":{"size":"big"},"Next":"Done"},
					{"Condition":"{% $states.input.n > 5 %}","Assign":{"size":"medium"},"Next":"Done"}],
					"Default":"Small"},
				"Small":{"Type":"Pass","Assign":{"size":"small"},"Next":"Done"},
				"Done":{"Type":"Succeed","Output":"{% $size %}"}}}`,
			input:      `{"n":7}`,
			wantOutput: `"medium"`,
		},
		{
			name: "choice_default_branch",
			definition: `{"QueryLanguage":"JSONata","StartAt":"C","States":{
				"C":{"Type":"Choice","Choices":[{"Condition":"{% $states.input.n > 10 %}","Next":"Done"}],"Default":"Small"},
				"Small":{"Type":"Pass","Output":"small","End":true},
				"Done":{"Type":"Succeed"}}}`,
			input:      `{"n":1}`,
			wantOutput: `"small"`,
		},
		{
			name: "task_arguments_result_output_assign",
			definition: `{"QueryLanguage":"JSONata","StartAt":"T","States":{
				"T":{"Type":"Task","Resource":"` + lambdaArn + `",
					"Arguments":{"id":"{% $states.input.id %}"},
					"Assign":{"answer":"{% $states.result.v %}"},
					"Output":{"name":"{% $states.result.name %}"},"Next":"U"},
				"U":{"Type":"Succeed","Output":{"name":"{% $states.input.name %}","answer":"{% $answer %}"}}}}`,
			input:      `{"id":"i-1"}`,
			wantOutput: `{"name":"widget","answer":42}`,
		},
		{
			name: "task_arguments_reach_lambda",
			definition: `{"QueryLanguage":"JSONata","StartAt":"T","States":{
				"T":{"Type":"Task","Resource":"arn:aws:lambda:us-east-1:000000000000:function:fn-echo",
					"Arguments":{"sum":"{% $states.input.a + $states.input.b %}"},"End":true}}}`,
			input:      `{"a":2,"b":5}`,
			wantOutput: `{"sum":7}`,
		},
		{
			name: "task_catch_errorOutput_output_and_assign",
			definition: `{"QueryLanguage":"JSONata","StartAt":"T","States":{
				"T":{"Type":"Task","Resource":"arn:aws:lambda:us-east-1:000000000000:function:fn-fail",
					"Catch":[{"ErrorEquals":["States.ALL"],"Assign":{"failed":"{% $exists($states.errorOutput.Error) %}"},
						"Output":{"recovered":true},"Next":"After"}],"Next":"Never"},
				"After":{"Type":"Succeed","Output":{"in":"{% $states.input %}","failed":"{% $failed %}"}},
				"Never":{"Type":"Fail","Error":"Unexpected"}}}`,
			input:      `{}`,
			wantOutput: `{"in":{"recovered":true},"failed":true}`,
		},
		{
			name: "task_catch_default_output_is_errorOutput",
			definition: `{"QueryLanguage":"JSONata","StartAt":"T","States":{
				"T":{"Type":"Task","Resource":"arn:aws:lambda:us-east-1:000000000000:function:fn-fail",
					"Catch":[{"ErrorEquals":["States.ALL"],"Next":"After"}],"End":true},
				"After":{"Type":"Succeed","Output":"{% $exists($states.input.Error) %}"}}}`,
			input:      `{}`,
			wantOutput: `true`,
		},
		{
			name: "task_arguments_error_is_catchable",
			definition: `{"QueryLanguage":"JSONata","StartAt":"T","States":{
				"T":{"Type":"Task","Resource":"` + lambdaArn + `","Arguments":{"x":"{% $states.input.missing %}"},
					"Catch":[{"ErrorEquals":["States.QueryEvaluationError"],"Output":"caught","Next":"After"}],"End":true},
				"After":{"Type":"Succeed"}}}`,
			input:      `{}`,
			wantOutput: `"caught"`,
		},
		{
			name: "parallel_output_merge",
			definition: `{"QueryLanguage":"JSONata","StartAt":"P","States":{
				"P":{"Type":"Parallel","Output":"{% $merge($states.result) %}","End":true,"Branches":[
					{"StartAt":"O","States":{"O":{"Type":"Pass","End":true,"Output":{"orderId":"{% $states.input.orderId %}"}}}},
					{"StartAt":"C","States":{"C":{"Type":"Pass","End":true,
						"Output":{"customerId":"{% $states.input.customerId %}"}}}}]}}}`,
			input:      `{"orderId":"o1","customerId":"c1"}`,
			wantOutput: `{"orderId":"o1","customerId":"c1"}`,
		},
		{
			name: "parallel_branches_read_outer_variables_and_scope_ends",
			definition: `{"QueryLanguage":"JSONata","StartAt":"A","States":{
				"A":{"Type":"Pass","Assign":{"outer":"o"},"Next":"P"},
				"P":{"Type":"Parallel","Next":"Z","Branches":[
					{"StartAt":"X","States":{
						"X":{"Type":"Pass","Assign":{"inner":1},"Next":"Y"},
						"Y":{"Type":"Pass","End":true,"Output":"{% $outer & $string($inner) %}"}}}]},
				"Z":{"Type":"Succeed","Output":{"r":"{% $states.input %}","leak":"{% $exists($inner) %}"}}}}`,
			input:      `{}`,
			wantOutput: `{"r":["o1"],"leak":false}`,
		},
		{
			name: "map_items_and_item_selector",
			definition: `{"QueryLanguage":"JSONata","StartAt":"A","States":{
				"A":{"Type":"Pass","Assign":{"rate":2},"Next":"M"},
				"M":{"Type":"Map","Items":"{% $states.input.items %}",
					"ItemSelector":{"v":"{% $states.context.Map.Item.Value %}","i":"{% $states.context.Map.Item.Index %}"},
					"ItemProcessor":{"ProcessorConfig":{"Mode":"INLINE"},"StartAt":"X","States":{
						"X":{"Type":"Pass","End":true,"Output":"{% $states.input.v * $states.input.i * $rate %}"}}},
					"End":true}}}`,
			input:      `{"items":[5,6,7]}`,
			wantOutput: `[0,12,28]`,
		},
		{
			name: "map_literal_items_with_expression",
			definition: `{"QueryLanguage":"JSONata","StartAt":"M","States":{
				"M":{"Type":"Map","Items":[1,"{% $states.input.two %}",3],
					"ItemProcessor":{"StartAt":"X","States":{"X":{"Type":"Pass","End":true,"Output":"{% $states.input * 10 %}"}}},
					"End":true}}}`,
			input:      `{"two":2}`,
			wantOutput: `[10,20,30]`,
		},
		{
			name: "wait_seconds_expression",
			definition: `{"QueryLanguage":"JSONata","StartAt":"W","States":{
				"W":{"Type":"Wait","Seconds":"{% $states.input.s %}","Assign":{"waited":true},"Next":"E"},
				"E":{"Type":"Succeed","Output":{"waited":"{% $waited %}","in":"{% $states.input.s %}"}}}}`,
			input:      `{"s":0}`,
			wantOutput: `{"waited":true,"in":0}`,
		},
		{
			name: "fail_error_cause_expressions",
			definition: `{"QueryLanguage":"JSONata","StartAt":"F","States":{
				"F":{"Type":"Fail","Error":"{% $states.input.code %}","Cause":"{% 'bad ' & $states.input.what %}"}}}`,
			input:      `{"code":"MyErr","what":"thing"}`,
			wantStatus: "FAILED",
			wantError:  "MyErr",
		},
		{
			name: "undefined_result_is_query_evaluation_error",
			definition: `{"QueryLanguage":"JSONata","StartAt":"P","States":{
				"P":{"Type":"Pass","End":true,"Output":"{% $states.input.nope %}"}}}`,
			input:      `{}`,
			wantStatus: "FAILED",
			wantError:  "States.QueryEvaluationError",
		},
		{
			name: "type_error_is_query_evaluation_error",
			definition: `{"QueryLanguage":"JSONata","StartAt":"P","States":{
				"P":{"Type":"Pass","End":true,"Output":"{% $states.input.a + $states.input.b %}"}}}`,
			input:      `{"a":1,"b":"x"}`,
			wantStatus: "FAILED",
			wantError:  "States.QueryEvaluationError",
		},
		{
			name: "choice_non_boolean_condition_fails",
			definition: `{"QueryLanguage":"JSONata","StartAt":"C","States":{
				"C":{"Type":"Choice","Choices":[{"Condition":"{% 'yes' %}","Next":"D"}]},
				"D":{"Type":"Succeed"}}}`,
			input:      `{}`,
			wantStatus: "FAILED",
			wantError:  "States.QueryEvaluationError",
		},
		{
			name: "sfn_functions",
			definition: `{"QueryLanguage":"JSONata","StartAt":"P","States":{"P":{"Type":"Pass","End":true,"Output":{
				"parts":"{% $partition([1,2,3,4,5], 2) %}",
				"range":"{% $range(0, 10, 5) %}",
				"hash":"{% $hash('abc', 'MD5') %}",
				"uuidLen":"{% $length($uuid()) %}",
				"parsed":"{% $parse('{\"a\":1}') %}",
				"seeded":"{% $random(7) = $random(7) %}"}}}}`,
			input: `{}`,
			wantOutput: `{"parts":[[1,2],[3,4],[5]],"range":[0,5,10],` +
				`"hash":"900150983cd24fb0d6963f7d28e17f72","uuidLen":36,"parsed":{"a":1},"seeded":true}`,
		},
		{
			name: "states_context",
			definition: `{"QueryLanguage":"JSONata","StartAt":"P","States":{"P":{"Type":"Pass","End":true,
				"Output":{"state":"{% $states.context.State.Name %}","sm":"{% $states.context.StateMachine.Name %}"}}}}`,
			input:      `{}`,
			wantOutput: `{"state":"P","sm":"jsonata-sm"}`,
		},
		{
			name: "state_level_jsonata_in_jsonpath_machine",
			definition: `{"StartAt":"A","States":{
				"A":{"Type":"Pass","Result":{"n":3},"ResultPath":"$.a","Next":"B"},
				"B":{"QueryLanguage":"JSONata","Type":"Pass","Output":{"doubled":"{% $states.input.a.n * 2 %}"},"End":true}}}`,
			input:      `{}`,
			wantOutput: `{"doubled":6}`,
		},
		{
			name: "state_level_jsonpath_in_jsonata_machine",
			definition: `{"QueryLanguage":"JSONata","StartAt":"A","States":{
				"A":{"Type":"Pass","Output":{"n":5},"Next":"B"},
				"B":{"QueryLanguage":"JSONPath","Type":"Pass","Parameters":{"m.$":"$.n"},"End":true}}}`,
			input:      `{}`,
			wantOutput: `{"m":5}`,
		},
		{
			name: "jsonpath_assign_and_variable_references",
			definition: `{"StartAt":"A","States":{
				"A":{"Type":"Pass","Assign":{"who.$":"$.name","fixed":"k","nested":{"city.$":"$.addr.city"}},"Next":"B"},
				"B":{"Type":"Pass","End":true,"Parameters":{
					"hello.$":"States.Format('Hi {}', $who)","city.$":"$nested.city","k.$":"$fixed"}}}}`,
			input:      `{"name":"Ana","addr":{"city":"Lima"}}`,
			wantOutput: `{"hello":"Hi Ana","city":"Lima","k":"k"}`,
		},
		{
			name: "jsonpath_task_assign_uses_result_and_catch_assign",
			definition: `{"StartAt":"T","States":{
				"T":{"Type":"Task","Resource":"` + lambdaArn + `","Assign":{"v.$":"$.v"},"ResultPath":"$.r","Next":"U"},
				"U":{"Type":"Pass","Parameters":{"v.$":"$v","kept.$":"$.id"},"End":true}}}`,
			input:      `{"id":"i"}`,
			wantOutput: `{"v":42,"kept":"i"}`,
		},
		{
			name: "jsonata_variable_visible_from_jsonpath_state",
			definition: `{"QueryLanguage":"JSONata","StartAt":"A","States":{
				"A":{"Type":"Pass","Assign":{"who":"{% $states.input.name %}"},"Next":"B"},
				"B":{"QueryLanguage":"JSONPath","Type":"Pass","Parameters":{"w.$":"$who"},"End":true}}}`,
			input:      `{"name":"Ana"}`,
			wantOutput: `{"w":"Ana"}`,
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

				return
			}

			assert.JSONEq(t, tt.wantOutput, got.output)
		})
	}
}

func TestJSONata_SDK_DescribeExecutionOutput(t *testing.T) {
	t.Parallel()

	backend := stepfunctions.NewInMemoryBackend()
	client := newJSONataClient(t, stepfunctions.NewHandler(backend))

	created, err := client.CreateStateMachine(t.Context(), &sfnsdk.CreateStateMachineInput{
		Name: aws.String("jsonata-describe"),
		Definition: aws.String(`{"QueryLanguage":"JSONata","StartAt":"P","States":{"P":{"Type":"Pass","End":true,
			"Assign":{"k":1},"Output":{"twice":"{% $states.input.n * 2 %}"}}}}`),
		RoleArn: aws.String(testRoleArn),
	})
	require.NoError(t, err)

	started, err := client.StartExecution(t.Context(), &sfnsdk.StartExecutionInput{
		StateMachineArn: created.StateMachineArn,
		Input:           aws.String(`{"n":21}`),
	})
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		d, descErr := client.DescribeExecution(t.Context(), &sfnsdk.DescribeExecutionInput{
			ExecutionArn: started.ExecutionArn,
		})

		return descErr == nil && d.Status == sfntypes.ExecutionStatusSucceeded &&
			aws.ToString(d.Output) == `{"twice":42}`
	}, 5*time.Second, 20*time.Millisecond)

	desc, err := client.DescribeStateMachine(t.Context(), &sfnsdk.DescribeStateMachineInput{
		StateMachineArn: created.StateMachineArn,
	})
	require.NoError(t, err)
	assert.Contains(t, aws.ToString(desc.Definition), `"QueryLanguage":"JSONata"`)
}

const jsonataLambdaRes = "arn:aws:lambda:us-east-1:000000000000:function:f"

// jsonataStateDef wraps one state body as a single-state JSONata machine.
func jsonataStateDef(state string) string {
	return `{"QueryLanguage":"JSONata","StartAt":"S","States":{"S":` + state + `}}`
}

func TestJSONata_SDK_DefinitionValidation(t *testing.T) {
	t.Parallel()

	task := `"Type":"Task","Resource":"` + jsonataLambdaRes + `"`
	tests := []struct {
		name       string
		definition string
	}{
		{"invalid_query_language", `{"QueryLanguage":"XPath","StartAt":"S","States":{"S":{"Type":"Succeed"}}}`},
		{"invalid_state_query_language", `{"StartAt":"S","States":{"S":{"QueryLanguage":"x","Type":"Succeed"}}}`},
		{"input_path_in_jsonata", jsonataStateDef(`{"Type":"Pass","InputPath":"$.a","End":true}`)},
		{"parameters_in_jsonata", jsonataStateDef(`{"Type":"Pass","Parameters":{"a":1},"End":true}`)},
		{"result_selector_in_jsonata", jsonataStateDef(`{` + task + `,"ResultSelector":{"a":1},"End":true}`)},
		{"result_path_in_jsonata", jsonataStateDef(`{"Type":"Pass","ResultPath":"$.x","End":true}`)},
		{"output_path_in_jsonata", jsonataStateDef(`{"Type":"Pass","OutputPath":"$.x","End":true}`)},
		{"result_in_jsonata_pass", jsonataStateDef(`{"Type":"Pass","Result":1,"End":true}`)},
		{"items_path_in_jsonata", jsonataStateDef(
			`{"Type":"Map","ItemsPath":"$.x","End":true,` +
				`"ItemProcessor":{"StartAt":"X","States":{"X":{"Type":"Succeed"}}}}`)},
		{"choice_variable_in_jsonata", `{"QueryLanguage":"JSONata","StartAt":"S","States":{
			"S":{"Type":"Choice","Choices":[{"Variable":"$.a","NumericEquals":1,"Next":"E"}]},
			"E":{"Type":"Succeed"}}}`},
		{"arguments_in_jsonpath", `{"StartAt":"S","States":{"S":{"Type":"Pass","Arguments":{"a":1},"End":true}}}`},
		{"output_in_jsonpath", `{"StartAt":"S","States":{"S":{"Type":"Pass","Output":{"a":1},"End":true}}}`},
		{"condition_in_jsonpath", `{"StartAt":"S","States":{
			"S":{"Type":"Choice","Choices":[{"Condition":"{% true %}","Next":"E"}]},"E":{"Type":"Succeed"}}}`},
		{"expression_leading_space", jsonataStateDef(`{"Type":"Pass","Output":" {% 1 %}","End":true}`)},
		{"expression_unclosed", jsonataStateDef(`{"Type":"Pass","Output":"{% 1 ","End":true}`)},
		{"expression_syntax_error", jsonataStateDef(`{"Type":"Pass","Output":"{% 1 + %}","End":true}`)},
		{"eval_function_banned", jsonataStateDef(`{"Type":"Pass","Output":"{% $eval('1') %}","End":true}`)},
		{"result_in_arguments", jsonataStateDef(`{` + task + `,"Arguments":"{% $states.result %}","End":true}`)},
		{"result_in_pass_output", jsonataStateDef(`{"Type":"Pass","Output":"{% $states.result %}","End":true}`)},
		{
			"error_output_outside_catch",
			jsonataStateDef(`{` + task + `,"Output":"{% $states.errorOutput %}","End":true}`),
		},
		{"arguments_on_pass", jsonataStateDef(`{"Type":"Pass","Arguments":{"a":1},"End":true}`)},
		{"assign_on_succeed", jsonataStateDef(`{"Type":"Succeed","Assign":{"a":1}}`)},
		{"assign_not_object", jsonataStateDef(`{"Type":"Pass","Assign":[1],"End":true}`)},
		{"variable_name_invalid", jsonataStateDef(`{"Type":"Pass","Assign":{"1abc":1},"End":true}`)},
		{"variable_name_part", jsonataStateDef(`{"Type":"Pass","Assign":{"x.y":1},"End":true}`)},
		{"variable_name_reserved", jsonataStateDef(`{"Type":"Pass","Assign":{"states":1},"End":true}`)},
		{"variable_name_too_long", jsonataStateDef(
			`{"Type":"Pass","Assign":{"` + strings.Repeat("a", 81) + `":1},"End":true}`)},
		{"inner_scope_redeclares_outer", jsonataStateDef(
			`{"Type":"Parallel","Assign":{"dup":1},"End":true,"Branches":[` +
				`{"StartAt":"X","States":{"X":{"Type":"Pass","Assign":{"dup":2},"End":true}}}]}`)},
		{"seconds_expression_on_pass", jsonataStateDef(`{"Type":"Pass","Seconds":"{% 1 %}","End":true}`)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newJSONataClient(t, stepfunctions.NewHandler(stepfunctions.NewInMemoryBackend()))

			_, err := client.CreateStateMachine(t.Context(), &sfnsdk.CreateStateMachineInput{
				Name:       aws.String("jsonata-invalid"),
				Definition: aws.String(tt.definition),
				RoleArn:    aws.String(testRoleArn),
			})
			require.Error(t, err)

			var invalid *sfntypes.InvalidDefinition
			require.ErrorAs(t, err, &invalid)
		})
	}
}

func TestJSONata_SDK_ValidateStateMachineDefinition(t *testing.T) {
	t.Parallel()

	client := newJSONataClient(t, stepfunctions.NewHandler(stepfunctions.NewInMemoryBackend()))

	tests := []struct {
		name       string
		definition string
		wantResult sfntypes.ValidateStateMachineDefinitionResultCode
	}{
		{
			"valid",
			`{"QueryLanguage":"JSONata","StartAt":"S","States":{"S":{"Type":"Pass","Output":"{% $states.input %}","End":true}}}`,
			sfntypes.ValidateStateMachineDefinitionResultCodeOk,
		},
		{
			"invalid",
			`{"QueryLanguage":"JSONata","StartAt":"S","States":{"S":{"Type":"Pass","InputPath":"$","End":true}}}`,
			sfntypes.ValidateStateMachineDefinitionResultCodeFail,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			out, err := client.ValidateStateMachineDefinition(t.Context(), &sfnsdk.ValidateStateMachineDefinitionInput{
				Definition: aws.String(tt.definition),
			})
			require.NoError(t, err)
			assert.Equal(t, tt.wantResult, out.Result)
		})
	}
}

func TestJSONata_SDK_TestState(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		definition string
		input      string
		wantOutput string
	}{
		{
			name:       "name_keyed_definition",
			definition: `{"S":{"QueryLanguage":"JSONata","Type":"Pass","Output":{"n":"{% $states.input.n + 1 %}"},"End":true}}`,
			input:      `{"n":1}`,
			wantOutput: `{"n":2}`,
		},
		{
			name:       "bare_state_definition",
			definition: `{"QueryLanguage":"JSONata","Type":"Pass","Output":"{% $states.input.n * 3 %}","End":true}`,
			input:      `{"n":2}`,
			wantOutput: `6`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newJSONataClient(t, stepfunctions.NewHandler(stepfunctions.NewInMemoryBackend()))

			out, err := client.TestState(t.Context(), &sfnsdk.TestStateInput{
				Definition: aws.String(tt.definition),
				Input:      aws.String(tt.input),
				RoleArn:    aws.String(testRoleArn),
			})
			require.NoError(t, err)
			assert.Equal(t, sfntypes.TestExecutionStatusSucceeded, out.Status)
			assert.JSONEq(t, tt.wantOutput, aws.ToString(out.Output))
		})
	}
}

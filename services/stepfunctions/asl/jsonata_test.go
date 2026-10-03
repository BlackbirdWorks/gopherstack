package asl_test

import (
	"fmt"
	"strconv"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/stepfunctions/asl"
)

func TestJSONata_WaitDurations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		state string
		input string
		want  time.Duration
	}{
		{"seconds_expression", `"Seconds":"{% $states.input.s %}"`, `{"s":3}`, 3 * time.Second},
		{"seconds_static", `"Seconds":2`, `{}`, 2 * time.Second},
		{"seconds_arithmetic", `"Seconds":"{% $states.input.s * 2 %}"`, `{"s":4}`, 8 * time.Second},
		{"timestamp_expression", `"Timestamp":"{% $states.input.ts %}"`, ``, 5 * time.Second},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			synctest.Test(t, func(t *testing.T) {
				input := tt.input
				if tt.name == "timestamp_expression" {
					input = fmt.Sprintf(`{"ts":%q}`, time.Now().Add(tt.want).UTC().Format(time.RFC3339Nano))
				}

				sm, err := asl.Parse(`{"QueryLanguage":"JSONata","StartAt":"W","States":{
					"W":{"Type":"Wait",` + tt.state + `,"End":true}}}`)
				require.NoError(t, err)

				start := time.Now()
				res, err := asl.NewExecutor(sm, nil, nil).Execute(t.Context(), "arn", input)
				require.NoError(t, err)
				require.False(t, res.Failed)
				assert.InDelta(t, tt.want.Seconds(), time.Since(start).Seconds(), 0.01)
			})
		})
	}
}

func TestJSONata_NegativeSecondsFails(t *testing.T) {
	t.Parallel()

	sm, err := asl.Parse(`{"QueryLanguage":"JSONata","StartAt":"W","States":{
		"W":{"Type":"Wait","Seconds":"{% -1 %}","End":true}}}`)
	require.NoError(t, err)

	res, err := asl.NewExecutor(sm, nil, nil).Execute(t.Context(), "arn", `{}`)
	require.NoError(t, err)
	require.True(t, res.Failed)
	assert.Equal(t, "States.QueryEvaluationError", res.Error)
}

func TestJSONata_MapConcurrentEvaluation(t *testing.T) {
	t.Parallel()

	items := make([]string, 200)
	for i := range items {
		items[i] = strconv.Itoa(i)
	}

	sm, err := asl.Parse(`{"QueryLanguage":"JSONata","StartAt":"A","States":{
		"A":{"Type":"Pass","Assign":{"k":3},"Next":"M"},
		"M":{"Type":"Map","Items":"{% $states.input.items %}","MaxConcurrency":10,
			"ItemProcessor":{"StartAt":"X","States":{"X":{"Type":"Pass","End":true,
				"Output":"{% $states.input * $k %}"}}},
			"End":true}}}`)
	require.NoError(t, err)

	res, err := asl.NewExecutor(sm, nil, nil).Execute(t.Context(), "arn", `{"items":[`+strings.Join(items, ",")+`]}`)
	require.NoError(t, err)
	require.False(t, res.Failed)

	out, ok := res.Output.([]any)
	require.True(t, ok)
	require.Len(t, out, len(items))
	assert.InDelta(t, 597.0, out[199], 0)
}

func TestJSONata_MaxConcurrencyExpression(t *testing.T) {
	t.Parallel()

	sm, err := asl.Parse(`{"QueryLanguage":"JSONata","StartAt":"M","States":{
		"M":{"Type":"Map","Items":"{% $states.input %}","MaxConcurrency":"{% 2 %}",
			"ItemProcessor":{"StartAt":"X","States":{"X":{"Type":"Pass","End":true}}},"End":true}}}`)
	require.NoError(t, err)

	res, err := asl.NewExecutor(sm, nil, nil).Execute(t.Context(), "arn", `[1,2,3]`)
	require.NoError(t, err)
	assert.Equal(t, []any{1.0, 2.0, 3.0}, res.Output)
}

func TestJSONata_EvalTimeout(t *testing.T) {
	t.Parallel()

	sm, err := asl.Parse(`{"QueryLanguage":"JSONata","StartAt":"P","States":{
		"P":{"Type":"Pass","End":true,"Output":"{% ($f := function($n){$f($n+1)}; $f(0)) %}"}}}`)
	require.NoError(t, err)

	res, err := asl.NewExecutor(sm, nil, nil).Execute(t.Context(), "arn", `{}`)
	require.NoError(t, err)
	require.True(t, res.Failed)
	assert.Equal(t, "States.QueryEvaluationError", res.Error)
}

package asl_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/stepfunctions/asl"
)

func TestJSONata_ToleratedFailureExpressions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		field     string
		wantError string
	}{
		{name: "count within", field: `"ToleratedFailureCount":"{% 1 %}"`},
		{
			name:      "count exceeded",
			field:     `"ToleratedFailureCount":"{% 0 %}"`,
			wantError: "States.ExceedToleratedFailureThreshold",
		},
		{name: "percentage within", field: `"ToleratedFailurePercentage":"{% 50 %}"`},
		{
			name: "percentage exceeded", field: `"ToleratedFailurePercentage":"{% 10 %}"`,
			wantError: "States.ExceedToleratedFailureThreshold",
		},
		{name: "expression reads input", field: `"ToleratedFailureCount":"{% $states.input[0] - 2 %}"`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			sm, err := asl.Parse(strings.Replace(`{"QueryLanguage":"JSONata","StartAt":"M","States":{
				"M":{"Type":"Map","Items":"{% $states.input %}",FIELD,
					"ItemProcessor":{"StartAt":"C","States":{
						"C":{"Type":"Choice","Choices":[{"Condition":"{% $states.input = 2 %}","Next":"F"}],"Default":"P"},
						"F":{"Type":"Fail","Error":"Boom"},
						"P":{"Type":"Pass","End":true}}},
					"End":true}}}`, "FIELD", tt.field, 1))
			require.NoError(t, err)

			res, err := asl.NewExecutor(sm, nil, nil).Execute(t.Context(), "arn", `[3,2,1]`)
			require.NoError(t, err)

			if tt.wantError == "" {
				assert.False(t, res.Failed)

				return
			}

			require.True(t, res.Failed)
			assert.Equal(t, tt.wantError, res.Error)
		})
	}
}

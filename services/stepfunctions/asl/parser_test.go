package asl_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/stepfunctions/asl"
)

// TestParse_JitterStrategyValidation locks down that Parse rejects an
// invalid Retry.JitterStrategy the same way real AWS's CreateStateMachine/
// UpdateStateMachine does at request-validation time -- AWS only accepts
// "FULL" or "NONE" (or omitted, defaulting to "NONE"). Before this fix, an
// invalid value was silently treated as "NONE" instead of being rejected.
func TestParse_JitterStrategyValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		def     string
		wantErr bool
	}{
		{
			name: "omitted_is_valid",
			def: `{"StartAt":"T","States":{"T":{"Type":"Task","Resource":"arn:aws:lambda:us-east-1:123:function:f",
				"Retry":[{"ErrorEquals":["States.ALL"]}],"End":true}}}`,
			wantErr: false,
		},
		{
			name: "NONE_is_valid",
			def: `{"StartAt":"T","States":{"T":{"Type":"Task","Resource":"arn:aws:lambda:us-east-1:123:function:f",
				"Retry":[{"ErrorEquals":["States.ALL"],"JitterStrategy":"NONE"}],"End":true}}}`,
			wantErr: false,
		},
		{
			name: "FULL_is_valid",
			def: `{"StartAt":"T","States":{"T":{"Type":"Task","Resource":"arn:aws:lambda:us-east-1:123:function:f",
				"Retry":[{"ErrorEquals":["States.ALL"],"JitterStrategy":"FULL"}],"End":true}}}`,
			wantErr: false,
		},
		{
			name: "lowercase_full_is_invalid",
			def: `{"StartAt":"T","States":{"T":{"Type":"Task","Resource":"arn:aws:lambda:us-east-1:123:function:f",
				"Retry":[{"ErrorEquals":["States.ALL"],"JitterStrategy":"full"}],"End":true}}}`,
			wantErr: true,
		},
		{
			name: "garbage_value_is_invalid",
			def: `{"StartAt":"T","States":{"T":{"Type":"Task","Resource":"arn:aws:lambda:us-east-1:123:function:f",
				"Retry":[{"ErrorEquals":["States.ALL"],"JitterStrategy":"RANDOM"}],"End":true}}}`,
			wantErr: true,
		},
		{
			name: "invalid_inside_map_iterator_is_rejected",
			def: `{"StartAt":"M","States":{"M":{"Type":"Map","ItemsPath":"$.items","End":true,
				"Iterator":{"StartAt":"T","States":{"T":{"Type":"Pass","End":true,
				"Retry":[{"ErrorEquals":["States.ALL"],"JitterStrategy":"BAD"}]}}}}}}`,
			wantErr: true,
		},
		{
			name: "invalid_inside_parallel_branch_is_rejected",
			def: `{"StartAt":"P","States":{"P":{"Type":"Parallel","End":true,"Branches":[
				{"StartAt":"T","States":{"T":{"Type":"Pass","End":true,
				"Retry":[{"ErrorEquals":["States.ALL"],"JitterStrategy":"BAD"}]}}}]}}}`,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := asl.Parse(tt.def)
			if tt.wantErr {
				require.Error(t, err)
				assert.ErrorIs(t, err, asl.ErrParseError)

				return
			}

			require.NoError(t, err)
		})
	}
}

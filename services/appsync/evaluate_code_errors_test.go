package appsync_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	appsyncsdk "github.com/aws/aws-sdk-go-v2/service/appsync"
	appsynctypes "github.com/aws/aws-sdk-go-v2/service/appsync/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEvaluateCode_CodeErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		code     string
		wantType string
		wantLine int32
		wantCol  int32
	}{
		{
			name:     "syntax error",
			code:     "import { util } from '@aws-appsync/utils';\nexport function request(ctx) {\n  return 1 +;\n}",
			wantType: "PARSER_ERROR",
			wantLine: 3,
			wantCol:  13,
		},
		{
			name:     "multiline import keeps lines",
			code:     "import {\n  util,\n} from '@aws-appsync/utils';\nexport function request(ctx) {\n  return 1 +;\n}",
			wantType: "PARSER_ERROR",
			wantLine: 5,
			wantCol:  13,
		},
		{
			name:     "runtime error",
			code:     "export function request(ctx) {\n  var x = null;\n  return x.y.z;\n}",
			wantLine: 3,
			wantCol:  12,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newTestHandler()
			client := newTestAppsyncClient(t, h)

			out, err := client.EvaluateCode(t.Context(), &appsyncsdk.EvaluateCodeInput{
				Code: aws.String(tt.code), Context: aws.String(`{}`), Function: aws.String("request"),
				Runtime: &appsynctypes.AppSyncRuntime{Name: appsynctypes.RuntimeNameAppsyncJs, RuntimeVersion: aws.String("1.0.0")},
			})
			require.NoError(t, err)
			require.NotNil(t, out.Error)
			require.Len(t, out.Error.CodeErrors, 1)

			ce := out.Error.CodeErrors[0]
			assert.Equal(t, tt.wantType, aws.ToString(ce.ErrorType))
			require.NotNil(t, ce.Location)
			assert.Equal(t, tt.wantLine, ce.Location.Line)
			assert.Equal(t, tt.wantCol, ce.Location.Column)
			assert.Equal(t, int32(-1), ce.Location.Span)
			assert.NotEmpty(t, aws.ToString(ce.Value))
		})
	}
}

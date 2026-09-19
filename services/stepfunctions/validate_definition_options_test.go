package stepfunctions_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sfnsdk "github.com/aws/aws-sdk-go-v2/service/sfn"
	sfntypes "github.com/aws/aws-sdk-go-v2/service/sfn/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/stepfunctions"
)

// TestValidateStateMachineDefinition_Options covers gopherstack-xhu2t:
// severity, type, and maxResults were previously dropped entirely (the
// handler's decode struct had only a definition field), so a real client
// setting an invalid severity or type got no ValidationException back at
// all -- and Truncated (a real ValidateStateMachineDefinitionOutput member)
// was never on the wire regardless of maxResults.
func TestValidateStateMachineDefinition_Options(t *testing.T) {
	t.Parallel()

	backend := stepfunctions.NewInMemoryBackendWithConfig("000000000000", "us-east-1")
	h := stepfunctions.NewHandler(backend)
	client := newSFNSDKClient(t, h)
	ctx := t.Context()

	tests := []struct {
		name       string
		severity   sfntypes.ValidateStateMachineDefinitionSeverity
		smType     sfntypes.StateMachineType
		maxResults int32
		wantErr    bool
	}{
		{name: "unset options accepted", wantErr: false},
		{name: "valid severity and type accepted",
			severity: sfntypes.ValidateStateMachineDefinitionSeverityWarning,
			smType:   sfntypes.StateMachineTypeExpress, wantErr: false},
		{name: "invalid severity rejected", severity: "BOGUS", wantErr: true},
		{name: "invalid type rejected", smType: "BOGUS", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			out, err := client.ValidateStateMachineDefinition(ctx, &sfnsdk.ValidateStateMachineDefinitionInput{
				Definition: aws.String(validPassDef),
				Severity:   tt.severity,
				Type:       tt.smType,
				MaxResults: tt.maxResults,
			})

			if tt.wantErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), "ValidationException")

				return
			}
			require.NoError(t, err)
			assert.Equal(t, sfntypes.ValidateStateMachineDefinitionResultCodeOk, out.Result)
			assert.False(t, aws.ToBool(out.Truncated))
		})
	}
}

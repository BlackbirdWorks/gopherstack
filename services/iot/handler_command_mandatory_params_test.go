package iot_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	iotsdk "github.com/aws/aws-sdk-go-v2/service/iot"
	"github.com/aws/aws-sdk-go-v2/service/iot/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/iot"
)

// TestCommand_MandatoryParameters_RealClient drives CreateCommand then
// GetCommand through the real SDK client. GetCommandOutput.MandatoryParameters
// is a real, settable field on CreateCommandInput too (iot@v1.83.0
// api_op_CreateCommand.go:47, api_op_GetCommand.go:74-75), but the pre-fix
// handler never read it off the request or stored it, so it was always
// absent from the response -- gopherstack-mven required-output
// nested-domain-struct sweep.
func TestCommand_MandatoryParameters_RealClient(t *testing.T) {
	t.Parallel()

	backend := iot.NewInMemoryBackend()
	client := newTestIoTClient(t, iot.NewHandler(backend, nil))
	ctx := t.Context()

	_, err := client.CreateCommand(ctx, &iotsdk.CreateCommandInput{
		CommandId:   aws.String("cmd-with-params"),
		DisplayName: aws.String("Has Params"),
		Namespace:   types.CommandNamespaceAWSIoT,
		MandatoryParameters: []types.CommandParameter{
			{
				Name:        aws.String("targetTemperature"),
				Description: aws.String("desired temperature"),
				DefaultValue: &types.CommandParameterValue{
					D: aws.Float64(21.5),
				},
			},
		},
	})
	require.NoError(t, err)

	out, err := client.GetCommand(ctx, &iotsdk.GetCommandInput{CommandId: aws.String("cmd-with-params")})
	require.NoError(t, err)

	require.Len(t, out.MandatoryParameters, 1, "MandatoryParameters dropped from GetCommand response")
	param := out.MandatoryParameters[0]
	assert.Equal(t, "targetTemperature", aws.ToString(param.Name))
	assert.Equal(t, "desired temperature", aws.ToString(param.Description))
	require.NotNil(t, param.DefaultValue)
	assert.InDelta(t, 21.5, aws.ToFloat64(param.DefaultValue.D), 0.0001)
}

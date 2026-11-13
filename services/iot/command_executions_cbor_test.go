package iot_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	iotsdk "github.com/aws/aws-sdk-go-v2/service/iot"
	"github.com/aws/aws-sdk-go-v2/service/iot/types"
	"github.com/aws/aws-sdk-go-v2/service/iotjobsdataplane"
	jdtypes "github.com/aws/aws-sdk-go-v2/service/iotjobsdataplane/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

func (e *cmdEnv) publishCBOR(t *testing.T, topic, jsonBody string) {
	t.Helper()

	payload, err := service.JSONToCBOR([]byte(jsonBody), nil)
	require.NoError(t, err)

	tok := e.mqtt.Publish(topic, 1, false, payload)
	require.True(t, tok.WaitTimeout(cmdDeadline))
	require.NoError(t, tok.Error())
}

func TestCommandExecutionCBOR(t *testing.T) {
	t.Parallel()

	e := newCmdEnv(t)
	ctx := t.Context()

	_, err := e.control.CreateCommand(ctx, &iotsdk.CreateCommandInput{
		CommandId:       aws.String("tpl"),
		PayloadTemplate: aws.String(`{"mode":"${aws:iot:commandexecution::parameter:mode}"}`),
		MandatoryParameters: []types.CommandParameter{
			{Name: aws.String("mode"), Type: types.CommandParameterTypeString},
		},
		Preprocessor: &types.CommandPreprocessor{
			AwsJsonSubstitution: &types.AwsJsonSubstitutionCommandPreprocessorConfig{
				OutputFormat: types.OutputFormatCbor,
			},
		},
	})
	require.NoError(t, err)

	cmd := "arn:aws:iot:us-east-1:000000000000:command/tpl"

	out, err := e.data.StartCommandExecution(ctx, &iotjobsdataplane.StartCommandExecutionInput{
		CommandArn: aws.String(cmd), TargetArn: aws.String(targetARN(cmd)),
		Parameters: map[string]jdtypes.CommandParameterValue{"mode": {S: aws.String("eco")}},
	})
	require.NoError(t, err)

	id := aws.ToString(out.ExecutionId)
	base := "$aws/commands/things/" + cmdThing + "/executions/" + id

	request, err := service.CBORToJSON(e.expect(t, base+"/request/cbor"))
	require.NoError(t, err)
	assert.JSONEq(t, `{"mode":"eco"}`, string(request))

	e.publishCBOR(t, base+"/response/cbor",
		`{"status":"SUCCEEDED","clientToken":"c1","result":{"sn":{"s":"ABC"}}}`)

	accepted, err := service.CBORToJSON(e.expect(t, base+"/response/accepted/cbor"))
	require.NoError(t, err)
	assert.JSONEq(t, `{"executionId":"`+id+`","clientToken":"c1"}`, string(accepted))

	got, err := e.control.GetCommandExecution(ctx, &iotsdk.GetCommandExecutionInput{
		ExecutionId: aws.String(id), TargetArn: aws.String(targetARN(cmd)), IncludeResult: aws.Bool(true),
	})
	require.NoError(t, err)
	assert.Equal(t, types.CommandExecutionStatusSucceeded, got.Status)
	assert.Equal(t, "ABC", aws.ToString(got.Result["sn"].S))

	e.publishCBOR(t, base+"/response/cbor", `{"status":"IN_PROGRESS","clientToken":"c2"}`)

	rejected, err := service.CBORToJSON(e.expect(t, base+"/response/rejected/cbor"))
	require.NoError(t, err)
	assert.Contains(t, string(rejected), "TerminalStateReached")
}

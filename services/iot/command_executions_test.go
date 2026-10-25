package iot_test

import (
	"context"
	"errors"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	iotsdk "github.com/aws/aws-sdk-go-v2/service/iot"
	"github.com/aws/aws-sdk-go-v2/service/iot/types"
	"github.com/aws/aws-sdk-go-v2/service/iotjobsdataplane"
	jdtypes "github.com/aws/aws-sdk-go-v2/service/iotjobsdataplane/types"
	"github.com/aws/smithy-go"
	pahomqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/iot"
)

const (
	cmdThing    = "car1"
	cmdDeadline = 5 * time.Second
)

type cmdEnv struct {
	backend *iot.InMemoryBackend
	control *iotsdk.Client
	data    *iotjobsdataplane.Client
	mqtt    pahomqtt.Client
	msgs    chan pahomqtt.Message
}

func newCmdEnv(t *testing.T) *cmdEnv {
	t.Helper()

	backend := iot.NewInMemoryBackend()
	broker := iot.NewBroker(backend, 0)
	h := iot.NewHandler(backend, broker)

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})

	go func() {
		_ = broker.Start(ctx)
		close(done)
	}()

	t.Cleanup(func() {
		cancel()
		<-done
		backend.DrainBackground(context.Background())
	})

	select {
	case <-broker.Ready():
	case <-time.After(cmdDeadline):
		require.FailNow(t, "broker not ready")
	}

	e := echo.New()
	registry := service.NewRegistry()
	require.NoError(t, registry.Register(h))
	e.Use(service.NewServiceRouter(registry).RouteHandler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	cfg, err := awscfg.LoadDefaultConfig(t.Context(), awscfg.WithRegion("us-east-1"),
		awscfg.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")))
	require.NoError(t, err)

	env := &cmdEnv{
		backend: backend,
		msgs:    make(chan pahomqtt.Message, 64),
		control: iotsdk.NewFromConfig(cfg, func(o *iotsdk.Options) { o.BaseEndpoint = aws.String(srv.URL) }),
		data:    iotjobsdataplane.NewFromConfig(cfg, func(o *iotjobsdataplane.Options) { o.BaseEndpoint = aws.String(srv.URL) }),
	}

	opts := pahomqtt.NewClientOptions().
		AddBroker(fmt.Sprintf("tcp://127.0.0.1:%d", broker.Port())).
		SetClientID(cmdThing).
		SetAutoReconnect(false).
		SetConnectTimeout(cmdDeadline).
		SetDefaultPublishHandler(func(_ pahomqtt.Client, m pahomqtt.Message) { env.msgs <- m })

	env.mqtt = pahomqtt.NewClient(opts)
	tok := env.mqtt.Connect()
	require.True(t, tok.WaitTimeout(cmdDeadline))
	require.NoError(t, tok.Error())
	t.Cleanup(func() { env.mqtt.Disconnect(100) })

	sub := env.mqtt.Subscribe("$aws/commands/things/"+cmdThing+"/#", 1, nil)
	require.True(t, sub.WaitTimeout(cmdDeadline))
	require.NoError(t, sub.Error())

	return env
}

func (e *cmdEnv) expect(t *testing.T, topic string) []byte {
	t.Helper()

	timer := time.NewTimer(cmdDeadline)
	defer timer.Stop()

	for {
		select {
		case m := <-e.msgs:
			if m.Topic() == topic {
				return m.Payload()
			}
		case <-timer.C:
			require.FailNow(t, "no message on "+topic)
		}
	}
}

func (e *cmdEnv) respond(t *testing.T, execID, format, payload string) {
	t.Helper()

	topic := "$aws/commands/things/" + cmdThing + "/executions/" + execID + "/response"
	if format != "" {
		topic += "/" + format
	}

	tok := e.mqtt.Publish(topic, 1, false, payload)
	require.True(t, tok.WaitTimeout(cmdDeadline))
	require.NoError(t, tok.Error())
}

func (e *cmdEnv) createStatic(t *testing.T, id, contentType, content string) string {
	t.Helper()

	out, err := e.control.CreateCommand(t.Context(), &iotsdk.CreateCommandInput{
		CommandId: aws.String(id),
		Payload:   &types.CommandPayload{Content: []byte(content), ContentType: aws.String(contentType)},
	})
	require.NoError(t, err)

	return aws.ToString(out.CommandArn)
}

func targetARN(commandARN string) string {
	return strings.Replace(commandARN, ":command/"+commandARN[strings.LastIndex(commandARN, "/")+1:], ":thing/"+cmdThing, 1)
}

func apiCode(err error) string {
	var ae smithy.APIError
	if err != nil && errors.As(err, &ae) {
		return ae.ErrorCode()
	}

	return ""
}

func TestStartCommandExecution(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T, e *cmdEnv)
		name string
	}{
		{name: "static_json_delivered_and_created", run: func(t *testing.T, e *cmdEnv) {
			cmd := e.createStatic(t, "lock", "application/json", `{"lock":true}`)

			out, err := e.data.StartCommandExecution(t.Context(), &iotjobsdataplane.StartCommandExecutionInput{
				CommandArn: aws.String(cmd), TargetArn: aws.String(targetARN(cmd)),
			})
			require.NoError(t, err)

			id := aws.ToString(out.ExecutionId)
			assert.Equal(t, `{"lock":true}`, string(e.expect(t, "$aws/commands/things/"+cmdThing+"/executions/"+id+"/request/json")))

			got, err := e.control.GetCommandExecution(t.Context(), &iotsdk.GetCommandExecutionInput{
				ExecutionId: aws.String(id), TargetArn: aws.String(targetARN(cmd)),
			})
			require.NoError(t, err)
			assert.Equal(t, types.CommandExecutionStatusCreated, got.Status)
			assert.EqualValues(t, 10, aws.ToInt64(got.ExecutionTimeoutSeconds))
			assert.NotNil(t, got.StartedAt)
			assert.Nil(t, got.CompletedAt)
		}},
		{name: "opaque_payload_uses_bare_request_topic", run: func(t *testing.T, e *cmdEnv) {
			cmd := e.createStatic(t, "raw", "application/octet-stream", "xyz")

			out, err := e.data.StartCommandExecution(t.Context(), &iotjobsdataplane.StartCommandExecutionInput{
				CommandArn: aws.String(cmd), TargetArn: aws.String(targetARN(cmd)),
			})
			require.NoError(t, err)
			assert.Equal(t, "xyz", string(e.expect(t, "$aws/commands/things/"+cmdThing+"/executions/"+aws.ToString(out.ExecutionId)+"/request")))
		}},
		{name: "unknown_command", run: func(t *testing.T, e *cmdEnv) {
			_, err := e.data.StartCommandExecution(t.Context(), &iotjobsdataplane.StartCommandExecutionInput{
				CommandArn: aws.String("arn:aws:iot:us-east-1:000000000000:command/ghost"),
				TargetArn:  aws.String("arn:aws:iot:us-east-1:000000000000:thing/" + cmdThing),
			})
			assert.Equal(t, "ResourceNotFoundException", apiCode(err))
		}},
		{name: "deprecated_command", run: func(t *testing.T, e *cmdEnv) {
			cmd := e.createStatic(t, "old", "application/json", `{}`)
			_, err := e.control.UpdateCommand(t.Context(), &iotsdk.UpdateCommandInput{
				CommandId: aws.String("old"), Deprecated: aws.Bool(true),
			})
			require.NoError(t, err)

			_, err = e.data.StartCommandExecution(t.Context(), &iotjobsdataplane.StartCommandExecutionInput{
				CommandArn: aws.String(cmd), TargetArn: aws.String(targetARN(cmd)),
			})
			assert.Equal(t, "ValidationException", apiCode(err))
		}},
		{name: "bad_timeout_and_target", run: func(t *testing.T, e *cmdEnv) {
			cmd := e.createStatic(t, "c", "application/json", `{}`)

			_, err := e.data.StartCommandExecution(t.Context(), &iotjobsdataplane.StartCommandExecutionInput{
				CommandArn: aws.String(cmd), TargetArn: aws.String(targetARN(cmd)), ExecutionTimeoutSeconds: aws.Int64(43201),
			})
			assert.Equal(t, "ValidationException", apiCode(err))

			_, err = e.data.StartCommandExecution(t.Context(), &iotjobsdataplane.StartCommandExecutionInput{
				CommandArn: aws.String(cmd), TargetArn: aws.String("not-an-arn"),
			})
			assert.Equal(t, "ValidationException", apiCode(err))
		}},
		{name: "client_token_replay_and_conflict", run: func(t *testing.T, e *cmdEnv) {
			cmd := e.createStatic(t, "c", "application/json", `{}`)
			in := &iotjobsdataplane.StartCommandExecutionInput{
				CommandArn: aws.String(cmd), TargetArn: aws.String(targetARN(cmd)), ClientToken: aws.String("tok"),
			}

			first, err := e.data.StartCommandExecution(t.Context(), in)
			require.NoError(t, err)

			again, err := e.data.StartCommandExecution(t.Context(), in)
			require.NoError(t, err)
			assert.Equal(t, aws.ToString(first.ExecutionId), aws.ToString(again.ExecutionId))

			in.ExecutionTimeoutSeconds = aws.Int64(99)
			_, err = e.data.StartCommandExecution(t.Context(), in)
			assert.Equal(t, "ConflictException", apiCode(err))
		}},
		{name: "dynamic_parameters", run: func(t *testing.T, e *cmdEnv) {
			_, err := e.control.CreateCommand(t.Context(), &iotsdk.CreateCommandInput{
				CommandId:       aws.String("temp"),
				PayloadTemplate: aws.String(`{"t":"${aws:iot:commandexecution::parameter:temperature}","m":"${aws:iot:commandexecution::parameter:mode}"}`),
				MandatoryParameters: []types.CommandParameter{
					{
						Name: aws.String("temperature"), Type: types.CommandParameterTypeInteger,
						DefaultValue: &types.CommandParameterValue{I: aws.Int32(72)},
						ValueConditions: []types.CommandParameterValueCondition{{
							ComparisonOperator: types.CommandParameterValueComparisonOperatorInRange,
							Operand: &types.CommandParameterValueComparisonOperand{
								NumberRange: &types.CommandParameterValueNumberRange{Min: aws.String("60"), Max: aws.String("80")},
							},
						}},
					},
					{Name: aws.String("mode"), Type: types.CommandParameterTypeString},
				},
			})
			require.NoError(t, err)

			cmd := "arn:aws:iot:us-east-1:000000000000:command/temp"
			start := func(params map[string]jdtypes.CommandParameterValue) (*iotjobsdataplane.StartCommandExecutionOutput, error) {
				return e.data.StartCommandExecution(t.Context(), &iotjobsdataplane.StartCommandExecutionInput{
					CommandArn: aws.String(cmd), TargetArn: aws.String(targetARN(cmd)), Parameters: params,
				})
			}

			_, err = start(nil)
			assert.Equal(t, "ValidationException", apiCode(err), "mode has no value")

			_, err = start(map[string]jdtypes.CommandParameterValue{
				"mode": {S: aws.String("eco")}, "temperature": {I: aws.Int32(90)},
			})
			assert.Equal(t, "ValidationException", apiCode(err), "out of range")

			out, err := start(map[string]jdtypes.CommandParameterValue{"mode": {S: aws.String("eco")}})
			require.NoError(t, err)
			assert.JSONEq(t, `{"t":"72","m":"eco"}`, string(e.expect(t, "$aws/commands/things/"+cmdThing+"/executions/"+aws.ToString(out.ExecutionId)+"/request/json")))
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			tt.run(t, newCmdEnv(t))
		})
	}
}

func TestCommandExecutionDeviceLifecycle(t *testing.T) {
	t.Parallel()

	e := newCmdEnv(t)
	ctx := t.Context()
	cmd := e.createStatic(t, "lock", "application/json", `{}`)
	target := targetARN(cmd)

	out, err := e.data.StartCommandExecution(ctx, &iotjobsdataplane.StartCommandExecutionInput{
		CommandArn: aws.String(cmd), TargetArn: aws.String(target),
	})
	require.NoError(t, err)

	id := aws.ToString(out.ExecutionId)
	base := "$aws/commands/things/" + cmdThing + "/executions/" + id + "/response/"

	get := func(includeResult bool) *iotsdk.GetCommandExecutionOutput {
		got, gerr := e.control.GetCommandExecution(ctx, &iotsdk.GetCommandExecutionInput{
			ExecutionId: aws.String(id), TargetArn: aws.String(target), IncludeResult: aws.Bool(includeResult),
		})
		require.NoError(t, gerr)

		return got
	}

	e.respond(t, id, "json", `{"status":"IN_PROGRESS","clientToken":"a"}`)
	assert.JSONEq(t, fmt.Sprintf(`{"executionId":%q,"clientToken":"a"}`, id), string(e.expect(t, base+"accepted/json")))
	assert.Equal(t, types.CommandExecutionStatusInProgress, get(false).Status)
	assert.Nil(t, get(false).CompletedAt)

	e.respond(t, id, "json", `{"status":"SUCCEEDED","statusReason":{"reasonCode":"DONE","reasonDescription":"ok"},"result":{"sn":{"s":"ABC"},"d":{"b":true}}}`)
	e.expect(t, base+"accepted/json")

	without := get(false)
	assert.Equal(t, types.CommandExecutionStatusSucceeded, without.Status)
	assert.Empty(t, without.Result)
	assert.NotNil(t, without.CompletedAt)
	assert.Equal(t, "DONE", aws.ToString(without.StatusReason.ReasonCode))

	with := get(true)
	require.Contains(t, with.Result, "sn")
	assert.Equal(t, "ABC", aws.ToString(with.Result["sn"].S))
	assert.True(t, aws.ToBool(with.Result["d"].B))

	e.respond(t, id, "json", `{"status":"IN_PROGRESS","clientToken":"b"}`)

	var rejected map[string]string
	require.NoError(t, json.Unmarshal(e.expect(t, base+"rejected/json"), &rejected))
	assert.Equal(t, "TerminalStateReached", rejected["error"])
	assert.Equal(t, "b", rejected["clientToken"])

	completed := time.Now().Add(-time.Hour).UTC().Format("2006-01-02T15:04")
	list, err := e.control.ListCommandExecutions(ctx, &iotsdk.ListCommandExecutionsInput{
		CommandArn: aws.String(cmd), CompletedTimeFilter: &types.TimeFilter{After: aws.String(completed)},
	})
	require.NoError(t, err)
	require.Len(t, list.CommandExecutions, 1)
	assert.NotNil(t, list.CommandExecutions[0].CompletedAt)
	assert.NotNil(t, list.CommandExecutions[0].StartedAt)

	future := time.Now().Add(time.Hour).UTC().Format("2006-01-02T15:04")
	list, err = e.control.ListCommandExecutions(ctx, &iotsdk.ListCommandExecutionsInput{
		CommandArn: aws.String(cmd), CompletedTimeFilter: &types.TimeFilter{After: aws.String(future)},
	})
	require.NoError(t, err)
	assert.Empty(t, list.CommandExecutions)
}

func TestCommandExecutionDeviceRejections(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name, payload, wantErr string
	}{
		{name: "invalid_json", payload: `nope`, wantErr: "InvalidJson"},
		{name: "created_not_reportable", payload: `{"status":"CREATED"}`, wantErr: "InvalidStateTransition"},
		{name: "bad_reason_code", payload: `{"status":"FAILED","statusReason":{"reasonCode":"lower"}}`, wantErr: "InvalidRequest"},
		{name: "empty_result", payload: `{"status":"FAILED","result":{}}`, wantErr: "InvalidRequest"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newCmdEnv(t)
			cmd := e.createStatic(t, "lock", "application/json", `{}`)
			out, err := e.data.StartCommandExecution(t.Context(), &iotjobsdataplane.StartCommandExecutionInput{
				CommandArn: aws.String(cmd), TargetArn: aws.String(targetARN(cmd)),
			})
			require.NoError(t, err)

			id := aws.ToString(out.ExecutionId)
			e.respond(t, id, "json", tt.payload)

			var got map[string]string
			require.NoError(t, json.Unmarshal(e.expect(t, "$aws/commands/things/"+cmdThing+"/executions/"+id+"/response/rejected/json"), &got))
			assert.Equal(t, tt.wantErr, got["error"])
		})
	}

	t.Run("unknown_execution", func(t *testing.T) {
		t.Parallel()

		e := newCmdEnv(t)
		e.respond(t, "ghost", "json", `{"status":"SUCCEEDED"}`)

		var got map[string]string
		require.NoError(t, json.Unmarshal(e.expect(t, "$aws/commands/things/"+cmdThing+"/executions/ghost/response/rejected/json"), &got))
		assert.Equal(t, "ResourceNotFound", got["error"])
	})
}

func TestCommandExecutionTimeout(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		deviceTime bool
		wantStatus string
		wantFinal  string
		reportAs   string
		wantErr    bool
	}{
		{name: "cloud_timeout_then_device_succeeds", reportAs: "SUCCEEDED", wantStatus: "TIMED_OUT", wantFinal: "SUCCEEDED"},
		{name: "cloud_timeout_blocks_in_progress", reportAs: "IN_PROGRESS", wantStatus: "TIMED_OUT", wantFinal: "TIMED_OUT", wantErr: true},
		{name: "device_timeout_is_terminal", deviceTime: true, reportAs: "TIMED_OUT", wantStatus: "TIMED_OUT", wantFinal: "TIMED_OUT"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			synctest.Test(t, func(t *testing.T) {
				b := iot.NewInMemoryBackend()
				cmdARN := "arn:aws:iot:us-east-1:000000000000:command/c"
				_, err := b.CreateCommand("c", "", "", "", map[string]any{"contentType": "application/json", "content": "e30="}, nil, nil, iot.CommandExtras{})
				require.NoError(t, err)

				started, err := b.StartCommandExecution(&iot.StartCommandExecutionInput{
					CommandARN: cmdARN, TargetARN: "arn:aws:iot:us-east-1:000000000000:thing/d", TimeoutSeconds: aws.Int64(30),
				})
				require.NoError(t, err)

				time.Sleep(29 * time.Second)

				ex, err := b.GetCommandExecutionByID(started.ExecutionID, "")
				require.NoError(t, err)
				assert.Equal(t, "CREATED", ex.Status)

				time.Sleep(2 * time.Second)

				ex, err = b.GetCommandExecutionByID(started.ExecutionID, "")
				require.NoError(t, err)
				assert.Equal(t, tt.wantStatus, ex.Status)
				assert.Equal(t, "$NO_RESPONSE_FROM_DEVICE", ex.StatusReason.ReasonCode)
				assert.Zero(t, ex.CompletedAt)

				err = b.UpdateCommandExecution(&iot.CommandExecutionUpdate{
					ExecutionID: started.ExecutionID, DeviceKind: "things", DeviceID: "d", Status: tt.reportAs,
				})
				if tt.wantErr {
					require.ErrorIs(t, err, iot.ErrInvalidStateTransition)
				} else {
					require.NoError(t, err)
				}

				ex, err = b.GetCommandExecutionByID(started.ExecutionID, "")
				require.NoError(t, err)
				assert.Equal(t, tt.wantFinal, ex.Status)

				if tt.deviceTime {
					err = b.UpdateCommandExecution(&iot.CommandExecutionUpdate{
						ExecutionID: started.ExecutionID, DeviceKind: "things", DeviceID: "d", Status: "SUCCEEDED",
					})
					require.ErrorIs(t, err, iot.ErrInvalidStateTransition)
				}
			})
		})
	}
}

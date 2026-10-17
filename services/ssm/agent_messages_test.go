package ssm_test

import (
	"encoding/json"
	"maps"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ssm"
)

const (
	agentInstance = "mi-0123456789abcdef0"
	agentTargetNS = "EC2WindowsMessageDeliveryService."
)

type agentMessage struct {
	CreatedDate string `json:"CreatedDate"`
	Destination string `json:"Destination"`
	MessageID   string `json:"MessageId"`
	Payload     string `json:"Payload"`
	Topic       string `json:"Topic"`
}

type getMessagesResp struct {
	Destination string         `json:"Destination"`
	Messages    []agentMessage `json:"Messages"`
}

func agentCall(t *testing.T, h *ssm.Handler, op string, body any) *httptest.ResponseRecorder {
	t.Helper()

	raw, err := json.Marshal(body)
	require.NoError(t, err)

	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(string(raw)))
	req.Header.Set("X-Amz-Target", agentTargetNS+op)
	req.Header.Set("Content-Type", "application/x-amz-json-1.1")

	rec := httptest.NewRecorder()
	require.NoError(t, h.Handler()(echo.New().NewContext(req, rec)))

	return rec
}

func agentGetMessages(t *testing.T, h *ssm.Handler) getMessagesResp {
	t.Helper()

	rec := agentCall(t, h, "GetMessages", map[string]any{
		"Destination": agentInstance, "MessagesRequestId": "req-1", "VisibilityTimeout": 10,
	})
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	var out getMessagesResp
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

	return out
}

func sendAgentCommand(t *testing.T, h *ssm.Handler, b *ssm.InMemoryBackend, cmds ...string) string {
	t.Helper()

	agentCall(t, h, "GetEndpoint", map[string]any{"Destination": agentInstance})

	out, err := b.SendCommand(t.Context(), shellInput([]string{agentInstance}, cmds...))
	require.NoError(t, err)

	return out.Command.CommandID
}

func replyPayload(t *testing.T, docStatus string, plugin map[string]any) string {
	t.Helper()

	raw, err := json.Marshal(map[string]any{
		"DocumentStatus":      docStatus,
		"DocumentTraceOutput": "",
		"RuntimeStatus":       map[string]any{"aws:runShellScript": plugin},
	})
	require.NoError(t, err)

	return string(raw)
}

func TestAgentPollingRunsCommandEndToEnd(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		docStatus  string
		plugin     map[string]any
		wantStatus string
		wantStdout string
		wantStderr string
		wantCode   int32
	}{
		{
			name:      "success",
			docStatus: "Success",
			plugin: map[string]any{
				"Status": "Success", "Code": 0, "Name": "aws:runShellScript",
				"StandardOutput": "hello\n", "StandardError": "",
			},
			wantStatus: "Success", wantStdout: "hello\n",
		},
		{
			name:      "failed exit code",
			docStatus: "Failed",
			plugin: map[string]any{
				"Status": "Failed", "Code": 7, "Name": "aws:runShellScript",
				"StandardOutput": "partial\n", "StandardError": "boom\n",
			},
			wantStatus: "Failed", wantStdout: "partial\n", wantStderr: "boom\n", wantCode: 7,
		},
		{
			name:      "timed out",
			docStatus: "TimedOut",
			plugin: map[string]any{
				"Status": "TimedOut", "Code": -1, "Name": "aws:runShellScript", "Output": "slow\n",
			},
			wantStatus: "TimedOut", wantStdout: "slow\n", wantCode: -1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			synctest.Test(t, func(t *testing.T) {
				h, b := newTestHandler(t)
				cmdID := sendAgentCommand(t, h, b, "echo hello")

				inv := invocation(t, b, cmdID, agentInstance)
				assert.Equal(t, "Pending", inv.Status)
				assert.Empty(t, inv.StandardOutputContent)

				got := agentGetMessages(t, h)
				require.Len(t, got.Messages, 1)

				msg := got.Messages[0]
				assert.Equal(t, "aws.ssm."+cmdID+"."+agentInstance, msg.MessageID)
				assert.Equal(t, agentInstance, msg.Destination)
				assert.Equal(t, "aws.ssm.sendCommand", msg.Topic)
				assert.NotEmpty(t, msg.CreatedDate)

				var payload struct {
					Parameters      map[string][]string `json:"Parameters"`
					CommandID       string              `json:"CommandId"`
					DocumentName    string              `json:"DocumentName"`
					DocumentContent struct {
						MainSteps []struct {
							Action string `json:"action"`
						} `json:"mainSteps"`
					} `json:"DocumentContent"`
				}
				require.NoError(t, json.Unmarshal([]byte(msg.Payload), &payload))
				assert.Equal(t, cmdID, payload.CommandID)
				assert.Equal(t, "AWS-RunShellScript", payload.DocumentName)
				assert.Equal(t, []string{"echo hello"}, payload.Parameters["commands"])
				require.NotEmpty(t, payload.DocumentContent.MainSteps)
				assert.Equal(t, "aws:runShellScript", payload.DocumentContent.MainSteps[0].Action)

				again := agentGetMessages(t, h)
				assert.Empty(t, again.Messages, "delivered message is invisible until the visibility timeout")

				rec := agentCall(t, h, "AcknowledgeMessage", map[string]any{"MessageId": msg.MessageID})
				require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
				assert.Equal(t, "InProgress", invocation(t, b, cmdID, agentInstance).Status)

				rec = agentCall(t, h, "SendReply", map[string]any{
					"MessageId": msg.MessageID, "ReplyId": "reply-1",
					"Payload": replyPayload(t, tt.docStatus, tt.plugin),
				})
				require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

				rec = agentCall(t, h, "DeleteMessage", map[string]any{"MessageId": msg.MessageID})
				require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

				inv = invocation(t, b, cmdID, agentInstance)
				assert.Equal(t, tt.wantStatus, inv.Status)
				assert.Equal(t, tt.wantStdout, inv.StandardOutputContent)
				assert.Equal(t, tt.wantStderr, inv.StandardErrorContent)
				assert.Equal(t, tt.wantCode, inv.ResponseCode)
				assert.NotEmpty(t, inv.ExecutionEndDateTime)
				assert.Equal(t, tt.wantStatus, commandStatus(t, b, cmdID).Status)

				time.Sleep(time.Minute)
				assert.Empty(t, agentGetMessages(t, h).Messages)
			})
		})
	}
}

func TestAgentPollingInterimReply(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		h, b := newTestHandler(t)
		cmdID := sendAgentCommand(t, h, b, "sleep 1")
		msg := agentGetMessages(t, h).Messages[0]

		interim := map[string]any{"Status": "InProgress", "Name": "aws:runShellScript"}
		agentCall(t, h, "SendReply", map[string]any{
			"MessageId": msg.MessageID, "ReplyId": "r1", "Payload": replyPayload(t, "InProgress", interim),
		})

		assert.Equal(t, "InProgress", invocation(t, b, cmdID, agentInstance).Status)
		assert.Equal(t, "InProgress", commandStatus(t, b, cmdID).Status)
	})
}

func TestAgentPollingLongPoll(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		send     bool
		wantMsgs int
	}{
		{name: "empty poll returns after wait", send: false, wantMsgs: 0},
		{name: "send wakes poller", send: true, wantMsgs: 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			synctest.Test(t, func(t *testing.T) {
				h, b := newTestHandler(t)
				agentCall(t, h, "GetEndpoint", map[string]any{"Destination": agentInstance})

				var (
					wg  sync.WaitGroup
					got getMessagesResp
				)

				start := time.Now()

				wg.Go(func() { got = agentGetMessages(t, h) })
				synctest.Wait()

				if tt.send {
					time.Sleep(3 * time.Second)

					_, err := b.SendCommand(t.Context(), shellInput([]string{agentInstance}, "echo x"))
					require.NoError(t, err)
				}

				wg.Wait()

				assert.Len(t, got.Messages, tt.wantMsgs)
				assert.Equal(t, agentInstance, got.Destination)

				if tt.send {
					assert.Equal(t, 3*time.Second, time.Since(start))
				} else {
					assert.Greater(t, time.Since(start), 10*time.Second)
				}
			})
		})
	}
}

func TestAgentPollingMessageLifecycle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		extra      map[string]any
		name       string
		op         string
		wantStatus string
		redeliver  bool
	}{
		{name: "acknowledged is not redelivered", op: "AcknowledgeMessage", wantStatus: "InProgress"},
		{name: "deleted is not redelivered", op: "DeleteMessage", wantStatus: "Pending"},
		{
			name: "failed marks invocation failed", op: "FailMessage",
			extra: map[string]any{"FailureType": "InternalHandlerException"}, wantStatus: "Failed",
		},
		{name: "unacknowledged is redelivered", op: "", wantStatus: "Pending", redeliver: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			synctest.Test(t, func(t *testing.T) {
				h, b := newTestHandler(t)
				cmdID := sendAgentCommand(t, h, b, "echo x")
				msg := agentGetMessages(t, h).Messages[0]

				if tt.op != "" {
					body := map[string]any{"MessageId": msg.MessageID}
					maps.Copy(body, tt.extra)

					rec := agentCall(t, h, tt.op, body)
					require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
				}

				time.Sleep(time.Minute)

				again := agentGetMessages(t, h)
				if tt.redeliver {
					require.Len(t, again.Messages, 1)
					assert.Equal(t, msg.MessageID, again.Messages[0].MessageID)
				} else {
					assert.Empty(t, again.Messages)
				}

				assert.Equal(t, tt.wantStatus, invocation(t, b, cmdID, agentInstance).Status)
			})
		})
	}
}

func TestAgentPollingCancelCommand(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		h, b := newTestHandler(t)
		cmdID := sendAgentCommand(t, h, b, "sleep 100")
		msg := agentGetMessages(t, h).Messages[0]
		agentCall(t, h, "AcknowledgeMessage", map[string]any{"MessageId": msg.MessageID})

		_, err := b.CancelCommand(t.Context(), &ssm.CancelCommandInput{CommandID: cmdID})
		require.NoError(t, err)

		got := agentGetMessages(t, h)
		require.Len(t, got.Messages, 1)
		assert.Equal(t, "aws.ssm.cancelCommand", got.Messages[0].Topic)
		assert.JSONEq(t, `{"CancelMessageId":"`+msg.MessageID+`"}`, got.Messages[0].Payload)
	})
}

func TestAgentPollingErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     map[string]any
		name     string
		op       string
		wantCode int
	}{
		{
			name: "get messages needs destination", op: "GetMessages",
			body: map[string]any{}, wantCode: http.StatusBadRequest,
		},
		{
			name: "ack needs message id", op: "AcknowledgeMessage",
			body: map[string]any{}, wantCode: http.StatusBadRequest,
		},
		{
			name: "reply to unknown command", op: "SendReply",
			body:     map[string]any{"MessageId": "aws.ssm.nope." + agentInstance, "Payload": "{}"},
			wantCode: http.StatusBadRequest,
		},
		{name: "unknown op", op: "Bogus", body: map[string]any{}, wantCode: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newTestHandler(t)
			assert.Equal(t, tt.wantCode, agentCall(t, h, tt.op, tt.body).Code)
		})
	}
}

func TestAgentPollingRouteMatch(t *testing.T) {
	t.Parallel()

	h, _ := newTestHandler(t)
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader("{}"))
	req.Header.Set("X-Amz-Target", agentTargetNS+"GetMessages")

	c := echo.New().NewContext(req, httptest.NewRecorder())
	assert.True(t, h.RouteMatcher()(c))
	assert.Equal(t, "GetMessages", h.ExtractOperation(c))
}

func TestAgentlessInstanceKeepsSynthesizedOutput(t *testing.T) {
	t.Parallel()

	_, b := newTestHandler(t)
	out, err := b.SendCommand(t.Context(), shellInput([]string{"i-noagent"}, "echo x"))
	require.NoError(t, err)

	assert.Equal(t, "Success", invocation(t, b, out.Command.CommandID, "i-noagent").Status)
}

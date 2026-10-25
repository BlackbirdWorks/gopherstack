package iot

import (
	"encoding/json"
	"errors"
	"strings"
)

const (
	commandsTopicPrefix = "$aws/commands/"
	commandsQoS         = 1

	commandsTopicMinParts = 5
	commandsTopicMaxParts = 6
)

// commandResponse is a parsed $aws/commands/{things|clients}/<id>/executions/<exec>/response[/<format>] topic.
type commandResponse struct {
	kind, id, executionID, format string
}

func parseCommandResponseTopic(topic string) (commandResponse, bool) {
	rest, ok := strings.CutPrefix(topic, commandsTopicPrefix)
	if !ok {
		return commandResponse{}, false
	}

	parts := strings.Split(rest, "/")
	if len(parts) < commandsTopicMinParts || len(parts) > commandsTopicMaxParts || parts[2] != "executions" ||
		parts[4] != "response" {
		return commandResponse{}, false
	}

	if parts[0] != keyThings && parts[0] != "clients" {
		return commandResponse{}, false
	}

	r := commandResponse{kind: parts[0], id: parts[1], executionID: parts[3]}

	if len(parts) == commandsTopicMaxParts {
		if parts[5] != keyJSON && parts[5] != keyCBOR {
			return commandResponse{}, false
		}

		r.format = parts[5]
	}

	return r, true
}

func commandTopic(kind, id, executionID, tail string) string {
	return commandsTopicPrefix + kind + "/" + id + "/executions/" + executionID + "/" + tail
}

// publishCommandRequest delivers a started command execution on its request topic.
func (b *Broker) publishCommandRequest(s *StartedCommandExecution) {
	tail := "request"
	if s.Format != "" {
		tail += "/" + s.Format
	}

	_ = b.Publish(commandTopic(s.DeviceKind, s.DeviceID, s.ExecutionID, tail), s.Payload, false, commandsQoS)
}

type commandResponseBody struct {
	Result       map[string]map[string]any `json:"result"`
	StatusReason *CommandStatusReason      `json:"statusReason"`
	ClientToken  string                    `json:"clientToken"`
	Status       string                    `json:"status"`
}

// handleCommandResponse applies a device's UpdateCommandExecution message and answers on accepted/rejected.
func (b *Broker) handleCommandResponse(r commandResponse, payload []byte) {
	suffix := ""
	if r.format != "" {
		suffix = "/" + r.format
	}

	reply := func(outcome string, body map[string]any) {
		out, err := json.Marshal(body)
		if err != nil {
			return
		}

		_ = b.Publish(commandTopic(r.kind, r.id, r.executionID, "response/"+outcome+suffix), out, false, commandsQoS)
	}

	reject := func(code, msg, token string) {
		body := map[string]any{keyExecutionID: r.executionID, "error": code, "errorMessage": msg}
		if token != "" {
			body["clientToken"] = token
		}

		reply("rejected", body)
	}

	if r.format == keyCBOR {
		reject(jobsCodeInvalidRequest, "CBOR payloads are not supported", "")

		return
	}

	var body commandResponseBody
	if err := json.Unmarshal(payload, &body); err != nil {
		reject(jobsCodeInvalidJSON, "payload is not valid JSON", "")

		return
	}

	backend := b.backendForCommandExecution(r.executionID)
	if backend == nil {
		reject(jobsCodeNotFound, "command execution "+r.executionID+" not found", body.ClientToken)

		return
	}

	err := backend.UpdateCommandExecution(&CommandExecutionUpdate{
		ExecutionID: r.executionID, DeviceKind: r.kind, DeviceID: r.id, Status: body.Status,
		StatusReason: body.StatusReason, Result: normalizeCommandResult(body.Result),
	})
	if err != nil {
		reject(commandErrorCode(err), publicMessage(err), body.ClientToken)

		return
	}

	accepted := map[string]any{keyExecutionID: r.executionID}
	if body.ClientToken != "" {
		accepted["clientToken"] = body.ClientToken
	}

	reply("accepted", accepted)
}

func commandErrorCode(err error) string {
	switch {
	case errors.Is(err, ErrResourceNotFound):
		return jobsCodeNotFound
	case errors.Is(err, ErrTerminalState):
		return jobsCodeTerminalState
	case errors.Is(err, ErrInvalidStateTransition):
		return jobsCodeInvalidState
	case errors.Is(err, ErrValidation):
		return jobsCodeInvalidRequest
	}

	return jobsCodeInternalError
}

// normalizeCommandResult maps the MQTT result keys (s, b, bin) to the control-plane ones (S, B, BIN).
func normalizeCommandResult(in map[string]map[string]any) map[string]map[string]any {
	if in == nil {
		return nil
	}

	keys := map[string]string{"s": "S", "b": "B", "bin": keyBIN}
	out := make(map[string]map[string]any, len(in))

	for name, v := range in {
		nv := make(map[string]any, len(v))

		for k, val := range v {
			if mapped, ok := keys[k]; ok {
				k = mapped
			}

			nv[k] = val
		}

		out[name] = nv
	}

	return out
}

func (b *Broker) backendForCommandExecution(executionID string) *InMemoryBackend {
	if b.backend != nil && b.backend.HasCommandExecution(executionID) {
		return b.backend
	}

	if b.others != nil {
		for _, ob := range b.others() {
			if ob.HasCommandExecution(executionID) {
				return ob
			}
		}
	}

	return nil
}

package ssm

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"slices"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

const ec2MessagesTargetPrefix = "EC2WindowsMessageDeliveryService."

type agentHostKey struct{}

type agentGetEndpointInput struct {
	Destination string `json:"Destination"`
}

type agentGetEndpointOutput struct {
	Endpoint string `json:"Endpoint"`
}

type agentMessageIDInput struct {
	MessageID   string `json:"MessageId"`
	FailureType string `json:"FailureType,omitempty"`
}

type agentSendReplyInput struct {
	MessageID string `json:"MessageId"`
	ReplyID   string `json:"ReplyId"`
	Payload   string `json:"Payload"`
}

func (h *Handler) agentOps() map[string]ssmActionFn {
	b := h.Backend

	return map[string]ssmActionFn{
		"GetEndpoint": func(ctx context.Context, body []byte) (any, error) {
			var in agentGetEndpointInput
			if err := json.Unmarshal(body, &in); err != nil {
				return nil, err
			}

			if in.Destination != "" {
				b.AgentRegister(ctx, in.Destination)
			}

			host, _ := ctx.Value(agentHostKey{}).(string)

			return agentGetEndpointOutput{Endpoint: host}, nil
		},
		"GetMessages": func(ctx context.Context, body []byte) (any, error) {
			var in AgentGetMessagesInput
			if err := json.Unmarshal(body, &in); err != nil {
				return nil, err
			}

			if in.Destination != "" {
				b.AgentRegister(ctx, in.Destination)
			}

			return b.AgentGetMessages(ctx, &in)
		},
		"AcknowledgeMessage": agentMessageOp(func(ctx context.Context, in *agentMessageIDInput) error {
			return b.AgentAcknowledgeMessage(ctx, in.MessageID)
		}),
		"DeleteMessage": agentMessageOp(func(ctx context.Context, in *agentMessageIDInput) error {
			return b.AgentDeleteMessage(ctx, in.MessageID)
		}),
		"FailMessage": agentMessageOp(func(ctx context.Context, in *agentMessageIDInput) error {
			return b.AgentFailMessage(ctx, in.MessageID, in.FailureType)
		}),
		"SendReply": func(ctx context.Context, body []byte) (any, error) {
			var in agentSendReplyInput
			if err := json.Unmarshal(body, &in); err != nil {
				return nil, err
			}

			return struct{}{}, b.AgentSendReply(ctx, in.MessageID, in.Payload)
		},
	}
}

func agentMessageOp(fn func(context.Context, *agentMessageIDInput) error) ssmActionFn {
	return func(ctx context.Context, body []byte) (any, error) {
		var in agentMessageIDInput
		if err := json.Unmarshal(body, &in); err != nil {
			return nil, err
		}

		return struct{}{}, fn(ctx, &in)
	}
}

// handleAgent serves the ec2messages protocol the SSM agent polls for Run Command work.
func (h *Handler) handleAgent(ctx context.Context, c *echo.Context) error {
	ctx = context.WithValue(ctx, agentHostKey{}, c.Request().Host)
	ops := h.agentOps()

	return service.HandleTarget(
		c, logger.Load(ctx),
		"SSM", "application/x-amz-json-1.1",
		slices.Sorted(maps.Keys(ops)),
		func(ctx context.Context, action string, body []byte) ([]byte, error) {
			fn, ok := ops[action]
			if !ok {
				return nil, fmt.Errorf("%w:%s", ErrUnknownOperation, action)
			}

			resp, err := fn(ctx, body)
			if err != nil {
				return nil, err
			}

			return json.Marshal(resp)
		},
		h.handleError,
	)
}

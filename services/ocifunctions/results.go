package ocifunctions

import (
	"context"
	"encoding/json/v2"

	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

const notifyTitleMax = 128

type failureBody struct {
	FunctionID   string `json:"functionId"`
	CallID       string `json:"callId"`
	ErrorCode    string `json:"errorCode"`
	ErrorMessage string `json:"errorMessage"`
}

// deliverResult sends the outcome of a detached invocation to the function's success or failure destination.
func (b *InMemoryBackend) deliverResult(ctx context.Context, p execPlan, res InvokeOut, aerr *APIError) {
	raw, payload, title := p.success, res.Body, "Function "+p.fnName+" succeeded"

	if aerr != nil {
		raw, title = p.failure, "Function "+p.fnName+" failed"

		out, err := json.Marshal(failureBody{
			FunctionID: p.fnID, CallID: res.CallID, ErrorCode: aerr.Code, ErrorMessage: aerr.Message,
		}, jsonx.V1())
		if err != nil {
			return
		}

		payload = out
	}

	d, perr := parseDest(raw, "destination")
	if perr != nil || d.Kind == "" || d.Kind == destNone {
		return
	}

	b.mu.RLock("deliverResult")
	dests := b.links.dests
	b.mu.RUnlock()

	if err := sendDest(ctx, dests, d, title, payload); err != nil {
		logger.Load(ctx).WarnContext(ctx, "ocifunctions: destination delivery failed",
			"function", p.fnID, "kind", d.Kind, "target", d.target(), "error", err)
	}
}

func sendDest(ctx context.Context, dests Destinations, d destSpec, title string, payload []byte) error {
	switch d.Kind {
	case destStream:
		if dests.Stream != nil {
			return dests.Stream(ctx, d.StreamID, payload)
		}
	case destQueue:
		if dests.Queue != nil {
			return dests.Queue(ctx, d.QueueID, d.ChannelID, string(payload))
		}
	case destNotification:
		if dests.Notify != nil {
			if len(title) > notifyTitleMax {
				title = title[:notifyTitleMax]
			}

			return dests.Notify(ctx, d.TopicID, title, string(payload))
		}
	}

	logger.Load(ctx).InfoContext(ctx, "ocifunctions: destination service not wired, delivery not performed",
		"kind", d.Kind, "target", d.target())

	return nil
}

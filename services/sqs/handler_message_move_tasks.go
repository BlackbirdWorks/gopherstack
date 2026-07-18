package sqs

import (
	"context"
	"encoding/json"
	"net/http"
)

type jsonStartMessageMoveTaskReq struct {
	SourceArn                    string `json:"SourceArn"`
	DestinationArn               string `json:"DestinationArn"`
	MaxNumberOfMessagesPerSecond int32  `json:"MaxNumberOfMessagesPerSecond"`
}

type jsonStartMessageMoveTaskResp struct {
	TaskHandle string `json:"TaskHandle"`
}

type jsonCancelMessageMoveTaskReq struct {
	TaskHandle string `json:"TaskHandle"`
}

type jsonCancelMessageMoveTaskResp struct {
	ApproximateNumberOfMessagesMoved int64 `json:"ApproximateNumberOfMessagesMoved"`
}

type jsonListMessageMoveTasksReq struct {
	SourceArn  string `json:"SourceArn"`
	MaxResults int32  `json:"MaxResults"`
}

type jsonMessageMoveTask struct {
	ApproximateNumberOfMessagesToMove *int64  `json:"ApproximateNumberOfMessagesToMove,omitempty"`
	MaxNumberOfMessagesPerSecond      *int32  `json:"MaxNumberOfMessagesPerSecond,omitempty"`
	FailureReason                     *string `json:"FailureReason,omitempty"`
	TaskHandle                        string  `json:"TaskHandle,omitempty"`
	SourceArn                         string  `json:"SourceArn"`
	DestinationArn                    string  `json:"DestinationArn,omitempty"`
	Status                            string  `json:"Status"`
	// Always present — matches AWS SDK ListMessageMoveTasksResultEntry.
	ApproximateNumberOfMessagesMoved int64 `json:"ApproximateNumberOfMessagesMoved"`
	StartedTimestamp                 int64 `json:"StartedTimestamp"`
}

type jsonListMessageMoveTasksResp struct {
	Results []jsonMessageMoveTask `json:"Results"`
}

// --- handler methods for new operations ---

func (h *Handler) handleStartMessageMoveTask(
	_ context.Context,
	_ *http.Request,
	body []byte,
) (any, error) {
	var req jsonStartMessageMoveTaskReq
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, ErrUnknownAction
	}

	out, err := h.Backend.StartMessageMoveTask(&StartMessageMoveTaskInput{
		SourceArn:                    req.SourceArn,
		DestinationArn:               req.DestinationArn,
		MaxNumberOfMessagesPerSecond: req.MaxNumberOfMessagesPerSecond,
	})
	if err != nil {
		return nil, err
	}

	return jsonStartMessageMoveTaskResp{TaskHandle: out.TaskHandle}, nil
}

func (h *Handler) handleCancelMessageMoveTask(
	_ context.Context,
	_ *http.Request,
	body []byte,
) (any, error) {
	var req jsonCancelMessageMoveTaskReq
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, ErrUnknownAction
	}

	out, err := h.Backend.CancelMessageMoveTask(&CancelMessageMoveTaskInput{
		TaskHandle: req.TaskHandle,
	})
	if err != nil {
		return nil, err
	}

	return jsonCancelMessageMoveTaskResp{
		ApproximateNumberOfMessagesMoved: out.ApproximateNumberOfMessagesMoved,
	}, nil
}

func (h *Handler) handleListMessageMoveTasks(
	_ context.Context,
	_ *http.Request,
	body []byte,
) (any, error) {
	var req jsonListMessageMoveTasksReq
	// Body may be empty; ignore unmarshal errors.
	_ = json.Unmarshal(body, &req)

	out, err := h.Backend.ListMessageMoveTasks(&ListMessageMoveTasksInput{
		SourceArn:  req.SourceArn,
		MaxResults: req.MaxResults,
	})
	if err != nil {
		return nil, err
	}

	results := make([]jsonMessageMoveTask, 0, len(out.Results))
	for _, t := range out.Results {
		results = append(results, jsonMessageMoveTask{
			TaskHandle:                        t.TaskHandle,
			SourceArn:                         t.SourceArn,
			DestinationArn:                    t.DestinationArn,
			Status:                            string(t.Status),
			StartedTimestamp:                  t.StartedTimestamp,
			ApproximateNumberOfMessagesMoved:  t.ApproximateNumberOfMessagesMoved,
			ApproximateNumberOfMessagesToMove: t.ApproximateNumberOfMessagesToMove,
			MaxNumberOfMessagesPerSecond:      t.MaxNumberOfMessagesPerSecond,
			FailureReason:                     t.FailureReason,
		})
	}

	return jsonListMessageMoveTasksResp{Results: results}, nil
}

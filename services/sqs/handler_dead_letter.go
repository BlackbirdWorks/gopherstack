package sqs

import (
	"context"
	"encoding/json"
	"net/http"
)

type jsonListDeadLetterSourceQueuesReq struct {
	QueueURL   string `json:"QueueUrl"`
	NextToken  string `json:"NextToken"`
	MaxResults int    `json:"MaxResults"`
}

type jsonListDeadLetterSourceQueuesResp struct {
	NextToken string   `json:"NextToken,omitempty"`
	QueueURLs []string `json:"queueUrls"`
}

func (h *Handler) handleListDeadLetterSourceQueues(
	_ context.Context,
	r *http.Request,
	body []byte,
) (any, error) {
	var req jsonListDeadLetterSourceQueuesReq
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, ErrUnknownAction
	}

	out, err := h.Backend.ListDeadLetterSourceQueues(&ListDeadLetterSourceQueuesInput{
		QueueURL:   req.QueueURL,
		Region:     h.requestScope(r),
		NextToken:  req.NextToken,
		MaxResults: req.MaxResults,
	})
	if err != nil {
		return nil, err
	}

	queueURLs := out.QueueURLs
	if queueURLs == nil {
		queueURLs = []string{}
	}

	return jsonListDeadLetterSourceQueuesResp{QueueURLs: queueURLs, NextToken: out.NextToken}, nil
}

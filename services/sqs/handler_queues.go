package sqs

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

type jsonCreateQueueReq struct {
	Attributes map[string]string `json:"Attributes"`
	Tags       *tags.Tags        `json:"tags"`
	QueueName  string            `json:"QueueName"`
}

type jsonGetQueueURLReq struct {
	QueueName string `json:"QueueName"`
}

type jsonListQueuesReq struct {
	QueueNamePrefix string `json:"QueueNamePrefix"`
	NextToken       string `json:"NextToken"`
	MaxResults      int    `json:"MaxResults"`
}

type jsonQueueURLReq struct {
	QueueURL string `json:"QueueUrl"`
}

type jsonQueueURLResp struct {
	QueueURL string `json:"QueueUrl"`
}

type jsonListQueuesResp struct {
	NextToken string   `json:"NextToken,omitempty"`
	QueueURLs []string `json:"QueueUrls"`
}

func (h *Handler) handleCreateQueue(
	ctx context.Context,
	r *http.Request,
	body []byte,
) (any, error) {
	var req jsonCreateQueueReq
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, ErrUnknownAction
	}

	endpoint := h.Endpoint
	if endpoint == "" {
		endpoint = r.Host
	}

	scheme := "http"
	if r.TLS != nil {
		scheme = "https"
	}

	region := httputils.ExtractRegionFromRequest(r, h.DefaultRegion)

	var initialTags map[string]string
	if req.Tags != nil {
		initialTags = req.Tags.Clone()
	}

	out, err := h.Backend.CreateQueue(&CreateQueueInput{
		QueueName:  req.QueueName,
		Attributes: req.Attributes,
		Tags:       initialTags,
		Endpoint:   endpoint,
		Scheme:     scheme,
		Region:     region,
	})
	if err != nil {
		if !errors.Is(err, ErrQueueAlreadyExists) {
			logger.Load(ctx).WarnContext(ctx, "CreateQueue failed", "error", err)
		}

		return nil, err
	}

	return jsonQueueURLResp{QueueURL: out.QueueURL}, nil
}

func (h *Handler) handleDeleteQueue(
	_ context.Context,
	r *http.Request,
	body []byte,
) (any, error) {
	var req jsonQueueURLReq
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, ErrUnknownAction
	}

	region := httputils.ExtractRegionFromRequest(r, h.DefaultRegion)

	if err := h.Backend.DeleteQueue(&DeleteQueueInput{QueueURL: req.QueueURL, Region: region}); err != nil {
		return nil, err
	}

	return struct{}{}, nil
}

func (h *Handler) handleListQueues(
	_ context.Context,
	r *http.Request,
	body []byte,
) (any, error) {
	var req jsonListQueuesReq
	// ListQueues body may be empty; ignore unmarshal errors
	_ = json.Unmarshal(body, &req)

	region := httputils.ExtractRegionFromRequest(r, h.DefaultRegion)

	out, err := h.Backend.ListQueues(&ListQueuesInput{
		QueueNamePrefix: req.QueueNamePrefix,
		NextToken:       req.NextToken,
		MaxResults:      req.MaxResults,
		Region:          region,
	})
	if err != nil {
		return nil, err
	}

	queueURLs := out.QueueURLs
	if queueURLs == nil {
		queueURLs = []string{}
	}

	return jsonListQueuesResp{QueueURLs: queueURLs, NextToken: out.NextToken}, nil
}

func (h *Handler) handleGetQueueURL(
	_ context.Context,
	r *http.Request,
	body []byte,
) (any, error) {
	var req jsonGetQueueURLReq
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, ErrUnknownAction
	}

	region := httputils.ExtractRegionFromRequest(r, h.DefaultRegion)

	out, err := h.Backend.GetQueueURL(&GetQueueURLInput{QueueName: req.QueueName, Region: region})
	if err != nil {
		return nil, err
	}

	return jsonQueueURLResp{QueueURL: out.QueueURL}, nil
}

func (h *Handler) handlePurgeQueue(
	_ context.Context,
	r *http.Request,
	body []byte,
) (any, error) {
	var req jsonQueueURLReq
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, ErrUnknownAction
	}

	if err := h.Backend.PurgeQueue(&PurgeQueueInput{
		QueueURL: req.QueueURL,
		Region:   httputils.ExtractRegionFromRequest(r, h.DefaultRegion),
	}); err != nil {
		return nil, err
	}

	return struct{}{}, nil
}

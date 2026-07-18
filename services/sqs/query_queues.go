package sqs

import (
	"net/http"
	"net/url"
	"strconv"
)

func (h *Handler) queryCreateQueue(vals url.Values, r *http.Request, region string) ([]byte, int, *queryError) {
	attrs := parseQueryAttrMap(vals)
	tagMap := parseQueryTagMap(vals)

	var initialTags map[string]string
	if len(tagMap) > 0 {
		initialTags = tagMap
	}

	out, err := h.Backend.CreateQueue(&CreateQueueInput{
		QueueName:  vals.Get("QueueName"),
		Attributes: attrs,
		Tags:       initialTags,
		Endpoint:   h.queueURLEndpoint(r),
		Region:     region,
	})
	if err != nil {
		return nil, 0, buildQueryError(err)
	}

	resp := CreateQueueResponse{
		Xmlns:             sqsNamespace,
		CreateQueueResult: CreateQueueResult{QueueURL: out.QueueURL},
		ResponseMetadata:  XMLResponseMetadata{RequestID: queryRequestID},
	}

	b, err := marshalXML(resp)
	if err != nil {
		return nil, 0, buildQueryError(err)
	}

	return b, http.StatusOK, nil
}

func (h *Handler) queryDeleteQueue(vals url.Values, region string) ([]byte, int, *queryError) {
	if err := h.Backend.DeleteQueue(&DeleteQueueInput{
		QueueURL: vals.Get("QueueUrl"),
		Region:   region,
	}); err != nil {
		return nil, 0, buildQueryError(err)
	}

	resp := DeleteQueueResponse{
		Xmlns:            sqsNamespace,
		ResponseMetadata: XMLResponseMetadata{RequestID: queryRequestID},
	}

	b, err := marshalXML(resp)
	if err != nil {
		return nil, 0, buildQueryError(err)
	}

	return b, http.StatusOK, nil
}

func (h *Handler) queryListQueues(vals url.Values, region string) ([]byte, int, *queryError) {
	maxResults, _ := strconv.Atoi(vals.Get("MaxResults"))

	out, err := h.Backend.ListQueues(&ListQueuesInput{
		QueueNamePrefix: vals.Get("QueueNamePrefix"),
		NextToken:       vals.Get("NextToken"),
		Region:          region,
		MaxResults:      maxResults,
	})
	if err != nil {
		return nil, 0, buildQueryError(err)
	}

	resp := ListQueuesResponse{
		Xmlns: sqsNamespace,
		ListQueuesResult: ListQueuesResult{
			QueueURLs: out.QueueURLs,
			NextToken: out.NextToken,
		},
		ResponseMetadata: XMLResponseMetadata{RequestID: queryRequestID},
	}

	b, err := marshalXML(resp)
	if err != nil {
		return nil, 0, buildQueryError(err)
	}

	return b, http.StatusOK, nil
}

func (h *Handler) queryGetQueueURL(vals url.Values, region string) ([]byte, int, *queryError) {
	out, err := h.Backend.GetQueueURL(&GetQueueURLInput{
		QueueName: vals.Get("QueueName"),
		Region:    region,
	})
	if err != nil {
		return nil, 0, buildQueryError(err)
	}

	resp := GetQueueURLResponse{
		Xmlns:             sqsNamespace,
		GetQueueURLResult: GetQueueURLResult{QueueURL: out.QueueURL},
		ResponseMetadata:  XMLResponseMetadata{RequestID: queryRequestID},
	}

	b, err := marshalXML(resp)
	if err != nil {
		return nil, 0, buildQueryError(err)
	}

	return b, http.StatusOK, nil
}

func (h *Handler) queryPurgeQueue(vals url.Values, region string) ([]byte, int, *queryError) {
	if err := h.Backend.PurgeQueue(&PurgeQueueInput{
		QueueURL: vals.Get("QueueUrl"),
		Region:   region,
	}); err != nil {
		return nil, 0, buildQueryError(err)
	}

	resp := PurgeQueueResponse{
		Xmlns:            sqsNamespace,
		ResponseMetadata: XMLResponseMetadata{RequestID: queryRequestID},
	}

	b, err := marshalXML(resp)
	if err != nil {
		return nil, 0, buildQueryError(err)
	}

	return b, http.StatusOK, nil
}

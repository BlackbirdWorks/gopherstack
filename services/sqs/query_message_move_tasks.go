package sqs

import (
	"encoding/xml"
	"net/http"
	"net/url"
)

func (h *Handler) queryStartMessageMoveTask(vals url.Values) ([]byte, int, *queryError) {
	maxPerSec := atoiInt32(vals.Get("MaxNumberOfMessagesPerSecond"))

	out, err := h.Backend.StartMessageMoveTask(&StartMessageMoveTaskInput{
		SourceArn:                    vals.Get("SourceArn"),
		DestinationArn:               vals.Get("DestinationArn"),
		MaxNumberOfMessagesPerSecond: maxPerSec,
	})
	if err != nil {
		return nil, 0, buildQueryError(err)
	}

	type result struct {
		TaskHandle string `xml:"TaskHandle"`
	}

	type response struct {
		XMLName          xml.Name            `xml:"StartMessageMoveTaskResponse"`
		Result           result              `xml:"StartMessageMoveTaskResult"`
		ResponseMetadata XMLResponseMetadata `xml:"ResponseMetadata"`
		Xmlns            string              `xml:"xmlns,attr"`
	}

	resp := response{
		Xmlns:            sqsNamespace,
		Result:           result{TaskHandle: out.TaskHandle},
		ResponseMetadata: XMLResponseMetadata{RequestID: queryRequestID},
	}

	b, err := marshalXML(resp)
	if err != nil {
		return nil, 0, buildQueryError(err)
	}

	return b, http.StatusOK, nil
}

func (h *Handler) queryCancelMessageMoveTask(vals url.Values) ([]byte, int, *queryError) {
	out, err := h.Backend.CancelMessageMoveTask(&CancelMessageMoveTaskInput{
		TaskHandle: vals.Get("TaskHandle"),
	})
	if err != nil {
		return nil, 0, buildQueryError(err)
	}

	type result struct {
		ApproximateNumberOfMessagesMoved int64 `xml:"ApproximateNumberOfMessagesMoved"`
	}

	type response struct {
		XMLName          xml.Name            `xml:"CancelMessageMoveTaskResponse"`
		ResponseMetadata XMLResponseMetadata `xml:"ResponseMetadata"`
		Xmlns            string              `xml:"xmlns,attr"`
		Result           result              `xml:"CancelMessageMoveTaskResult"`
	}

	resp := response{
		Xmlns:            sqsNamespace,
		Result:           result{ApproximateNumberOfMessagesMoved: out.ApproximateNumberOfMessagesMoved},
		ResponseMetadata: XMLResponseMetadata{RequestID: queryRequestID},
	}

	b, err := marshalXML(resp)
	if err != nil {
		return nil, 0, buildQueryError(err)
	}

	return b, http.StatusOK, nil
}

func (h *Handler) queryListMessageMoveTasks(vals url.Values) ([]byte, int, *queryError) {
	maxResults := atoiInt32(vals.Get("MaxResults"))

	out, err := h.Backend.ListMessageMoveTasks(&ListMessageMoveTasksInput{
		SourceArn:  vals.Get("SourceArn"),
		MaxResults: maxResults,
	})
	if err != nil {
		return nil, 0, buildQueryError(err)
	}

	type taskEntry struct {
		ApproximateNumberOfMessagesToMove *int64 `xml:"ApproximateNumberOfMessagesToMove,omitempty"`
		MaxNumberOfMessagesPerSecond      *int32 `xml:"MaxNumberOfMessagesPerSecond,omitempty"`
		FailureReason                     string `xml:"FailureReason,omitempty"`
		TaskHandle                        string `xml:"TaskHandle"`
		SourceArn                         string `xml:"SourceArn"`
		DestinationArn                    string `xml:"DestinationArn"`
		Status                            string `xml:"Status"`
		ApproximateNumberOfMessagesMoved  int64  `xml:"ApproximateNumberOfMessagesMoved"`
		StartedTimestamp                  int64  `xml:"StartedTimestamp"`
	}

	type result struct {
		Results []taskEntry `xml:"Results"`
	}

	type response struct {
		XMLName          xml.Name            `xml:"ListMessageMoveTasksResponse"`
		ResponseMetadata XMLResponseMetadata `xml:"ResponseMetadata"`
		Xmlns            string              `xml:"xmlns,attr"`
		Result           result              `xml:"ListMessageMoveTasksResult"`
	}

	entries := make([]taskEntry, 0, len(out.Results))
	for _, t := range out.Results {
		var failure string
		if t.FailureReason != nil {
			failure = *t.FailureReason
		}

		entries = append(entries, taskEntry{
			TaskHandle:                        t.TaskHandle,
			SourceArn:                         t.SourceArn,
			DestinationArn:                    t.DestinationArn,
			Status:                            string(t.Status),
			StartedTimestamp:                  t.StartedTimestamp,
			ApproximateNumberOfMessagesMoved:  t.ApproximateNumberOfMessagesMoved,
			ApproximateNumberOfMessagesToMove: t.ApproximateNumberOfMessagesToMove,
			MaxNumberOfMessagesPerSecond:      t.MaxNumberOfMessagesPerSecond,
			FailureReason:                     failure,
		})
	}

	resp := response{
		Xmlns:            sqsNamespace,
		Result:           result{Results: entries},
		ResponseMetadata: XMLResponseMetadata{RequestID: queryRequestID},
	}

	b, err := marshalXML(resp)
	if err != nil {
		return nil, 0, buildQueryError(err)
	}

	return b, http.StatusOK, nil
}

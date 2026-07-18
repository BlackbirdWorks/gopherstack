package sqs

import (
	"encoding/xml"
	"net/http"
	"net/url"
	"strconv"
)

func (h *Handler) queryListDeadLetterSourceQueues(vals url.Values, region string) ([]byte, int, *queryError) {
	maxResults, _ := strconv.Atoi(vals.Get("MaxResults"))

	out, err := h.Backend.ListDeadLetterSourceQueues(&ListDeadLetterSourceQueuesInput{
		QueueURL:   vals.Get("QueueUrl"),
		Region:     region,
		NextToken:  vals.Get("NextToken"),
		MaxResults: maxResults,
	})
	if err != nil {
		return nil, 0, buildQueryError(err)
	}

	type result struct {
		NextToken string   `xml:"NextToken,omitempty"`
		QueueURLs []string `xml:"queueUrls"`
	}

	type response struct {
		XMLName          xml.Name            `xml:"ListDeadLetterSourceQueuesResponse"`
		ResponseMetadata XMLResponseMetadata `xml:"ResponseMetadata"`
		Xmlns            string              `xml:"xmlns,attr"`
		Result           result              `xml:"ListDeadLetterSourceQueuesResult"`
	}

	resp := response{
		Xmlns: sqsNamespace,
		Result: result{
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

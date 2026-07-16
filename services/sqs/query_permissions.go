package sqs

import (
	"encoding/xml"
	"net/http"
	"net/url"
)

func (h *Handler) queryAddPermission(vals url.Values, region string) ([]byte, int, *queryError) {
	actions := parseQueryList(vals, "ActionName")
	accountIDs := parseQueryList(vals, "AWSAccountId")

	if err := h.Backend.AddPermission(&AddPermissionInput{
		QueueURL:      vals.Get("QueueUrl"),
		Region:        region,
		Label:         vals.Get("Label"),
		Actions:       actions,
		AWSAccountIDs: accountIDs,
	}); err != nil {
		return nil, 0, buildQueryError(err)
	}

	type addPermResp struct {
		XMLName          xml.Name            `xml:"AddPermissionResponse"`
		ResponseMetadata XMLResponseMetadata `xml:"ResponseMetadata"`
		Xmlns            string              `xml:"xmlns,attr"`
	}

	resp := addPermResp{
		Xmlns:            sqsNamespace,
		ResponseMetadata: XMLResponseMetadata{RequestID: queryRequestID},
	}

	b, err := marshalXML(resp)
	if err != nil {
		return nil, 0, buildQueryError(err)
	}

	return b, http.StatusOK, nil
}

func (h *Handler) queryRemovePermission(vals url.Values, region string) ([]byte, int, *queryError) {
	if err := h.Backend.RemovePermission(&RemovePermissionInput{
		QueueURL: vals.Get("QueueUrl"),
		Region:   region,
		Label:    vals.Get("Label"),
	}); err != nil {
		return nil, 0, buildQueryError(err)
	}

	type removePermResp struct {
		XMLName          xml.Name            `xml:"RemovePermissionResponse"`
		ResponseMetadata XMLResponseMetadata `xml:"ResponseMetadata"`
		Xmlns            string              `xml:"xmlns,attr"`
	}

	resp := removePermResp{
		Xmlns:            sqsNamespace,
		ResponseMetadata: XMLResponseMetadata{RequestID: queryRequestID},
	}

	b, err := marshalXML(resp)
	if err != nil {
		return nil, 0, buildQueryError(err)
	}

	return b, http.StatusOK, nil
}

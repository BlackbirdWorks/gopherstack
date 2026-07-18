package sqs

import (
	"net/http"
	"net/url"
)

func (h *Handler) queryGetQueueAttributes(vals url.Values, region string) ([]byte, int, *queryError) {
	attrNames := parseQueryList(vals, "AttributeName")

	out, err := h.Backend.GetQueueAttributes(&GetQueueAttributesInput{
		QueueURL:       vals.Get("QueueUrl"),
		Region:         region,
		AttributeNames: attrNames,
	})
	if err != nil {
		return nil, 0, buildQueryError(err)
	}

	xmlAttrs := make([]XMLAttribute, 0, len(out.Attributes))
	for k, v := range out.Attributes {
		xmlAttrs = append(xmlAttrs, XMLAttribute{Name: k, Value: v})
	}

	resp := GetQueueAttributesResponse{
		Xmlns: sqsNamespace,
		GetQueueAttributesResult: GetQueueAttributesResult{
			Attributes: xmlAttrs,
		},
		ResponseMetadata: XMLResponseMetadata{RequestID: queryRequestID},
	}

	b, err := marshalXML(resp)
	if err != nil {
		return nil, 0, buildQueryError(err)
	}

	return b, http.StatusOK, nil
}

func (h *Handler) querySetQueueAttributes(vals url.Values, region string) ([]byte, int, *queryError) {
	attrs := parseQueryAttrMap(vals)

	if err := h.Backend.SetQueueAttributes(&SetQueueAttributesInput{
		QueueURL:   vals.Get("QueueUrl"),
		Region:     region,
		Attributes: attrs,
	}); err != nil {
		return nil, 0, buildQueryError(err)
	}

	resp := SetQueueAttributesResponse{
		Xmlns:            sqsNamespace,
		ResponseMetadata: XMLResponseMetadata{RequestID: queryRequestID},
	}

	b, err := marshalXML(resp)
	if err != nil {
		return nil, 0, buildQueryError(err)
	}

	return b, http.StatusOK, nil
}

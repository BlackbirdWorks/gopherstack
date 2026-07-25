package cloudwatch

import (
	"errors"
	"net/http"

	"github.com/aws/smithy-go/encoding/cbor"
	"github.com/labstack/echo/v5"
)

// cborMetricStreamFilters extracts a list of MetricStreamFilter from a CBOR map key.
func cborMetricStreamFilters(input cbor.Map, key string) []MetricStreamFilter {
	listVal, ok := input[key]
	if !ok {
		return nil
	}
	list, isList := listVal.(cbor.List)
	if !isList {
		return nil
	}
	filters := make([]MetricStreamFilter, 0, len(list))
	for _, item := range list {
		fm, isMap := item.(cbor.Map)
		if !isMap {
			continue
		}
		ns := cborStr(fm, keyNamespace)
		if ns == "" {
			continue
		}
		metricNames := cborStrList(fm, "MetricNames")
		filters = append(filters, MetricStreamFilter{Namespace: ns, MetricNames: metricNames})
	}

	return filters
}

func (h *Handler) cborPutMetricStream(input cbor.Map, c *echo.Context) error {
	name := cborStr(input, keyName)
	if name == "" {
		return h.cborError(c, http.StatusBadRequest, "InvalidParameterValue", "Name is required")
	}

	if err := h.Backend.PutMetricStream(&MetricStream{
		Name:           name,
		FirehoseArn:    cborStr(input, "FirehoseArn"),
		RoleArn:        cborStr(input, "RoleArn"),
		OutputFormat:   cborStr(input, "OutputFormat"),
		State:          cborStr(input, keyState),
		IncludeFilters: cborMetricStreamFilters(input, "IncludeFilters"),
		ExcludeFilters: cborMetricStreamFilters(input, "ExcludeFilters"),
	}); err != nil {
		if errors.Is(err, ErrValidation) {
			return h.cborError(c, http.StatusBadRequest, "InvalidParameterValue", err.Error())
		}

		return h.cborError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
	}

	return writeCBOR(c, cbor.Map{})
}

func (h *Handler) cborListMetricStreams(input cbor.Map, c *echo.Context) error {
	nextToken := cborStr(input, "NextToken")
	maxResults := int(cborInt32(input, "MaxResults"))

	p, err := h.Backend.ListMetricStreams(nextToken, maxResults)
	if err != nil {
		return h.cborError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
	}

	entries := make(cbor.List, 0, len(p.Data))
	for _, s := range p.Data {
		entry := cbor.Map{
			keyName:        cbor.String(s.Name),
			"Arn":          cbor.String(s.Arn),
			"FirehoseArn":  cbor.String(s.FirehoseArn),
			keyState:       cbor.String(s.State),
			"OutputFormat": cbor.String(s.OutputFormat),
		}
		if !s.CreationDate.IsZero() {
			entry["CreationDate"] = cborFromTime(s.CreationDate)
		}
		if !s.LastUpdateDate.IsZero() {
			entry["LastUpdateDate"] = cborFromTime(s.LastUpdateDate)
		}
		entries = append(entries, entry)
	}

	out := cbor.Map{
		"Entries": entries,
	}
	if p.Next != "" {
		out["NextToken"] = cbor.String(p.Next)
	}

	return writeCBOR(c, out)
}

func (h *Handler) cborGetMetricStream(input cbor.Map, c *echo.Context) error {
	name := cborStr(input, keyName)
	if name == "" {
		return h.cborError(c, http.StatusBadRequest, "InvalidParameterValue", "Name is required")
	}

	stream, err := h.Backend.GetMetricStream(name)
	if err != nil {
		return h.cborError(c, http.StatusBadRequest, "ResourceNotFoundException", err.Error())
	}

	out := cbor.Map{
		keyName:        cbor.String(stream.Name),
		"Arn":          cbor.String(stream.Arn),
		"FirehoseArn":  cbor.String(stream.FirehoseArn),
		"RoleArn":      cbor.String(stream.RoleArn),
		keyState:       cbor.String(stream.State),
		"OutputFormat": cbor.String(stream.OutputFormat),
	}
	if !stream.CreationDate.IsZero() {
		out["CreationDate"] = cborFromTime(stream.CreationDate)
	}
	if !stream.LastUpdateDate.IsZero() {
		out["LastUpdateDate"] = cborFromTime(stream.LastUpdateDate)
	}

	return writeCBOR(c, out)
}

func (h *Handler) cborDeleteMetricStream(input cbor.Map, c *echo.Context) error {
	name := cborStr(input, keyName)
	if name == "" {
		return h.cborError(c, http.StatusBadRequest, "InvalidParameterValue", "Name is required")
	}

	if err := h.Backend.DeleteMetricStream(name); err != nil {
		return h.cborError(c, http.StatusBadRequest, "ResourceNotFoundException", err.Error())
	}

	return writeCBOR(c, cbor.Map{})
}

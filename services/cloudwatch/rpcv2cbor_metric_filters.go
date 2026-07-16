package cloudwatch

import (
	"net/http"

	"github.com/aws/smithy-go/encoding/cbor"
	"github.com/labstack/echo/v5"
)

func (h *Handler) cborPutMetricFilter(input cbor.Map, c *echo.Context) error {
	filterName := cborStr(input, "FilterName")
	if filterName == "" {
		return h.cborError(
			c,
			http.StatusBadRequest,
			"InvalidParameterValue",
			"FilterName is required",
		)
	}
	logGroupName := cborStr(input, "LogGroupName")
	if logGroupName == "" {
		return h.cborError(
			c,
			http.StatusBadRequest,
			"InvalidParameterValue",
			"LogGroupName is required",
		)
	}

	filter := &MetricFilter{
		FilterName:    filterName,
		LogGroupName:  logGroupName,
		FilterPattern: cborStr(input, "FilterPattern"),
	}

	if mtsRaw, hasMts := input["MetricTransformations"]; hasMts {
		if mtsList, isList := mtsRaw.(cbor.List); isList {
			for _, mtRaw := range mtsList {
				if mt, isMap := mtRaw.(cbor.Map); isMap {
					filter.MetricTransformations = append(
						filter.MetricTransformations,
						MetricTransformation{
							MetricName:      cborStr(mt, keyMetricName),
							MetricNamespace: cborStr(mt, "MetricNamespace"),
							MetricValue:     cborStr(mt, "MetricValue"),
							Unit:            cborStr(mt, "Unit"),
							DefaultValue:    cborFloat(mt, "DefaultValue"),
						},
					)
				}
			}
		}
	}

	if err := h.Backend.PutMetricFilter(filter); err != nil {
		return h.cborError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
	}

	return writeCBOR(c, cbor.Map{})
}

func (h *Handler) cborDescribeMetricFilters(input cbor.Map, c *echo.Context) error {
	filterNamePrefix := cborStr(input, "FilterNamePrefix")
	logGroupName := cborStr(input, "LogGroupName")
	nextToken := cborStr(input, "NextToken")
	maxResults := int(cborInt32(input, "MaxResults"))

	p, err := h.Backend.DescribeMetricFilters(filterNamePrefix, logGroupName, nextToken, maxResults)
	if err != nil {
		return h.cborError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
	}

	filters := make(cbor.List, 0, len(p.Data))
	for _, f := range p.Data {
		entry := cbor.Map{
			"FilterName":    cbor.String(f.FilterName),
			"LogGroupName":  cbor.String(f.LogGroupName),
			"FilterPattern": cbor.String(f.FilterPattern),
			"CreationTime":  cbor.Uint(uint64(f.CreationTime.UnixMilli())),
		}
		if len(f.MetricTransformations) > 0 {
			mts := make(cbor.List, 0, len(f.MetricTransformations))
			for _, mt := range f.MetricTransformations {
				mts = append(mts, cbor.Map{
					keyMetricName:     cbor.String(mt.MetricName),
					"MetricNamespace": cbor.String(mt.MetricNamespace),
					"MetricValue":     cbor.String(mt.MetricValue),
					"Unit":            cbor.String(mt.Unit),
					"DefaultValue":    cbor.Float64(mt.DefaultValue),
				})
			}
			entry["MetricTransformations"] = mts
		}
		filters = append(filters, entry)
	}

	out := cbor.Map{
		"MetricFilters": filters,
	}
	if p.Next != "" {
		out["NextToken"] = cbor.String(p.Next)
	}

	return writeCBOR(c, out)
}

func (h *Handler) cborDeleteMetricFilter(input cbor.Map, c *echo.Context) error {
	filterName := cborStr(input, "FilterName")
	if filterName == "" {
		return h.cborError(
			c,
			http.StatusBadRequest,
			"InvalidParameterValue",
			"FilterName is required",
		)
	}
	logGroupName := cborStr(input, "LogGroupName")
	if logGroupName == "" {
		return h.cborError(
			c,
			http.StatusBadRequest,
			"InvalidParameterValue",
			"LogGroupName is required",
		)
	}

	if err := h.Backend.DeleteMetricFilter(filterName, logGroupName); err != nil {
		return h.cborError(c, http.StatusBadRequest, "ResourceNotFoundException", err.Error())
	}

	return writeCBOR(c, cbor.Map{})
}

// cborTestMetricFilter returns an empty matches response (log events are not stored by this emulator).
func (h *Handler) cborTestMetricFilter(_ cbor.Map, c *echo.Context) error {
	return writeCBOR(c, cbor.Map{
		"Matches": cbor.List{},
	})
}

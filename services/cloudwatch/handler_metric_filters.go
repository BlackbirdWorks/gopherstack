package cloudwatch

import (
	"encoding/xml"
	"fmt"
	"net/http"
	"net/url"
	"strconv"

	"github.com/google/uuid"
	"github.com/labstack/echo/v5"
)

// parseMetricTransformationsFromForm reads MetricTransformations.member.N.* from the form.
func parseMetricTransformationsFromForm(form url.Values) []MetricTransformation {
	var transformations []MetricTransformation
	for i := 1; ; i++ {
		prefix := fmt.Sprintf("MetricTransformations.member.%d.", i)
		name := form.Get(prefix + "MetricName")
		if name == "" {
			return transformations
		}
		defaultValue, _ := strconv.ParseFloat(form.Get(prefix+"DefaultValue"), 64)
		transformations = append(transformations, MetricTransformation{
			MetricName:      name,
			MetricNamespace: form.Get(prefix + "MetricNamespace"),
			MetricValue:     form.Get(prefix + "MetricValue"),
			DefaultValue:    defaultValue,
			Unit:            form.Get(prefix + "Unit"),
		})
	}
}

func (h *Handler) handlePutMetricFilter(form url.Values, c *echo.Context) error {
	filterName := form.Get("FilterName")
	if filterName == "" {
		return h.xmlError(
			c,
			http.StatusBadRequest,
			"InvalidParameterValue",
			"FilterName is required",
		)
	}
	logGroupName := form.Get("LogGroupName")
	if logGroupName == "" {
		return h.xmlError(
			c,
			http.StatusBadRequest,
			"InvalidParameterValue",
			"LogGroupName is required",
		)
	}

	filter := &MetricFilter{
		FilterName:            filterName,
		LogGroupName:          logGroupName,
		FilterPattern:         form.Get("FilterPattern"),
		MetricTransformations: parseMetricTransformationsFromForm(form),
	}
	if err := h.Backend.PutMetricFilter(filter); err != nil {
		return h.xmlError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
	}

	type response struct {
		XMLName   xml.Name `xml:"PutMetricFilterResponse"`
		Xmlns     string   `xml:"xmlns,attr"`
		RequestID string   `xml:"ResponseMetadata>RequestId"`
	}

	return writeXML(c, response{Xmlns: cloudwatchNS, RequestID: uuid.New().String()})
}

func (h *Handler) handleDescribeMetricFilters(form url.Values, c *echo.Context) error {
	filterNamePrefix := form.Get("FilterNamePrefix")
	logGroupName := form.Get("LogGroupName")
	nextToken := form.Get("NextToken")
	maxResults, _ := strconv.Atoi(form.Get("MaxResults"))

	p, err := h.Backend.DescribeMetricFilters(filterNamePrefix, logGroupName, nextToken, maxResults)
	if err != nil {
		return h.xmlError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
	}

	type metricTransXML struct {
		MetricName      string  `xml:"MetricName"`
		MetricNamespace string  `xml:"MetricNamespace"`
		MetricValue     string  `xml:"MetricValue"`
		Unit            string  `xml:"Unit,omitempty"`
		DefaultValue    float64 `xml:"DefaultValue,omitempty"`
	}
	type filterXML struct {
		FilterName            string           `xml:"FilterName"`
		LogGroupName          string           `xml:"LogGroupName"`
		FilterPattern         string           `xml:"FilterPattern,omitempty"`
		MetricTransformations []metricTransXML `xml:"MetricTransformations>member,omitempty"`
		CreationTime          int64            `xml:"CreationTime"`
	}

	members := make([]filterXML, 0, len(p.Data))
	for _, f := range p.Data {
		fx := filterXML{
			FilterName:    f.FilterName,
			LogGroupName:  f.LogGroupName,
			FilterPattern: f.FilterPattern,
			CreationTime:  f.CreationTime.UnixMilli(),
		}
		for _, t := range f.MetricTransformations {
			fx.MetricTransformations = append(fx.MetricTransformations, metricTransXML(t))
		}
		members = append(members, fx)
	}

	type descResult struct {
		NextToken     string      `xml:"NextToken,omitempty"`
		MetricFilters []filterXML `xml:"MetricFilters>member"`
	}
	type response struct {
		XMLName   xml.Name   `xml:"DescribeMetricFiltersResponse"`
		Xmlns     string     `xml:"xmlns,attr"`
		RequestID string     `xml:"ResponseMetadata>RequestId"`
		Result    descResult `xml:"DescribeMetricFiltersResult"`
	}

	return writeXML(c, response{
		Xmlns:     cloudwatchNS,
		RequestID: uuid.New().String(),
		Result:    descResult{MetricFilters: members, NextToken: p.Next},
	})
}

func (h *Handler) handleDeleteMetricFilter(form url.Values, c *echo.Context) error {
	filterName := form.Get("FilterName")
	if filterName == "" {
		return h.xmlError(
			c,
			http.StatusBadRequest,
			"InvalidParameterValue",
			"FilterName is required",
		)
	}
	logGroupName := form.Get("LogGroupName")
	if logGroupName == "" {
		return h.xmlError(
			c,
			http.StatusBadRequest,
			"InvalidParameterValue",
			"LogGroupName is required",
		)
	}

	if err := h.Backend.DeleteMetricFilter(filterName, logGroupName); err != nil {
		return h.xmlError(c, http.StatusBadRequest, "ResourceNotFoundException", err.Error())
	}

	type response struct {
		XMLName   xml.Name `xml:"DeleteMetricFilterResponse"`
		Xmlns     string   `xml:"xmlns,attr"`
		RequestID string   `xml:"ResponseMetadata>RequestId"`
	}

	return writeXML(c, response{Xmlns: cloudwatchNS, RequestID: uuid.New().String()})
}

// handleTestMetricFilter is a stub that returns empty matches (log events not stored by this emulator).
func (h *Handler) handleTestMetricFilter(_ url.Values, c *echo.Context) error {
	type match struct {
		ExtractedValues struct{} `xml:"ExtractedValues"`
		EventMessage    string   `xml:"EventMessage"`
		EventNumber     int64    `xml:"EventNumber"`
	}
	type response struct {
		XMLName   xml.Name `xml:"TestMetricFilterResponse"`
		Xmlns     string   `xml:"xmlns,attr"`
		RequestID string   `xml:"ResponseMetadata>RequestId"`
		Matches   []match  `xml:"TestMetricFilterResult>Matches>member"`
	}

	return writeXML(c, response{Xmlns: cloudwatchNS, RequestID: uuid.New().String()})
}

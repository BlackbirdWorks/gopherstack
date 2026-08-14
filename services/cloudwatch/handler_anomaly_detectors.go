package cloudwatch

import (
	"encoding/xml"
	"net/http"
	"net/url"
	"strconv"

	"github.com/google/uuid"
	"github.com/labstack/echo/v5"
)

func (h *Handler) handleDeleteAnomalyDetector(form url.Values, c *echo.Context) error {
	namespace := form.Get("SingleMetricAnomalyDetector.Namespace")
	if namespace == "" {
		namespace = form.Get("Namespace")
	}

	metricName := form.Get("SingleMetricAnomalyDetector.MetricName")
	if metricName == "" {
		metricName = form.Get("MetricName")
	}

	stat := form.Get("SingleMetricAnomalyDetector.Stat")
	if stat == "" {
		stat = form.Get("Stat")
	}

	if namespace == "" || metricName == "" {
		return h.xmlError(
			c,
			http.StatusBadRequest,
			"InvalidParameterValue",
			"Namespace and MetricName are required",
		)
	}

	dimsD := parseDimensionsFromForm(form, "SingleMetricAnomalyDetector.Dimensions")
	if len(dimsD) == 0 {
		dimsD = parseDimensionsFromForm(form, "Dimensions")
	}

	if err := h.Backend.DeleteAnomalyDetector(namespace, metricName, stat, dimsD); err != nil {
		return h.xmlError(c, http.StatusBadRequest, "ResourceNotFoundException", err.Error())
	}

	// Clean up tags for the deleted anomaly detector's ARN.
	detectorARN := buildAnomalyDetectorARN(namespace, metricName, stat)
	h.deleteResourceTags(detectorARN)

	type response struct {
		XMLName   xml.Name `xml:"DeleteAnomalyDetectorResponse"`
		Xmlns     string   `xml:"xmlns,attr"`
		RequestID string   `xml:"ResponseMetadata>RequestId"`
	}

	return writeXML(c, response{Xmlns: cloudwatchNS, RequestID: uuid.New().String()})
}

// buildAnomalyDetectorARN constructs a synthetic ARN for an anomaly detector (for tag storage).
func buildAnomalyDetectorARN(namespace, metricName, stat string) string {
	return "arn:aws:cloudwatch::anomaly-detector:" + namespace + "/" + metricName + "/" + stat
}

func (h *Handler) handleDescribeAnomalyDetectors(form url.Values, c *echo.Context) error {
	namespace := form.Get("Namespace")
	metricName := form.Get("MetricName")
	nextToken := form.Get("NextToken")
	maxResults, _ := strconv.Atoi(form.Get("MaxResults"))

	p, err := h.Backend.DescribeAnomalyDetectors(namespace, metricName, nextToken, maxResults)
	if err != nil {
		return h.xmlError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
	}

	type dimXML struct {
		Name  string `xml:"Name"`
		Value string `xml:"Value"`
	}
	type detectorXML struct {
		Namespace  string   `xml:"SingleMetricAnomalyDetector>Namespace"`
		MetricName string   `xml:"SingleMetricAnomalyDetector>MetricName"`
		Stat       string   `xml:"SingleMetricAnomalyDetector>Stat"`
		StateValue string   `xml:"StateValue"`
		Dimensions []dimXML `xml:"SingleMetricAnomalyDetector>Dimensions>member,omitempty"`
	}
	members := make([]detectorXML, 0, len(p.Data))
	for _, d := range p.Data {
		var dims []dimXML
		for _, dim := range d.Dimensions {
			dims = append(dims, dimXML(dim))
		}
		members = append(members, detectorXML{
			Namespace:  d.Namespace,
			MetricName: d.MetricName,
			Stat:       d.Stat,
			Dimensions: dims,
			StateValue: d.StateValue,
		})
	}

	type descResult struct {
		NextToken        string        `xml:"NextToken,omitempty"`
		AnomalyDetectors []detectorXML `xml:"AnomalyDetectors>member"`
	}
	type response struct {
		XMLName   xml.Name   `xml:"DescribeAnomalyDetectorsResponse"`
		Xmlns     string     `xml:"xmlns,attr"`
		RequestID string     `xml:"ResponseMetadata>RequestId"`
		Result    descResult `xml:"DescribeAnomalyDetectorsResult"`
	}

	return writeXML(c, response{
		Xmlns:     cloudwatchNS,
		RequestID: uuid.New().String(),
		Result:    descResult{AnomalyDetectors: members, NextToken: p.Next},
	})
}

func (h *Handler) handlePutAnomalyDetector(form url.Values, c *echo.Context) error {
	namespace := form.Get("SingleMetricAnomalyDetector.Namespace")
	if namespace == "" {
		namespace = form.Get("Namespace")
	}
	metricName := form.Get("SingleMetricAnomalyDetector.MetricName")
	if metricName == "" {
		metricName = form.Get("MetricName")
	}
	stat := form.Get("SingleMetricAnomalyDetector.Stat")
	if stat == "" {
		stat = form.Get("Stat")
	}

	if namespace == "" || metricName == "" {
		return h.xmlError(
			c,
			http.StatusBadRequest,
			"InvalidParameterValue",
			"Namespace and MetricName are required",
		)
	}

	// Parse dimensions for the anomaly detector.
	dims := parseDimensionsFromForm(form, "SingleMetricAnomalyDetector.Dimensions")
	if len(dims) == 0 {
		dims = parseDimensionsFromForm(form, "Dimensions")
	}

	// Parse optional band width.
	bandWidth := 0.0
	if bwStr := form.Get("Configuration.BandWidth"); bwStr != "" {
		bandWidth, _ = strconv.ParseFloat(bwStr, 64)
	}

	if err := h.Backend.PutAnomalyDetector(&AnomalyDetector{
		Namespace:  namespace,
		MetricName: metricName,
		Stat:       stat,
		Dimensions: dims,
		BandWidth:  bandWidth,
		StateValue: statusTrainedInsufficient,
	}); err != nil {
		return h.xmlError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
	}

	type response struct {
		XMLName   xml.Name `xml:"PutAnomalyDetectorResponse"`
		Xmlns     string   `xml:"xmlns,attr"`
		RequestID string   `xml:"ResponseMetadata>RequestId"`
	}

	return writeXML(c, response{Xmlns: cloudwatchNS, RequestID: uuid.New().String()})
}

package cloudwatch

import (
	"net/http"

	"github.com/aws/smithy-go/encoding/cbor"
	"github.com/labstack/echo/v5"
)

func (h *Handler) cborPutAnomalyDetector(input cbor.Map, c *echo.Context) error {
	namespace := ""
	metricName := ""
	stat := ""

	if smadRaw, hasSmad := input["SingleMetricAnomalyDetector"]; hasSmad {
		if smad, isMap := smadRaw.(cbor.Map); isMap {
			namespace = cborStr(smad, keyNamespace)
			metricName = cborStr(smad, keyMetricName)
			stat = cborStr(smad, "Stat")
		}
	}
	if namespace == "" {
		namespace = cborStr(input, keyNamespace)
	}
	if metricName == "" {
		metricName = cborStr(input, keyMetricName)
	}
	if stat == "" {
		stat = cborStr(input, "Stat")
	}

	if namespace == "" || metricName == "" {
		return h.cborError(
			c,
			http.StatusBadRequest,
			"InvalidParameterValue",
			"Namespace and MetricName are required",
		)
	}

	if err := h.Backend.PutAnomalyDetector(&AnomalyDetector{
		Namespace:  namespace,
		MetricName: metricName,
		Stat:       stat,
		StateValue: statusTrainedInsufficient,
	}); err != nil {
		return h.cborError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
	}

	return writeCBOR(c, cbor.Map{})
}

func (h *Handler) cborDeleteAnomalyDetector(input cbor.Map, c *echo.Context) error {
	namespace := ""
	metricName := ""
	stat := ""

	var dimsD []Dimension
	if smadRaw, hasSmad := input["SingleMetricAnomalyDetector"]; hasSmad {
		if smad, isMap := smadRaw.(cbor.Map); isMap {
			namespace = cborStr(smad, keyNamespace)
			metricName = cborStr(smad, keyMetricName)
			stat = cborStr(smad, "Stat")
			dimsD = cborDimensions(smad)
		}
	}
	if namespace == "" {
		namespace = cborStr(input, keyNamespace)
	}
	if metricName == "" {
		metricName = cborStr(input, keyMetricName)
	}
	if stat == "" {
		stat = cborStr(input, "Stat")
	}
	if dimsD == nil {
		dimsD = cborDimensions(input)
	}

	if err := h.Backend.DeleteAnomalyDetector(namespace, metricName, stat, dimsD); err != nil {
		return h.cborError(c, http.StatusBadRequest, "ResourceNotFoundException", err.Error())
	}

	return writeCBOR(c, cbor.Map{})
}

func (h *Handler) cborDescribeAnomalyDetectors(input cbor.Map, c *echo.Context) error {
	namespace := cborStr(input, keyNamespace)
	metricName := cborStr(input, keyMetricName)
	nextToken := cborStr(input, "NextToken")
	maxResults := int(cborInt32(input, "MaxResults"))

	p, err := h.Backend.DescribeAnomalyDetectors(namespace, metricName, nextToken, maxResults)
	if err != nil {
		return h.cborError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
	}

	members := make(cbor.List, 0, len(p.Data))
	for _, d := range p.Data {
		entry := cbor.Map{
			keyStateValue: cbor.String(d.StateValue),
			"SingleMetricAnomalyDetector": cbor.Map{
				keyNamespace:  cbor.String(d.Namespace),
				keyMetricName: cbor.String(d.MetricName),
				"Stat":        cbor.String(d.Stat),
			},
		}
		members = append(members, entry)
	}

	out := cbor.Map{
		"AnomalyDetectors": members,
	}
	if p.Next != "" {
		out["NextToken"] = cbor.String(p.Next)
	}

	return writeCBOR(c, out)
}

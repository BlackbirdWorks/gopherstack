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
	var dims []Dimension

	if smadRaw, hasSmad := input["SingleMetricAnomalyDetector"]; hasSmad {
		if smad, isMap := smadRaw.(cbor.Map); isMap {
			namespace = cborStr(smad, keyNamespace)
			metricName = cborStr(smad, keyMetricName)
			stat = cborStr(smad, "Stat")
			dims = cborDimensions(smad)
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
	if dims == nil {
		dims = cborDimensions(input)
	}

	mathQueries := cborMetricMathQueries(input)

	if len(mathQueries) == 0 && (namespace == "" || metricName == "") {
		return h.cborError(
			c,
			http.StatusBadRequest,
			"InvalidParameterValue",
			"Namespace and MetricName are required",
		)
	}

	detector := &AnomalyDetector{
		Namespace:             namespace,
		MetricName:            metricName,
		Stat:                  stat,
		Dimensions:            dims,
		StateValue:            statusTrainedInsufficient,
		MetricMath:            mathQueries,
		Configuration:         cborAnomalyConfiguration(input),
		MetricCharacteristics: cborMetricCharacteristics(input),
	}
	if len(mathQueries) > 0 {
		detector.Namespace, detector.MetricName, detector.Stat, detector.Dimensions = "", "", "", nil
	}

	if err := h.Backend.PutAnomalyDetector(detector); err != nil {
		return h.cborError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
	}

	return writeCBOR(c, cbor.Map{"AnomalyDetectorId": cbor.String(detector.ID)})
}

func cborMetricMathQueries(input cbor.Map) []MetricDataQuery {
	mm, ok := input["MetricMathAnomalyDetector"].(cbor.Map)
	if !ok {
		return nil
	}

	return parseMetricDataQueries(mm, "MetricDataQueries")
}

func cborAnomalyConfiguration(input cbor.Map) *AnomalyDetectorConfiguration {
	cfgMap, ok := input["Configuration"].(cbor.Map)
	if !ok {
		return nil
	}

	cfg := &AnomalyDetectorConfiguration{MetricTimezone: cborStr(cfgMap, "MetricTimezone")}

	if ranges, isList := cfgMap["ExcludedTimeRanges"].(cbor.List); isList {
		for _, r := range ranges {
			if rm, isMap := r.(cbor.Map); isMap {
				cfg.ExcludedTimeRanges = append(cfg.ExcludedTimeRanges, AnomalyTimeRange{
					StartTime: cborTime(rm, "StartTime"),
					EndTime:   cborTime(rm, "EndTime"),
				})
			}
		}
	}

	return cfg
}

func cborMetricCharacteristics(input cbor.Map) *MetricCharacteristics {
	mc, ok := input["MetricCharacteristics"].(cbor.Map)
	if !ok {
		return nil
	}

	spikes, _ := mc["PeriodicSpikes"].(cbor.Bool)

	return &MetricCharacteristics{PeriodicSpikes: bool(spikes)}
}

func cborAnomalyConfigurationValue(cfg *AnomalyDetectorConfiguration) cbor.Map {
	out := cbor.Map{}
	if cfg.MetricTimezone != "" {
		out["MetricTimezone"] = cbor.String(cfg.MetricTimezone)
	}

	if len(cfg.ExcludedTimeRanges) > 0 {
		ranges := make(cbor.List, 0, len(cfg.ExcludedTimeRanges))
		for _, r := range cfg.ExcludedTimeRanges {
			ranges = append(ranges, cbor.Map{
				"StartTime": cborFromTime(r.StartTime),
				"EndTime":   cborFromTime(r.EndTime),
			})
		}

		out["ExcludedTimeRanges"] = ranges
	}

	return out
}

func (h *Handler) cborDeleteAnomalyDetector(input cbor.Map, c *echo.Context) error {
	if id := cborStr(input, "AnomalyDetectorId"); id != "" {
		if err := h.Backend.DeleteAnomalyDetectorByID(id); err != nil {
			return h.cborError(c, http.StatusBadRequest, "ResourceNotFoundException", err.Error())
		}

		return writeCBOR(c, cbor.Map{})
	}

	if queries := cborMetricMathQueries(input); len(queries) > 0 {
		if err := h.Backend.DeleteMetricMathAnomalyDetector(queries); err != nil {
			return h.cborError(c, http.StatusBadRequest, "ResourceNotFoundException", err.Error())
		}

		return writeCBOR(c, cbor.Map{})
	}

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

	filter := AnomalyDetectorFilter{
		Namespace:  namespace,
		MetricName: metricName,
		IDs:        cborStrList(input, "AnomalyDetectorIds"),
		Types:      cborStrList(input, "AnomalyDetectorTypes"),
		Dimensions: cborDimensions(input),
	}

	if len(filter.IDs) > 0 && (namespace != "" || metricName != "" || len(filter.Types) > 0 ||
		len(filter.Dimensions) > 0) {
		return h.cborError(c, http.StatusBadRequest, "InvalidParameterCombinationException",
			"AnomalyDetectorIds cannot be combined with Namespace, MetricName, Dimensions or AnomalyDetectorTypes")
	}

	p, err := h.Backend.DescribeAnomalyDetectorsFiltered(filter, nextToken, maxResults)
	if err != nil {
		return h.cborError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
	}

	members := make(cbor.List, 0, len(p.Data))
	for _, d := range p.Data {
		smad := cbor.Map{
			keyNamespace:  cbor.String(d.Namespace),
			keyMetricName: cbor.String(d.MetricName),
			"Stat":        cbor.String(d.Stat),
		}
		entry := cbor.Map{keyStateValue: cbor.String(d.StateValue)}

		if len(d.MetricMath) > 0 {
			entry["MetricMathAnomalyDetector"] = cbor.Map{"MetricDataQueries": buildMetricDataQueriesCBOR(d.MetricMath)}
		} else {
			entry["SingleMetricAnomalyDetector"] = smad
		}

		if d.Configuration != nil {
			entry["Configuration"] = cborAnomalyConfigurationValue(d.Configuration)
		}

		if d.MetricCharacteristics != nil {
			entry["MetricCharacteristics"] = cbor.Map{
				"PeriodicSpikes": cbor.Bool(d.MetricCharacteristics.PeriodicSpikes),
			}
		}

		if d.ID != "" {
			entry["AnomalyDetectorId"] = cbor.String(d.ID)
		}
		if len(d.Dimensions) > 0 {
			dimList := make(cbor.List, 0, len(d.Dimensions))
			for _, dim := range d.Dimensions {
				dimList = append(dimList, cbor.Map{
					keyName:  cbor.String(dim.Name),
					keyValue: cbor.String(dim.Value),
				})
			}
			smad["Dimensions"] = dimList
			// AnomalyDetector.Dimensions (top-level) is deprecated in favor of
			// SingleMetricAnomalyDetector.Dimensions but is still a real member
			// on the wire (cloudwatch@v1.66.3 schemas/schemas.go:3415);
			// populate both so older callers reading the deprecated field see
			// the same data.
			entry["Dimensions"] = dimList
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

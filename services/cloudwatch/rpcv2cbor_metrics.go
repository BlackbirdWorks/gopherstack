package cloudwatch

import (
	"errors"
	"net/http"

	"github.com/aws/smithy-go/encoding/cbor"
	"github.com/labstack/echo/v5"
)

// cborDecodeDatum parses a single MetricDatum from a CBOR map. The Values/Counts
// array takes precedence over StatisticValues, which takes precedence over
// Value, mirroring the order AWS documents them (the Has* flags let
// validateMetricDatum reject any invalid combination of these instead).
func cborDecodeDatum(m cbor.Map) MetricDatum {
	ts := cborTime(m, "Timestamp")
	dims := cborDimensions(m)
	storageRes := cborInt32(m, "StorageResolution")
	name := cborStr(m, keyMetricName)
	unit := cborStr(m, "Unit")

	datum := MetricDatum{
		MetricName:        name,
		Unit:              unit,
		Timestamp:         ts,
		Dimensions:        dims,
		StorageResolution: storageRes,
	}

	if values, hasValues := cborFloatList(m, "Values"); hasValues {
		counts, hasCounts := cborFloatList(m, "Counts")
		if !hasCounts {
			counts = make([]float64, len(values))
			for i := range counts {
				counts[i] = 1
			}
		}

		// The backend (storeDatum) derives Sum/SampleCount/Min/Max from
		// Values/Counts at store time, so this only carries the raw arrays.
		datum.HasValuesArray = true
		datum.Values = values
		datum.Counts = counts

		return datum
	}

	if ssMap, ok := cborStatisticValues(m); ok {
		datum.HasStatisticSet = true
		datum.Count = cborFloat(ssMap, "SampleCount")
		datum.Sum = cborFloat(ssMap, statSum)
		datum.Min = cborFloat(ssMap, "Minimum")
		datum.Max = cborFloat(ssMap, "Maximum")

		return datum
	}

	_, hasValue := m[keyValue]
	val := cborFloat(m, keyValue)
	datum.HasValue = hasValue
	datum.Value = val
	datum.Count = 1
	datum.Sum = val
	datum.Min = val
	datum.Max = val

	return datum
}

// cborStatisticValues returns the StatisticValues sub-map when present.
func cborStatisticValues(m cbor.Map) (cbor.Map, bool) {
	ssVal, hasSS := m["StatisticValues"]
	if !hasSS {
		return nil, false
	}
	ssMap, isSS := ssVal.(cbor.Map)

	return ssMap, isSS
}

// cborFloatList extracts a []float64 from a CBOR map by key. ok is false when
// the key is absent so callers can distinguish "no list" from an empty one.
func cborFloatList(m cbor.Map, key string) ([]float64, bool) {
	v, present := m[key]
	if !present {
		return nil, false
	}

	list, isList := v.(cbor.List)
	if !isList {
		return nil, false
	}

	vals := make([]float64, 0, len(list))
	for _, item := range list {
		vals = append(vals, cborValFloat(item))
	}

	return vals, true
}

func (h *Handler) cborPutMetricData(input cbor.Map, c *echo.Context) error {
	namespace := cborStr(input, keyNamespace)
	if namespace == "" {
		return h.cborError(
			c,
			http.StatusBadRequest,
			"InvalidParameterValue",
			"Namespace is required",
		)
	}

	var data []MetricDatum
	if listVal, hasData := input["MetricData"]; hasData {
		if list, isList := listVal.(cbor.List); isList {
			for _, item := range list {
				m, isMap := item.(cbor.Map)
				if !isMap {
					continue
				}
				data = append(data, cborDecodeDatum(m))
			}
		}
	}

	if err := h.Backend.PutMetricData(namespace, data); err != nil {
		return h.cborError(c, putMetricDataErrorStatus(err), putMetricDataCBORErrorCode(err), err.Error())
	}

	// PutMetricDataOutput has no members besides the request ID: CloudWatch has
	// no partial-success concept for this operation.
	return writeCBOR(c, cbor.Map{})
}

func (h *Handler) cborGetMetricStatistics(input cbor.Map, c *echo.Context) error {
	namespace := cborStr(input, keyNamespace)
	metricName := cborStr(input, keyMetricName)
	startTime := cborTime(input, "StartTime")
	endTime := cborTime(input, "EndTime")
	period := cborInt32(input, "Period")

	if period <= 0 {
		period = 60
	}

	dimensions := cborDimensions(input)
	statistics := cborStrList(input, "Statistics")
	extendedStatistics := cborStrList(input, "ExtendedStatistics")

	dps, err := h.Backend.GetMetricStatistics(
		namespace,
		metricName,
		dimensions,
		startTime,
		endTime,
		period,
		statistics,
		extendedStatistics,
	)
	if err != nil {
		return h.cborError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
	}

	dpList := make(cbor.List, 0, len(dps))

	for _, dp := range dps {
		m := cbor.Map{"Timestamp": cborFromTime(dp.Timestamp)}

		if dp.Average != nil {
			m["Average"] = cbor.Float64(*dp.Average)
		}

		if dp.Sum != nil {
			m[statSum] = cbor.Float64(*dp.Sum)
		}

		if dp.Minimum != nil {
			m["Minimum"] = cbor.Float64(*dp.Minimum)
		}

		if dp.Maximum != nil {
			m["Maximum"] = cbor.Float64(*dp.Maximum)
		}

		if dp.SampleCount != nil {
			m["SampleCount"] = cbor.Float64(*dp.SampleCount)
		}

		if len(dp.ExtendedStatistics) > 0 {
			es := make(cbor.Map, len(dp.ExtendedStatistics))
			for k, v := range dp.ExtendedStatistics {
				es[k] = cbor.Float64(v)
			}

			m["ExtendedStatistics"] = es
		}

		if dp.Unit != "" {
			m["Unit"] = cbor.String(dp.Unit)
		}

		dpList = append(dpList, m)
	}

	return writeCBOR(c, cbor.Map{
		"Label":      cbor.String(metricName),
		"Datapoints": dpList,
	})
}

// applyMetricStatToQuery fills q.MetricStat from a CBOR MetricStat map.
func applyMetricStatToQuery(q *MetricDataQuery, msMap cbor.Map) {
	q.MetricStat = MetricStat{
		Stat:   cborStr(msMap, "Stat"),
		Period: cborInt32(msMap, "Period"),
	}

	if mVal, hasMet := msMap["Metric"]; hasMet {
		if mMap, isMMap := mVal.(cbor.Map); isMMap {
			q.MetricStat.Namespace = cborStr(mMap, keyNamespace)
			q.MetricStat.MetricName = cborStr(mMap, keyMetricName)
			q.MetricStat.Dimensions = cborDimensions(mMap)
		}
	}
}

// parseMetricDataQueries extracts MetricDataQueries from a CBOR map.
func parseMetricDataQueries(input cbor.Map) []MetricDataQuery {
	listVal, hasQueries := input["MetricDataQueries"]
	if !hasQueries {
		return nil
	}

	list, isList := listVal.(cbor.List)
	if !isList {
		return nil
	}

	queries := make([]MetricDataQuery, 0, len(list))

	for _, item := range list {
		m, isMap := item.(cbor.Map)
		if !isMap {
			continue
		}

		// ReturnData defaults to true; only suppress when caller explicitly passes false.
		returnData := true
		if rdVal, hasRD := m["ReturnData"]; hasRD {
			if rdBool, isBool := rdVal.(cbor.Bool); isBool {
				returnData = bool(rdBool)
			}
		}

		q := MetricDataQuery{
			ID:         cborStr(m, "Id"),
			Label:      cborStr(m, "Label"),
			Expression: cborStr(m, "Expression"),
			AccountID:  cborStr(m, "AccountId"),
			ReturnData: returnData,
		}

		if msVal, hasMS := m["MetricStat"]; hasMS {
			if msMap, isMSMap := msVal.(cbor.Map); isMSMap {
				applyMetricStatToQuery(&q, msMap)
			}
		}

		if q.ID != "" {
			queries = append(queries, q)
		}
	}

	return queries
}

func (h *Handler) cborGetMetricData(input cbor.Map, c *echo.Context) error {
	startTime := cborTime(input, "StartTime")
	endTime := cborTime(input, "EndTime")
	scanBy := cborStr(input, "ScanBy")
	nextToken := cborStr(input, "NextToken")
	maxDatapoints := int(cborInt32(input, "MaxDatapoints"))
	queries := parseMetricDataQueries(input)

	var pageResult GetMetricDataPage
	var err error
	if bk, ok := h.Backend.(*InMemoryBackend); ok {
		pageResult, err = bk.GetMetricDataPaged(
			queries, startTime, endTime, scanBy, nextToken, maxDatapoints,
		)
	} else {
		var results []MetricDataResult
		results, err = h.Backend.GetMetricData(queries, startTime, endTime)
		pageResult.Results = results
	}
	if err != nil {
		return h.cborError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
	}

	resultList := make(cbor.List, 0, len(pageResult.Results))

	for _, r := range pageResult.Results {
		tsList := make(cbor.List, 0, len(r.Timestamps))
		for _, ts := range r.Timestamps {
			tsList = append(tsList, cborFromTime(ts))
		}

		valList := make(cbor.List, 0, len(r.Values))
		for _, v := range r.Values {
			valList = append(valList, cbor.Float64(v))
		}

		entry := cbor.Map{
			"Id":         cbor.String(r.ID),
			"Label":      cbor.String(r.Label),
			"StatusCode": cbor.String(r.StatusCode),
			"Timestamps": tsList,
			"Values":     valList,
		}
		if len(r.Messages) > 0 {
			entry["Messages"] = cborMetricDataMessages(r.Messages)
		}

		resultList = append(resultList, entry)
	}

	resp := cbor.Map{
		"MetricDataResults": resultList,
	}
	if pageResult.NextToken != "" {
		resp["NextToken"] = cbor.String(pageResult.NextToken)
	}
	if len(pageResult.Messages) > 0 {
		resp["Messages"] = cborMetricDataMessages(pageResult.Messages)
	}

	return writeCBOR(c, resp)
}

// cborMetricDataMessages encodes a slice of MetricDataMessage into a CBOR list.
func cborMetricDataMessages(msgs []MetricDataMessage) cbor.List {
	list := make(cbor.List, 0, len(msgs))
	for _, m := range msgs {
		list = append(list, cbor.Map{
			"Code":  cbor.String(m.Code),
			"Value": cbor.String(m.Value),
		})
	}

	return list
}

func (h *Handler) cborListMetrics(input cbor.Map, c *echo.Context) error {
	namespace := cborStr(input, keyNamespace)
	metricName := cborStr(input, keyMetricName)
	nextToken := cborStr(input, "NextToken")
	recentlyActive := cborStr(input, "RecentlyActive")
	maxResults := int(cborInt32(input, "MaxResults"))
	dimensions := cborDimensions(input)

	p, err := h.Backend.ListMetrics(namespace, metricName, dimensions, recentlyActive, nextToken, maxResults)
	if err != nil {
		if errors.Is(err, ErrValidation) {
			return h.cborError(c, http.StatusBadRequest, "InvalidParameterValueException", err.Error())
		}

		return h.cborError(c, http.StatusInternalServerError, errCodeInternalFailure, err.Error())
	}

	mList := make(cbor.List, 0, len(p.Data))

	for _, m := range p.Data {
		mm := cbor.Map{
			keyNamespace:  cbor.String(m.Namespace),
			keyMetricName: cbor.String(m.MetricName),
		}
		if len(m.Dimensions) > 0 {
			dimList := make(cbor.List, 0, len(m.Dimensions))
			for _, d := range m.Dimensions {
				dimList = append(dimList, cbor.Map{
					keyName:  cbor.String(d.Name),
					keyValue: cbor.String(d.Value),
				})
			}
			mm["Dimensions"] = dimList
		}
		mList = append(mList, mm)
	}

	resp := cbor.Map{
		"Metrics": mList,
	}
	if p.Next != "" {
		resp["NextToken"] = cbor.String(p.Next)
	}

	return writeCBOR(c, resp)
}

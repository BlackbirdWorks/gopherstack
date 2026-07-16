package rds

import (
	"encoding/xml"
	"fmt"
	"net/url"
	"strconv"
	"time"
)

func (h *Handler) handleGetPerformanceInsightsMetricsReal(vals url.Values) (any, error) {
	resourceID := vals.Get("DBResourceIdentifier")
	if resourceID == "" {
		resourceID = vals.Get("Identifier")
	}

	period := parsePIPeriod(vals.Get("PeriodInSeconds"))
	startTime, endTime := parsePITimeRange(vals.Get("StartTime"), vals.Get("EndTime"))
	metricKeyDataPoints, err := h.collectPIMetrics(vals, resourceID, startTime, endTime, period)
	if err != nil {
		return nil, err
	}

	return &getPerformanceInsightsMetricsResponse{
		Xmlns:            rdsXMLNS,
		AlignedStartTime: startTime.UTC().Format(time.RFC3339),
		AlignedEndTime:   endTime.UTC().Format(time.RFC3339),
		MetricList:       xmlMetricKeyDataPointsList{Members: metricKeyDataPoints},
	}, nil
}

func parsePIPeriod(raw string) int {
	const defaultPeriod = 60
	if raw == "" {
		return defaultPeriod
	}
	v, err := strconv.Atoi(raw)
	if err != nil || v <= 0 {
		return defaultPeriod
	}

	return v
}

func parsePITimeRange(startRaw, endRaw string) (time.Time, time.Time) {
	var start, end time.Time

	if endRaw != "" {
		if t, err := time.Parse(time.RFC3339, endRaw); err == nil {
			end = t
		}
	}

	if end.IsZero() {
		end = time.Now().UTC()
	}

	if startRaw != "" {
		if t, err := time.Parse(time.RFC3339, startRaw); err == nil {
			start = t
		}
	}

	if start.IsZero() {
		start = end.Add(-time.Hour)
	}

	return start, end
}

func (h *Handler) collectPIMetrics(
	vals url.Values,
	resourceID string,
	startTime, endTime time.Time,
	period int,
) ([]xmlMetricKeyDataPoints, error) {
	var result []xmlMetricKeyDataPoints

	for i := 1; ; i++ {
		metric := vals.Get(fmt.Sprintf("MetricQueries.member.%d.Metric", i))
		if metric == "" {
			if i == 1 {
				metric = "db.load.avg"
			} else {
				break
			}
		}

		points, err := h.Backend.GetPerformanceInsightsData(resourceID, metric, startTime, endTime, period)
		if err != nil {
			return nil, err
		}
		dataPoints := make([]xmlDataPoint, 0, len(points))

		for _, p := range points {
			dataPoints = append(dataPoints, xmlDataPoint(p))
		}

		result = append(result, xmlMetricKeyDataPoints{Metric: metric, DataPoints: dataPoints})

		if vals.Get(fmt.Sprintf("MetricQueries.member.%d.Metric", i+1)) == "" {
			break
		}
	}

	return result, nil
}

// parseFloat parses a string as float64, returning 0 on error.
func parseFloat(s string) float64 {
	if s == "" {
		return 0
	}
	v, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return 0
	}

	return v
}

// parseInt parses a string as int, returning 0 on error.
func parseInt(s string) int {
	if s == "" {
		return 0
	}
	v, err := strconv.Atoi(s)
	if err != nil {
		return 0
	}

	return v
}

type xmlDataPoint struct {
	Timestamp string  `xml:"Timestamp"`
	Value     float64 `xml:"Value"`
}

type xmlMetricKeyDataPoints struct {
	Metric     string         `xml:"Key>Metric"`
	DataPoints []xmlDataPoint `xml:"DataPoints>DataPoint"`
}

type xmlMetricKeyDataPointsList struct {
	Members []xmlMetricKeyDataPoints `xml:"MetricKeyDataPoints"`
}

type getPerformanceInsightsMetricsResponse struct {
	XMLName          xml.Name                   `xml:"GetPerformanceInsightsMetricsResponse"`
	Xmlns            string                     `xml:"xmlns,attr"`
	AlignedStartTime string                     `xml:"GetPerformanceInsightsMetricsResult>AlignedStartTime,omitempty"`
	AlignedEndTime   string                     `xml:"GetPerformanceInsightsMetricsResult>AlignedEndTime,omitempty"`
	MetricList       xmlMetricKeyDataPointsList `xml:"GetPerformanceInsightsMetricsResult>MetricList"`
}

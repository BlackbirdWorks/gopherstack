package rds

import (
	"encoding/xml"
	"fmt"
	"net/url"
	"strconv"
	"time"
)

// dispatchExtended16 routes batch-3 operations: DescribeCustomDBEngineVersions.
// Called from the default branch of dispatchExtended15.
func (h *Handler) dispatchExtended16(action string, vals url.Values) (any, error) {
	switch action {
	case "DescribeCustomDBEngineVersions":
		return h.handleDescribeCustomDBEngineVersions(vals)
	default:
		return nil, fmt.Errorf("%w: %s is not a valid RDS action", ErrUnknownAction, action)
	}
}

// ---- DescribeCustomDBEngineVersions ----

type xmlCustomDBEngineVersionItem struct {
	Engine                 string `xml:"Engine"`
	EngineVersion          string `xml:"EngineVersion"`
	Status                 string `xml:"Status,omitempty"`
	Description            string `xml:"Description,omitempty"`
	DBParameterGroupFamily string `xml:"DBParameterGroupFamily,omitempty"`
}

type xmlCustomDBEngineVersionList struct {
	Members []xmlCustomDBEngineVersionItem `xml:"CustomDBEngineVersion"`
}

type describeCustomDBEngineVersionsResponse struct {
	XMLName                xml.Name                     `xml:"DescribeCustomDBEngineVersionsResponse"`
	Xmlns                  string                       `xml:"xmlns,attr"`
	Marker                 string                       `xml:"DescribeCustomDBEngineVersionsResult>Marker,omitempty"`
	CustomDBEngineVersions xmlCustomDBEngineVersionList `xml:"DescribeCustomDBEngineVersionsResult>CustomDBEngineVersions"`
}

func (h *Handler) handleDescribeCustomDBEngineVersions(vals url.Values) (any, error) {
	engine := vals.Get("Engine")
	engineVersion := vals.Get("EngineVersion")

	versions := h.Backend.DescribeCustomDBEngineVersions(engine, engineVersion)

	members, marker, err := paginateDescribe(
		vals,
		versions,
		func(a, b CustomDBEngineVersion) bool {
			return a.Engine+"/"+a.EngineVersion < b.Engine+"/"+b.EngineVersion
		},
		func(cev CustomDBEngineVersion) xmlCustomDBEngineVersionItem {
			return xmlCustomDBEngineVersionItem{
				Engine:        cev.Engine,
				EngineVersion: cev.EngineVersion,
				Status:        cev.Status,
				Description:   cev.Description,
			}
		},
	)
	if err != nil {
		return nil, err
	}

	return &describeCustomDBEngineVersionsResponse{
		Xmlns:                  rdsXMLNS,
		Marker:                 marker,
		CustomDBEngineVersions: xmlCustomDBEngineVersionList{Members: members},
	}, nil
}

// ---- Performance Insights (real stateful) ----

func (h *Handler) handleGetPerformanceInsightsMetricsReal(vals url.Values) (any, error) {
	resourceID := vals.Get("DBResourceIdentifier")
	if resourceID == "" {
		resourceID = vals.Get("ResourceIdentifier")
	}

	period := parsePIPeriod(vals.Get("PeriodInSeconds"))
	startTime, endTime := parsePITimeRange(vals.Get("StartTime"), vals.Get("EndTime"))
	metricKeyDataPoints := h.collectPIMetrics(vals, resourceID, startTime, endTime, period)

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
) []xmlMetricKeyDataPoints {
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

		points := h.Backend.GetPerformanceInsightsData(resourceID, metric, startTime, endTime, period)
		dataPoints := make([]xmlDataPoint, 0, len(points))

		for _, p := range points {
			dataPoints = append(dataPoints, xmlDataPoint(p))
		}

		result = append(result, xmlMetricKeyDataPoints{Metric: metric, DataPoints: dataPoints})

		if vals.Get(fmt.Sprintf("MetricQueries.member.%d.Metric", i+1)) == "" {
			break
		}
	}

	return result
}

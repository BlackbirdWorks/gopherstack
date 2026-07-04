package ec2

import (
	"encoding/xml"
	"fmt"
	"net/url"
	"time"
)

// ---- Handler registration ----

func registerNetworkPerformanceOps(h *Handler, ops map[string]ec2ActionFn) {
	ops["EnableAwsNetworkPerformanceMetricSubscription"] = h.handleEnableAwsNetworkPerformanceMetricSubscription
	ops["DisableAwsNetworkPerformanceMetricSubscription"] = h.handleDisableAwsNetworkPerformanceMetricSubscription
	ops["DescribeAwsNetworkPerformanceMetricSubscriptions"] = h.handleDescribeAwsNetworkPerformanceMetricSubscriptions
	ops["GetAwsNetworkPerformanceData"] = h.handleGetAwsNetworkPerformanceData
}

func networkPerformanceSupportedOperations() []string {
	return []string{
		"EnableAwsNetworkPerformanceMetricSubscription",
		"DisableAwsNetworkPerformanceMetricSubscription",
		"DescribeAwsNetworkPerformanceMetricSubscriptions",
		"GetAwsNetworkPerformanceData",
	}
}

// ---- XML entity types ----

type networkPerformanceSubscriptionItem struct {
	Source      string `xml:"source"`
	Destination string `xml:"destination"`
	Metric      string `xml:"metric"`
	Statistic   string `xml:"statistic"`
	Period      string `xml:"period"`
}

func networkPerformanceSubscriptionToItem(s *NetworkPerformanceSubscription) networkPerformanceSubscriptionItem {
	return networkPerformanceSubscriptionItem{
		Source:      s.Source,
		Destination: s.Destination,
		Metric:      s.Metric,
		Statistic:   s.Statistic,
		Period:      s.Period,
	}
}

type metricPointItem struct {
	StartDate string  `xml:"startDate,omitempty"`
	EndDate   string  `xml:"endDate,omitempty"`
	Status    string  `xml:"status,omitempty"`
	Value     float32 `xml:"value"`
}

type dataResponseItem struct {
	ID           string            `xml:"id,omitempty"`
	Source       string            `xml:"source"`
	Destination  string            `xml:"destination"`
	Metric       string            `xml:"metric"`
	Statistic    string            `xml:"statistic"`
	Period       string            `xml:"period"`
	MetricPoints []metricPointItem `xml:"metricPointSet>item"`
}

func dataResponseToItem(r *NetworkPerformanceDataResponse) dataResponseItem {
	item := dataResponseItem{
		ID:          r.ID,
		Source:      r.Source,
		Destination: r.Destination,
		Metric:      r.Metric,
		Statistic:   r.Statistic,
		Period:      r.Period,
	}

	for _, p := range r.MetricPoints {
		item.MetricPoints = append(item.MetricPoints, metricPointItem{
			StartDate: p.StartDate.Format(time.RFC3339),
			EndDate:   p.EndDate.Format(time.RFC3339),
			Status:    p.Status,
			Value:     p.Value,
		})
	}

	return item
}

// ---- XML response types ----

type enableAwsNetworkPerformanceMetricSubscriptionResponse struct {
	XMLName   xml.Name `xml:"EnableAwsNetworkPerformanceMetricSubscriptionResponse"`
	Xmlns     string   `xml:"xmlns,attr"`
	RequestID string   `xml:"requestId"`
	Output    bool     `xml:"output"`
}

type disableAwsNetworkPerformanceMetricSubscriptionResponse struct {
	XMLName   xml.Name `xml:"DisableAwsNetworkPerformanceMetricSubscriptionResponse"`
	Xmlns     string   `xml:"xmlns,attr"`
	RequestID string   `xml:"requestId"`
	Output    bool     `xml:"output"`
}

type describeAwsNetworkPerformanceMetricSubscriptionsResponse struct {
	XMLName       xml.Name                             `xml:"DescribeAwsNetworkPerformanceMetricSubscriptionsResponse"`
	Xmlns         string                               `xml:"xmlns,attr"`
	RequestID     string                               `xml:"requestId"`
	NextToken     string                               `xml:"nextToken,omitempty"`
	Subscriptions []networkPerformanceSubscriptionItem `xml:"subscriptionSet>item"`
}

type getAwsNetworkPerformanceDataResponse struct {
	XMLName       xml.Name           `xml:"GetAwsNetworkPerformanceDataResponse"`
	Xmlns         string             `xml:"xmlns,attr"`
	RequestID     string             `xml:"requestId"`
	NextToken     string             `xml:"nextToken,omitempty"`
	DataResponses []dataResponseItem `xml:"dataResponseSet>item"`
}

// ---- handlers ----

func (h *Handler) handleEnableAwsNetworkPerformanceMetricSubscription(vals url.Values, reqID string) (any, error) {
	ok, err := h.Backend.EnableAwsNetworkPerformanceMetricSubscription(
		vals.Get("Source"),
		vals.Get("Destination"),
		vals.Get("Metric"),
		vals.Get("Statistic"),
	)
	if err != nil {
		return nil, err
	}

	return &enableAwsNetworkPerformanceMetricSubscriptionResponse{
		Xmlns:     ec2XMLNS,
		RequestID: reqID,
		Output:    ok,
	}, nil
}

func (h *Handler) handleDisableAwsNetworkPerformanceMetricSubscription(vals url.Values, reqID string) (any, error) {
	ok, err := h.Backend.DisableAwsNetworkPerformanceMetricSubscription(
		vals.Get("Source"),
		vals.Get("Destination"),
		vals.Get("Metric"),
		vals.Get("Statistic"),
	)
	if err != nil {
		return nil, err
	}

	return &disableAwsNetworkPerformanceMetricSubscriptionResponse{
		Xmlns:     ec2XMLNS,
		RequestID: reqID,
		Output:    ok,
	}, nil
}

func (h *Handler) handleDescribeAwsNetworkPerformanceMetricSubscriptions(_ url.Values, reqID string) (any, error) {
	subs := h.Backend.DescribeAwsNetworkPerformanceMetricSubscriptions()

	resp := &describeAwsNetworkPerformanceMetricSubscriptionsResponse{Xmlns: ec2XMLNS, RequestID: reqID}
	for _, s := range subs {
		resp.Subscriptions = append(resp.Subscriptions, networkPerformanceSubscriptionToItem(s))
	}

	return resp, nil
}

// parseNetworkPerformanceDataQueries extracts DataQuery.N.{Id,Source,Destination,
// Metric,Period,Statistic} form values into backend data query structs.
func parseNetworkPerformanceDataQueries(vals url.Values) []NetworkPerformanceDataQuery {
	var queries []NetworkPerformanceDataQuery

	for i := 1; ; i++ {
		prefix := fmt.Sprintf("DataQuery.%d.", i)

		source := vals.Get(prefix + "Source")
		destination := vals.Get(prefix + "Destination")

		if source == "" && destination == "" {
			break
		}

		queries = append(queries, NetworkPerformanceDataQuery{
			ID:          vals.Get(prefix + "Id"),
			Source:      source,
			Destination: destination,
			Metric:      vals.Get(prefix + "Metric"),
			Statistic:   vals.Get(prefix + "Statistic"),
			Period:      vals.Get(prefix + "Period"),
		})
	}

	return queries
}

func (h *Handler) handleGetAwsNetworkPerformanceData(vals url.Values, reqID string) (any, error) {
	queries := parseNetworkPerformanceDataQueries(vals)
	startTime, _ := time.Parse(time.RFC3339, vals.Get("StartTime"))
	endTime, _ := time.Parse(time.RFC3339, vals.Get("EndTime"))

	responses, err := h.Backend.GetAwsNetworkPerformanceData(queries, startTime, endTime)
	if err != nil {
		return nil, err
	}

	resp := &getAwsNetworkPerformanceDataResponse{Xmlns: ec2XMLNS, RequestID: reqID}
	for _, r := range responses {
		resp.DataResponses = append(resp.DataResponses, dataResponseToItem(r))
	}

	return resp, nil
}

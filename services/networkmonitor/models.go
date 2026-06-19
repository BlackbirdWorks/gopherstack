package networkmonitor

import "time"

// Monitor represents an AWS CloudWatch Network Monitor monitor.
type Monitor struct {
	CreatedAt         *time.Time        `json:"createdAt,omitempty"`
	ModifiedAt        *time.Time        `json:"modifiedAt,omitempty"`
	Tags              map[string]string `json:"tags,omitempty"`
	MonitorArn        string            `json:"monitorArn"`
	MonitorName       string            `json:"monitorName"`
	State             string            `json:"state"`
	Probes            []*Probe          `json:"probes,omitempty"`
	AggregationPeriod int64             `json:"aggregationPeriod"`
}

// Probe represents a network monitor probe.
type Probe struct {
	CreatedAt       *time.Time        `json:"createdAt,omitempty"`
	ModifiedAt      *time.Time        `json:"modifiedAt,omitempty"`
	Tags            map[string]string `json:"tags,omitempty"`
	PacketSize      *int32            `json:"packetSize,omitempty"`
	DestinationPort *int32            `json:"destinationPort,omitempty"`
	Destination     string            `json:"destination"`
	SourceArn       string            `json:"sourceArn"`
	Protocol        string            `json:"protocol"`
	State           string            `json:"state"`
	AddressFamily   string            `json:"addressFamily,omitempty"`
	VpcID           string            `json:"vpcId,omitempty"`
	ProbeID         string            `json:"probeId,omitempty"`
	ProbeArn        string            `json:"probeArn,omitempty"`
}

// monitorSummary is the short form returned by ListMonitors.
type monitorSummary struct {
	Tags              map[string]string `json:"tags,omitempty"`
	AggregationPeriod *int64            `json:"aggregationPeriod,omitempty"`
	MonitorArn        string            `json:"monitorArn"`
	MonitorName       string            `json:"monitorName"`
	State             string            `json:"state"`
}

// createMonitorProbeInput is the probe input nested in CreateMonitor.
type createMonitorProbeInput struct {
	Tags            map[string]string `json:"probeTags,omitempty"`
	DestinationPort *int32            `json:"destinationPort,omitempty"`
	PacketSize      *int32            `json:"packetSize,omitempty"`
	Destination     string            `json:"destination"`
	Protocol        string            `json:"protocol"`
	SourceArn       string            `json:"sourceArn"`
}

// createMonitorRequest is the request body for POST /monitors.
type createMonitorRequest struct {
	Tags              map[string]string         `json:"tags,omitempty"`
	AggregationPeriod *int64                    `json:"aggregationPeriod,omitempty"`
	MonitorName       string                    `json:"monitorName"`
	ClientToken       string                    `json:"clientToken,omitempty"`
	Probes            []createMonitorProbeInput `json:"probes,omitempty"`
}

// createMonitorResponse is the response body for POST /monitors.
type createMonitorResponse struct {
	Tags              map[string]string `json:"tags,omitempty"`
	AggregationPeriod *int64            `json:"aggregationPeriod,omitempty"`
	MonitorArn        string            `json:"monitorArn"`
	MonitorName       string            `json:"monitorName"`
	State             string            `json:"state"`
}

// updateMonitorRequest is the request body for PATCH /monitors/{monitorName}.
type updateMonitorRequest struct {
	AggregationPeriod int64 `json:"aggregationPeriod"`
}

// updateMonitorResponse is the response body for PATCH /monitors/{monitorName}.
type updateMonitorResponse struct {
	Tags              map[string]string `json:"tags,omitempty"`
	AggregationPeriod *int64            `json:"aggregationPeriod,omitempty"`
	MonitorArn        string            `json:"monitorArn"`
	MonitorName       string            `json:"monitorName"`
	State             string            `json:"state"`
}

// listMonitorsResponse is the response body for GET /monitors.
type listMonitorsResponse struct {
	NextToken string           `json:"nextToken,omitempty"`
	Monitors  []monitorSummary `json:"monitors"`
}

// probeInput is the probe input for CreateProbe.
type probeInput struct {
	Tags            map[string]string `json:"tags,omitempty"`
	DestinationPort *int32            `json:"destinationPort,omitempty"`
	PacketSize      *int32            `json:"packetSize,omitempty"`
	Destination     string            `json:"destination"`
	Protocol        string            `json:"protocol"`
	SourceArn       string            `json:"sourceArn"`
}

// createProbeRequest is the request body for POST /monitors/{monitorName}/probes.
type createProbeRequest struct {
	Tags        map[string]string `json:"tags,omitempty"`
	Probe       *probeInput       `json:"probe"`
	ClientToken string            `json:"clientToken,omitempty"`
}

// updateProbeRequest is the request body for PATCH /monitors/{monitorName}/probes/{probeId}.
type updateProbeRequest struct {
	Tags            map[string]string `json:"tags,omitempty"`
	DestinationPort *int32            `json:"destinationPort,omitempty"`
	PacketSize      *int32            `json:"packetSize,omitempty"`
	Destination     string            `json:"destination,omitempty"`
	Protocol        string            `json:"protocol,omitempty"`
	State           string            `json:"state,omitempty"`
}

// getMonitorResponse is the response body for GET /monitors/{monitorName}.
// The AWS networkmonitor API models createdAt/modifiedAt as epoch-second
// timestamps (Iso8601Timestamp wire format = JSON Number), so they are emitted
// as numbers rather than RFC3339 strings; otherwise the SDK fails to
// deserialize the response.
type getMonitorResponse struct {
	CreatedAt         *float64          `json:"createdAt,omitempty"`
	ModifiedAt        *float64          `json:"modifiedAt,omitempty"`
	Tags              map[string]string `json:"tags,omitempty"`
	MonitorArn        string            `json:"monitorArn"`
	MonitorName       string            `json:"monitorName"`
	State             string            `json:"state"`
	Probes            []*Probe          `json:"probes,omitempty"`
	AggregationPeriod int64             `json:"aggregationPeriod"`
}

// listTagsForResourceResponse is the response body for GET /tags/{resourceArn}.
type listTagsForResourceResponse struct {
	Tags map[string]string `json:"tags"`
}

// tagResourceRequest is the request body for POST /tags/{resourceArn}.
type tagResourceRequest struct {
	Tags map[string]string `json:"tags"`
}

// errorResponse is the standard error response body.
type errorResponse struct {
	Message string `json:"message"`
}

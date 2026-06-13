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
	ProbeArn        string            `json:"probeArn,omitempty"`
	ProbeId         string            `json:"probeId,omitempty"`
	SourceArn       string            `json:"sourceArn"`
	Destination     string            `json:"destination"`
	Protocol        string            `json:"protocol"`
	State           string            `json:"state"`
	AddressFamily   string            `json:"addressFamily,omitempty"`
	VpcId           string            `json:"vpcId,omitempty"`
	DestinationPort *int32            `json:"destinationPort,omitempty"`
	PacketSize      *int32            `json:"packetSize,omitempty"`
}

// monitorSummary is the short form returned by ListMonitors.
type monitorSummary struct {
	Tags              map[string]string `json:"tags,omitempty"`
	MonitorArn        string            `json:"monitorArn"`
	MonitorName       string            `json:"monitorName"`
	State             string            `json:"state"`
	AggregationPeriod *int64            `json:"aggregationPeriod,omitempty"`
}

// createMonitorProbeInput is the probe input nested in CreateMonitor.
type createMonitorProbeInput struct {
	Tags            map[string]string `json:"probeTags,omitempty"`
	Destination     string            `json:"destination"`
	Protocol        string            `json:"protocol"`
	SourceArn       string            `json:"sourceArn"`
	DestinationPort *int32            `json:"destinationPort,omitempty"`
	PacketSize      *int32            `json:"packetSize,omitempty"`
}

// createMonitorRequest is the request body for POST /monitors.
type createMonitorRequest struct {
	Tags              map[string]string         `json:"tags,omitempty"`
	MonitorName       string                    `json:"monitorName"`
	ClientToken       string                    `json:"clientToken,omitempty"`
	Probes            []createMonitorProbeInput `json:"probes,omitempty"`
	AggregationPeriod *int64                    `json:"aggregationPeriod,omitempty"`
}

// createMonitorResponse is the response body for POST /monitors.
type createMonitorResponse struct {
	Tags              map[string]string `json:"tags,omitempty"`
	MonitorArn        string            `json:"monitorArn"`
	MonitorName       string            `json:"monitorName"`
	State             string            `json:"state"`
	AggregationPeriod *int64            `json:"aggregationPeriod,omitempty"`
}

// updateMonitorRequest is the request body for PATCH /monitors/{monitorName}.
type updateMonitorRequest struct {
	AggregationPeriod int64 `json:"aggregationPeriod"`
}

// updateMonitorResponse is the response body for PATCH /monitors/{monitorName}.
type updateMonitorResponse struct {
	Tags              map[string]string `json:"tags,omitempty"`
	MonitorArn        string            `json:"monitorArn"`
	MonitorName       string            `json:"monitorName"`
	State             string            `json:"state"`
	AggregationPeriod *int64            `json:"aggregationPeriod,omitempty"`
}

// listMonitorsResponse is the response body for GET /monitors.
type listMonitorsResponse struct {
	Monitors  []monitorSummary `json:"monitors"`
	NextToken string           `json:"nextToken,omitempty"`
}

// probeInput is the probe input for CreateProbe.
type probeInput struct {
	Tags            map[string]string `json:"tags,omitempty"`
	Destination     string            `json:"destination"`
	Protocol        string            `json:"protocol"`
	SourceArn       string            `json:"sourceArn"`
	DestinationPort *int32            `json:"destinationPort,omitempty"`
	PacketSize      *int32            `json:"packetSize,omitempty"`
}

// createProbeRequest is the request body for POST /monitors/{monitorName}/probes.
type createProbeRequest struct {
	Tags        map[string]string `json:"tags,omitempty"`
	ClientToken string            `json:"clientToken,omitempty"`
	Probe       *probeInput       `json:"probe"`
}

// updateProbeRequest is the request body for PATCH /monitors/{monitorName}/probes/{probeId}.
type updateProbeRequest struct {
	Tags            map[string]string `json:"tags,omitempty"`
	Destination     string            `json:"destination,omitempty"`
	Protocol        string            `json:"protocol,omitempty"`
	State           string            `json:"state,omitempty"`
	DestinationPort *int32            `json:"destinationPort,omitempty"`
	PacketSize      *int32            `json:"packetSize,omitempty"`
}

// getMonitorResponse is the response body for GET /monitors/{monitorName}.
type getMonitorResponse struct {
	CreatedAt         *time.Time        `json:"createdAt"`
	ModifiedAt        *time.Time        `json:"modifiedAt"`
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

// untagResourceQuery holds query parameters for DELETE /tags/{resourceArn}.
type untagResourceQuery struct {
	TagKeys []string
}

// errorResponse is the standard error response body.
type errorResponse struct {
	Message string `json:"message"`
}

package globalaccelerator

import (
	"maps"
	"slices"
	"time"
)

const (
	statusDeployed   = "DEPLOYED"
	statusInProgress = "IN_PROGRESS"

	ipTypeV4        = "IPV4"
	ipTypeDualStack = "DUAL_STACK"

	protocolTCP = "TCP"
	protocolUDP = "UDP"

	healthProtocolHTTP  = "HTTP"
	healthProtocolHTTPS = "HTTPS"

	affinityNone     = "NONE"
	affinitySourceIP = "SOURCE_IP"

	healthInitial = "INITIAL"
	healthHealthy = "HEALTHY"

	trafficAllow = "ALLOW"
	trafficDeny  = "DENY"

	byoipPendingProvisioning   = "PENDING_PROVISIONING"
	byoipReady                 = "READY"
	byoipPendingAdvertising    = "PENDING_ADVERTISING"
	byoipAdvertising           = "ADVERTISING"
	byoipPendingWithdrawing    = "PENDING_WITHDRAWING"
	byoipPendingDeprovisioning = "PENDING_DEPROVISIONING"
	byoipDeprovisioned         = "DEPROVISIONED"
)

// PortRange is an inclusive port range.
type PortRange struct {
	FromPort int32 `json:"FromPort"`
	ToPort   int32 `json:"ToPort"`
}

// PortOverride maps a listener port to an endpoint port.
type PortOverride struct {
	ListenerPort int32 `json:"ListenerPort"`
	EndpointPort int32 `json:"EndpointPort"`
}

// Resource is a cross-account attachment resource.
type Resource struct {
	Cidr       string `json:"Cidr,omitempty"`
	EndpointID string `json:"EndpointId,omitempty"`
	Region     string `json:"Region,omitempty"`
}

// Event is a timestamped history message.
type Event struct {
	Timestamp time.Time `json:"timestamp"`
	Message   string    `json:"message"`
}

// Accelerator is a standard or custom routing accelerator.
type Accelerator struct {
	CreatedTime      time.Time         `json:"createdTime"`
	LastModifiedTime time.Time         `json:"lastModifiedTime"`
	PendingUntil     time.Time         `json:"pendingUntil"`
	Tags             map[string]string `json:"tags,omitempty"`
	ARN              string            `json:"arn"`
	Name             string            `json:"name"`
	IPAddressType    string            `json:"ipAddressType"`
	DNSName          string            `json:"dnsName"`
	FlowLogsS3Bucket string            `json:"flowLogsS3Bucket,omitempty"`
	FlowLogsS3Prefix string            `json:"flowLogsS3Prefix,omitempty"`
	Status           string            `json:"-"`
	IPv4             []string          `json:"ipv4"`
	IPv6             []string          `json:"ipv6,omitempty"`
	Events           []Event           `json:"events,omitempty"`
	Seq              int64             `json:"seq"`
	Enabled          bool              `json:"enabled"`
	Custom           bool              `json:"custom,omitempty"`
	FlowLogsEnabled  bool              `json:"flowLogsEnabled,omitempty"`
}

// Listener is a standard or custom routing listener.
type Listener struct {
	ARN            string      `json:"arn"`
	AcceleratorARN string      `json:"acceleratorArn"`
	Protocol       string      `json:"protocol,omitempty"`
	ClientAffinity string      `json:"clientAffinity,omitempty"`
	PortRanges     []PortRange `json:"portRanges"`
	Seq            int64       `json:"seq"`
	Custom         bool        `json:"custom,omitempty"`
}

// TrafficRule is one Allow/DenyCustomRoutingTraffic request; later rules win.
type TrafficRule struct {
	State     string   `json:"state"`
	Addresses []string `json:"addresses,omitempty"`
	Ports     []int32  `json:"ports,omitempty"`
}

// Endpoint is an endpoint group member; for custom routing it is a VPC subnet.
type Endpoint struct {
	AddedAt              time.Time     `json:"addedAt"`
	ID                   string        `json:"id"`
	AttachmentARN        string        `json:"attachmentArn,omitempty"`
	SubnetCIDR           string        `json:"subnetCidr,omitempty"`
	DefaultState         string        `json:"defaultState,omitempty"`
	HealthState          string        `json:"-"`
	Rules                []TrafficRule `json:"rules,omitempty"`
	Weight               int32         `json:"weight"`
	ClientIPPreservation bool          `json:"clientIpPreservation"`
}

// Destination is a custom routing destination port range.
type Destination struct {
	Protocols []string `json:"protocols"`
	FromPort  int32    `json:"fromPort"`
	ToPort    int32    `json:"toPort"`
}

// EndpointGroup is a standard or custom routing endpoint group.
type EndpointGroup struct {
	HealthCheckPath     string         `json:"healthCheckPath,omitempty"`
	ARN                 string         `json:"arn"`
	ListenerARN         string         `json:"listenerArn"`
	Region              string         `json:"region"`
	HealthCheckProtocol string         `json:"healthCheckProtocol,omitempty"`
	Endpoints           []Endpoint     `json:"endpoints,omitempty"`
	Destinations        []Destination  `json:"destinations,omitempty"`
	PortOverrides       []PortOverride `json:"portOverrides,omitempty"`
	Seq                 int64          `json:"seq"`
	TrafficDial         float32        `json:"trafficDial"`
	HealthCheckPort     int32          `json:"healthCheckPort"`
	HealthCheckInterval int32          `json:"healthCheckInterval"`
	ThresholdCount      int32          `json:"thresholdCount"`
	Custom              bool           `json:"custom,omitempty"`
}

// Attachment is a cross-account attachment.
type Attachment struct {
	CreatedTime      time.Time         `json:"createdTime"`
	LastModifiedTime time.Time         `json:"lastModifiedTime"`
	Tags             map[string]string `json:"tags,omitempty"`
	ARN              string            `json:"arn"`
	Name             string            `json:"name"`
	OwnerAccountID   string            `json:"ownerAccountId"`
	Principals       []string          `json:"principals,omitempty"`
	Resources        []Resource        `json:"resources,omitempty"`
	Seq              int64             `json:"seq"`
}

// ByoipCidr is an address range brought to Global Accelerator.
type ByoipCidr struct {
	PendingUntil time.Time `json:"pendingUntil"`
	Cidr         string    `json:"cidr"`
	State        string    `json:"state"`
	Events       []Event   `json:"events,omitempty"`
}

func cloneAccelerator(a *Accelerator) *Accelerator {
	cp := *a
	cp.Tags = maps.Clone(a.Tags)
	cp.IPv4 = slices.Clone(a.IPv4)
	cp.IPv6 = slices.Clone(a.IPv6)
	cp.Events = slices.Clone(a.Events)

	return &cp
}

func cloneListener(l *Listener) *Listener {
	cp := *l
	cp.PortRanges = slices.Clone(l.PortRanges)

	return &cp
}

func cloneEndpointGroup(g *EndpointGroup) *EndpointGroup {
	cp := *g
	cp.Endpoints = make([]Endpoint, len(g.Endpoints))

	for i := range g.Endpoints {
		cp.Endpoints[i] = g.Endpoints[i]
		cp.Endpoints[i].Rules = cloneRules(g.Endpoints[i].Rules)
	}

	cp.Destinations = make([]Destination, len(g.Destinations))

	for i, d := range g.Destinations {
		cp.Destinations[i] = Destination{FromPort: d.FromPort, ToPort: d.ToPort, Protocols: slices.Clone(d.Protocols)}
	}

	cp.PortOverrides = slices.Clone(g.PortOverrides)

	return &cp
}

func cloneRules(rules []TrafficRule) []TrafficRule {
	if rules == nil {
		return nil
	}

	out := make([]TrafficRule, len(rules))
	for i, r := range rules {
		out[i] = TrafficRule{State: r.State, Addresses: slices.Clone(r.Addresses), Ports: slices.Clone(r.Ports)}
	}

	return out
}

func cloneAttachment(a *Attachment) *Attachment {
	cp := *a
	cp.Tags = maps.Clone(a.Tags)
	cp.Principals = slices.Clone(a.Principals)
	cp.Resources = slices.Clone(a.Resources)

	return &cp
}

func cloneByoip(c *ByoipCidr) *ByoipCidr {
	cp := *c
	cp.Events = slices.Clone(c.Events)

	return &cp
}

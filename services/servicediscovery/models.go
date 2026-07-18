package servicediscovery

import "time"

// DNSRecord represents a single DNS record configuration in a Cloud Map service.
type DNSRecord struct {
	Type string `json:"type"`
	TTL  int64  `json:"ttl"`
}

// DNSConfig holds the DNS configuration for a Cloud Map service.
type DNSConfig struct {
	NamespaceID   string      `json:"namespaceID,omitempty"`
	RoutingPolicy string      `json:"routingPolicy,omitempty"`
	DNSRecords    []DNSRecord `json:"dnsRecords,omitempty"`
}

// HealthCheckConfig holds the configuration for an AWS-managed HTTP/TCP health check.
type HealthCheckConfig struct {
	Type             string `json:"type"`
	ResourcePath     string `json:"resourcePath,omitempty"`
	FailureThreshold int    `json:"failureThreshold,omitempty"`
}

// HealthCheckCustomConfig holds the configuration for a custom health check.
type HealthCheckCustomConfig struct {
	FailureThreshold int `json:"failureThreshold,omitempty"`
}

// SOA holds the Start of Authority TTL for a DNS namespace.
type SOA struct {
	TTL int64 `json:"ttl"`
}

// DNSProperties holds the DNS-specific properties of a namespace.
type DNSProperties struct {
	SOA          *SOA   `json:"soa,omitempty"`
	HostedZoneID string `json:"hostedZoneId,omitempty"`
}

// HTTPProperties holds the HTTP-specific properties of a namespace.
type HTTPProperties struct {
	HTTPName string `json:"httpName,omitempty"`
}

// NamespaceProperties holds the type-specific properties of a namespace.
type NamespaceProperties struct {
	DNSProperties  *DNSProperties  `json:"dnsProperties,omitempty"`
	HTTPProperties *HTTPProperties `json:"httpProperties,omitempty"`
}

// Namespace represents an AWS Cloud Map namespace.
type Namespace struct {
	CreatedAt    time.Time            `json:"createdAt"`
	Tags         map[string]string    `json:"tags,omitempty"`
	Properties   *NamespaceProperties `json:"properties,omitempty"`
	ID           string               `json:"id"`
	ARN          string               `json:"arn"`
	Name         string               `json:"name"`
	Type         string               `json:"type"`
	Description  string               `json:"description,omitempty"`
	VPC          string               `json:"vpc,omitempty"`
	ServiceCount int                  `json:"serviceCount,omitempty"`
}

// Service represents an AWS Cloud Map service.
type Service struct {
	CreatedAt               time.Time                `json:"createdAt"`
	Tags                    map[string]string        `json:"tags,omitempty"`
	DNSConfig               *DNSConfig               `json:"dnsConfig,omitempty"`
	HealthCheckConfig       *HealthCheckConfig       `json:"healthCheckConfig,omitempty"`
	HealthCheckCustomConfig *HealthCheckCustomConfig `json:"healthCheckCustomConfig,omitempty"`
	ID                      string                   `json:"id"`
	ARN                     string                   `json:"arn"`
	Name                    string                   `json:"name"`
	NamespaceID             string                   `json:"namespaceID"`
	Description             string                   `json:"description,omitempty"`
	Type                    string                   `json:"type,omitempty"`
	InstanceCount           int                      `json:"instanceCount,omitempty"`
}

// Instance represents a registered instance in a Cloud Map service.
type Instance struct {
	Attributes map[string]string `json:"attributes,omitempty"`
	ID         string            `json:"id"`
	ServiceID  string            `json:"serviceID"`
}

// DiscoveredInstance is the richer per-instance response for DiscoverInstances.
type DiscoveredInstance struct {
	Attributes    map[string]string
	InstanceID    string
	NamespaceName string
	ServiceName   string
	HealthStatus  string
}

// Operation represents an async Cloud Map operation (e.g., create/delete namespace).
type Operation struct {
	CreateDate   time.Time         `json:"createDate"`
	UpdateDate   time.Time         `json:"updateDate"`
	Targets      map[string]string `json:"targets,omitempty"`
	ID           string            `json:"id"`
	Type         string            `json:"type"`
	Status       string            `json:"status"`
	ErrorCode    string            `json:"errorCode,omitempty"`
	ErrorMessage string            `json:"errorMessage,omitempty"`
}

// ListNamespacesFilter contains optional filter parameters for ListNamespaces.
type ListNamespacesFilter struct {
	Type string
	Name string
}

// ListServicesFilter contains optional filter parameters for ListServices.
type ListServicesFilter struct {
	NamespaceID string
}

// ListOperationsFilter contains optional filter parameters for ListOperations.
type ListOperationsFilter struct {
	Status string
	Type   string
}

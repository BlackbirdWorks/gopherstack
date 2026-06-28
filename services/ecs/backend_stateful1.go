package ecs

import (
	"fmt"

	"github.com/google/uuid"
)

// DeploymentCircuitBreaker configures the deployment circuit breaker for a service.
type DeploymentCircuitBreaker struct {
	Enable   bool `json:"enable"`
	Rollback bool `json:"rollback"`
}

// DeploymentConfiguration holds deployment settings for a service.
// MinimumHealthyPercent and MaximumPercent default to the AWS values of 100 and 200.
type DeploymentConfiguration struct {
	DeploymentCircuitBreaker *DeploymentCircuitBreaker `json:"deploymentCircuitBreaker,omitempty"`
	MinimumHealthyPercent    *int                      `json:"minimumHealthyPercent,omitempty"`
	MaximumPercent           *int                      `json:"maximumPercent,omitempty"`
}

const (
	defaultMinimumHealthyPercent = 100
	defaultMaximumPercent        = 200
)

// withAWSDefaults fills in AWS-mandated defaults for unset deployment configuration fields.
func (dc *DeploymentConfiguration) withAWSDefaults() *DeploymentConfiguration {
	if dc == nil {
		minPct := defaultMinimumHealthyPercent
		maxPct := defaultMaximumPercent

		return &DeploymentConfiguration{
			MinimumHealthyPercent: &minPct,
			MaximumPercent:        &maxPct,
		}
	}

	out := *dc

	if out.MinimumHealthyPercent == nil {
		minPct := defaultMinimumHealthyPercent
		out.MinimumHealthyPercent = &minPct
	}

	if out.MaximumPercent == nil {
		maxPct := defaultMaximumPercent
		out.MaximumPercent = &maxPct
	}

	return &out
}

// PlacementConstraint specifies a placement constraint for a task or service.
type PlacementConstraint struct {
	Type       string `json:"type,omitempty"`
	Expression string `json:"expression,omitempty"`
}

// PlacementStrategy specifies a placement strategy for tasks.
type PlacementStrategy struct {
	Type  string `json:"type,omitempty"`
	Field string `json:"field,omitempty"`
}

// TaskAttachment represents an ENI or other attachment on a task (e.g. Fargate ENI).
type TaskAttachment struct {
	ID      string         `json:"id"`
	Type    string         `json:"type"`
	Status  string         `json:"status"`
	Details []KeyValuePair `json:"details,omitempty"`
}

const (
	connectivityConnected = "CONNECTED"

	// eniIDLen is the length of a UUID-derived suffix used as ENI attachment IDs.
	eniIDLen = 36

	// eniSubnetSuffixLen is the number of task ARN chars used as the subnet ID suffix.
	eniSubnetSuffixLen = 8

	// ipOctetMod is the modulus applied to UUID bytes to form private IP octets.
	ipOctetMod = 256

	// ipPrivateClass is the first octet of simulated Fargate private IPs (10.x.y.z).
	ipPrivateClass = 10
)

// safeLastN returns the last n bytes of s, padding by repetition if shorter.
func safeLastN(s string, n int) string {
	if len(s) >= n {
		return s[len(s)-n:]
	}

	// pad by repeating until long enough
	for len(s) < n {
		s += s
	}

	return s[len(s)-n:]
}

// newFargateTaskAttachment builds a simulated Fargate ENI attachment for a task ARN.
// Each task gets a unique ENI ID, MAC address, and private IP derived from a random UUID
// so that callers can distinguish attachments across tasks.
func newFargateTaskAttachment(taskArn string) TaskAttachment {
	id := uuid.NewString()

	// Derive a stable but unique 12-hex-char "MAC" from the first 12 chars of the UUID
	// (no dashes). Format as colon-separated pairs.
	macRaw := id[:8] + id[9:13] // 12 hex chars from UUID without dashes
	mac := fmt.Sprintf("%s:%s:%s:%s:%s:%s",
		macRaw[0:2], macRaw[2:4], macRaw[4:6],
		macRaw[6:8], macRaw[8:10], macRaw[10:12],
	)

	// Derive a unique /16 private IP: 10.x.y.z where x.y come from the last two
	// bytes of the attachment UUID (gives 65536 distinct IPs before collision).
	ipSuffix := id[len(id)-9:] // last 9 chars of UUID: "xxxxxxxx-" → no, use raw hex
	_ = ipSuffix
	eniShort := id[:8] // 8-char prefix as eni ID suffix
	eniID := "eni-" + eniShort

	// Use the task ARN suffix as a stable subnet hint (all tasks in the same cluster
	// share a synthetic subnet derived from that cluster's tasks).
	subnetID := "subnet-" + safeLastN(taskArn, eniSubnetSuffixLen)

	// Build a unique private IP from the attachment UUID bytes.
	octet3 := int(id[0]) % ipOctetMod
	octet4 := int(id[1]) % ipOctetMod
	privateIP := fmt.Sprintf("%d.0.%d.%d", ipPrivateClass, octet3, octet4)
	privateDNS := fmt.Sprintf("ip-%d-0-%d-%d.ec2.internal", ipPrivateClass, octet3, octet4)

	return TaskAttachment{
		ID:     safeLastN(taskArn, eniIDLen),
		Type:   "ElasticNetworkInterface",
		Status: "ATTACHED",
		Details: []KeyValuePair{
			{Name: "subnetId", Value: subnetID},
			{Name: "networkInterfaceId", Value: eniID},
			{Name: "macAddress", Value: mac},
			{Name: "privateDnsName", Value: privateDNS},
			{Name: "privateIPv4Address", Value: privateIP},
		},
	}
}

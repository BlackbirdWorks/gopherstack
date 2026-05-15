package ecs

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
func newFargateTaskAttachment(taskArn string) TaskAttachment {
	return TaskAttachment{
		ID:     safeLastN(taskArn, eniIDLen),
		Type:   "ElasticNetworkInterface",
		Status: "ATTACHED",
		Details: []KeyValuePair{
			{Name: "subnetId", Value: "subnet-00000000"},
			{Name: "networkInterfaceId", Value: "eni-00000000"},
			{Name: "macAddress", Value: "02:00:00:00:00:00"},
			{Name: "privateDnsName", Value: "ip-10-0-0-1.ec2.internal"},
			{Name: "privateIPv4Address", Value: "10.0.0.1"},
		},
	}
}

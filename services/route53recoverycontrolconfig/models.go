package route53recoverycontrolconfig

import "time"

const (
	statusPending         = "PENDING"
	statusDeployed        = "DEPLOYED"
	statusPendingDeletion = "PENDING_DELETION"

	ruleAssertion = "ASSERTION"
	ruleGating    = "GATING"

	ruleTypeAtLeast = "ATLEAST"
	ruleTypeAnd     = "AND"
	ruleTypeOr      = "OR"

	stateOn  = "On"
	stateOff = "Off"

	networkIPv4      = "IPV4"
	networkDualstack = "DUALSTACK"

	arnService = "route53-recovery-control"
)

// Cluster is a Route 53 ARC cluster.
type Cluster struct {
	CreatedAt   time.Time         `json:"createdAt"`
	DeletedAt   *time.Time        `json:"deletedAt,omitempty"`
	Tags        map[string]string `json:"tags,omitempty"`
	ARN         string            `json:"arn"`
	Name        string            `json:"name"`
	NetworkType string            `json:"networkType"`
}

// ControlPanel groups routing controls and safety rules inside a cluster.
type ControlPanel struct {
	CreatedAt  time.Time         `json:"createdAt"`
	DeletedAt  *time.Time        `json:"deletedAt,omitempty"`
	Tags       map[string]string `json:"tags,omitempty"`
	ARN        string            `json:"arn"`
	ClusterARN string            `json:"clusterArn"`
	Name       string            `json:"name"`
	Default    bool              `json:"default,omitempty"`
}

// RoutingControl is an On/Off switch inside a control panel.
type RoutingControl struct {
	CreatedAt  time.Time  `json:"createdAt"`
	DeletedAt  *time.Time `json:"deletedAt,omitempty"`
	ARN        string     `json:"arn"`
	PanelARN   string     `json:"panelArn"`
	ClusterARN string     `json:"clusterArn"`
	Name       string     `json:"name"`
	State      string     `json:"state"`
}

// SafetyRule is an ASSERTION or GATING rule guarding routing control updates.
type SafetyRule struct {
	CreatedAt    time.Time         `json:"createdAt"`
	LastChangeAt time.Time         `json:"lastChangeAt"`
	DeletedAt    *time.Time        `json:"deletedAt,omitempty"`
	Tags         map[string]string `json:"tags,omitempty"`
	ARN          string            `json:"arn"`
	PanelARN     string            `json:"panelArn"`
	Name         string            `json:"name"`
	Kind         string            `json:"kind"`
	RuleType     string            `json:"ruleType"`
	Controls     []string          `json:"controls"`
	Targets      []string          `json:"targets,omitempty"`
	Threshold    int32             `json:"threshold"`
	WaitPeriodMs int32             `json:"waitPeriodMs"`
	Inverted     bool              `json:"inverted"`
}

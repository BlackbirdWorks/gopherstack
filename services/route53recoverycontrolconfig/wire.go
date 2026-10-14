package route53recoverycontrolconfig

// Wire shapes follow route53recoverycontrolconfig@v1.40.4 serializers.go/deserializers.go (PascalCase members).

type wireEndpoint struct {
	Endpoint string `json:"Endpoint"`
	Region   string `json:"Region"`
}

type wireCluster struct {
	ClusterArn       string         `json:"ClusterArn"`
	Name             string         `json:"Name"`
	NetworkType      string         `json:"NetworkType"`
	Owner            string         `json:"Owner"`
	Status           string         `json:"Status"`
	ClusterEndpoints []wireEndpoint `json:"ClusterEndpoints,omitempty"`
}

type wireControlPanel struct {
	ClusterArn          string `json:"ClusterArn"`
	ControlPanelArn     string `json:"ControlPanelArn"`
	Name                string `json:"Name"`
	Owner               string `json:"Owner"`
	Status              string `json:"Status"`
	DefaultControlPanel bool   `json:"DefaultControlPanel"`
	RoutingControlCount int32  `json:"RoutingControlCount"`
}

type wireRoutingControl struct {
	ControlPanelArn   string `json:"ControlPanelArn"`
	Name              string `json:"Name"`
	Owner             string `json:"Owner"`
	RoutingControlArn string `json:"RoutingControlArn"`
	Status            string `json:"Status"`
}

type wireRuleConfig struct {
	Type      string `json:"Type"`
	Threshold int32  `json:"Threshold"`
	Inverted  bool   `json:"Inverted"`
}

type wireAssertionRule struct {
	RuleConfig       *wireRuleConfig `json:"RuleConfig"`
	ControlPanelArn  string          `json:"ControlPanelArn"`
	Name             string          `json:"Name"`
	SafetyRuleArn    string          `json:"SafetyRuleArn"`
	Status           string          `json:"Status"`
	Owner            string          `json:"Owner"`
	AssertedControls []string        `json:"AssertedControls"`
	WaitPeriodMs     int32           `json:"WaitPeriodMs"`
}

type wireGatingRule struct {
	RuleConfig      *wireRuleConfig `json:"RuleConfig"`
	ControlPanelArn string          `json:"ControlPanelArn"`
	Name            string          `json:"Name"`
	SafetyRuleArn   string          `json:"SafetyRuleArn"`
	Status          string          `json:"Status"`
	Owner           string          `json:"Owner"`
	GatingControls  []string        `json:"GatingControls"`
	TargetControls  []string        `json:"TargetControls"`
	WaitPeriodMs    int32           `json:"WaitPeriodMs"`
}

type wireRule struct {
	Assertion *wireAssertionRule `json:"ASSERTION,omitempty"`
	Gating    *wireGatingRule    `json:"GATING,omitempty"`
}

type wireNewRuleConfig struct {
	Inverted  *bool  `json:"Inverted"`
	Threshold *int32 `json:"Threshold"`
	Type      string `json:"Type"`
}

type wireNewAssertionRule struct {
	RuleConfig       *wireNewRuleConfig `json:"RuleConfig"`
	WaitPeriodMs     *int32             `json:"WaitPeriodMs"`
	ControlPanelArn  string             `json:"ControlPanelArn"`
	Name             string             `json:"Name"`
	AssertedControls []string           `json:"AssertedControls"`
}

type wireNewGatingRule struct {
	RuleConfig      *wireNewRuleConfig `json:"RuleConfig"`
	WaitPeriodMs    *int32             `json:"WaitPeriodMs"`
	ControlPanelArn string             `json:"ControlPanelArn"`
	Name            string             `json:"Name"`
	GatingControls  []string           `json:"GatingControls"`
	TargetControls  []string           `json:"TargetControls"`
}

type wireRuleUpdate struct {
	WaitPeriodMs  *int32 `json:"WaitPeriodMs"`
	Name          string `json:"Name"`
	SafetyRuleArn string `json:"SafetyRuleArn"`
}

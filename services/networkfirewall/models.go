package networkfirewall

import "time"

const (
	statusProvisioning = "PROVISIONING"
	statusReady        = "READY"
	statusDeleting     = "DELETING"
	statusFailed       = "FAILED"

	syncPending = "PENDING"
	syncInSync  = "IN_SYNC"

	attachmentCreating = "CREATING"
	attachmentReady    = "READY"
	attachmentFailed   = "FAILED"

	resourceStatusActive = "ACTIVE"

	ruleGroupStateless = "STATELESS"
	ruleGroupStateful  = "STATEFUL"

	kindFirewall  = "firewall"
	kindPolicy    = "firewall-policy"
	kindStateful  = "stateful-rulegroup"
	kindStateless = "stateless-rulegroup"
	kindTLS       = "tls-configuration"

	kindProxy          = "proxy"
	kindProxyConfig    = "proxy-configuration"
	kindProxyRuleGroup = "proxy-rule-group"
	kindVpcEndpoint    = "vpc-endpoint-association"
	kindContainer      = "container-association"
)

type firewallRecord struct {
	TGWUntil            time.Time                `json:"tgwUntil"`
	ProxySettings       *ProxySettings           `json:"proxySettings,omitempty"`
	TGWID               string                   `json:"transitGatewayId,omitempty"`
	TGWOwner            string                   `json:"transitGatewayOwner,omitempty"`
	TGWAttachmentID     string                   `json:"tgwAttachmentId,omitempty"`
	TGWStatus           string                   `json:"tgwStatus,omitempty"`
	AZMappings          []string                 `json:"azMappings,omitempty"`
	ProvisionUntil      time.Time                `json:"provisionUntil"`
	SyncUntil           time.Time                `json:"syncUntil"`
	DeleteUntil         time.Time                `json:"deleteUntil"`
	Encryption          *EncryptionConfiguration `json:"encryption,omitempty"`
	Logging             *LoggingConfiguration    `json:"logging,omitempty"`
	Tags                map[string]string        `json:"tags,omitempty"`
	Endpoints           map[string]string        `json:"endpoints"`
	Zones               map[string]string        `json:"zones"`
	Name                string                   `json:"name"`
	ID                  string                   `json:"id"`
	ARN                 string                   `json:"arn"`
	PolicyARN           string                   `json:"policyArn"`
	VpcID               string                   `json:"vpcId"`
	Description         string                   `json:"description,omitempty"`
	UpdateToken         string                   `json:"updateToken"`
	SubnetMappings      []SubnetMapping          `json:"subnetMappings"`
	AnalysisTypes       []string                 `json:"analysisTypes,omitempty"`
	DeleteProtection    bool                     `json:"deleteProtection"`
	SubnetProtection    bool                     `json:"subnetProtection"`
	PolicyProtection    bool                     `json:"policyProtection"`
	ZoneProtection      bool                     `json:"zoneProtection"`
	MonitoringDashboard bool                     `json:"monitoringDashboard"`
}

type policyRecord struct {
	LastModified time.Time                `json:"lastModified"`
	Encryption   *EncryptionConfiguration `json:"encryption,omitempty"`
	Tags         map[string]string        `json:"tags,omitempty"`
	Name         string                   `json:"name"`
	ID           string                   `json:"id"`
	ARN          string                   `json:"arn"`
	Description  string                   `json:"description,omitempty"`
	UpdateToken  string                   `json:"updateToken"`
	Policy       FirewallPolicy           `json:"policy"`
}

type ruleGroupRecord struct {
	LastModified time.Time                `json:"lastModified"`
	Tags         map[string]string        `json:"tags,omitempty"`
	Encryption   *EncryptionConfiguration `json:"encryption,omitempty"`
	Source       *SourceMetadata          `json:"source,omitempty"`
	Summary      *SummaryConfiguration    `json:"summary,omitempty"`
	ID           string                   `json:"id"`
	ARN          string                   `json:"arn"`
	Type         string                   `json:"type"`
	Description  string                   `json:"description,omitempty"`
	UpdateToken  string                   `json:"updateToken"`
	Name         string                   `json:"name"`
	RuleGroup    RuleGroup                `json:"ruleGroup"`
	Capacity     int32                    `json:"capacity"`
}

type tlsRecord struct {
	LastModified time.Time                  `json:"lastModified"`
	Encryption   *EncryptionConfiguration   `json:"encryption,omitempty"`
	Tags         map[string]string          `json:"tags,omitempty"`
	Name         string                     `json:"name"`
	ID           string                     `json:"id"`
	ARN          string                     `json:"arn"`
	Description  string                     `json:"description,omitempty"`
	UpdateToken  string                     `json:"updateToken"`
	Config       TLSInspectionConfiguration `json:"config"`
}

type resourcePolicyRecord struct {
	ResourceARN string `json:"resourceArn"`
	Policy      string `json:"policy"`
}

type proxyRecord struct {
	CreateTime   time.Time               `json:"createTime"`
	UpdateTime   time.Time               `json:"updateTime"`
	AttachUntil  time.Time               `json:"attachUntil"`
	ModifyUntil  time.Time               `json:"modifyUntil"`
	DeleteUntil  time.Time               `json:"deleteUntil"`
	TLS          *TLSInterceptProperties `json:"tls,omitempty"`
	Tags         map[string]string       `json:"tags,omitempty"`
	Name         string                  `json:"name"`
	ARN          string                  `json:"arn"`
	ConfigARN    string                  `json:"configArn"`
	ConfigName   string                  `json:"configName"`
	NatGatewayID string                  `json:"natGatewayId"`
	UpdateToken  string                  `json:"updateToken"`
	DNSName      string                  `json:"dnsName"`
	ServiceName  string                  `json:"serviceName"`
	Listeners    []ListenerProperty      `json:"listeners"`
}

type proxyConfigRecord struct {
	CreateTime  time.Time                           `json:"createTime"`
	Defaults    *ProxyConfigDefaultRulePhaseActions `json:"defaults,omitempty"`
	Tags        map[string]string                   `json:"tags,omitempty"`
	Name        string                              `json:"name"`
	ARN         string                              `json:"arn"`
	Description string                              `json:"description,omitempty"`
	UpdateToken string                              `json:"updateToken"`
	Groups      []string                            `json:"groups"`
}

type proxyRuleGroupRecord struct {
	CreateTime  time.Time                `json:"createTime"`
	Tags        map[string]string        `json:"tags,omitempty"`
	Name        string                   `json:"name"`
	ARN         string                   `json:"arn"`
	Description string                   `json:"description,omitempty"`
	UpdateToken string                   `json:"updateToken"`
	Rules       ProxyRulesByRequestPhase `json:"rules"`
}

type vpcEndpointRecord struct {
	ProvisionUntil time.Time         `json:"provisionUntil"`
	DeleteUntil    time.Time         `json:"deleteUntil"`
	Tags           map[string]string `json:"tags,omitempty"`
	Subnet         SubnetMapping     `json:"subnet"`
	ARN            string            `json:"arn"`
	ID             string            `json:"id"`
	FirewallARN    string            `json:"firewallArn"`
	VpcID          string            `json:"vpcId"`
	Zone           string            `json:"zone"`
	EndpointID     string            `json:"endpointId"`
	Description    string            `json:"description,omitempty"`
}

type flowOpRecord struct {
	Requested   time.Time    `json:"requested"`
	DoneAt      time.Time    `json:"doneAt"`
	MinAge      *int32       `json:"minAge,omitempty"`
	Key         string       `json:"key"`
	FirewallARN string       `json:"firewallArn"`
	ID          string       `json:"id"`
	Type        string       `json:"type"`
	Zone        string       `json:"zone,omitempty"`
	AssocARN    string       `json:"assocArn,omitempty"`
	EndpointID  string       `json:"endpointId,omitempty"`
	Filters     []FlowFilter `json:"filters,omitempty"`
}

type analysisReportRecord struct {
	ReportTime  time.Time `json:"reportTime"`
	DoneAt      time.Time `json:"doneAt"`
	Key         string    `json:"key"`
	FirewallARN string    `json:"firewallArn"`
	ID          string    `json:"id"`
	Type        string    `json:"type"`
}

type containerRecord struct {
	LastUpdated time.Time                          `json:"lastUpdated"`
	SettleAt    time.Time                          `json:"settleAt"`
	DeleteUntil time.Time                          `json:"deleteUntil"`
	Tags        map[string]string                  `json:"tags,omitempty"`
	Name        string                             `json:"name"`
	ARN         string                             `json:"arn"`
	Type        string                             `json:"type"`
	Description string                             `json:"description,omitempty"`
	UpdateToken string                             `json:"updateToken"`
	Configs     []ContainerMonitoringConfiguration `json:"configs"`
	Updating    bool                               `json:"updating,omitempty"`
}

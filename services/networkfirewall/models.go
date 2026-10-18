package networkfirewall

import "time"

const (
	statusProvisioning = "PROVISIONING"
	statusReady        = "READY"
	statusDeleting     = "DELETING"

	syncPending = "PENDING"
	syncInSync  = "IN_SYNC"

	attachmentCreating = "CREATING"
	attachmentReady    = "READY"

	resourceStatusActive = "ACTIVE"

	ruleGroupStateless = "STATELESS"
	ruleGroupStateful  = "STATEFUL"

	kindFirewall  = "firewall"
	kindPolicy    = "firewall-policy"
	kindStateful  = "stateful-rulegroup"
	kindStateless = "stateless-rulegroup"
	kindTLS       = "tls-configuration"
)

type firewallRecord struct {
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

package networkfirewall

import "github.com/blackbirdworks/gopherstack/pkgs/tags"

// Wire shapes mirror aws-sdk-go-v2/service/networkfirewall@v1.76.1 types (awsjson1.0, PascalCase members).

// Tag is a key/value resource tag.
type Tag = tags.KV

// EncryptionConfiguration selects the KMS key for a resource.
type EncryptionConfiguration struct {
	KeyID string `json:"KeyId,omitempty"`
	Type  string `json:"Type"`
}

// SubnetMapping is one firewall subnet.
type SubnetMapping struct {
	SubnetID      string `json:"SubnetId"`
	IPAddressType string `json:"IPAddressType,omitempty"`
}

// AvailabilityZoneMapping is a transit-gateway firewall zone.
type AvailabilityZoneMapping struct {
	AvailabilityZone string `json:"AvailabilityZone"`
}

// NatGatewayMapping is a NAT gateway reference.
type NatGatewayMapping struct {
	NatGatewayID string `json:"NatGatewayId,omitempty"`
}

// ListenerProperty is a proxy listener.
type ListenerProperty struct {
	Port *int32 `json:"Port,omitempty"`
	Type string `json:"Type,omitempty"`
}

// ProxySettings holds proxy listeners.
type ProxySettings struct {
	ListenerProperties []ListenerProperty `json:"ListenerProperties,omitempty"`
}

// VpcEndpoint is a VPC endpoint description.
type VpcEndpoint struct {
	VpcID          string          `json:"VpcId,omitempty"`
	SubnetMappings []SubnetMapping `json:"SubnetMappings,omitempty"`
}

// Firewall is the Firewall response shape.
type Firewall struct {
	ProxySettings                    *ProxySettings            `json:"ProxySettings,omitempty"`
	EncryptionConfiguration          *EncryptionConfiguration  `json:"EncryptionConfiguration,omitempty"`
	TransitGatewayID                 string                    `json:"TransitGatewayId,omitempty"`
	FirewallArn                      string                    `json:"FirewallArn"`
	FirewallPolicyArn                string                    `json:"FirewallPolicyArn"`
	Description                      string                    `json:"Description,omitempty"`
	VpcID                            string                    `json:"VpcId"`
	TransitGatewayOwnerAccountID     string                    `json:"TransitGatewayOwnerAccountId,omitempty"`
	FirewallName                     string                    `json:"FirewallName"`
	FirewallID                       string                    `json:"FirewallId"`
	AvailabilityZoneMappings         []AvailabilityZoneMapping `json:"AvailabilityZoneMappings,omitempty"`
	Tags                             []Tag                     `json:"Tags,omitempty"`
	EnabledAnalysisTypes             []string                  `json:"EnabledAnalysisTypes,omitempty"`
	SubnetMappings                   []SubnetMapping           `json:"SubnetMappings"`
	DeleteProtection                 bool                      `json:"DeleteProtection"`
	SubnetChangeProtection           bool                      `json:"SubnetChangeProtection"`
	FirewallPolicyChangeProtection   bool                      `json:"FirewallPolicyChangeProtection"`
	AvailabilityZoneChangeProtection bool                      `json:"AvailabilityZoneChangeProtection"`
}

// Attachment is a firewall endpoint attachment.
type Attachment struct {
	EndpointID    string `json:"EndpointId"`
	Status        string `json:"Status"`
	StatusMessage string `json:"StatusMessage,omitempty"`
	SubnetID      string `json:"SubnetId,omitempty"`
}

// PerObjectStatus is the sync state of one policy or rule group.
type PerObjectStatus struct {
	SyncStatus  string `json:"SyncStatus"`
	UpdateToken string `json:"UpdateToken,omitempty"`
}

// SyncState is the per-zone sync state.
type SyncState struct {
	Attachment *Attachment                `json:"Attachment,omitempty"`
	Config     map[string]PerObjectStatus `json:"Config,omitempty"`
}

// FirewallStatus is the FirewallStatus response shape.
type FirewallStatus struct {
	TransitGatewayAttachmentSyncState *TransitGatewayAttachmentSyncState `json:"TransitGatewayAttachmentSyncState,omitempty"` //nolint:lll // wire name
	SyncStates                        map[string]SyncState               `json:"SyncStates,omitempty"`
	ConfigurationSyncStateSummary     string                             `json:"ConfigurationSyncStateSummary"`
	Status                            string                             `json:"Status"`
}

// AvailabilityZoneMetadata is a supported zone entry.
type AvailabilityZoneMetadata struct {
	IPAddressType string `json:"IPAddressType,omitempty"`
}

// LogDestinationConfig is one log destination.
type LogDestinationConfig struct {
	LogDestination     map[string]string `json:"LogDestination"`
	LogDestinationType string            `json:"LogDestinationType"`
	LogType            string            `json:"LogType"`
}

// LoggingConfiguration is the set of log destinations.
type LoggingConfiguration struct {
	LogDestinationConfigs []LogDestinationConfig `json:"LogDestinationConfigs"`
}

// Dimension is a CloudWatch dimension value.
type Dimension struct {
	Value string `json:"Value"`
}

// PublishMetricAction publishes CloudWatch metrics.
type PublishMetricAction struct {
	Dimensions []Dimension `json:"Dimensions"`
}

// ActionDefinition defines a custom action.
type ActionDefinition struct {
	PublishMetricAction *PublishMetricAction `json:"PublishMetricAction,omitempty"`
}

// CustomAction is a named custom stateless action.
type CustomAction struct {
	ActionDefinition ActionDefinition `json:"ActionDefinition"`
	ActionName       string           `json:"ActionName"`
}

// Address is a CIDR.
type Address struct {
	AddressDefinition string `json:"AddressDefinition"`
}

// PortRange is an inclusive port range.
type PortRange struct {
	FromPort int32 `json:"FromPort"`
	ToPort   int32 `json:"ToPort"`
}

// TCPFlagField matches TCP flags.
type TCPFlagField struct {
	Flags []string `json:"Flags"`
	Masks []string `json:"Masks,omitempty"`
}

// MatchAttributes are stateless match criteria.
type MatchAttributes struct {
	Sources          []Address      `json:"Sources,omitempty"`
	Destinations     []Address      `json:"Destinations,omitempty"`
	SourcePorts      []PortRange    `json:"SourcePorts,omitempty"`
	DestinationPorts []PortRange    `json:"DestinationPorts,omitempty"`
	Protocols        []int32        `json:"Protocols,omitempty"`
	TCPFlags         []TCPFlagField `json:"TCPFlags,omitempty"`
}

// RuleDefinition is a stateless rule body.
type RuleDefinition struct {
	MatchAttributes MatchAttributes `json:"MatchAttributes"`
	Actions         []string        `json:"Actions"`
}

// StatelessRule is a prioritized stateless rule.
type StatelessRule struct {
	RuleDefinition RuleDefinition `json:"RuleDefinition"`
	Priority       int32          `json:"Priority"`
}

// StatelessRulesAndCustomActions holds stateless rules.
type StatelessRulesAndCustomActions struct {
	StatelessRules []StatelessRule `json:"StatelessRules"`
	CustomActions  []CustomAction  `json:"CustomActions,omitempty"`
}

// Header is a stateful rule header.
type Header struct {
	Protocol        string `json:"Protocol"`
	Source          string `json:"Source"`
	SourcePort      string `json:"SourcePort"`
	Direction       string `json:"Direction"`
	Destination     string `json:"Destination"`
	DestinationPort string `json:"DestinationPort"`
}

// RuleOption is a Suricata rule option.
type RuleOption struct {
	Keyword  string   `json:"Keyword"`
	Settings []string `json:"Settings,omitempty"`
}

// StatefulRule is a 5-tuple stateful rule.
type StatefulRule struct {
	Action      string       `json:"Action"`
	Header      Header       `json:"Header"`
	RuleOptions []RuleOption `json:"RuleOptions"`
}

// RulesSourceList is a domain list.
type RulesSourceList struct {
	GeneratedRulesType string   `json:"GeneratedRulesType"`
	TargetTypes        []string `json:"TargetTypes"`
	Targets            []string `json:"Targets"`
}

// RulesSource is where rules come from.
type RulesSource struct {
	RulesSourceList                *RulesSourceList                `json:"RulesSourceList,omitempty"`
	StatelessRulesAndCustomActions *StatelessRulesAndCustomActions `json:"StatelessRulesAndCustomActions,omitempty"`
	RulesString                    string                          `json:"RulesString,omitempty"`
	StatefulRules                  []StatefulRule                  `json:"StatefulRules,omitempty"`
}

// IPSet is a list of CIDRs.
type IPSet struct {
	Definition []string `json:"Definition"`
}

// PortSet is a list of ports.
type PortSet struct {
	Definition []string `json:"Definition"`
}

// RuleVariables are named IP and port sets.
type RuleVariables struct {
	IPSets   map[string]IPSet   `json:"IPSets,omitempty"`
	PortSets map[string]PortSet `json:"PortSets,omitempty"`
}

// IPSetReference references a managed IP set.
type IPSetReference struct {
	ReferenceArn string `json:"ReferenceArn,omitempty"`
}

// ReferenceSets holds IP set references.
type ReferenceSets struct {
	IPSetReferences map[string]IPSetReference `json:"IPSetReferences,omitempty"`
}

// StatefulRuleOptions configures stateful evaluation order.
type StatefulRuleOptions struct {
	RuleOrder string `json:"RuleOrder,omitempty"`
}

// RuleGroup is the RuleGroup body.
type RuleGroup struct {
	RuleVariables       *RuleVariables       `json:"RuleVariables,omitempty"`
	ReferenceSets       *ReferenceSets       `json:"ReferenceSets,omitempty"`
	StatefulRuleOptions *StatefulRuleOptions `json:"StatefulRuleOptions,omitempty"`
	RulesSource         RulesSource          `json:"RulesSource"`
}

// SourceMetadata records rule group provenance.
type SourceMetadata struct {
	SourceArn         string `json:"SourceArn,omitempty"`
	SourceUpdateToken string `json:"SourceUpdateToken,omitempty"`
}

// SummaryConfiguration selects summary rule options.
type SummaryConfiguration struct {
	RuleOptions []string `json:"RuleOptions,omitempty"`
}

// RuleGroupResponse is the RuleGroupResponse shape.
type RuleGroupResponse struct {
	Capacity                *int32                   `json:"Capacity,omitempty"`
	ConsumedCapacity        *int32                   `json:"ConsumedCapacity,omitempty"`
	NumberOfAssociations    *int32                   `json:"NumberOfAssociations,omitempty"`
	LastModifiedTime        *float64                 `json:"LastModifiedTime,omitempty"`
	EncryptionConfiguration *EncryptionConfiguration `json:"EncryptionConfiguration,omitempty"`
	SourceMetadata          *SourceMetadata          `json:"SourceMetadata,omitempty"`
	SummaryConfiguration    *SummaryConfiguration    `json:"SummaryConfiguration,omitempty"`
	RuleGroupArn            string                   `json:"RuleGroupArn"`
	RuleGroupID             string                   `json:"RuleGroupId"`
	RuleGroupName           string                   `json:"RuleGroupName"`
	Description             string                   `json:"Description,omitempty"`
	RuleGroupStatus         string                   `json:"RuleGroupStatus,omitempty"`
	Type                    string                   `json:"Type,omitempty"`
	Tags                    []Tag                    `json:"Tags,omitempty"`
}

// StatelessRuleGroupReference references a stateless group.
type StatelessRuleGroupReference struct {
	ResourceArn string `json:"ResourceArn"`
	Priority    int32  `json:"Priority"`
}

// StatefulRuleGroupOverride overrides group actions.
type StatefulRuleGroupOverride struct {
	Action string `json:"Action,omitempty"`
}

// StatefulRuleGroupReference references a stateful group.
type StatefulRuleGroupReference struct {
	Override             *StatefulRuleGroupOverride `json:"Override,omitempty"`
	Priority             *int32                     `json:"Priority,omitempty"`
	DeepThreatInspection *bool                      `json:"DeepThreatInspection,omitempty"`
	ResourceArn          string                     `json:"ResourceArn"`
}

// FlowTimeouts configures stateful flow timeouts.
type FlowTimeouts struct {
	TCPIdleTimeoutSeconds *int32 `json:"TcpIdleTimeoutSeconds,omitempty"`
}

// StatefulEngineOptions configures the stateful engine.
type StatefulEngineOptions struct {
	FlowTimeouts          *FlowTimeouts `json:"FlowTimeouts,omitempty"`
	RuleOrder             string        `json:"RuleOrder,omitempty"`
	StreamExceptionPolicy string        `json:"StreamExceptionPolicy,omitempty"`
}

// PolicyVariables are policy-wide IP sets.
type PolicyVariables struct {
	RuleVariables map[string]IPSet `json:"RuleVariables,omitempty"`
}

// FirewallPolicy is the policy body.
type FirewallPolicy struct {
	StatefulEngineOptions           *StatefulEngineOptions        `json:"StatefulEngineOptions,omitempty"`
	PolicyVariables                 *PolicyVariables              `json:"PolicyVariables,omitempty"`
	EnableTLSSessionHolding         *bool                         `json:"EnableTLSSessionHolding,omitempty"`
	TLSInspectionConfigurationArn   string                        `json:"TLSInspectionConfigurationArn,omitempty"`
	StatelessDefaultActions         []string                      `json:"StatelessDefaultActions"`
	StatelessFragmentDefaultActions []string                      `json:"StatelessFragmentDefaultActions"`
	StatelessCustomActions          []CustomAction                `json:"StatelessCustomActions,omitempty"`
	StatelessRuleGroupReferences    []StatelessRuleGroupReference `json:"StatelessRuleGroupReferences,omitempty"`
	StatefulRuleGroupReferences     []StatefulRuleGroupReference  `json:"StatefulRuleGroupReferences,omitempty"`
	StatefulDefaultActions          []string                      `json:"StatefulDefaultActions,omitempty"`
}

// FirewallPolicyResponse is the FirewallPolicyResponse shape.
type FirewallPolicyResponse struct {
	ConsumedStatelessRuleCapacity  *int32                   `json:"ConsumedStatelessRuleCapacity,omitempty"`
	ConsumedStatefulRuleCapacity   *int32                   `json:"ConsumedStatefulRuleCapacity,omitempty"`
	ConsumedStatefulDomainCapacity *int32                   `json:"ConsumedStatefulDomainCapacity,omitempty"`
	NumberOfAssociations           *int32                   `json:"NumberOfAssociations,omitempty"`
	LastModifiedTime               *float64                 `json:"LastModifiedTime,omitempty"`
	EncryptionConfiguration        *EncryptionConfiguration `json:"EncryptionConfiguration,omitempty"`
	FirewallPolicyArn              string                   `json:"FirewallPolicyArn"`
	FirewallPolicyID               string                   `json:"FirewallPolicyId"`
	FirewallPolicyName             string                   `json:"FirewallPolicyName"`
	Description                    string                   `json:"Description,omitempty"`
	FirewallPolicyStatus           string                   `json:"FirewallPolicyStatus,omitempty"`
	Tags                           []Tag                    `json:"Tags,omitempty"`
}

// ServerCertificate references an ACM certificate.
type ServerCertificate struct {
	ResourceArn string `json:"ResourceArn,omitempty"`
}

// ServerCertificateScope scopes TLS inspection.
type ServerCertificateScope struct {
	Sources          []Address   `json:"Sources,omitempty"`
	Destinations     []Address   `json:"Destinations,omitempty"`
	SourcePorts      []PortRange `json:"SourcePorts,omitempty"`
	DestinationPorts []PortRange `json:"DestinationPorts,omitempty"`
	Protocols        []int32     `json:"Protocols,omitempty"`
}

// CheckCertificateRevocationStatusActions sets revocation behaviour.
type CheckCertificateRevocationStatusActions struct {
	RevokedStatusAction string `json:"RevokedStatusAction,omitempty"`
	UnknownStatusAction string `json:"UnknownStatusAction,omitempty"`
}

// ServerCertificateConfiguration is one TLS inspection entry.
type ServerCertificateConfiguration struct {
	CheckCertificateRevocationStatus *CheckCertificateRevocationStatusActions `json:"CheckCertificateRevocationStatus,omitempty"` //nolint:lll // wire name
	CertificateAuthorityArn          string                                   `json:"CertificateAuthorityArn,omitempty"`
	ServerCertificates               []ServerCertificate                      `json:"ServerCertificates,omitempty"`
	Scopes                           []ServerCertificateScope                 `json:"Scopes,omitempty"`
}

// TLSInspectionConfiguration is the TLS configuration body.
type TLSInspectionConfiguration struct {
	ServerCertificateConfigurations []ServerCertificateConfiguration `json:"ServerCertificateConfigurations,omitempty"`
}

// TLSCertificateData describes a referenced certificate.
type TLSCertificateData struct {
	CertificateArn string `json:"CertificateArn,omitempty"`
	Status         string `json:"Status,omitempty"`
}

// TLSInspectionConfigurationResponse is the response shape.
type TLSInspectionConfigurationResponse struct {
	NumberOfAssociations             *int32                   `json:"NumberOfAssociations,omitempty"`
	LastModifiedTime                 *float64                 `json:"LastModifiedTime,omitempty"`
	EncryptionConfiguration          *EncryptionConfiguration `json:"EncryptionConfiguration,omitempty"`
	CertificateAuthority             *TLSCertificateData      `json:"CertificateAuthority,omitempty"`
	TLSInspectionConfigurationArn    string                   `json:"TLSInspectionConfigurationArn"`
	TLSInspectionConfigurationID     string                   `json:"TLSInspectionConfigurationId"`
	TLSInspectionConfigurationName   string                   `json:"TLSInspectionConfigurationName"`
	Description                      string                   `json:"Description,omitempty"`
	TLSInspectionConfigurationStatus string                   `json:"TLSInspectionConfigurationStatus,omitempty"`
	Certificates                     []TLSCertificateData     `json:"Certificates,omitempty"`
	Tags                             []Tag                    `json:"Tags,omitempty"`
}

// FirewallMetadata is a ListFirewalls entry.
type FirewallMetadata struct {
	FirewallArn  string `json:"FirewallArn"`
	FirewallName string `json:"FirewallName"`
}

// ResourceMetadata is a list entry for policies, rule groups and TLS configurations.
type ResourceMetadata struct {
	Arn  string `json:"Arn"`
	Name string `json:"Name"`
}

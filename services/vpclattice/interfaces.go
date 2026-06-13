package vpclattice

import "time"

// StorageBackend is the interface for VPC Lattice storage operations.
type StorageBackend interface {
	CreateService(name, authType, certificateArn, customDomainName string, tags map[string]string) (*Service, error)
	GetService(serviceID string) (*Service, error)
	UpdateService(serviceID, authType, certificateArn string) (*Service, error)
	DeleteService(serviceID string) (*Service, error)
	ListServices(maxResults int32, nextToken string) ([]*ServiceSummary, string, error)

	CreateServiceNetwork(name, authType string, tags map[string]string) (*ServiceNetwork, error)
	GetServiceNetwork(snID string) (*ServiceNetwork, error)
	UpdateServiceNetwork(snID, authType string) (*ServiceNetwork, error)
	DeleteServiceNetwork(snID string) error
	ListServiceNetworks(maxResults int32, nextToken string) ([]*ServiceNetworkSummary, string, error)

	CreateServiceNetworkServiceAssociation(serviceNetworkID, serviceID string, tags map[string]string) (*ServiceNetworkServiceAssociation, error)
	GetServiceNetworkServiceAssociation(snsaID string) (*ServiceNetworkServiceAssociation, error)
	DeleteServiceNetworkServiceAssociation(snsaID string) error
	ListServiceNetworkServiceAssociations(serviceNetworkID, serviceID string, maxResults int32, nextToken string) ([]*ServiceNetworkServiceAssociationSummary, string, error)

	CreateServiceNetworkVpcAssociation(serviceNetworkID, vpcID string, securityGroupIDs []string, tags map[string]string) (*ServiceNetworkVpcAssociation, error)
	GetServiceNetworkVpcAssociation(snvaID string) (*ServiceNetworkVpcAssociation, error)
	UpdateServiceNetworkVpcAssociation(snvaID string, securityGroupIDs []string) (*ServiceNetworkVpcAssociation, error)
	DeleteServiceNetworkVpcAssociation(snvaID string) error
	ListServiceNetworkVpcAssociations(serviceNetworkID, vpcID string, maxResults int32, nextToken string) ([]*ServiceNetworkVpcAssociationSummary, string, error)

	CreateListener(serviceID, name, protocol string, port int32, defaultAction *RuleAction, tags map[string]string) (*Listener, error)
	GetListener(serviceID, listenerID string) (*Listener, error)
	UpdateListener(serviceID, listenerID string, defaultAction *RuleAction) (*Listener, error)
	DeleteListener(serviceID, listenerID string) error
	ListListeners(serviceID string, maxResults int32, nextToken string) ([]*ListenerSummary, string, error)

	CreateRule(serviceID, listenerID, name string, priority int32, action *RuleAction, match *RuleMatch, tags map[string]string) (*Rule, error)
	GetRule(serviceID, listenerID, ruleID string) (*Rule, error)
	UpdateRule(serviceID, listenerID, ruleID string, priority int32, action *RuleAction, match *RuleMatch) (*Rule, error)
	DeleteRule(serviceID, listenerID, ruleID string) error
	ListRules(serviceID, listenerID string, maxResults int32, nextToken string) ([]*RuleSummary, string, error)
	BatchUpdateRule(serviceID, listenerID string, updates []*RuleUpdate) ([]*RuleUpdateSuccess, []*RuleUpdateFailure, error)

	CreateTargetGroup(name, tgType string, config *TargetGroupConfig, tags map[string]string) (*TargetGroup, error)
	GetTargetGroup(tgID string) (*TargetGroup, error)
	UpdateTargetGroup(tgID string, healthCheck *HealthCheckConfig) (*TargetGroup, error)
	DeleteTargetGroup(tgID string) error
	ListTargetGroups(tgType, serviceArn string, maxResults int32, nextToken string) ([]*TargetGroupSummary, string, error)
	RegisterTargets(tgID string, targets []*Target) ([]*TargetFailure, error)
	DeregisterTargets(tgID string, targets []*Target) ([]*TargetFailure, error)
	ListTargets(tgID string, maxResults int32, nextToken string) ([]*TargetSummary, string, error)

	CreateAccessLogSubscription(resourceID, destinationArn, logType string, tags map[string]string) (*AccessLogSubscription, error)
	GetAccessLogSubscription(alsID string) (*AccessLogSubscription, error)
	UpdateAccessLogSubscription(alsID, destinationArn string) (*AccessLogSubscription, error)
	DeleteAccessLogSubscription(alsID string) error
	ListAccessLogSubscriptions(resourceID string, maxResults int32, nextToken string) ([]*AccessLogSubscriptionSummary, string, error)

	PutAuthPolicy(resourceID, policy string) (*AuthPolicy, error)
	GetAuthPolicy(resourceID string) (*AuthPolicy, error)
	DeleteAuthPolicy(resourceID string) error

	PutResourcePolicy(resourceArn, policy string) error
	GetResourcePolicy(resourceArn string) (string, error)
	DeleteResourcePolicy(resourceArn string) error

	TagResource(resourceArn string, tags map[string]string) error
	UntagResource(resourceArn string, keys []string) error
	ListTagsForResource(resourceArn string) (map[string]string, error)

	AccountID() string
	Region() string
	Reset()
	Snapshot() []byte
	Restore(data []byte) error
}

// Service represents a VPC Lattice service.
type Service struct {
	CreatedAt        time.Time
	LastUpdatedAt    time.Time
	ARN              string
	ID               string
	Name             string
	AuthType         string
	CertificateArn   string
	CustomDomainName string
	DNSName          string
	Status           string
}

// ServiceSummary is a service entry for list responses.
type ServiceSummary struct {
	CreatedAt        time.Time
	LastUpdatedAt    time.Time
	ARN              string
	ID               string
	Name             string
	CustomDomainName string
	DNSName          string
	Status           string
}

// ServiceNetwork represents a VPC Lattice service network.
type ServiceNetwork struct {
	CreatedAt                 time.Time
	LastUpdatedAt             time.Time
	ARN                       string
	ID                        string
	Name                      string
	AuthType                  string
	NumberOfAssociatedServices int64
	NumberOfAssociatedVPCs    int64
}

// ServiceNetworkSummary is a service network entry for list responses.
type ServiceNetworkSummary struct {
	CreatedAt                 time.Time
	ARN                       string
	ID                        string
	Name                      string
	NumberOfAssociatedServices int64
	NumberOfAssociatedVPCs    int64
}

// ServiceNetworkServiceAssociation is a service-to-service-network association.
type ServiceNetworkServiceAssociation struct {
	CreatedAt          time.Time
	ARN                string
	ID                 string
	ServiceARN         string
	ServiceID          string
	ServiceName        string
	ServiceNetworkARN  string
	ServiceNetworkID   string
	ServiceNetworkName string
	Status             string
	CreatedBy          string
	CustomDomainName   string
	DNSName            string
}

// ServiceNetworkServiceAssociationSummary is a summary for list responses.
type ServiceNetworkServiceAssociationSummary struct {
	CreatedAt          time.Time
	ARN                string
	ID                 string
	ServiceARN         string
	ServiceID          string
	ServiceName        string
	ServiceNetworkARN  string
	ServiceNetworkID   string
	ServiceNetworkName string
	Status             string
	CustomDomainName   string
	DNSName            string
}

// ServiceNetworkVpcAssociation is a VPC-to-service-network association.
type ServiceNetworkVpcAssociation struct {
	CreatedAt          time.Time
	LastUpdatedAt      time.Time
	ARN                string
	ID                 string
	VpcID              string
	ServiceNetworkARN  string
	ServiceNetworkID   string
	ServiceNetworkName string
	SecurityGroupIDs   []string
	Status             string
	CreatedBy          string
}

// ServiceNetworkVpcAssociationSummary is a summary for list responses.
type ServiceNetworkVpcAssociationSummary struct {
	CreatedAt          time.Time
	ARN                string
	ID                 string
	VpcID              string
	ServiceNetworkARN  string
	ServiceNetworkID   string
	ServiceNetworkName string
	Status             string
}

// Listener represents a VPC Lattice listener.
type Listener struct {
	CreatedAt     time.Time
	LastUpdatedAt time.Time
	ARN           string
	ID            string
	ServiceARN    string
	ServiceID     string
	Name          string
	Protocol      string
	Port          int32
	DefaultAction *RuleAction
}

// ListenerSummary is a listener entry for list responses.
type ListenerSummary struct {
	CreatedAt     time.Time
	LastUpdatedAt time.Time
	ARN           string
	ID            string
	Name          string
	Protocol      string
	Port          int32
}

// Rule represents a VPC Lattice listener rule.
type Rule struct {
	CreatedAt     time.Time
	LastUpdatedAt time.Time
	ARN           string
	ID            string
	Name          string
	Priority      int32
	Action        *RuleAction
	Match         *RuleMatch
	IsDefault     bool
}

// RuleSummary is a rule entry for list responses.
type RuleSummary struct {
	ARN       string
	ID        string
	Name      string
	Priority  int32
	IsDefault bool
}

// RuleUpdate is an update spec for BatchUpdateRule.
type RuleUpdate struct {
	RuleIdentifier string
	Priority       int32
	Action         *RuleAction
	Match          *RuleMatch
}

// RuleUpdateSuccess is a successful rule update result.
type RuleUpdateSuccess struct {
	ARN       string
	ID        string
	Name      string
	Priority  int32
	IsDefault bool
	Action    *RuleAction
	Match     *RuleMatch
}

// RuleUpdateFailure is a failed rule update result.
type RuleUpdateFailure struct {
	RuleIdentifier string
	Message        string
	Code           string
}

// RuleAction is the action for a listener rule.
type RuleAction struct {
	FixedResponseStatusCode int32
	ForwardTargetGroups     []*WeightedTargetGroup
	IsFixedResponse         bool
}

// WeightedTargetGroup is a weighted target group for forward actions.
type WeightedTargetGroup struct {
	TargetGroupID string
	Weight        int32
}

// RuleMatch is the match conditions for a listener rule.
type RuleMatch struct {
	HTTPMethod      string
	PathMatchType   string
	PathMatchValue  string
	HeaderMatches   []*HeaderMatch
}

// HeaderMatch is an HTTP header match condition.
type HeaderMatch struct {
	Name            string
	MatchType       string
	MatchValue      string
	CaseSensitive   bool
}

// TargetGroup represents a VPC Lattice target group.
type TargetGroup struct {
	CreatedAt     time.Time
	LastUpdatedAt time.Time
	ARN           string
	ID            string
	Name          string
	Type          string
	Status        string
	Config        *TargetGroupConfig
	ServiceARNs   []string
}

// TargetGroupSummary is a target group entry for list responses.
type TargetGroupSummary struct {
	CreatedAt   time.Time
	ARN         string
	ID          string
	Name        string
	Type        string
	Status      string
	Port        int32
	Protocol    string
	VpcID       string
	ServiceARNs []string
}

// TargetGroupConfig is the configuration for a target group.
type TargetGroupConfig struct {
	Port                        int32
	Protocol                    string
	ProtocolVersion             string
	VpcID                       string
	HealthCheck                 *HealthCheckConfig
	IPAddressType               string
	LambdaEventStructureVersion string
}

// HealthCheckConfig is the health check configuration for a target group.
type HealthCheckConfig struct {
	Enabled                    bool
	Protocol                   string
	ProtocolVersion            string
	Path                       string
	Port                       int32
	HealthyThresholdCount      int32
	UnhealthyThresholdCount    int32
	HealthCheckIntervalSeconds int32
	HealthCheckTimeoutSeconds  int32
	MatcherHTTPCode            string
}

// Target is a target registered to a target group.
type Target struct {
	ID   string
	Port int32
}

// TargetSummary is a target entry for list responses.
type TargetSummary struct {
	ID            string
	Port          int32
	Status        string
	ReasonCode    string
}

// TargetFailure is a target registration/deregistration failure.
type TargetFailure struct {
	ID      string
	Port    int32
	Code    string
	Message string
}

// AccessLogSubscription represents a VPC Lattice access log subscription.
type AccessLogSubscription struct {
	CreatedAt      time.Time
	LastUpdatedAt  time.Time
	ARN            string
	ID             string
	ResourceARN    string
	ResourceID     string
	DestinationARN string
	ServiceNetworkLogType string
}

// AccessLogSubscriptionSummary is a summary for list responses.
type AccessLogSubscriptionSummary struct {
	CreatedAt      time.Time
	LastUpdatedAt  time.Time
	ARN            string
	ID             string
	ResourceARN    string
	ResourceID     string
	DestinationARN string
}

// AuthPolicy represents an auth policy on a VPC Lattice resource.
type AuthPolicy struct {
	Policy string
	State  string
}

var _ StorageBackend = (*InMemoryBackend)(nil)

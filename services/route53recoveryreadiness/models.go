package route53recoveryreadiness

const (
	arnService = "route53-recovery-readiness"

	readinessReady         = "READY"
	readinessNotReady      = "NOT_READY"
	readinessUnknown       = "UNKNOWN"
	readinessNotAuthorized = "NOT_AUTHORIZED"

	typeDNSTarget   = "AWS::Route53RecoveryReadiness::DNSTargetResource"
	typeELBv2       = "AWS::ElasticLoadBalancingV2::LoadBalancer"
	typeHealthCheck = "AWS::Route53::HealthCheck"
	typeSQS         = "AWS::SQS::Queue"
	typeSNS         = "AWS::SNS::Topic"
	typeLambda      = "AWS::Lambda::Function"
	typeVPC         = "AWS::EC2::VPC"
	typeAlarm       = "AWS::CloudWatch::Alarm"
	typeTable       = "AWS::DynamoDB::Table"

	typeAPIGWStage   = "AWS::ApiGateway::Stage"
	typeAPIGWV2Stage = "AWS::ApiGatewayV2::Stage"
	typeASG          = "AWS::AutoScaling::AutoScalingGroup"
	typeCGW          = "AWS::EC2::CustomerGateway"
	typeVolume       = "AWS::EC2::Volume"
	typeELBClassic   = "AWS::ElasticLoadBalancing::LoadBalancer"
	typeMSK          = "AWS::MSK::Cluster"
	typeRDSCluster   = "AWS::RDS::DBCluster"
	typeSNSSub       = "AWS::SNS::Subscription"
	typeVPNConn      = "AWS::EC2::VPNConnection"
	typeVPNGateway   = "AWS::EC2::VPNGateway"
)

// Cell is a failover unit that may nest other cells.
type Cell struct {
	Tags   map[string]string `json:"tags,omitempty"`
	ARN    string            `json:"arn"`
	Name   string            `json:"name"`
	Parent string            `json:"parent,omitempty"`
	Cells  []string          `json:"cells"`
}

// RecoveryGroup is the top-level grouping of cells.
type RecoveryGroup struct {
	Tags  map[string]string `json:"tags,omitempty"`
	ARN   string            `json:"arn"`
	Name  string            `json:"name"`
	Cells []string          `json:"cells"`
}

// NLBResource and R53Resource identify a DNS target.
type NLBResource struct {
	Arn string `json:"arn,omitempty"`
}

// R53Resource is a Route 53 record target.
type R53Resource struct {
	DomainName  string `json:"domainName,omitempty"`
	RecordSetID string `json:"recordSetId,omitempty"`
}

// TargetResource is the target of a DNS target resource.
type TargetResource struct {
	NLB *NLBResource `json:"nLBResource,omitempty"`
	R53 *R53Resource `json:"r53Resource,omitempty"`
}

// DNSTarget describes a DNS record as a readiness resource.
type DNSTarget struct {
	Target        *TargetResource `json:"targetResource,omitempty"`
	DomainName    string          `json:"domainName,omitempty"`
	HostedZoneArn string          `json:"hostedZoneArn,omitempty"`
	RecordSetID   string          `json:"recordSetId,omitempty"`
	RecordType    string          `json:"recordType,omitempty"`
}

// Resource is one member of a resource set.
type Resource struct {
	DNS             *DNSTarget `json:"dnsTargetResource,omitempty"`
	ComponentID     string     `json:"componentId,omitempty"`
	ResourceArn     string     `json:"resourceArn,omitempty"`
	ReadinessScopes []string   `json:"readinessScopes,omitempty"`
}

// ResourceSet is a typed collection of resources.
type ResourceSet struct {
	Tags      map[string]string `json:"tags,omitempty"`
	ARN       string            `json:"arn"`
	Name      string            `json:"name"`
	Type      string            `json:"type"`
	Resources []Resource        `json:"resources"`
}

// ReadinessCheck binds a resource set to continuous evaluation.
type ReadinessCheck struct {
	Tags        map[string]string `json:"tags,omitempty"`
	ARN         string            `json:"arn"`
	Name        string            `json:"name"`
	ResourceSet string            `json:"resourceSet"`
}

// Authorization is a cross-account authorization string.
type Authorization struct {
	Value string `json:"value"`
}

package resourceexplorer2

import (
	_ "embed" // documented resource type list
	"encoding/json"
	"slices"
	"strings"
	"sync"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

const (
	svcEC2      = "ec2"
	svcECS      = "ecs"
	typeCluster = "cluster"
)

// SupportedType is one entry of ListSupportedResourceTypes.
type SupportedType struct {
	Service      string   `json:"Service"`
	ResourceType string   `json:"ResourceType"`
	CFNTypes     []string `json:"CFNResourceTypes,omitempty"`
}

//go:embed supported_types.json
var documentedTypesJSON []byte

var supportedTypes = sync.OnceValue(func() []SupportedType { //nolint:gochecknoglobals // immutable catalog
	rows := [][3]string{
		{"acm", "certificate", "AWS::CertificateManager::Certificate"},
		{"apigateway", "restapis", "AWS::ApiGateway::RestApi"},
		{"athena", "workgroup", "AWS::Athena::WorkGroup"},
		{"autoscaling", "autoScalingGroup", "AWS::AutoScaling::AutoScalingGroup"},
		{"backup", "backup-vault", "AWS::Backup::BackupVault"},
		{"cloudformation", "stack", "AWS::CloudFormation::Stack"},
		{"cloudfront", "distribution", "AWS::CloudFront::Distribution"},
		{"cloudwatch", "alarm", "AWS::CloudWatch::Alarm"},
		{"dynamodb", "table", "AWS::DynamoDB::Table"},
		{svcEC2, "instance", "AWS::EC2::Instance"},
		{svcEC2, "internet-gateway", "AWS::EC2::InternetGateway"},
		{svcEC2, "launch-template", "AWS::EC2::LaunchTemplate"},
		{svcEC2, "natgateway", "AWS::EC2::NatGateway"},
		{svcEC2, "network-interface", "AWS::EC2::NetworkInterface"},
		{svcEC2, "route-table", "AWS::EC2::RouteTable"},
		{svcEC2, "security-group", "AWS::EC2::SecurityGroup"},
		{svcEC2, "snapshot", "AWS::EC2::Snapshot"},
		{svcEC2, "subnet", "AWS::EC2::Subnet"},
		{svcEC2, "volume", "AWS::EC2::Volume"},
		{svcEC2, "vpc", "AWS::EC2::VPC"},
		{"ecr", "repository", "AWS::ECR::Repository"},
		{svcECS, typeCluster, "AWS::ECS::Cluster"},
		{svcECS, "service", "AWS::ECS::Service"},
		{svcECS, "task-definition", "AWS::ECS::TaskDefinition"},
		{"eks", typeCluster, "AWS::EKS::Cluster"},
		{"elasticache", typeCluster, "AWS::ElastiCache::CacheCluster"},
		{"elasticloadbalancing", "loadbalancer", "AWS::ElasticLoadBalancingV2::LoadBalancer"},
		{"es", "domain", "AWS::OpenSearchService::Domain"},
		{"events", "event-bus", "AWS::Events::EventBus"},
		{"events", "rule", "AWS::Events::Rule"},
		{"glue", "database", "AWS::Glue::Database"},
		{svcIAM, "group", "AWS::IAM::Group"},
		{svcIAM, "policy", "AWS::IAM::ManagedPolicy"},
		{svcIAM, "role", "AWS::IAM::Role"},
		{svcIAM, "user", "AWS::IAM::User"},
		{"kinesis", "stream", "AWS::Kinesis::Stream"},
		{"kms", "key", "AWS::KMS::Key"},
		{"lambda", "function", "AWS::Lambda::Function"},
		{"logs", "log-group", "AWS::Logs::LogGroup"},
		{"rds", typeCluster, "AWS::RDS::DBCluster"},
		{"rds", "db", "AWS::RDS::DBInstance"},
		{"route53", "hostedzone", "AWS::Route53::HostedZone"},
		{"s3", "bucket", "AWS::S3::Bucket"},
		{"secretsmanager", "secret", "AWS::SecretsManager::Secret"},
		{"sns", "topic", "AWS::SNS::Topic"},
		{"sqs", "queue", "AWS::SQS::Queue"},
		{"ssm", "parameter", "AWS::SSM::Parameter"},
		{"states", "stateMachine", "AWS::StepFunctions::StateMachine"},
	}

	out := make([]SupportedType, 0, len(rows))
	seen := make(map[string]bool, len(rows))

	for _, r := range rows {
		out = append(out, SupportedType{Service: r[0], ResourceType: r[0] + ":" + r[1], CFNTypes: []string{r[2]}})
		seen[r[0]+":"+r[1]] = true
	}

	var documented struct {
		Types []string `json:"types"`
	}

	if err := json.Unmarshal(documentedTypesJSON, &documented); err != nil {
		panic("resourceexplorer2: embedded supported types are invalid: " + err.Error())
	}

	for _, t := range documented.Types {
		if svc, _, ok := strings.Cut(t, ":"); ok && !seen[t] {
			out = append(out, SupportedType{Service: svc, ResourceType: t})
		}
	}

	slices.SortFunc(out, func(a, b SupportedType) int { return strings.Compare(a.ResourceType, b.ResourceType) })

	return out
})

func cfnTypeOf(resourceType string) string {
	for _, t := range supportedTypes() {
		if t.ResourceType == resourceType && len(t.CFNTypes) > 0 {
			return t.CFNTypes[0]
		}
	}

	return ""
}

// ListSupportedResourceTypes pages the catalog of searchable resource types.
func (b *InMemoryBackend) ListSupportedResourceTypes(token string, maxResults int) (page.Page[SupportedType], error) {
	return paginate(supportedTypes(), token, maxResults, maxPage)
}

// ManagedViewWire is the ManagedView shape.
type ManagedViewWire struct {
	Filters            *SearchFilter      `json:"Filters,omitempty"`
	LastUpdatedAt      string             `json:"LastUpdatedAt"`
	ManagedViewArn     string             `json:"ManagedViewArn"`
	ManagedViewName    string             `json:"ManagedViewName"`
	Owner              string             `json:"Owner"`
	ResourcePolicy     string             `json:"ResourcePolicy"`
	Scope              string             `json:"Scope"`
	TrustedService     string             `json:"TrustedService"`
	Version            string             `json:"Version"`
	IncludedProperties []IncludedProperty `json:"IncludedProperties,omitempty"`
}

type managedViewDef struct {
	name, service string
}

var managedViewDefs = []managedViewDef{ //nolint:gochecknoglobals // immutable catalog
	{name: "AWSDefaultViewForSystemsManager", service: "ssm.amazonaws.com"},
	{name: "AWSDefaultViewForResilienceHub", service: "resiliencehub.amazonaws.com"},
}

const managedViewEpoch = "2024-01-01T00:00:00Z"

var managedViewNamespace = uuid.MustParse("6f1c5f0e-2f0b-4c3b-9d54-2b8b6a1f7a10") //nolint:gochecknoglobals // constant

func managedViewARN(region string, d managedViewDef) string {
	return arn.Build(signingName, region, "aws",
		"managed-view/"+d.name+"/"+uuid.NewSHA1(managedViewNamespace, []byte(d.name)).String())
}

// ListManagedViews lists managed view ARNs, optionally for one trusted service.
func (b *InMemoryBackend) ListManagedViews(
	region, principal, token string, maxResults int,
) (page.Page[string], error) {
	var arns []string

	for _, d := range managedViewDefs {
		if principal == "" || d.service == principal {
			arns = append(arns, managedViewARN(region, d))
		}
	}

	slices.Sort(arns)

	return paginate(arns, token, maxResults, maxViewPage)
}

// GetManagedView returns a managed view by ARN.
func (b *InMemoryBackend) GetManagedView(account, region, viewARN string) (*ManagedViewWire, error) {
	if viewARN == "" {
		return nil, invalid("ManagedViewArn is required")
	}

	for _, d := range managedViewDefs {
		if managedViewARN(region, d) != viewARN {
			continue
		}

		return &ManagedViewWire{
			Filters:            &SearchFilter{FilterString: ""},
			IncludedProperties: []IncludedProperty{{Name: propertyTags}},
			LastUpdatedAt:      managedViewEpoch,
			ManagedViewArn:     viewARN,
			ManagedViewName:    d.name,
			Owner:              "aws",
			ResourcePolicy:     managedViewPolicy(d.service, account),
			Scope:              arn.BuildGlobal("iam", "", account, "root"),
			TrustedService:     d.service,
			Version:            "1",
		}, nil
	}

	return nil, notFound("managed view %s not found", viewARN)
}

func managedViewPolicy(service, account string) string {
	return `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"` + service +
		`"},"Action":"resource-explorer-2:Search","Resource":"*","Condition":{"StringEquals":{"aws:SourceAccount":"` +
		account + `"}}}]}`
}

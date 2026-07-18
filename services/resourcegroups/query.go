package resourcegroups

import (
	"context"
	"encoding/json"
	"slices"
	"sort"
	"strings"
)

// arnSplitParts is the number of colon-separated segments in a well-formed AWS ARN.
const arnSplitParts = 6

// resourceTypeFromARN derives an AWS CloudFormation resource type string from an ARN.
// Returns an empty string for ARNs whose service/type combination is not in the mapping.
func resourceTypeFromARN(arnStr string) string {
	parts := strings.SplitN(arnStr, ":", arnSplitParts)
	if len(parts) < arnSplitParts {
		return ""
	}

	service := parts[2]
	resource := parts[5]

	// SNS topic ARNs: arn:aws:sns:region:account:TopicName (no type prefix)
	// SQS queue ARNs: arn:aws:sqs:region:account:QueueName (no type prefix)
	switch service {
	case "s3":
		return "AWS::S3::Bucket"
	case "sns":
		return "AWS::SNS::Topic"
	case "sqs":
		return "AWS::SQS::Queue"
	}

	// Extract resource type before the first "/" or ":"
	resType := resource
	if idx := strings.IndexAny(resource, "/:"); idx >= 0 {
		resType = resource[:idx]
	}

	key := service + ":" + strings.ToLower(resType)
	if t, ok := arnServiceTypeMap[key]; ok {
		return t
	}

	return ""
}

// arnServiceTypeMap maps "service:resource-type" to AWS CloudFormation type strings.
var arnServiceTypeMap = map[string]string{ //nolint:gochecknoglobals,gosec // static lookup table; no credentials
	"ec2:instance":                      "AWS::EC2::Instance",
	"ec2:volume":                        "AWS::EC2::Volume",
	"ec2:vpc":                           "AWS::EC2::VPC",
	"ec2:subnet":                        "AWS::EC2::Subnet",
	"ec2:security-group":                "AWS::EC2::SecurityGroup",
	"ec2:key-pair":                      "AWS::EC2::KeyPair",
	"ec2:image":                         "AWS::EC2::Image",
	"ec2:network-interface":             "AWS::EC2::NetworkInterface",
	"ec2:route-table":                   "AWS::EC2::RouteTable",
	"ec2:internet-gateway":              "AWS::EC2::InternetGateway",
	"ec2:natgateway":                    "AWS::EC2::NatGateway",
	"ec2:elastic-ip":                    "AWS::EC2::EIP",
	"ec2:snapshot":                      "AWS::EC2::Snapshot",
	"ec2:dhcp-options":                  "AWS::EC2::DHCPOptions",
	"ec2:network-acl":                   "AWS::EC2::NetworkAcl",
	"lambda:function":                   "AWS::Lambda::Function",
	"rds:db":                            "AWS::RDS::DBInstance",
	"rds:cluster":                       "AWS::RDS::DBCluster",
	"rds:snapshot":                      "AWS::RDS::DBSnapshot",
	"rds:cluster-snapshot":              "AWS::RDS::DBClusterSnapshot",
	"iam:role":                          "AWS::IAM::Role",
	"iam:user":                          "AWS::IAM::User",
	"iam:group":                         "AWS::IAM::Group",
	"iam:policy":                        "AWS::IAM::ManagedPolicy",
	"iam:instance-profile":              "AWS::IAM::InstanceProfile",
	"dynamodb:table":                    "AWS::DynamoDB::Table",
	"kinesis:stream":                    "AWS::Kinesis::Stream",
	"cloudformation:stack":              "AWS::CloudFormation::Stack",
	"elasticloadbalancing:loadbalancer": "AWS::ElasticLoadBalancingV2::LoadBalancer",
	"ecs:cluster":                       "AWS::ECS::Cluster",
	"ecs:service":                       "AWS::ECS::Service",
	"ecs:task-definition":               "AWS::ECS::TaskDefinition",
	"eks:cluster":                       "AWS::EKS::Cluster",
	"secretsmanager:secret":             "AWS::SecretsManager::Secret",
	"kms:key":                           "AWS::KMS::Key",
	"cloudwatch:alarm":                  "AWS::CloudWatch::Alarm",
	"logs:log-group":                    "AWS::Logs::LogGroup",
	"apigateway:restapis":               "AWS::ApiGateway::RestApi",
	"glue:database":                     "AWS::Glue::Database",
	"glue:table":                        "AWS::Glue::Table",
	"glue:job":                          "AWS::Glue::Job",
	"elasticache:cluster":               "AWS::ElastiCache::CacheCluster",
	"elasticache:replicationgroup":      "AWS::ElastiCache::ReplicationGroup",
	"redshift:cluster":                  "AWS::Redshift::Cluster",
	"es:domain":                         "AWS::Elasticsearch::Domain",
	"opensearchservice:domain":          "AWS::OpenSearchService::Domain",
	"firehose:deliverystream":           "AWS::KinesisFirehose::DeliveryStream",
	"codecommit:repository":             "AWS::CodeCommit::Repository",
	"codebuild:project":                 "AWS::CodeBuild::Project",
	"codepipeline:pipeline":             "AWS::CodePipeline::Pipeline",
	"ecr:repository":                    "AWS::ECR::Repository",
	"route53:hostedzone":                "AWS::Route53::HostedZone",
	"ssm:parameter":                     "AWS::SSM::Parameter",
	"wafv2:webacl":                      "AWS::WAFv2::WebACL",
	"wafv2:rulegroup":                   "AWS::WAFv2::RuleGroup",
	"acm:certificate":                   "AWS::CertificateManager::Certificate",
	"backup:backup-vault":               "AWS::Backup::BackupVault",
	"backup:backup-plan":                "AWS::Backup::BackupPlan",
	"kafka:cluster":                     "AWS::MSK::Cluster",
	"mq:broker":                         "AWS::AmazonMQ::Broker",
	"stepfunctions:stateMachine":        "AWS::StepFunctions::StateMachine",
	"appsync:graphqlapi":                "AWS::AppSync::GraphQLApi",
	"servicecatalog:portfolio":          "AWS::ServiceCatalog::Portfolio",
	"servicecatalog:product":            "AWS::ServiceCatalog::CloudFormationProduct",
	"sagemaker:endpoint":                "AWS::SageMaker::Endpoint",
	"sagemaker:model":                   "AWS::SageMaker::Model",
	"sagemaker:notebook-instance":       "AWS::SageMaker::NotebookInstance",
	"dax:cluster":                       "AWS::DAX::Cluster",
	"networkfirewall:firewall":          "AWS::NetworkFirewall::Firewall",
	"networkfirewall:firewall-policy":   "AWS::NetworkFirewall::FirewallPolicy",
}

// parseResourceTypeFilters parses the JSON query of a TAG_FILTERS_1_0 ResourceQuery and
// returns the set of desired resource types (nil when the query is "match all" or malformed).
// The special value "AWS::AllSupported" means match all types and returns nil.
func parseResourceTypeFilters(queryJSON string) map[string]bool {
	var tfq tagFilterQuery
	if err := json.Unmarshal([]byte(queryJSON), &tfq); err != nil {
		return nil
	}

	if len(tfq.ResourceTypeFilters) == 0 {
		return nil
	}

	// "AWS::AllSupported" is a special pass-through value meaning "no type restriction".
	if slices.Contains(tfq.ResourceTypeFilters, "AWS::AllSupported") {
		return nil
	}

	types := make(map[string]bool, len(tfq.ResourceTypeFilters))

	for _, rt := range tfq.ResourceTypeFilters {
		types[rt] = true
	}

	return types
}

// SearchResources returns resource identifiers that have been grouped into any group
// within the request's region, filtered by the ResourceQuery.
// For TAG_FILTERS_1_0 queries, ResourceTypeFilters are applied when non-empty.
// A nil query matches all grouped resources (match-all).
// Results are de-duplicated and paginated.
// Returns identifiers, a continuation token (empty when no more results), and any error.
func (b *InMemoryBackend) SearchResources(
	ctx context.Context,
	q *ResourceQuery,
	nextToken string,
	maxResults int,
) ([]ResourceIdentifier, string, error) {
	// Parse the query to extract any resource type filters.
	var wantTypes map[string]bool

	if q != nil && q.Type == "TAG_FILTERS_1_0" && q.Query != "" {
		wantTypes = parseResourceTypeFilters(q.Query)
	}

	b.mu.RLock("SearchResources")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)
	regionRes := b.groupResources[region]

	seen := make(map[string]struct{})
	out := make([]ResourceIdentifier, 0)

	for _, arns := range regionRes {
		for _, a := range arns {
			if _, ok := seen[a]; ok {
				continue
			}

			seen[a] = struct{}{}
			resType := resourceTypeFromARN(a)

			if len(wantTypes) > 0 && !wantTypes[resType] {
				continue
			}

			out = append(out, ResourceIdentifier{ResourceArn: a, ResourceType: resType})
		}
	}

	// Stable sort by ARN for deterministic pagination.
	sort.Slice(out, func(i, j int) bool { return out[i].ResourceArn < out[j].ResourceArn })

	page, token := paginate(out, func(id ResourceIdentifier) string { return id.ResourceArn }, nextToken, maxResults)

	return page, token, nil
}

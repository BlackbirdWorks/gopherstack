package resourcegroups

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

// regionContextKey is the context key under which the per-request AWS region is stored.
type regionContextKey struct{}

// getRegion extracts the region from ctx, falling back to defaultRegion when unset.
// Resource Groups resources are isolated per region: every backend operation resolves
// the caller's region from the request context and operates only on that region's
// nested store.
func getRegion(ctx context.Context, defaultRegion string) string {
	if r, ok := ctx.Value(regionContextKey{}).(string); ok && r != "" {
		return r
	}

	return defaultRegion
}

var (
	// ErrNotFound is returned when a resource group is not found.
	ErrNotFound = awserr.New("NotFoundException", awserr.ErrNotFound)
	// ErrAlreadyExists is returned when a resource group already exists.
	ErrAlreadyExists = awserr.New("BadRequestException", awserr.ErrAlreadyExists)
	// ErrValidation is returned when request validation fails.
	ErrValidation = awserr.New("BadRequestException", awserr.ErrInvalidParameter)
	// ErrTagSyncTaskNotFound is returned when a tag-sync task is not found.
	ErrTagSyncTaskNotFound = awserr.New(
		"NotFoundException: tag-sync task not found",
		awserr.ErrNotFound,
	)
)

// Grouping action constants.
const (
	groupingActionGroup   = "GROUP"
	groupingActionUngroup = "UNGROUP"
	groupingStatusSuccess = "SUCCESS"
	groupingStatusFailed  = "FAILED"
)

// GroupingStatus error codes for failed grouping operations.
const (
	groupingErrInvalidARN       = "INVALID_ARN"
	groupingErrResourceNotFound = "RESOURCE_NOT_FOUND"
)

// TagSyncTask status constants.
const (
	tagSyncTaskStatusActive    = "ACTIVE"
	tagSyncTaskStatusCancelled = "CANCELLED"
)

// tagSyncTaskTTL is the maximum age of a completed or cancelled tag-sync task
// before it is evicted from memory during list operations.
const tagSyncTaskTTL = 24 * time.Hour

// AccountLifecycleEventStatus constants.
const (
	accountLifecycleEventsActive   = "ACTIVE"
	accountLifecycleEventsInactive = "INACTIVE"
)

// groupNameMaxLen and groupDescMaxLen match AWS limits.
const (
	groupNameMaxLen = 300
	groupDescMaxLen = 512
)

const configParamAllowedResourceTypes = "allowed-resource-types"

// listGroupsFilterNamePrefix is the filter name for filtering groups by name prefix.
const listGroupsFilterNamePrefix = "name-prefix"

// arnSplitParts is the number of colon-separated segments in a well-formed AWS ARN.
const arnSplitParts = 6

// groupNameRe matches valid Resource Groups group names (AWS rule).
var groupNameRe = regexp.MustCompile(`^[a-zA-Z0-9_.−\-]+$`)

// groupNameReservedPrefixes lists prefixes that AWS does not allow for group names.
var groupNameReservedPrefixes = []string{ //nolint:gochecknoglobals // lookup table, initialized once
	"aws",
	"AWS",
}

// validResourceQueryTypes lists the only two supported query types.
var validResourceQueryTypes = map[string]bool{ //nolint:gochecknoglobals // lookup table, initialized once
	"TAG_FILTERS_1_0":          true,
	"CLOUDFORMATION_STACK_1_0": true,
}

// ListGroupsFilterName constants for ListGroups Filters field.
const (
	listGroupsFilterConfigurationType = "configuration-type"
	listGroupsFilterResourceType      = "resource-type"
)

// listGroupResourcesFilterResourceType is the filter name for filtering ListGroupResources by resource type.
const listGroupResourcesFilterResourceType = "resource-type"

// validConfigTypes maps each recognized configuration Type to its allowed
// parameter names.  An empty slice means the type takes no parameters.
var validConfigTypes = map[string][]string{ //nolint:gochecknoglobals // lookup table, initialized once
	"AWS::EC2::HostManagement": {
		configParamAllowedResourceTypes,
		"any-of-allowed-resource-types",
		"deletion-protection",
	},
	"AWS::EC2::CapacityReservationPool": {},
	"AWS::ResourceGroups::Generic": {
		configParamAllowedResourceTypes,
		"any-of-allowed-resource-types",
	},
	"AWS::AppRegistry::Application":               {configParamAllowedResourceTypes},
	"AWS::NetworkFirewall::RuleGroup":             {configParamAllowedResourceTypes},
	"AWS::Route53Resolver::FirewallRuleGroup":     {configParamAllowedResourceTypes},
	"AWS::ServiceCatalogAppRegistry::Application": {configParamAllowedResourceTypes},
}

// ResourceQuery represents a tag-based resource query for a group.
type ResourceQuery struct {
	Type  string `json:"Type"`
	Query string `json:"Query"`
}

// Group represents a Resource Group.
// Field names use PascalCase JSON tags to match what the AWS SDK expects in responses.
type Group struct {
	Tags           *tags.Tags        `json:"Tags,omitempty"`
	ResourceQuery  *ResourceQuery    `json:"ResourceQuery,omitempty"`
	ApplicationTag map[string]string `json:"ApplicationTag,omitempty"`
	Name           string            `json:"Name"`
	ARN            string            `json:"GroupArn"`
	Description    string            `json:"Description,omitempty"`
	OwnerID        string            `json:"OwnerId,omitempty"`
	DisplayName    string            `json:"DisplayName,omitempty"`
	Criticality    int               `json:"Criticality,omitempty"`
}

// ListGroupsFilter holds a single filter for the ListGroups operation.
type ListGroupsFilter struct {
	Name   string   `json:"Name"`
	Values []string `json:"Values"`
}

// validateGroupName validates that a group name conforms to AWS naming rules.
func validateGroupName(name string) error {
	if name == "" {
		return fmt.Errorf("%w: Name is required", ErrValidation)
	}

	if len(name) > groupNameMaxLen {
		return fmt.Errorf("%w: Name must be at most %d characters", ErrValidation, groupNameMaxLen)
	}

	if !groupNameRe.MatchString(name) {
		return fmt.Errorf("%w: Name must match pattern [a-zA-Z0-9_.−-]+", ErrValidation)
	}

	nameLower := strings.ToLower(name)
	for _, prefix := range groupNameReservedPrefixes {
		if strings.HasPrefix(nameLower, strings.ToLower(prefix)) {
			return fmt.Errorf(
				"%w: Name must not start with reserved prefix %q",
				ErrValidation,
				prefix,
			)
		}
	}

	return nil
}

// validateDescription validates that a description conforms to AWS length rules.
func validateDescription(desc string) error {
	if len(desc) > groupDescMaxLen {
		return fmt.Errorf(
			"%w: Description must be at most %d characters",
			ErrValidation,
			groupDescMaxLen,
		)
	}

	return nil
}

// validateResourceQuery validates that a ResourceQuery is well-formed.
func validateResourceQuery(q *ResourceQuery) error {
	if q == nil {
		return nil
	}

	if !validResourceQueryTypes[q.Type] {
		return fmt.Errorf(
			"%w: ResourceQuery.Type must be TAG_FILTERS_1_0 or CLOUDFORMATION_STACK_1_0, got %q",
			ErrValidation,
			q.Type,
		)
	}

	if q.Query == "" {
		return fmt.Errorf("%w: ResourceQuery.Query must be a non-empty JSON string", ErrValidation)
	}

	var raw json.RawMessage
	if err := json.Unmarshal([]byte(q.Query), &raw); err != nil {
		return fmt.Errorf(
			"%w: ResourceQuery.Query is not valid JSON: %s",
			ErrValidation,
			err.Error(),
		)
	}

	return nil
}

// validateConfiguration validates each GroupConfigurationItem against the allow-list.
func validateConfiguration(items []GroupConfigurationItem) error {
	for _, item := range items {
		allowedParams, ok := validConfigTypes[item.Type]
		if !ok {
			return fmt.Errorf(
				"%w: unsupported configuration type %q; must be one of AWS::EC2::HostManagement, "+
					"AWS::EC2::CapacityReservationPool, AWS::ResourceGroups::Generic, "+
					"AWS::AppRegistry::Application, etc",
				ErrValidation,
				item.Type,
			)
		}

		if len(allowedParams) == 0 && len(item.Parameters) > 0 {
			return fmt.Errorf(
				"%w: configuration type %q does not accept any parameters",
				ErrValidation,
				item.Type,
			)
		}

		allowed := make(map[string]bool, len(allowedParams))
		for _, p := range allowedParams {
			allowed[p] = true
		}

		for _, param := range item.Parameters {
			if !allowed[param.Name] {
				return fmt.Errorf(
					"%w: parameter %q is not valid for configuration type %q",
					ErrValidation,
					param.Name,
					item.Type,
				)
			}
		}
	}

	return nil
}

// validateTagKeys validates that no reserved aws: prefix tag keys are present.
func validateTagKeys(tagMap map[string]string) error {
	for k := range tagMap {
		if strings.HasPrefix(strings.ToLower(k), "aws:") {
			return fmt.Errorf(
				"%w: tag key %q uses the reserved prefix \"aws:\"; these keys are managed by AWS",
				ErrValidation,
				k,
			)
		}
	}

	return nil
}

// GroupConfigurationParameter is a key-value parameter for a group configuration item.
type GroupConfigurationParameter struct {
	Name   string   `json:"Name"`
	Values []string `json:"Values"`
}

// GroupConfigurationItem is a single configuration item for a group.
type GroupConfigurationItem struct {
	Type       string                        `json:"Type"`
	Parameters []GroupConfigurationParameter `json:"Parameters,omitempty"`
}

// AccountSettings holds account-level settings for Resource Groups.
type AccountSettings struct {
	GroupLifecycleEventsDesiredStatus string `json:"GroupLifecycleEventsDesiredStatus,omitempty"`
	GroupLifecycleEventsStatus        string `json:"GroupLifecycleEventsStatus,omitempty"`
	GroupLifecycleEventsStatusMessage string `json:"GroupLifecycleEventsStatusMessage,omitempty"`
}

// TagSyncTask represents a tag-sync task for an application group.
type TagSyncTask struct {
	CreatedAt     time.Time      `json:"CreatedAt"`
	ResourceQuery *ResourceQuery `json:"ResourceQuery,omitempty"`
	GroupArn      string         `json:"GroupArn"`
	GroupName     string         `json:"GroupName"`
	RoleArn       string         `json:"RoleArn"`
	TagKey        string         `json:"TagKey,omitempty"`
	TagValue      string         `json:"TagValue,omitempty"`
	TaskArn       string         `json:"TaskArn"`
	Status        string         `json:"Status"`
	ErrorMessage  string         `json:"ErrorMessage,omitempty"`
}

// ResourceIdentifier holds an ARN and resource type.
type ResourceIdentifier struct {
	ResourceArn  string `json:"ResourceArn,omitempty"`
	ResourceType string `json:"ResourceType,omitempty"`
}

// GroupingStatusItem holds the grouping/ungrouping status for a resource.
type GroupingStatusItem struct {
	UpdatedAt    time.Time `json:"UpdatedAt"`
	ResourceArn  string    `json:"ResourceArn,omitempty"`
	Action       string    `json:"Action,omitempty"`
	Status       string    `json:"Status,omitempty"`
	ErrorCode    string    `json:"ErrorCode,omitempty"`
	ErrorMessage string    `json:"ErrorMessage,omitempty"`
}

// ListTagSyncTasksFilter holds filter criteria for listing tag-sync tasks.
type ListTagSyncTasksFilter struct {
	GroupArn  string `json:"GroupArn,omitempty"`
	GroupName string `json:"GroupName,omitempty"`
}

// ListGroupResourcesFilter holds a single filter criterion for ListGroupResources.
// Supported Name: "resource-type" (filter by AWS CloudFormation resource type string).
type ListGroupResourcesFilter struct {
	Name   string   `json:"Name"`
	Values []string `json:"Values"`
}

// tagFilterQuery is the parsed form of a TAG_FILTERS_1_0 ResourceQuery string.
type tagFilterQuery struct {
	ResourceTypeFilters []string    `json:"ResourceTypeFilters"`
	TagFilters          []tagFilter `json:"TagFilters"`
}

// tagFilter holds a tag key and allowed values for SearchResources filtering.
type tagFilter struct {
	Key    string   `json:"Key"`
	Values []string `json:"Values"`
}

// paginate returns a page of items starting after nextToken, limited to maxResults.
// keyFn extracts a unique, stable sort key from each item (used as the continuation token).
// If maxResults is 0, all items are returned. If nextToken is empty, results start from the beginning.
func paginate[T any](list []T, keyFn func(T) string, nextToken string, maxResults int) ([]T, string) {
	if nextToken != "" {
		start := 0

		for i, item := range list {
			if keyFn(item) == nextToken {
				start = i + 1

				break
			}
		}

		list = list[start:]
	}

	if maxResults <= 0 || maxResults >= len(list) {
		return list, ""
	}

	page := list[:maxResults]

	return page, keyFn(page[len(page)-1])
}

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

// InMemoryBackend is the in-memory store for Resource Groups.
//
// Phase 3.3 of the datalayer refactor replaces the region-nested
// map[string]map[string]*Group and its companion map[string]map[string]string
// ARN reverse index with a single flat *store.Table[Group], keyed by the
// composite "region|name" string (see regionKey below), with companion
// *store.Index values grouping entries by region (replacing the old outer map
// nesting used by ListGroups) and by "region|ARN" (replacing the old
// per-region arnIndex reverse map used by findByARN). tagSyncTasks receives
// the identical treatment. See store_setup.go.
//
// Group and TagSyncTask carry no exported Region field (not part of either's
// AWS wire shape); both always embed their owning region in their ARN (see
// CreateGroup/StartTagSyncTask), so groupRegionOf/taskRegionOf derive it from
// there instead of adding a new field.
//
// groups is a "dirty" table in the store_setup.go/persistence.go sense: each
// Group carries a live *tags.Tags field (a Prometheus-instrumented map) that
// plain json.Marshal cannot round-trip while preserving the per-resource
// metric name -- so it is NOT registered on the shared b.registry; it is
// built with store.New only, and persistence.go handles its Snapshot/Restore
// via a plain-map DTO. tagSyncTasks has no such field and is registered
// directly.
//
// groupConfigurations, groupResources, and groupingStatuses remain plain
// region-nested maps: their values (a slice of items, a slice of ARN
// strings, a slice of status records) have no identity of their own for
// store.Table to key on, so there is nothing to convert -- each is persisted
// here directly instead (see persistence.go).
type InMemoryBackend struct {
	groups               *store.Table[Group]
	groupsByRegion       *store.Index[Group]
	groupsByARN          *store.Index[Group]
	tagSyncTasks         *store.Table[TagSyncTask]
	tagSyncTasksByRegion *store.Index[TagSyncTask]
	registry             *store.Registry
	groupConfigurations  map[string]map[string][]GroupConfigurationItem
	groupResources       map[string]map[string][]string // region → group name → []resourceARN
	groupingStatuses     map[string]map[string][]GroupingStatusItem
	mu                   *lockmetrics.RWMutex
	accountSettings      AccountSettings
	accountID            string
	region               string
	taskIDCounter        int64 // monotonically incremented for unique task ARNs
}

// NewInMemoryBackend creates a new InMemoryBackend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	b := &InMemoryBackend{
		groupConfigurations: make(map[string]map[string][]GroupConfigurationItem),
		groupResources:      make(map[string]map[string][]string),
		groupingStatuses:    make(map[string]map[string][]GroupingStatusItem),
		accountID:           accountID,
		region:              region,
		mu:                  lockmetrics.New("resourcegroups"),
		registry:            store.NewRegistry(),
	}
	registerAllTables(b)

	return b
}

// regionKey builds the composite store.Table primary key ("region|id") shared
// by both region-qualified tables registered in store_setup.go.
func regionKey(region, id string) string { return region + "|" + id }

// regionFromARN extracts the region component (index 3) from an AWS ARN
// (arn:partition:service:region:account:resource), falling back to defaultRegion.
func regionFromARN(resourceARN, defaultRegion string) string {
	parts := strings.Split(resourceARN, ":")
	const regionIndex = 3

	if len(parts) > regionIndex && parts[regionIndex] != "" {
		return parts[regionIndex]
	}

	return defaultRegion
}

// groupRegionOf returns the region a Group belongs to, derived from its ARN
// (always built with the owning region -- see CreateGroup), falling back to
// the backend's own default region for a malformed/test-injected ARN.
func (b *InMemoryBackend) groupRegionOf(g *Group) string {
	return regionFromARN(g.ARN, b.region)
}

// groupTableKeyFn is the store.Table primary key function for groups.
func (b *InMemoryBackend) groupTableKeyFn(g *Group) string {
	return regionKey(b.groupRegionOf(g), g.Name)
}

// groupRegionIndexKeyFn is the groupsByRegion index key function.
func (b *InMemoryBackend) groupRegionIndexKeyFn(g *Group) string {
	return b.groupRegionOf(g)
}

// groupARNIndexKeyFn is the groupsByARN index key function.
func (b *InMemoryBackend) groupARNIndexKeyFn(g *Group) string {
	return regionKey(b.groupRegionOf(g), g.ARN)
}

// taskRegionOf returns the region a TagSyncTask belongs to, derived from its
// TaskArn (always built with the owning region -- see StartTagSyncTask),
// falling back to the backend's own default region for a malformed/
// test-injected ARN.
func (b *InMemoryBackend) taskRegionOf(t *TagSyncTask) string {
	return regionFromARN(t.TaskArn, b.region)
}

// tagSyncTaskTableKeyFn is the store.Table primary key function for tagSyncTasks.
func (b *InMemoryBackend) tagSyncTaskTableKeyFn(t *TagSyncTask) string {
	return regionKey(b.taskRegionOf(t), t.TaskArn)
}

// tagSyncTaskRegionIndexKeyFn is the tagSyncTasksByRegion index key function.
func (b *InMemoryBackend) tagSyncTaskRegionIndexKeyFn(t *TagSyncTask) string {
	return b.taskRegionOf(t)
}

// The *Store helpers return the per-region inner map, lazily creating it.
// Callers must hold b.mu (write lock).

func (b *InMemoryBackend) groupConfigurationsStore(region string) map[string][]GroupConfigurationItem {
	if b.groupConfigurations[region] == nil {
		b.groupConfigurations[region] = make(map[string][]GroupConfigurationItem)
	}

	return b.groupConfigurations[region]
}

func (b *InMemoryBackend) groupResourcesStore(region string) map[string][]string {
	if b.groupResources[region] == nil {
		b.groupResources[region] = make(map[string][]string)
	}

	return b.groupResources[region]
}

func (b *InMemoryBackend) groupingStatusesStore(region string) map[string][]GroupingStatusItem {
	if b.groupingStatuses[region] == nil {
		b.groupingStatuses[region] = make(map[string][]GroupingStatusItem)
	}

	return b.groupingStatuses[region]
}

// Region returns the AWS region this backend is configured for.
func (b *InMemoryBackend) Region() string { return b.region }

// AccountID returns the AWS account ID this backend is configured for.
func (b *InMemoryBackend) AccountID() string { return b.accountID }

// Reset clears all in-memory state. It closes all group Tags to release
// Prometheus metrics before discarding the groups table.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	for _, g := range b.groups.All() {
		if g.Tags != nil {
			g.Tags.Close()
		}
	}

	b.groups.Reset()
	b.tagSyncTasks.Reset()
	b.groupConfigurations = make(map[string]map[string][]GroupConfigurationItem)
	b.groupResources = make(map[string]map[string][]string)
	b.groupingStatuses = make(map[string]map[string][]GroupingStatusItem)
	b.accountSettings = AccountSettings{}
}

// resolveGroupName extracts the group name from a name-or-ARN value.
func resolveGroupName(nameOrARN string) string {
	if idx := strings.LastIndex(nameOrARN, "group/"); idx >= 0 {
		return nameOrARN[idx+len("group/"):]
	}

	return nameOrARN
}

// CreateGroup creates a new resource group.
// The Tags field in the returned Group points to a fresh Tags copy; it is
// safe to read but callers should not pass it back to mutation methods.
// configuration is optional; when non-nil it is stored atomically with the group.
func (b *InMemoryBackend) CreateGroup(
	ctx context.Context,
	name, description string,
	resourceQuery *ResourceQuery,
	inputTags *tags.Tags,
	configuration []GroupConfigurationItem,
) (*Group, error) {
	if err := validateGroupName(name); err != nil {
		return nil, err
	}

	if err := validateDescription(description); err != nil {
		return nil, err
	}

	if err := validateResourceQuery(resourceQuery); err != nil {
		return nil, err
	}

	if len(configuration) > 0 {
		if err := validateConfiguration(configuration); err != nil {
			return nil, err
		}

		// AWS rejects groups that specify both a ResourceQuery and a Configuration.
		if resourceQuery != nil {
			return nil, fmt.Errorf(
				"%w: a group cannot have both a ResourceQuery and a Configuration; "+
					"use one or the other",
				ErrValidation,
			)
		}
	}

	if inputTags != nil {
		tagMap := inputTags.Clone()
		if err := validateTagKeys(tagMap); err != nil {
			return nil, err
		}
	}

	b.mu.Lock("CreateGroup")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	tableKey := regionKey(region, name)

	if b.groups.Has(tableKey) {
		return nil, fmt.Errorf("%w: group %s already exists", ErrAlreadyExists, name)
	}

	groupARN := arn.Build("resource-groups", region, b.accountID, "group/"+name)

	// Clone caller-provided tags into a backend-owned collection so that the
	// caller cannot mutate backend state by keeping a reference to inputTags.
	var backendTags *tags.Tags
	if inputTags == nil {
		backendTags = tags.New("rg." + name + ".tags")
	} else {
		backendTags = tags.FromMap("rg."+name+".tags", inputTags.Clone())
	}

	g := &Group{
		Name:          name,
		ARN:           groupARN,
		Description:   description,
		Tags:          backendTags,
		ResourceQuery: resourceQuery,
		OwnerID:       b.accountID,
	}
	b.groups.Put(g)

	if len(configuration) > 0 {
		b.groupConfigurationsStore(region)[name] = cloneConfigItems(configuration)
	}

	cp := *g

	return &cp, nil
}

// GetGroup returns a resource group by name or ARN.
func (b *InMemoryBackend) GetGroup(ctx context.Context, nameOrARN string) (*Group, error) {
	b.mu.RLock("GetGroup")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)
	name := resolveGroupName(nameOrARN)

	g, ok := b.groups.Get(regionKey(region, name))
	if !ok {
		return nil, fmt.Errorf("%w: group %s not found", ErrNotFound, name)
	}

	cp := *g

	return &cp, nil
}

// UpdateGroup updates the description, display name, and criticality of a resource group.
// Pass an empty displayName to leave it unchanged. Pass criticality=0 to leave it unchanged.
// Criticality must be 1-5 if non-zero.
func (b *InMemoryBackend) UpdateGroup(
	ctx context.Context,
	nameOrARN, description, displayName string,
	criticality int,
) (*Group, error) {
	if err := validateDescription(description); err != nil {
		return nil, err
	}

	if criticality != 0 && (criticality < 1 || criticality > 5) {
		return nil, fmt.Errorf("%w: Criticality must be between 1 and 5", ErrValidation)
	}

	b.mu.Lock("UpdateGroup")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	name := resolveGroupName(nameOrARN)

	g, ok := b.groups.Get(regionKey(region, name))
	if !ok {
		return nil, fmt.Errorf("%w: group %s not found", ErrNotFound, name)
	}

	g.Description = description

	if displayName != "" {
		g.DisplayName = displayName
	}

	if criticality != 0 {
		g.Criticality = criticality
	}

	cp := *g

	return &cp, nil
}

// UpdateGroupQuery updates the resource query of a resource group identified by name or ARN.
func (b *InMemoryBackend) UpdateGroupQuery(
	ctx context.Context,
	nameOrARN string,
	query *ResourceQuery,
) (*Group, error) {
	if err := validateResourceQuery(query); err != nil {
		return nil, err
	}

	b.mu.Lock("UpdateGroupQuery")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	name := resolveGroupName(nameOrARN)

	g, ok := b.groups.Get(regionKey(region, name))
	if !ok {
		return nil, fmt.Errorf("%w: group %s not found", ErrNotFound, name)
	}

	g.ResourceQuery = query
	cp := *g

	return &cp, nil
}

// DeleteGroup deletes a resource group by name or ARN.
// It cascades to remove all associated resources, configurations,
// grouping-status records, and tag-sync tasks for the group.
func (b *InMemoryBackend) DeleteGroup(ctx context.Context, nameOrARN string) error {
	b.mu.Lock("DeleteGroup")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	name := resolveGroupName(nameOrARN)
	tableKey := regionKey(region, name)

	g, ok := b.groups.Get(tableKey)
	if !ok {
		return fmt.Errorf("%w: group %s not found", ErrNotFound, name)
	}

	b.groups.Delete(tableKey)
	g.Tags.Close()

	// Cascade: remove all derived state for this group.
	if b.groupResources[region] != nil {
		delete(b.groupResources[region], name)
	}

	if b.groupingStatuses[region] != nil {
		delete(b.groupingStatuses[region], name)
	}

	if b.groupConfigurations[region] != nil {
		delete(b.groupConfigurations[region], name)
	}

	// Cancel any tag-sync tasks bound to this group. slices.Clone the index
	// result first: Table.Delete mutates the index's backing slice in place,
	// which would otherwise corrupt this in-progress range.
	for _, task := range slices.Clone(b.tagSyncTasksByRegion.Get(region)) {
		if task.GroupName == name {
			b.tagSyncTasks.Delete(regionKey(region, task.TaskArn))
		}
	}

	return nil
}

// ListGroups returns resource groups sorted by name, optionally filtered and paginated.
// Supported filter names: "configuration-type", "resource-type", "name-prefix".
// An empty filters slice returns all groups (up to maxResults).
// Returns the page of groups and a continuation token (empty when no more results).
func (b *InMemoryBackend) ListGroups(
	ctx context.Context,
	filters []ListGroupsFilter,
	nextToken string,
	maxResults int,
) ([]Group, string) {
	b.mu.RLock("ListGroups")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)
	regionGroups := b.groupsByRegion.Get(region)
	out := make([]Group, 0, len(regionGroups))

	for _, g := range regionGroups {
		if !b.groupMatchesFilters(region, g.Name, filters) {
			continue
		}

		cp := *g
		cp.Tags = nil
		out = append(out, cp)
	}

	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })

	page, token := paginate(out, func(g Group) string { return g.Name }, nextToken, maxResults)

	return page, token
}

// groupMatchesFilters returns true when a group satisfies all provided filter criteria.
// Must be called under an active read lock.
func (b *InMemoryBackend) groupMatchesFilters(region, name string, filters []ListGroupsFilter) bool {
	if len(filters) == 0 {
		return true
	}

	var configs []GroupConfigurationItem
	if b.groupConfigurations[region] != nil {
		configs = b.groupConfigurations[region][name]
	}

	for _, f := range filters {
		switch f.Name {
		case listGroupsFilterConfigurationType:
			if !configMatchesTypeFilter(configs, f.Values) {
				return false
			}
		case listGroupsFilterResourceType:
			if !configMatchesResourceTypeFilter(configs, f.Values) {
				return false
			}
		case listGroupsFilterNamePrefix:
			if !nameMatchesPrefixFilter(name, f.Values) {
				return false
			}
		}
	}

	return true
}

// configMatchesTypeFilter returns true if any configuration item has a Type matching one of values.
func configMatchesTypeFilter(configs []GroupConfigurationItem, values []string) bool {
	for _, item := range configs {
		if slices.Contains(values, item.Type) {
			return true
		}
	}

	return false
}

// nameMatchesPrefixFilter returns true if name starts with any of the given prefix values.
func nameMatchesPrefixFilter(name string, values []string) bool {
	for _, prefix := range values {
		if strings.HasPrefix(name, prefix) {
			return true
		}
	}

	return false
}

// configMatchesResourceTypeFilter returns true if any configuration item has an
// allowed-resource-types parameter containing one of values.
func configMatchesResourceTypeFilter(configs []GroupConfigurationItem, values []string) bool {
	for _, item := range configs {
		for _, param := range item.Parameters {
			if param.Name != configParamAllowedResourceTypes {
				continue
			}

			for _, pv := range param.Values {
				if slices.Contains(values, pv) {
					return true
				}
			}
		}
	}

	return false
}

// GetTagsByARN returns the tags for the resource group identified by ARN.
func (b *InMemoryBackend) GetTagsByARN(ctx context.Context, resourceARN string) (map[string]string, error) {
	b.mu.RLock("GetTagsByARN")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	g := b.findByARN(region, resourceARN)
	if g == nil {
		return nil, fmt.Errorf("%w: group with ARN %s not found", ErrNotFound, resourceARN)
	}

	return g.Tags.Clone(), nil
}

// AddTagsByARN merges newTags into the resource group identified by ARN and
// returns the resulting tag set. Rejects reserved aws: tag key prefixes.
func (b *InMemoryBackend) AddTagsByARN(
	ctx context.Context,
	resourceARN string,
	newTags map[string]string,
) (map[string]string, error) {
	if err := validateTagKeys(newTags); err != nil {
		return nil, err
	}

	b.mu.Lock("AddTagsByARN")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	g := b.findByARN(region, resourceARN)
	if g == nil {
		return nil, fmt.Errorf("%w: group with ARN %s not found", ErrNotFound, resourceARN)
	}

	g.Tags.Merge(newTags)

	return g.Tags.Clone(), nil
}

// RemoveTagsByARN removes the specified tag keys from the resource group
// identified by ARN.
func (b *InMemoryBackend) RemoveTagsByARN(ctx context.Context, resourceARN string, keys []string) error {
	b.mu.Lock("RemoveTagsByARN")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	g := b.findByARN(region, resourceARN)
	if g == nil {
		return fmt.Errorf("%w: group with ARN %s not found", ErrNotFound, resourceARN)
	}

	g.Tags.DeleteKeys(keys)

	return nil
}

// findByARN looks up a group by its ARN within the given region (must be called under a lock).
// An ARN uniquely identifies a group, so at most one entry is ever grouped under the index key.
func (b *InMemoryBackend) findByARN(region, resourceARN string) *Group {
	if matches := b.groupsByARN.Get(regionKey(region, resourceARN)); len(matches) > 0 {
		return matches[0]
	}

	return nil
}

// GetAccountSettings returns the account-level settings.
func (b *InMemoryBackend) GetAccountSettings() AccountSettings {
	b.mu.RLock("GetAccountSettings")
	defer b.mu.RUnlock()

	return b.accountSettings
}

// UpdateAccountSettings updates the account-level lifecycle event desired status.
func (b *InMemoryBackend) UpdateAccountSettings(desiredStatus string) error {
	if desiredStatus != accountLifecycleEventsActive &&
		desiredStatus != accountLifecycleEventsInactive {
		return fmt.Errorf(
			"%w: GroupLifecycleEventsDesiredStatus must be %s or %s",
			ErrValidation,
			accountLifecycleEventsActive,
			accountLifecycleEventsInactive,
		)
	}

	b.mu.Lock("UpdateAccountSettings")
	defer b.mu.Unlock()

	b.accountSettings.GroupLifecycleEventsDesiredStatus = desiredStatus
	b.accountSettings.GroupLifecycleEventsStatus = desiredStatus

	return nil
}

// PutGroupConfiguration stores a deep copy of items for the named group.
// It validates each item's Type and Parameters against the known allow-list.
func (b *InMemoryBackend) PutGroupConfiguration(
	ctx context.Context,
	nameOrARN string,
	items []GroupConfigurationItem,
) error {
	if err := validateConfiguration(items); err != nil {
		return err
	}

	b.mu.Lock("PutGroupConfiguration")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	name := resolveGroupName(nameOrARN)

	if !b.groups.Has(regionKey(region, name)) {
		return fmt.Errorf("%w: group %s not found", ErrNotFound, name)
	}

	b.groupConfigurationsStore(region)[name] = cloneConfigItems(items)

	return nil
}

// GetGroupConfigurationItems returns a deep copy of the stored configuration for a group.
func (b *InMemoryBackend) GetGroupConfigurationItems(
	ctx context.Context,
	nameOrARN string,
) ([]GroupConfigurationItem, error) {
	b.mu.RLock("GetGroupConfigurationItems")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)
	name := resolveGroupName(nameOrARN)

	if !b.groups.Has(regionKey(region, name)) {
		return nil, fmt.Errorf("%w: group %s not found", ErrNotFound, name)
	}

	var configs []GroupConfigurationItem
	if b.groupConfigurations[region] != nil {
		configs = b.groupConfigurations[region][name]
	}

	return cloneConfigItems(configs), nil
}

// cloneConfigItems returns a deep copy of a GroupConfigurationItem slice.
func cloneConfigItems(items []GroupConfigurationItem) []GroupConfigurationItem {
	if items == nil {
		return []GroupConfigurationItem{}
	}

	cp := make([]GroupConfigurationItem, len(items))

	for i, item := range items {
		cp[i] = GroupConfigurationItem{Type: item.Type}
		if len(item.Parameters) > 0 {
			cp[i].Parameters = make([]GroupConfigurationParameter, len(item.Parameters))
			for j, p := range item.Parameters {
				cp[i].Parameters[j] = GroupConfigurationParameter{Name: p.Name}
				if len(p.Values) > 0 {
					cp[i].Parameters[j].Values = make([]string, len(p.Values))
					copy(cp[i].Parameters[j].Values, p.Values)
				}
			}
		}
	}

	return cp
}

// GroupResources associates a list of resource ARNs with a group.
// Duplicate ARNs are silently ignored; each ARN is only added once.
func (b *InMemoryBackend) GroupResources(
	ctx context.Context,
	nameOrARN string,
	resourceARNs []string,
) ([]string, error) {
	b.mu.Lock("GroupResources")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	name := resolveGroupName(nameOrARN)

	if !b.groups.Has(regionKey(region, name)) {
		return nil, fmt.Errorf("%w: group %s not found", ErrNotFound, name)
	}

	resStore := b.groupResourcesStore(region)

	if resStore[name] == nil {
		resStore[name] = []string{}
	}

	existing := make(map[string]struct{}, len(resStore[name]))

	for _, a := range resStore[name] {
		existing[a] = struct{}{}
	}

	now := time.Now().UTC()
	succeeded := make([]string, 0, len(resourceARNs))
	statusStore := b.groupingStatusesStore(region)

	for _, a := range resourceARNs {
		if _, dup := existing[a]; !dup {
			resStore[name] = append(resStore[name], a)
			existing[a] = struct{}{}
		}

		succeeded = append(succeeded, a)
		statusStore[name] = append(statusStore[name], GroupingStatusItem{
			ResourceArn: a,
			Action:      groupingActionGroup,
			Status:      groupingStatusSuccess,
			UpdatedAt:   now,
		})
	}

	return succeeded, nil
}

// UngroupResourcesResult holds the result of an UngroupResources call.
type UngroupResourcesResult struct {
	Succeeded []string
	Failed    []GroupingFailedItem
}

// GroupingFailedItem describes a resource that could not be grouped or ungrouped.
type GroupingFailedItem struct {
	ResourceArn  string `json:"ResourceArn"`
	ErrorCode    string `json:"ErrorCode"`
	ErrorMessage string `json:"ErrorMessage"`
}

// UngroupResources removes a list of resource ARNs from a group.
// ARNs that are not currently in the group are returned in Failed[].
func (b *InMemoryBackend) UngroupResources(
	ctx context.Context,
	nameOrARN string,
	resourceARNs []string,
) (*UngroupResourcesResult, error) {
	b.mu.Lock("UngroupResources")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	name := resolveGroupName(nameOrARN)

	if !b.groups.Has(regionKey(region, name)) {
		return nil, fmt.Errorf("%w: group %s not found", ErrNotFound, name)
	}

	resStore := b.groupResourcesStore(region)
	existing := make(map[string]struct{}, len(resStore[name]))

	for _, a := range resStore[name] {
		existing[a] = struct{}{}
	}

	remove := make(map[string]struct{}, len(resourceARNs))
	for _, a := range resourceARNs {
		remove[a] = struct{}{}
	}

	kept := resStore[name][:0:0]
	for _, a := range resStore[name] {
		if _, ok := remove[a]; !ok {
			kept = append(kept, a)
		}
	}

	resStore[name] = kept

	now := time.Now().UTC()
	result := &UngroupResourcesResult{
		Succeeded: make([]string, 0),
		Failed:    make([]GroupingFailedItem, 0),
	}

	statusStore := b.groupingStatusesStore(region)

	for _, a := range resourceARNs {
		if _, wasMember := existing[a]; wasMember {
			result.Succeeded = append(result.Succeeded, a)
			statusStore[name] = append(statusStore[name], GroupingStatusItem{
				ResourceArn: a,
				Action:      groupingActionUngroup,
				Status:      groupingStatusSuccess,
				UpdatedAt:   now,
			})
		} else {
			result.Failed = append(result.Failed, GroupingFailedItem{
				ResourceArn:  a,
				ErrorCode:    groupingErrResourceNotFound,
				ErrorMessage: fmt.Sprintf("resource %s is not a member of group %s", a, name),
			})
			statusStore[name] = append(statusStore[name], GroupingStatusItem{
				ResourceArn:  a,
				Action:       groupingActionUngroup,
				Status:       groupingStatusFailed,
				ErrorCode:    groupingErrResourceNotFound,
				ErrorMessage: fmt.Sprintf("resource %s is not a member of group %s", a, name),
				UpdatedAt:    now,
			})
		}
	}

	return result, nil
}

// ListGroupResources returns resource identifiers associated with a group, optionally
// filtered and paginated. Supported filter Name: "resource-type" (filter by AWS resource type).
// Returns identifiers, a continuation token (empty when no more results), and any error.
func (b *InMemoryBackend) ListGroupResources(
	ctx context.Context,
	nameOrARN string,
	filters []ListGroupResourcesFilter,
	nextToken string,
	maxResults int,
) ([]ResourceIdentifier, string, error) {
	b.mu.RLock("ListGroupResources")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)
	name := resolveGroupName(nameOrARN)

	if !b.groups.Has(regionKey(region, name)) {
		return nil, "", fmt.Errorf("%w: group %s not found", ErrNotFound, name)
	}

	var arns []string
	if b.groupResources[region] != nil {
		arns = b.groupResources[region][name]
	}

	// Build the desired resource type set from filters (if any).
	wantTypes := make(map[string]bool)
	for _, f := range filters {
		if f.Name == listGroupResourcesFilterResourceType {
			for _, v := range f.Values {
				wantTypes[v] = true
			}
		}
	}

	out := make([]ResourceIdentifier, 0, len(arns))

	for _, a := range arns {
		resType := resourceTypeFromARN(a)

		if len(wantTypes) > 0 && !wantTypes[resType] {
			continue
		}

		out = append(out, ResourceIdentifier{ResourceArn: a, ResourceType: resType})
	}

	// Stable sort by ARN for deterministic pagination.
	sort.Slice(out, func(i, j int) bool { return out[i].ResourceArn < out[j].ResourceArn })

	page, token := paginate(out, func(id ResourceIdentifier) string { return id.ResourceArn }, nextToken, maxResults)

	return page, token, nil
}

// ListGroupingStatuses returns the grouping/ungrouping status history for a group,
// paginated. Returns statuses, a continuation token (empty when no more results), and any error.
func (b *InMemoryBackend) ListGroupingStatuses(
	ctx context.Context,
	nameOrARN string,
	nextToken string,
	maxResults int,
) ([]GroupingStatusItem, string, error) {
	b.mu.RLock("ListGroupingStatuses")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)
	name := resolveGroupName(nameOrARN)

	if !b.groups.Has(regionKey(region, name)) {
		return nil, "", fmt.Errorf("%w: group %s not found", ErrNotFound, name)
	}

	var statuses []GroupingStatusItem
	if b.groupingStatuses[region] != nil {
		statuses = b.groupingStatuses[region][name]
	}

	out := make([]GroupingStatusItem, len(statuses))
	copy(out, statuses)

	page, token := paginate(out, func(s GroupingStatusItem) string {
		return s.ResourceArn + "|" + s.Action + "|" + s.UpdatedAt.Format(time.RFC3339Nano)
	}, nextToken, maxResults)

	return page, token, nil
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

// StartTagSyncTask creates a new tag-sync task for an application group.
func (b *InMemoryBackend) StartTagSyncTask(
	ctx context.Context,
	nameOrARN, roleARN, tagKey, tagValue string,
	resourceQuery *ResourceQuery,
) (*TagSyncTask, error) {
	b.mu.Lock("StartTagSyncTask")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	name := resolveGroupName(nameOrARN)

	g, ok := b.groups.Get(regionKey(region, name))
	if !ok {
		return nil, fmt.Errorf("%w: group %s not found", ErrNotFound, name)
	}

	b.taskIDCounter++
	taskARN := arn.Build(
		"resource-groups",
		region,
		b.accountID,
		fmt.Sprintf("tag-sync-task/%s-%s-%d", name, time.Now().Format("20060102150405"), b.taskIDCounter),
	)

	task := &TagSyncTask{
		TaskArn:       taskARN,
		GroupArn:      g.ARN,
		GroupName:     name,
		RoleArn:       roleARN,
		TagKey:        tagKey,
		TagValue:      tagValue,
		ResourceQuery: resourceQuery,
		Status:        tagSyncTaskStatusActive,
		CreatedAt:     time.Now().UTC(),
	}

	b.tagSyncTasks.Put(task)

	cp := *task

	return &cp, nil
}

// CancelTagSyncTask transitions a tag-sync task to CANCELLED status.
// The task remains visible via GetTagSyncTask and ListTagSyncTasks until
// the tagSyncTaskTTL eviction window expires (issue #22 accuracy fix).
func (b *InMemoryBackend) CancelTagSyncTask(ctx context.Context, taskARN string) error {
	b.mu.Lock("CancelTagSyncTask")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	task, ok := b.tagSyncTasks.Get(regionKey(region, taskARN))
	if !ok {
		return fmt.Errorf("%w: task %s not found", ErrTagSyncTaskNotFound, taskARN)
	}

	task.Status = tagSyncTaskStatusCancelled

	return nil
}

// GetTagSyncTask returns a copy of a tag-sync task by ARN.
func (b *InMemoryBackend) GetTagSyncTask(ctx context.Context, taskARN string) (*TagSyncTask, error) {
	b.mu.RLock("GetTagSyncTask")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	task, ok := b.tagSyncTasks.Get(regionKey(region, taskARN))
	if !ok {
		return nil, fmt.Errorf("%w: task %s not found", ErrTagSyncTaskNotFound, taskARN)
	}

	cp := *task

	return &cp, nil
}

// ListTagSyncTasks returns all tag-sync tasks, optionally filtered by group ARN or name,
// paginated. Inactive tasks older than tagSyncTaskTTL are evicted before results are assembled.
// Results are sorted by TaskArn for deterministic ordering.
// Returns tasks, a continuation token (empty when no more results), and any error.
func (b *InMemoryBackend) ListTagSyncTasks(
	ctx context.Context,
	filters []ListTagSyncTasksFilter,
	nextToken string,
	maxResults int,
) ([]TagSyncTask, string, error) {
	b.mu.Lock("ListTagSyncTasks")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	cutoff := time.Now().UTC().Add(-tagSyncTaskTTL)

	// Evict stale non-active tasks. slices.Clone the index result first:
	// Table.Delete mutates the index's backing slice in place, which would
	// otherwise corrupt this in-progress range.
	for _, task := range slices.Clone(b.tagSyncTasksByRegion.Get(region)) {
		if task.Status != tagSyncTaskStatusActive && task.CreatedAt.Before(cutoff) {
			b.tagSyncTasks.Delete(regionKey(region, task.TaskArn))
		}
	}

	tasks := b.tagSyncTasksByRegion.Get(region)
	out := make([]TagSyncTask, 0, len(tasks))

	for _, task := range tasks {
		if !taskMatchesFilters(task, filters) {
			continue
		}

		out = append(out, *task)
	}

	sort.Slice(out, func(i, j int) bool { return out[i].TaskArn < out[j].TaskArn })

	page, token := paginate(out, func(t TagSyncTask) string { return t.TaskArn }, nextToken, maxResults)

	return page, token, nil
}

// taskMatchesFilters returns true when task satisfies all provided filter criteria.
// An empty filter list matches all tasks.
func taskMatchesFilters(task *TagSyncTask, filters []ListTagSyncTasksFilter) bool {
	if len(filters) == 0 {
		return true
	}

	for _, f := range filters {
		if (f.GroupArn == "" || f.GroupArn == task.GroupArn) &&
			(f.GroupName == "" || f.GroupName == task.GroupName) {
			return true
		}
	}

	return false
}

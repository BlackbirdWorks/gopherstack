package amp

import (
	"time"

	"github.com/blackbirdworks/gopherstack/services/amp/promql"
)

const (
	statusActive   = "ACTIVE"
	statusCreating = "CREATING"
	statusUpdating = "UPDATING"
	statusDeleting = "DELETING"

	policyNone = "NO_POLICY"
)

// Pending is a transitional state that lasts until Until; a deleting resource is gone after it.
type Pending struct {
	Until time.Time `json:"until"`
	Kind  string    `json:"kind"`
}

// Doc is a base64-on-the-wire YAML or JSON document with its lifecycle.
type Doc struct {
	CreatedAt  time.Time `json:"createdAt"`
	ModifiedAt time.Time `json:"modifiedAt"`
	Pending    Pending   `json:"pending"`
	Data       []byte    `json:"data"`
}

// LoggingConfig is a workspace's rules-and-alerting log delivery.
type LoggingConfig struct {
	CreatedAt   time.Time `json:"createdAt"`
	ModifiedAt  time.Time `json:"modifiedAt"`
	Pending     Pending   `json:"pending"`
	LogGroupARN string    `json:"logGroupArn"`
}

// LoggingDestination is one query-logging destination.
type LoggingDestination struct {
	LogGroupARN  string `json:"logGroupArn"`
	QspThreshold int64  `json:"qspThreshold"`
}

// QueryLoggingConfig is a workspace's query log delivery.
type QueryLoggingConfig struct {
	CreatedAt    time.Time            `json:"createdAt"`
	ModifiedAt   time.Time            `json:"modifiedAt"`
	Pending      Pending              `json:"pending"`
	Destinations []LoggingDestination `json:"destinations"`
}

// LabelSetLimit caps the active series matching a label set.
type LabelSetLimit struct {
	LabelSet  map[string]string `json:"labelSet"`
	MaxSeries *int64            `json:"maxSeries,omitempty"`
}

// WorkspaceConfig holds the tunable workspace settings.
type WorkspaceConfig struct {
	Pending                       Pending         `json:"pending"`
	LimitsPerLabelSet             []LabelSetLimit `json:"limitsPerLabelSet,omitempty"`
	RetentionPeriodInDays         int32           `json:"retentionPeriodInDays"`
	OutOfOrderTimeWindowInSeconds int32           `json:"outOfOrderTimeWindowInSeconds"`
	RuleQueryOffsetInSeconds      int32           `json:"ruleQueryOffsetInSeconds"`
	OutOfOrderSet                 bool            `json:"outOfOrderSet,omitempty"`
	RuleQueryOffsetSet            bool            `json:"ruleQueryOffsetSet,omitempty"`
}

// ResourcePolicy is a workspace's resource-based policy.
type ResourcePolicy struct {
	Pending    Pending `json:"pending"`
	Document   string  `json:"document"`
	RevisionID string  `json:"revisionId"`
}

// Workspace is an AMP workspace with its singleton sub-resources.
type Workspace struct {
	CreatedAt    time.Time           `json:"createdAt"`
	Tags         map[string]string   `json:"tags,omitempty"`
	AlertManager *Doc                `json:"alertManager,omitempty"`
	Logging      *LoggingConfig      `json:"logging,omitempty"`
	QueryLogging *QueryLoggingConfig `json:"queryLogging,omitempty"`
	Policy       *ResourcePolicy     `json:"policy,omitempty"`
	Pending      Pending             `json:"pending"`
	ID           string              `json:"id"`
	AccountID    string              `json:"accountId"`
	Region       string              `json:"region"`
	Alias        string              `json:"alias,omitempty"`
	KMSKeyARN    string              `json:"kmsKeyArn,omitempty"`
	ARN          string              `json:"arn"`
	Config       WorkspaceConfig     `json:"config"`
}

// RuleGroupsNamespace is a Prometheus rules file inside a workspace.
type RuleGroupsNamespace struct {
	Tags        map[string]string `json:"tags,omitempty"`
	WorkspaceID string            `json:"workspaceId"`
	Name        string            `json:"name"`
	ARN         string            `json:"arn"`
	Doc         Doc               `json:"doc"`
}

// AnomalyDetector is a Random Cut Forest anomaly detector.
type AnomalyDetector struct {
	ModifiedAt                  time.Time         `json:"modifiedAt"`
	CreatedAt                   time.Time         `json:"createdAt"`
	IgnoreAbove                 *IgnoreNear       `json:"ignoreAbove,omitempty"`
	Tags                        map[string]string `json:"tags,omitempty"`
	Labels                      map[string]string `json:"labels,omitempty"`
	IgnoreBelow                 *IgnoreNear       `json:"ignoreBelow,omitempty"`
	Pending                     Pending           `json:"pending"`
	ARN                         string            `json:"arn"`
	Alias                       string            `json:"alias"`
	Query                       string            `json:"query"`
	MissingDataAction           string            `json:"missingDataAction,omitempty"`
	ID                          string            `json:"id"`
	WorkspaceID                 string            `json:"workspaceId"`
	EvaluationIntervalInSeconds int32             `json:"evaluationIntervalInSeconds"`
	ShingleSize                 int32             `json:"shingleSize"`
	SampleSize                  int32             `json:"sampleSize"`
}

// IgnoreNear is an amount or ratio tolerance around the expected value.
type IgnoreNear struct {
	Amount *float64 `json:"amount,omitempty"`
	Ratio  *float64 `json:"ratio,omitempty"`
}

// Scraper is an AWS-managed Prometheus collector.
type Scraper struct {
	CreatedAt      time.Time         `json:"createdAt"`
	LastModifiedAt time.Time         `json:"lastModifiedAt"`
	Tags           map[string]string `json:"tags,omitempty"`
	Logging        *ScraperLogging   `json:"logging,omitempty"`
	Pending        Pending           `json:"pending"`
	Destination    Destination       `json:"destination"`
	ARN            string            `json:"arn"`
	AccountID      string            `json:"accountId"`
	Region         string            `json:"region"`
	Alias          string            `json:"alias,omitempty"`
	RoleARN        string            `json:"roleArn"`
	ID             string            `json:"id"`
	SourceRole     string            `json:"sourceRoleArn,omitempty"`
	TargetRole     string            `json:"targetRoleArn,omitempty"`
	ScrapeConfig   []byte            `json:"scrapeConfig"`
	Exporters      []string          `json:"exporters,omitempty"`
	Source         Source            `json:"source"`
}

// Destination is where a scraper sends metrics; exactly one field is set.
type Destination struct {
	WorkspaceARN string `json:"workspaceArn,omitempty"`
	DatasetARN   string `json:"datasetArn,omitempty"`
}

// Source is what a scraper collects from; EKS or VPC.
type Source struct {
	ClusterARN       string   `json:"clusterArn,omitempty"`
	SubnetIDs        []string `json:"subnetIds"`
	SecurityGroupIDs []string `json:"securityGroupIds,omitempty"`
	VPC              bool     `json:"vpc,omitempty"`
}

// ScraperComponent selects one component for scraper logging.
type ScraperComponent struct {
	Options map[string]string `json:"options,omitempty"`
	Type    string            `json:"type"`
}

// ScraperLogging is a scraper's log delivery configuration.
type ScraperLogging struct {
	ModifiedAt  time.Time          `json:"modifiedAt"`
	Pending     Pending            `json:"pending"`
	LogGroupARN string             `json:"logGroupArn"`
	Components  []ScraperComponent `json:"components"`
}

// SeriesRecord is the stored samples of one series in a workspace.
type SeriesRecord struct {
	Labels      promql.Labels   `json:"labels"`
	Key         string          `json:"key"`
	WorkspaceID string          `json:"workspaceId"`
	Samples     []promql.Sample `json:"samples"`
}

package amp

import (
	"maps"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/awstime"
)

type wStatus struct {
	StatusCode   string `json:"statusCode"`
	StatusReason string `json:"statusReason,omitempty"`
}

type wCode struct {
	StatusCode string `json:"statusCode"`
}

type wWorkspace struct {
	Tags               map[string]string `json:"tags,omitempty"`
	Alias              string            `json:"alias,omitempty"`
	Arn                string            `json:"arn"`
	KmsKeyArn          string            `json:"kmsKeyArn,omitempty"`
	PrometheusEndpoint string            `json:"prometheusEndpoint,omitempty"`
	WorkspaceID        string            `json:"workspaceId"`
	Status             wCode             `json:"status"`
	CreatedAt          float64           `json:"createdAt"`
}

type wAlertManager struct {
	Status     wStatus `json:"status"`
	Data       []byte  `json:"data"`
	CreatedAt  float64 `json:"createdAt"`
	ModifiedAt float64 `json:"modifiedAt"`
}

type wLogging struct {
	Workspace   string  `json:"workspace"`
	LogGroupArn string  `json:"logGroupArn"`
	Status      wStatus `json:"status"`
	CreatedAt   float64 `json:"createdAt"`
	ModifiedAt  float64 `json:"modifiedAt"`
}

type wCloudWatch struct {
	LogGroupArn string `json:"logGroupArn"`
}

type wQspFilter struct {
	QspThreshold int64 `json:"qspThreshold"`
}

type wLoggingDestination struct {
	CloudWatchLogs wCloudWatch `json:"cloudWatchLogs"`
	Filters        wQspFilter  `json:"filters"`
}

type wQueryLogging struct {
	Status       wStatus               `json:"status"`
	Workspace    string                `json:"workspace"`
	Destinations []wLoggingDestination `json:"destinations"`
	CreatedAt    float64               `json:"createdAt"`
	ModifiedAt   float64               `json:"modifiedAt"`
}

type wLimitEntry struct {
	MaxSeries *int64 `json:"maxSeries,omitempty"`
}

type wLimit struct {
	LabelSet map[string]string `json:"labelSet"`
	Limits   wLimitEntry       `json:"limits"`
}

type wWorkspaceConfig struct {
	OutOfOrderTimeWindowInSeconds *int32   `json:"outOfOrderTimeWindowInSeconds,omitempty"`
	RuleQueryOffsetInSeconds      *int32   `json:"ruleQueryOffsetInSeconds,omitempty"`
	Status                        wStatus  `json:"status"`
	LimitsPerLabelSet             []wLimit `json:"limitsPerLabelSet,omitempty"`
	RetentionPeriodInDays         int32    `json:"retentionPeriodInDays"`
}

type wNamespace struct {
	Tags       map[string]string `json:"tags,omitempty"`
	Status     wStatus           `json:"status"`
	Arn        string            `json:"arn"`
	Name       string            `json:"name"`
	Data       []byte            `json:"data,omitempty"`
	CreatedAt  float64           `json:"createdAt"`
	ModifiedAt float64           `json:"modifiedAt"`
}

type wIgnoreNear struct {
	Amount *float64 `json:"amount,omitempty"`
	Ratio  *float64 `json:"ratio,omitempty"`
}

type wRandomCutForest struct {
	IgnoreNearExpectedFromAbove *wIgnoreNear `json:"ignoreNearExpectedFromAbove,omitempty"`
	IgnoreNearExpectedFromBelow *wIgnoreNear `json:"ignoreNearExpectedFromBelow,omitempty"`
	Query                       string       `json:"query"`
	ShingleSize                 int32        `json:"shingleSize"`
	SampleSize                  int32        `json:"sampleSize"`
}

type wDetectorConfig struct {
	RandomCutForest wRandomCutForest `json:"randomCutForest"`
}

type wDetector struct {
	Tags                        map[string]string `json:"tags,omitempty"`
	Labels                      map[string]string `json:"labels,omitempty"`
	MissingDataAction           map[string]bool   `json:"missingDataAction,omitempty"`
	Configuration               *wDetectorConfig  `json:"configuration,omitempty"`
	EvaluationIntervalInSeconds *int32            `json:"evaluationIntervalInSeconds,omitempty"`
	Alias                       string            `json:"alias"`
	AnomalyDetectorID           string            `json:"anomalyDetectorId"`
	Arn                         string            `json:"arn"`
	Status                      wStatus           `json:"status"`
	CreatedAt                   float64           `json:"createdAt"`
	ModifiedAt                  float64           `json:"modifiedAt"`
}

type wAmpDestination struct {
	WorkspaceArn string `json:"workspaceArn"`
}

type wCloudWatchDestination struct {
	DatasetArn string `json:"datasetArn"`
}

type wDestination struct {
	AmpConfiguration        *wAmpDestination        `json:"ampConfiguration,omitempty"`
	CloudWatchConfiguration *wCloudWatchDestination `json:"cloudWatchConfiguration,omitempty"`
}

type wEks struct {
	ClusterArn       string   `json:"clusterArn"`
	SubnetIDs        []string `json:"subnetIds"`
	SecurityGroupIDs []string `json:"securityGroupIds,omitempty"`
}

type wVPC struct {
	SubnetIDs        []string `json:"subnetIds"`
	SecurityGroupIDs []string `json:"securityGroupIds"`
}

type wSource struct {
	EksConfiguration *wEks `json:"eksConfiguration,omitempty"`
	VpcConfiguration *wVPC `json:"vpcConfiguration,omitempty"`
}

type wScrapeConfig struct {
	ConfigurationBlob []byte `json:"configurationBlob"`
}

type wOpenSearch struct {
	DomainArn string `json:"domainArn"`
}

type wExporter struct {
	OpenSearchConfiguration wOpenSearch `json:"openSearchConfiguration"`
}

type wRoleConfig struct {
	SourceRoleArn string `json:"sourceRoleArn,omitempty"`
	TargetRoleArn string `json:"targetRoleArn,omitempty"`
}

type wScraper struct {
	Destination         wDestination      `json:"destination"`
	Source              wSource           `json:"source"`
	RoleConfiguration   *wRoleConfig      `json:"roleConfiguration,omitempty"`
	Tags                map[string]string `json:"tags,omitempty"`
	Status              wCode             `json:"status"`
	Alias               string            `json:"alias,omitempty"`
	Arn                 string            `json:"arn"`
	RoleArn             string            `json:"roleArn"`
	ScraperID           string            `json:"scraperId"`
	Exporters           []wExporter       `json:"exporters,omitempty"`
	ScrapeConfiguration wScrapeConfig     `json:"scrapeConfiguration"`
	CreatedAt           float64           `json:"createdAt"`
	LastModifiedAt      float64           `json:"lastModifiedAt"`
}

type wScraperLogging struct {
	LoggingDestination map[string]wCloudWatch `json:"loggingDestination"`
	Status             wStatus                `json:"status"`
	ScraperID          string                 `json:"scraperId"`
	ScraperComponents  []wScraperComponent    `json:"scraperComponents"`
	ModifiedAt         float64                `json:"modifiedAt"`
}

type wScraperComponent struct {
	Config *wComponentConfig `json:"config,omitempty"`
	Type   string            `json:"type"`
}

type wComponentConfig struct {
	Options map[string]string `json:"options,omitempty"`
}

func epoch(t time.Time) float64 { return awstime.Epoch(t) }

func cloneTags(t map[string]string) map[string]string {
	if len(t) == 0 {
		return nil
	}

	return maps.Clone(t)
}

func (b *InMemoryBackend) wireStatus(p Pending) wStatus { return wStatus{StatusCode: b.state(p)} }

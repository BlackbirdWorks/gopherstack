package shield

import (
	"maps"
	"time"
)

// Aggregation values for protection groups.
const (
	AggregationSum  = "SUM"
	AggregationMean = "MEAN"
	AggregationMax  = "MAX"
)

// Pattern values for protection groups.
const (
	PatternAll            = "ALL"
	PatternArbitrary      = "ARBITRARY"
	PatternByResourceType = "BY_RESOURCE_TYPE"
)

// AutoRenew values for subscriptions.
const (
	AutoRenewEnabled  = "ENABLED"
	AutoRenewDisabled = "DISABLED"
)

// ResourceType values for Shield Advanced protected resources.
const (
	ResourceTypeCloudFrontDistribution  = "CLOUDFRONT_DISTRIBUTION"
	ResourceTypeRoute53HostedZone       = "ROUTE_53_HOSTED_ZONE"
	ResourceTypeApplicationLoadBalancer = "APPLICATION_LOAD_BALANCER"
	ResourceTypeClassicLoadBalancer     = "CLASSIC_LOAD_BALANCER"
	ResourceTypeElasticIPAllocation     = "ELASTIC_IP_ALLOCATION"
	ResourceTypeGlobalAccelerator       = "GLOBAL_ACCELERATOR"
)

// ProactiveEngagementStatus values.
const (
	ProactiveEngagementEnabled  = "ENABLED"
	ProactiveEngagementDisabled = "DISABLED"
	ProactiveEngagementPending  = "PENDING"
)

// Protection represents an AWS Shield Advanced protection.
type Protection struct {
	CreationTime   time.Time         `json:"creationTime"`
	Tags           map[string]string `json:"tags,omitempty"`
	ID             string            `json:"id"`
	ProtectionArn  string            `json:"protectionArn"`
	Name           string            `json:"name"`
	ResourceARN    string            `json:"resourceARN"`
	HealthCheckIDs []string          `json:"healthCheckIds,omitempty"`
}

// cloneProtection returns a deep copy of p, including its Tags map.
func cloneProtection(p *Protection) *Protection {
	cp := *p
	cp.Tags = maps.Clone(p.Tags)

	if p.HealthCheckIDs != nil {
		cp.HealthCheckIDs = append([]string(nil), p.HealthCheckIDs...)
	}

	return &cp
}

// Subscription represents an AWS Shield Advanced subscription.
type Subscription struct {
	StartTime            time.Time `json:"startTime"`
	EndTime              time.Time `json:"endTime"`
	AutoRenew            string    `json:"autoRenew"`
	TimeCommitmentInDays int64     `json:"timeCommitmentInDays"`
}

// EmergencyContact represents an emergency contact for proactive engagement.
type EmergencyContact struct {
	EmailAddress string `json:"emailAddress"`
	PhoneNumber  string `json:"phoneNumber,omitempty"`
	ContactNotes string `json:"contactNotes,omitempty"`
}

// DRTAccess holds DRT log bucket and role configuration.
type DRTAccess struct {
	RoleArn       string   `json:"roleArn"`
	LogBucketList []string `json:"logBucketList"`
}

// ProtectionGroup represents a Shield Advanced protection group.
type ProtectionGroup struct {
	CreationTime       time.Time `json:"creationTime"`
	ID                 string    `json:"id"`
	ProtectionGroupArn string    `json:"protectionGroupArn"`
	Aggregation        string    `json:"aggregation"`
	Pattern            string    `json:"pattern"`
	ResourceType       string    `json:"resourceType,omitempty"`
	Members            []string  `json:"members"`
}

// cloneProtectionGroup returns a deep copy of a ProtectionGroup.
func cloneProtectionGroup(pg *ProtectionGroup) *ProtectionGroup {
	cp := *pg

	if pg.Members != nil {
		cp.Members = append([]string(nil), pg.Members...)
	}

	return &cp
}

// AttackVector represents a type of attack traffic seen during an attack.
type AttackVector struct {
	VectorType string `json:"VectorType"`
}

// AttackCounter represents a named counter during an attack.
//

type AttackCounter struct {
	Name    string  `json:"Name"`
	Unit    string  `json:"Unit"`
	Max     float64 `json:"Max"`
	Average float64 `json:"Average"`
	Sum     float64 `json:"Sum"`
	N       int64   `json:"N"`
}

// Mitigation represents a mitigation applied during an attack.
type Mitigation struct {
	MitigationName string `json:"MitigationName"`
}

// Attack represents a Shield Advanced attack event.
type Attack struct {
	StartTime      time.Time       `json:"startTime"`
	EndTime        time.Time       `json:"endTime"`
	AttackID       string          `json:"attackId"`
	ResourceARN    string          `json:"resourceArn"`
	AttackVectors  []AttackVector  `json:"attackVectors,omitempty"`
	AttackCounters []AttackCounter `json:"attackCounters,omitempty"`
	Mitigations    []Mitigation    `json:"mitigations,omitempty"`
}

// AttackVolume represents volume metrics in attack statistics.
type AttackVolume struct {
	BitsPerSecond     *AttackVolumeStatistics `json:"BitsPerSecond,omitempty"`
	PacketsPerSecond  *AttackVolumeStatistics `json:"PacketsPerSecond,omitempty"`
	RequestsPerSecond *AttackVolumeStatistics `json:"RequestsPerSecond,omitempty"`
}

// AttackVolumeStatistics is a single volume metric.
type AttackVolumeStatistics struct {
	Max float64 `json:"Max"`
}

// AttackStatistics represents Shield Advanced attack statistics.
type AttackStatistics struct {
	DataItems []AttackStatisticsItem `json:"dataItems"`
	TimeRange AttackTimeRange        `json:"timeRange"`
}

// AttackTimeRange represents a time range for attack statistics.
type AttackTimeRange struct {
	FromInclusive int64 `json:"fromInclusive"`
	ToExclusive   int64 `json:"toExclusive"`
}

// AttackStatisticsItem is a single item in attack statistics.
type AttackStatisticsItem struct {
	AttackVolume *AttackVolume `json:"AttackVolume,omitempty"`
	AttackCount  int64         `json:"AttackCount"`
}

// ALARConfig holds Application Layer Automatic Response configuration for a protection.
type ALARConfig struct {
	// ResourceARN identifies the protected resource this ALAR configuration
	// applies to. It is tagged json:"-" because ALARConfig has no natural
	// identity field of its own -- it was only ever reached via the external
	// map[string]*ALARConfig key. The alarConfigs Table's keyFn derives its
	// key from this field (see store_setup.go), and persistence.go's
	// alarConfigDTO carries it as a real JSON field so Snapshot/Restore
	// round-trips correctly.
	ResourceARN string `json:"-"`
	// Action is either "BLOCK" or "COUNT".
	Action  string `json:"action"`
	Enabled bool   `json:"enabled"`
}

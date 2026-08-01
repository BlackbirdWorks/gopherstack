// Package resiliencehub emulates the AWS Resilience Hub control-plane API
// (aws-sdk-go-v2/service/resiliencehub, protocol restjson1).
//
// See PARITY.md for the full pre-implementation audit this package was built
// from, including the honest-gap list for AssessmentSummary, ResiliencyScore,
// and the four recommendation families (SOP/alarm/test/component) plus the
// resource-grouping-recommendation feature -- all proprietary
// scoring/ML/curated-knowledge-base outputs this emulator cannot derive from
// the SDK and therefore never fabricates.
package resiliencehub

import (
	"maps"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

// EventSubscription mirrors types.EventSubscription.
type EventSubscription struct {
	EventType   string
	Name        string
	SnsTopicArn string
}

// PermissionModel mirrors types.PermissionModel.
type PermissionModel struct {
	Type                 string
	InvokerRoleName      string
	CrossAccountRoleArns []string
}

// clone returns a deep copy of m, or nil if m is nil.
func (m *PermissionModel) clone() *PermissionModel {
	if m == nil {
		return nil
	}

	cp := *m
	cp.CrossAccountRoleArns = cloneStrs(m.CrossAccountRoleArns)

	return &cp
}

// LogicalResourceID mirrors types.LogicalResourceId.
type LogicalResourceID struct {
	Identifier          string
	EksSourceName       string
	LogicalStackName    string
	ResourceGroupName   string
	TerraformSourceName string
}

// clone returns a deep copy of l, or nil if l is nil.
func (l *LogicalResourceID) clone() *LogicalResourceID {
	if l == nil {
		return nil
	}

	cp := *l

	return &cp
}

// PhysicalResourceID mirrors types.PhysicalResourceId.
type PhysicalResourceID struct {
	Identifier   string
	Type         string
	AwsAccountID string
	AwsRegion    string
}

// clone returns a deep copy of p, or nil if p is nil.
func (p *PhysicalResourceID) clone() *PhysicalResourceID {
	if p == nil {
		return nil
	}

	cp := *p

	return &cp
}

// AppComponent mirrors types.AppComponent.
type AppComponent struct {
	AdditionalInfo map[string][]string
	ID             string
	Name           string
	Type           string
}

// clone returns a deep copy of c, or nil if c is nil.
func (c *AppComponent) clone() *AppComponent {
	if c == nil {
		return nil
	}

	cp := *c
	cp.AdditionalInfo = cloneStrSliceMap(c.AdditionalInfo)

	return &cp
}

// PhysicalResource mirrors types.PhysicalResource.
type PhysicalResource struct {
	LogicalResourceID  *LogicalResourceID
	PhysicalResourceID *PhysicalResourceID
	AdditionalInfo     map[string][]string
	ResourceType       string
	ParentResourceName string
	ResourceName       string
	SourceType         string
	AppComponents      []string
	Excluded           bool
}

// clone returns a deep copy of r, or nil if r is nil.
func (r *PhysicalResource) clone() *PhysicalResource {
	if r == nil {
		return nil
	}

	cp := *r
	cp.LogicalResourceID = r.LogicalResourceID.clone()
	cp.PhysicalResourceID = r.PhysicalResourceID.clone()
	cp.AdditionalInfo = cloneStrSliceMap(r.AdditionalInfo)
	cp.AppComponents = cloneStrs(r.AppComponents)

	return &cp
}

// ResourceMapping mirrors types.ResourceMapping.
type ResourceMapping struct {
	MappingType         string
	PhysicalResourceID  *PhysicalResourceID
	AppRegistryAppName  string
	EksSourceName       string
	LogicalStackName    string
	ResourceGroupName   string
	ResourceName        string
	TerraformSourceName string
}

// clone returns a deep copy of m.
func (m ResourceMapping) clone() ResourceMapping {
	cp := m
	cp.PhysicalResourceID = m.PhysicalResourceID.clone()

	return cp
}

// cloneResourceMappings returns a deep copy of ms.
func cloneResourceMappings(ms []ResourceMapping) []ResourceMapping {
	if ms == nil {
		return nil
	}

	cp := make([]ResourceMapping, len(ms))
	for i, m := range ms {
		cp[i] = m.clone()
	}

	return cp
}

// EksSourceClusterNamespace mirrors types.EksSourceClusterNamespace.
type EksSourceClusterNamespace struct {
	EksClusterArn string
	Namespace     string
}

// EksSource mirrors types.EksSource.
type EksSource struct {
	EksClusterArn string
	Namespaces    []string
}

// TerraformSource mirrors types.TerraformSource.
type TerraformSource struct {
	S3StateFileURL string
}

// AppInputSource mirrors types.AppInputSource.
type AppInputSource struct {
	EksSourceClusterNamespace *EksSourceClusterNamespace
	TerraformSource           *TerraformSource
	ImportType                string
	SourceArn                 string
	SourceName                string
	ResourceCount             int32
}

// ResolutionState is the internal bookkeeping for one AppVersion's most
// recent ResolveAppVersionResources call.
type ResolutionState struct {
	ResolutionID string
	Status       string
	ErrorMessage string
}

// ImportState is the internal bookkeeping for one App's most recent
// ImportResourcesToDraftAppVersion call (draft-only, per PARITY.md).
type ImportState struct {
	StatusChangeTime time.Time
	Status           string
	ErrorMessage     string
	ErrorDetails     []string
}

// AppVersion is the internal state of one version (draft, or a published
// numbered snapshot) of an App's template/resources/components. Number is
// either the draftVersion sentinel or a decimal string ("1", "2", ...).
type AppVersion struct {
	CreationTime     time.Time
	AdditionalInfo   map[string][]string
	Resolution       *ResolutionState
	Number           string
	VersionName      string
	TemplateBody     string
	AppComponents    []*AppComponent
	Resources        []*PhysicalResource
	ResourceMappings []ResourceMapping
	InputSources     []AppInputSource
}

// clone returns a deep copy of v, or nil if v is nil.
func (v *AppVersion) clone() *AppVersion {
	if v == nil {
		return nil
	}

	cp := *v
	cp.AdditionalInfo = cloneStrSliceMap(v.AdditionalInfo)

	cp.AppComponents = make([]*AppComponent, len(v.AppComponents))
	for i, c := range v.AppComponents {
		cp.AppComponents[i] = c.clone()
	}

	cp.Resources = make([]*PhysicalResource, len(v.Resources))
	for i, r := range v.Resources {
		cp.Resources[i] = r.clone()
	}

	cp.ResourceMappings = cloneResourceMappings(v.ResourceMappings)
	cp.InputSources = append([]AppInputSource(nil), v.InputSources...)

	if v.Resolution != nil {
		res := *v.Resolution
		cp.Resolution = &res
	}

	return &cp
}

// App is the internal state of one Resilience Hub application, a superset of
// types.App plus the draft/published AppVersion bookkeeping the wire type
// itself carries no fields for.
type App struct {
	ID                                string
	ARN                               string
	Name                              string
	Description                       string
	CreationTime                      time.Time
	AssessmentSchedule                string
	AwsApplicationArn                 string
	ComplianceStatus                  string
	DriftStatus                       string
	EventSubscriptions                []EventSubscription
	LastAppComplianceEvaluationTime   time.Time
	LastDriftEvaluationTime           time.Time
	LastResiliencyScoreEvaluationTime time.Time
	PermissionModel                   *PermissionModel
	PolicyArn                         string
	Status                            string
	Tags                              *tags.Tags

	// Import is DescribeDraftAppVersionResourcesImportStatus's state --
	// draft-only, per PARITY.md.
	Import *ImportState

	// Draft is the mutable working version every App is created with.
	// Published holds every snapshot PublishAppVersion has produced, ordered
	// oldest-first (Number "1", "2", ...).
	Draft             *AppVersion
	Published         []*AppVersion
	NextVersionNumber int64
}

// clone returns a deep copy of a, so the returned App shares no mutable
// memory with the backend's stored copy. Tags is deliberately NOT cloned:
// *tags.Tags is its own concurrency-safe type -- see
// services/outposts/outposts.go's identical clone() doc comment.
func (a *App) clone() *App {
	cp := *a
	cp.EventSubscriptions = append([]EventSubscription(nil), a.EventSubscriptions...)
	cp.PermissionModel = a.PermissionModel.clone()
	cp.Draft = a.Draft.clone()

	cp.Published = make([]*AppVersion, len(a.Published))
	for i, v := range a.Published {
		cp.Published[i] = v.clone()
	}

	if a.Import != nil {
		imp := *a.Import
		imp.ErrorDetails = cloneStrs(a.Import.ErrorDetails)
		cp.Import = &imp
	}

	return &cp
}

// FailurePolicy mirrors types.FailurePolicy -- plain int32 (not pointers),
// always present on the wire.
type FailurePolicy struct {
	RpoInSecs int32
	RtoInSecs int32
}

// ResiliencyPolicy is the internal state of one resiliency policy.
type ResiliencyPolicy struct {
	CreationTime           time.Time
	Policy                 map[string]FailurePolicy
	Tags                   *tags.Tags
	ARN                    string
	Name                   string
	Tier                   string
	DataLocationConstraint string
	EstimatedCostTier      string
	Description            string
}

// clone returns a deep copy of p, or nil if p is nil.
func (p *ResiliencyPolicy) clone() *ResiliencyPolicy {
	if p == nil {
		return nil
	}

	cp := *p
	cp.Policy = make(map[string]FailurePolicy, len(p.Policy))
	maps.Copy(cp.Policy, p.Policy)

	return &cp
}

// DisruptionCompliance mirrors types.DisruptionCompliance.
type DisruptionCompliance struct {
	ComplianceStatus    string
	Message             string
	RpoDescription      string
	RpoReferenceID      string
	RtoDescription      string
	RtoReferenceID      string
	AchievableRpoInSecs int32
	AchievableRtoInSecs int32
	CurrentRpoInSecs    int32
	CurrentRtoInSecs    int32
}

// ResourceError mirrors types.ResourceError.
type ResourceError struct {
	LogicalResourceID  string
	PhysicalResourceID string
	Reason             string
}

// ResourceErrorsDetails mirrors types.ResourceErrorsDetails.
type ResourceErrorsDetails struct {
	ResourceErrors []ResourceError
	HasMoreErrors  bool
}

// ScoringComponentResiliencyScore mirrors types.ScoringComponentResiliencyScore.
type ScoringComponentResiliencyScore struct {
	ExcludedCount    int64
	OutstandingCount int64
	PossibleScore    float64
	Score            float64
}

// ResiliencyScore mirrors types.ResiliencyScore. PLACEHOLDER ONLY: Score and
// DisruptionScore are outputs of AWS's proprietary resiliency-scoring model,
// which weighs resource redundancy, failover configuration, and backup
// posture across the real resource graph. No formula is derivable from the
// SDK (see PARITY.md's honest-gap section) -- this backend always sets Score
// to a fixed, documented placeholder (scorePlaceholder, see assessments.go)
// rather than inventing a number that would read as real analysis.
type ResiliencyScore struct {
	DisruptionScore map[string]float64
	ComponentScore  map[string]ScoringComponentResiliencyScore
	Score           float64
}

// AppAssessment is the internal state of one assessment run.
type AppAssessment struct {
	StartTime             time.Time
	EndTime               time.Time
	Compliance            map[string]DisruptionCompliance
	Policy                *ResiliencyPolicy
	ResiliencyScore       *ResiliencyScore
	ResourceErrorsDetails *ResourceErrorsDetails
	Tags                  *tags.Tags
	ARN                   string
	AppArn                string
	AppID                 string
	AppVersion            string
	AssessmentName        string
	Status                string
	Invoker               string
	ComplianceStatus      string
	DriftStatus           string
	Message               string
	VersionName           string
	ClientToken           string
}

// clone returns a deep copy of a, or nil if a is nil.
func (a *AppAssessment) clone() *AppAssessment {
	if a == nil {
		return nil
	}

	cp := *a
	cp.Compliance = make(map[string]DisruptionCompliance, len(a.Compliance))
	maps.Copy(cp.Compliance, a.Compliance)

	cp.Policy = a.Policy.clone()

	if a.ResiliencyScore != nil {
		rs := *a.ResiliencyScore
		rs.DisruptionScore = make(map[string]float64, len(a.ResiliencyScore.DisruptionScore))
		maps.Copy(rs.DisruptionScore, a.ResiliencyScore.DisruptionScore)

		rs.ComponentScore = make(map[string]ScoringComponentResiliencyScore, len(a.ResiliencyScore.ComponentScore))
		maps.Copy(rs.ComponentScore, a.ResiliencyScore.ComponentScore)

		cp.ResiliencyScore = &rs
	}

	if a.ResourceErrorsDetails != nil {
		red := *a.ResourceErrorsDetails
		red.ResourceErrors = append([]ResourceError(nil), a.ResourceErrorsDetails.ResourceErrors...)
		cp.ResourceErrorsDetails = &red
	}

	return &cp
}

// S3Location mirrors types.S3Location.
type S3Location struct {
	Bucket string
	Prefix string
}

// RecommendationTemplate is the internal state of one recommendation
// template. TemplatesLocation is a synthetic, documented placeholder
// (bucket/prefix strings only): no actual S3 object is written -- see
// PARITY.md's "Recommendation families" gap. RecommendationIDs/
// RecommendationTypes are always empty since this backend never generates
// real SOP/alarm/test/component recommendations to package.
type RecommendationTemplate struct {
	StartTime           time.Time
	EndTime             time.Time
	TemplatesLocation   *S3Location
	Tags                *tags.Tags
	ARN                 string
	AssessmentArn       string
	Name                string
	Format              string
	AppArn              string
	Message             string
	Status              string
	RecommendationTypes []string
	RecommendationIDs   []string
	NeedsReplacements   bool
}

// clone returns a deep copy of t, or nil if t is nil.
func (t *RecommendationTemplate) clone() *RecommendationTemplate {
	if t == nil {
		return nil
	}

	cp := *t
	cp.RecommendationTypes = cloneStrs(t.RecommendationTypes)
	cp.RecommendationIDs = cloneStrs(t.RecommendationIDs)

	if t.TemplatesLocation != nil {
		loc := *t.TemplatesLocation
		cp.TemplatesLocation = &loc
	}

	return &cp
}

// MetricsExport is the internal state of one StartMetricsExport call.
type MetricsExport struct {
	CreationTime   time.Time
	ExportLocation *S3Location
	ID             string
	Status         string
	ErrorMessage   string
	BucketName     string
}

// clone returns a deep copy of m, or nil if m is nil.
func (m *MetricsExport) clone() *MetricsExport {
	if m == nil {
		return nil
	}

	cp := *m
	if m.ExportLocation != nil {
		loc := *m.ExportLocation
		cp.ExportLocation = &loc
	}

	return &cp
}

// GroupingTask is the internal state of one
// StartResourceGroupingRecommendationTask call. This backend always
// completes a grouping task with zero generated recommendations -- see
// PARITY.md's "Resource-grouping recommendations" gap: the real feature is an
// ML clustering heuristic with no public derivation rule, so returning none
// (rather than inventing plausible clusters) is the honest behavior.
type GroupingTask struct {
	GroupingID   string
	AppArn       string
	Status       string
	ErrorMessage string
}

// clone returns a deep copy of t, or nil if t is nil.
func (t *GroupingTask) clone() *GroupingTask {
	if t == nil {
		return nil
	}

	cp := *t

	return &cp
}

// cloneStrs returns a deep copy of a string slice (nil-safe).
func cloneStrs(ss []string) []string {
	if ss == nil {
		return nil
	}

	cp := make([]string, len(ss))
	copy(cp, ss)

	return cp
}

// cloneStrSliceMap returns a deep copy of a map[string][]string (nil-safe).
func cloneStrSliceMap(m map[string][]string) map[string][]string {
	if m == nil {
		return nil
	}

	cp := make(map[string][]string, len(m))
	for k, v := range m {
		cp[k] = cloneStrs(v)
	}

	return cp
}

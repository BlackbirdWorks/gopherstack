package resiliencehub

import (
	"maps"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/awstime"
	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

// epochPtrT returns a pointer to t's epoch-seconds value, or nil if t is
// zero -- matches the SDK's own omission of unset TimeStamp members.
func epochPtrT(t time.Time) *float64 {
	if t.IsZero() {
		return nil
	}

	e := awstime.Epoch(t)

	return &e
}

// tagsClone returns t's tag map, or nil if t is nil (nil-safe: *tags.Tags is
// a concrete pointer, so this must check for nil explicitly rather than via
// a generic interface -- an interface holding a nil *tags.Tags is not itself
// a nil interface).
func tagsClone(t *tags.Tags) map[string]string {
	if t == nil {
		return nil
	}

	return t.Clone()
}

// toEventSubscriptionsWire converts a slice of internal EventSubscription to
// its wire shape.
func toEventSubscriptionsWire(es []EventSubscription) []eventSubscriptionWire {
	out := make([]eventSubscriptionWire, 0, len(es))
	for _, e := range es {
		out = append(out, eventSubscriptionWire(e))
	}

	return out
}

func fromEventSubscriptionsWire(es []eventSubscriptionWire) []EventSubscription {
	out := make([]EventSubscription, 0, len(es))
	for _, e := range es {
		out = append(out, EventSubscription(e))
	}

	return out
}

func toPermissionModelWire(m *PermissionModel) *permissionModelWire {
	if m == nil {
		return nil
	}

	return &permissionModelWire{
		Type:                 m.Type,
		CrossAccountRoleArns: cloneStrs(m.CrossAccountRoleArns),
		InvokerRoleName:      m.InvokerRoleName,
	}
}

func fromPermissionModelWire(w *permissionModelWire) *PermissionModel {
	if w == nil {
		return nil
	}

	return &PermissionModel{
		Type:                 w.Type,
		CrossAccountRoleArns: cloneStrs(w.CrossAccountRoleArns),
		InvokerRoleName:      w.InvokerRoleName,
	}
}

// toAppWire converts an internal App to its full wire shape. ResiliencyScore
// is always scorePlaceholder (see assessments.go's doc comment): a
// documented stand-in, never a fabricated real analysis result.
func toAppWire(a *App) appWire {
	return appWire{
		AppArn:                            a.ARN,
		Name:                              a.Name,
		CreationTime:                      epochPtrT(a.CreationTime),
		AssessmentSchedule:                a.AssessmentSchedule,
		AwsApplicationArn:                 a.AwsApplicationArn,
		ComplianceStatus:                  a.ComplianceStatus,
		Description:                       a.Description,
		DriftStatus:                       a.DriftStatus,
		EventSubscriptions:                toEventSubscriptionsWire(a.EventSubscriptions),
		LastAppComplianceEvaluationTime:   epochPtrT(a.LastAppComplianceEvaluationTime),
		LastDriftEvaluationTime:           epochPtrT(a.LastDriftEvaluationTime),
		LastResiliencyScoreEvaluationTime: epochPtrT(a.LastResiliencyScoreEvaluationTime),
		PermissionModel:                   toPermissionModelWire(a.PermissionModel),
		PolicyArn:                         a.PolicyArn,
		ResiliencyScore:                   scorePlaceholder,
		Status:                            a.Status,
		Tags:                              tagsClone(a.Tags),
	}
}

func toAppSummaryWire(a *App) appSummaryWire {
	return appSummaryWire{
		AppArn:                          a.ARN,
		Name:                            a.Name,
		CreationTime:                    epochPtrT(a.CreationTime),
		AssessmentSchedule:              a.AssessmentSchedule,
		AwsApplicationArn:               a.AwsApplicationArn,
		ComplianceStatus:                a.ComplianceStatus,
		Description:                     a.Description,
		DriftStatus:                     a.DriftStatus,
		LastAppComplianceEvaluationTime: epochPtrT(a.LastAppComplianceEvaluationTime),
		ResiliencyScore:                 scorePlaceholder,
		Status:                          a.Status,
	}
}

func toLogicalResourceIDWire(l *LogicalResourceID) *logicalResourceIDWire {
	if l == nil {
		return nil
	}

	return &logicalResourceIDWire{
		Identifier:          l.Identifier,
		EksSourceName:       l.EksSourceName,
		LogicalStackName:    l.LogicalStackName,
		ResourceGroupName:   l.ResourceGroupName,
		TerraformSourceName: l.TerraformSourceName,
	}
}

func fromLogicalResourceIDWire(w *logicalResourceIDWire) *LogicalResourceID {
	if w == nil {
		return nil
	}

	return &LogicalResourceID{
		Identifier:          w.Identifier,
		EksSourceName:       w.EksSourceName,
		LogicalStackName:    w.LogicalStackName,
		ResourceGroupName:   w.ResourceGroupName,
		TerraformSourceName: w.TerraformSourceName,
	}
}

func toPhysicalResourceIDWire(p *PhysicalResourceID) *physicalResourceIDWire {
	if p == nil {
		return nil
	}

	return &physicalResourceIDWire{
		Identifier:   p.Identifier,
		Type:         p.Type,
		AwsAccountID: p.AwsAccountID,
		AwsRegion:    p.AwsRegion,
	}
}

func fromPhysicalResourceIDWire(w *physicalResourceIDWire) *PhysicalResourceID {
	if w == nil {
		return nil
	}

	return &PhysicalResourceID{
		Identifier:   w.Identifier,
		Type:         w.Type,
		AwsAccountID: w.AwsAccountID,
		AwsRegion:    w.AwsRegion,
	}
}

func toAppComponentWire(c *AppComponent) appComponentWire {
	return appComponentWire{ID: c.ID, Name: c.Name, Type: c.Type, AdditionalInfo: cloneStrSliceMap(c.AdditionalInfo)}
}

func toAppComponentsWire(cs []*AppComponent) []appComponentWire {
	out := make([]appComponentWire, 0, len(cs))
	for _, c := range cs {
		out = append(out, toAppComponentWire(c))
	}

	return out
}

// toPhysicalResourceWire converts an internal PhysicalResource to its wire
// shape. AppComponents on the wire is a list of full appComponentWire
// objects in the real SDK type, but this backend only tracks component
// ASSIGNMENT by name on PhysicalResource.AppComponents (see models.go) --
// each entry here carries only Name, which is the field callers actually key
// off of (matches the shape without inventing Id/Type data this backend does
// not independently track per-resource).
func toPhysicalResourceWire(r *PhysicalResource) physicalResourceWire {
	components := make([]appComponentWire, 0, len(r.AppComponents))
	for _, name := range r.AppComponents {
		components = append(components, appComponentWire{Name: name})
	}

	excluded := r.Excluded

	return physicalResourceWire{
		LogicalResourceID:  toLogicalResourceIDWire(r.LogicalResourceID),
		PhysicalResourceID: toPhysicalResourceIDWire(r.PhysicalResourceID),
		ResourceType:       r.ResourceType,
		AdditionalInfo:     cloneStrSliceMap(r.AdditionalInfo),
		AppComponents:      components,
		Excluded:           &excluded,
		ParentResourceName: r.ParentResourceName,
		ResourceName:       r.ResourceName,
		SourceType:         r.SourceType,
	}
}

func toResourceMappingWire(m ResourceMapping) resourceMappingWire {
	return resourceMappingWire{
		MappingType:         m.MappingType,
		PhysicalResourceID:  toPhysicalResourceIDWire(m.PhysicalResourceID),
		AppRegistryAppName:  m.AppRegistryAppName,
		EksSourceName:       m.EksSourceName,
		LogicalStackName:    m.LogicalStackName,
		ResourceGroupName:   m.ResourceGroupName,
		ResourceName:        m.ResourceName,
		TerraformSourceName: m.TerraformSourceName,
	}
}

func fromResourceMappingWire(w resourceMappingWire) ResourceMapping {
	return ResourceMapping{
		MappingType:         w.MappingType,
		PhysicalResourceID:  fromPhysicalResourceIDWire(w.PhysicalResourceID),
		AppRegistryAppName:  w.AppRegistryAppName,
		EksSourceName:       w.EksSourceName,
		LogicalStackName:    w.LogicalStackName,
		ResourceGroupName:   w.ResourceGroupName,
		ResourceName:        w.ResourceName,
		TerraformSourceName: w.TerraformSourceName,
	}
}

func toResourceMappingsWire(ms []ResourceMapping) []resourceMappingWire {
	out := make([]resourceMappingWire, 0, len(ms))
	for _, m := range ms {
		out = append(out, toResourceMappingWire(m))
	}

	return out
}

func toResiliencyPolicyWire(p *ResiliencyPolicy) *resiliencyPolicyWire {
	if p == nil {
		return nil
	}

	policy := make(map[string]failurePolicyWire, len(p.Policy))
	for k, v := range p.Policy {
		policy[k] = failurePolicyWire(v)
	}

	return &resiliencyPolicyWire{
		PolicyArn:              p.ARN,
		PolicyName:             p.Name,
		Tier:                   p.Tier,
		Policy:                 policy,
		CreationTime:           epochPtrT(p.CreationTime),
		DataLocationConstraint: p.DataLocationConstraint,
		EstimatedCostTier:      p.EstimatedCostTier,
		PolicyDescription:      p.Description,
		Tags:                   tagsClone(p.Tags),
	}
}

func fromFailurePolicyMapWire(w map[string]failurePolicyWire) map[string]FailurePolicy {
	out := make(map[string]FailurePolicy, len(w))
	for k, v := range w {
		out[k] = FailurePolicy(v)
	}

	return out
}

func toDisruptionComplianceWire(d DisruptionCompliance) disruptionComplianceWire {
	return disruptionComplianceWire(d)
}

func toComplianceMapWire(m map[string]DisruptionCompliance) map[string]disruptionComplianceWire {
	out := make(map[string]disruptionComplianceWire, len(m))
	for k, v := range m {
		out[k] = toDisruptionComplianceWire(v)
	}

	return out
}

func toResiliencyScoreWire(s *ResiliencyScore) *resiliencyScoreWire {
	if s == nil {
		return nil
	}

	comp := make(map[string]scoringComponentResiliencyScoreWire, len(s.ComponentScore))
	for k, v := range s.ComponentScore {
		comp[k] = scoringComponentResiliencyScoreWire(v)
	}

	return &resiliencyScoreWire{Score: s.Score, DisruptionScore: cloneFloatMap(s.DisruptionScore), ComponentScore: comp}
}

func cloneFloatMap(m map[string]float64) map[string]float64 {
	if m == nil {
		return nil
	}

	cp := make(map[string]float64, len(m))
	maps.Copy(cp, m)

	return cp
}

func toResourceErrorsDetailsWire(d *ResourceErrorsDetails) *resourceErrorsDetailsWire {
	if d == nil {
		return nil
	}

	errs := make([]resourceErrorWire, 0, len(d.ResourceErrors))
	for _, e := range d.ResourceErrors {
		errs = append(errs, resourceErrorWire(e))
	}

	hasMore := d.HasMoreErrors

	return &resourceErrorsDetailsWire{HasMoreErrors: &hasMore, ResourceErrors: errs}
}

// toAppAssessmentWire converts an internal AppAssessment to its full wire
// shape. Summary is always nil (AssessmentSummary is a Bedrock-LLM-backed
// field this emulator never fabricates -- see PARITY.md).
func toAppAssessmentWire(a *AppAssessment) *appAssessmentWire {
	return &appAssessmentWire{
		AssessmentArn:         a.ARN,
		AssessmentStatus:      a.Status,
		Invoker:               a.Invoker,
		AppArn:                a.AppArn,
		AppVersion:            a.AppVersion,
		AssessmentName:        a.AssessmentName,
		Compliance:            toComplianceMapWire(a.Compliance),
		ComplianceStatus:      a.ComplianceStatus,
		DriftStatus:           a.DriftStatus,
		StartTime:             epochPtrT(a.StartTime),
		EndTime:               epochPtrT(a.EndTime),
		Message:               a.Message,
		Policy:                toResiliencyPolicyWire(a.Policy),
		ResiliencyScore:       toResiliencyScoreWire(a.ResiliencyScore),
		ResourceErrorsDetails: toResourceErrorsDetailsWire(a.ResourceErrorsDetails),
		Summary:               nil,
		Tags:                  tagsClone(a.Tags),
		VersionName:           a.VersionName,
	}
}

func toAppAssessmentSummaryWire(a *AppAssessment) appAssessmentSummaryWire {
	score := 0.0
	if a.ResiliencyScore != nil {
		score = a.ResiliencyScore.Score
	}

	return appAssessmentSummaryWire{
		AssessmentArn:    a.ARN,
		AssessmentStatus: a.Status,
		AppArn:           a.AppArn,
		AppVersion:       a.AppVersion,
		AssessmentName:   a.AssessmentName,
		ComplianceStatus: a.ComplianceStatus,
		DriftStatus:      a.DriftStatus,
		EndTime:          epochPtrT(a.EndTime),
		Invoker:          a.Invoker,
		Message:          a.Message,
		ResiliencyScore:  score,
		StartTime:        epochPtrT(a.StartTime),
		VersionName:      a.VersionName,
	}
}

func toRecommendationTemplateWire(t *RecommendationTemplate) *recommendationTemplateWire {
	if t == nil {
		return nil
	}

	var loc *s3LocationWire
	if t.TemplatesLocation != nil {
		loc = &s3LocationWire{Bucket: t.TemplatesLocation.Bucket, Prefix: t.TemplatesLocation.Prefix}
	}

	needsReplacements := t.NeedsReplacements

	return &recommendationTemplateWire{
		AssessmentArn:             t.AssessmentArn,
		Format:                    t.Format,
		Name:                      t.Name,
		RecommendationTemplateArn: t.ARN,
		RecommendationTypes:       cloneStrs(t.RecommendationTypes),
		Status:                    t.Status,
		AppArn:                    t.AppArn,
		StartTime:                 epochPtrT(t.StartTime),
		EndTime:                   epochPtrT(t.EndTime),
		Message:                   t.Message,
		NeedsReplacements:         &needsReplacements,
		RecommendationIDs:         cloneStrs(t.RecommendationIDs),
		Tags:                      tagsClone(t.Tags),
		TemplatesLocation:         loc,
	}
}

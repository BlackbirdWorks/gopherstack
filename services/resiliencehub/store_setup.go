package resiliencehub

import (
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

func appKeyFn(v *App) string { return v.ID }

func policyKeyFn(v *ResiliencyPolicy) string {
	id, _ := resourceIDFromARN(v.ARN, ":resiliency-policy/")

	return id
}

func assessmentKeyFn(v *AppAssessment) string {
	id, _ := resourceIDFromARN(v.ARN, ":app-assessment/")

	return id
}

func assessmentAppIndexKeyFn(v *AppAssessment) string { return v.AppID }

func templateKeyFn(v *RecommendationTemplate) string {
	id, _ := resourceIDFromARN(v.ARN, ":recommendation-template/")

	return id
}

func templateAssessmentIndexKeyFn(v *RecommendationTemplate) string { return v.AssessmentArn }

func metricsExportKeyFn(v *MetricsExport) string { return v.ID }

func groupingTaskKeyFn(v *GroupingTask) string { return v.GroupingID }

func groupingTaskAppIndexKeyFn(v *GroupingTask) string { return v.AppArn }

// registerAllTables registers every resource collection exactly once. Must
// be called during construction only -- see services/outposts/store_setup.go's
// doc comment for why (store.Register panics on a duplicate name).
func registerAllTables(b *InMemoryBackend) {
	b.apps = store.Register(b.registry, "apps", store.New(appKeyFn))
	b.policies = store.Register(b.registry, "policies", store.New(policyKeyFn))

	b.assessments = store.Register(b.registry, "assessments", store.New(assessmentKeyFn))
	b.assessmentsByApp = b.assessments.AddIndex("byApp", assessmentAppIndexKeyFn)

	b.templates = store.Register(b.registry, "templates", store.New(templateKeyFn))
	b.templatesByAssess = b.templates.AddIndex("byAssessment", templateAssessmentIndexKeyFn)

	b.metricsExports = store.Register(b.registry, "metricsExports", store.New(metricsExportKeyFn))

	b.groupingTasks = store.Register(b.registry, "groupingTasks", store.New(groupingTaskKeyFn))
	b.groupingByApp = b.groupingTasks.AddIndex("byApp", groupingTaskAppIndexKeyFn)
}

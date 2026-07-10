package transcribe

// Code in this file supports Phase 3.3 of the datalayer refactor: every
// map[string]*T resource field on InMemoryBackend is replaced with a
// *store.Table[T], following the pattern established by services/sesv2 /
// services/codebuild / services/dax (data-driven, direct registration -- see
// pkgs/store's package doc for the underlying primitive).
//
// Every convertible value type here already carries its own identity as a
// real (non-json:"-") field -- TranscriptionJob.JobName,
// CallAnalyticsCategory.CategoryName, LanguageModel.ModelName,
// MedicalVocabulary.VocabularyName, Vocabulary.VocabularyName,
// VocabularyFilter.VocabularyFilterName, CallAnalyticsJob.CallAnalyticsJobName,
// MedicalScribeJob.MedicalScribeJobName,
// MedicalTranscriptionJob.MedicalTranscriptionJobName -- so all 9 tables
// below are "clean" tables registered directly on b.registry; no DTO
// indirection is needed. There is no ARN-keyed reverse-lookup map and no
// parent/child grouping among these resources, so no store.Index is needed
// either.
//
// resourceTags (map[string]map[string]string, keyed by ARN rather than *T) is
// left as a plain field -- its values are plain string maps, not *T, so it
// does not fit store.Table's keyed-collection shape. See persistence.go for
// how it round-trips alongside the registered tables.
import (
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

func transcriptionJobKeyFn(v *TranscriptionJob) string { return v.JobName }

func callAnalyticsCategoryKeyFn(v *CallAnalyticsCategory) string { return v.CategoryName }

func languageModelKeyFn(v *LanguageModel) string { return v.ModelName }

func medicalVocabularyKeyFn(v *MedicalVocabulary) string { return v.VocabularyName }

func vocabularyKeyFn(v *Vocabulary) string { return v.VocabularyName }

func vocabularyFilterKeyFn(v *VocabularyFilter) string { return v.VocabularyFilterName }

func callAnalyticsJobKeyFn(v *CallAnalyticsJob) string { return v.CallAnalyticsJobName }

func medicalScribeJobKeyFn(v *MedicalScribeJob) string { return v.MedicalScribeJobName }

func medicalTranscriptionJobKeyFn(v *MedicalTranscriptionJob) string {
	return v.MedicalTranscriptionJobName
}

// registerAllTables registers every backend resource table exactly once. It
// must be called during construction only, immediately after b.registry is
// created -- store.Register panics on a duplicate name, so runtime resets go
// through b.registry.ResetAll() instead (see InMemoryBackend.Reset in backend.go).
func registerAllTables(b *InMemoryBackend) {
	b.jobs = store.Register(b.registry, "jobs", store.New(transcriptionJobKeyFn))
	b.callAnalyticsCategories = store.Register(
		b.registry, "callAnalyticsCategories", store.New(callAnalyticsCategoryKeyFn),
	)
	b.languageModels = store.Register(b.registry, "languageModels", store.New(languageModelKeyFn))
	b.medicalVocabularies = store.Register(b.registry, "medicalVocabularies", store.New(medicalVocabularyKeyFn))
	b.vocabularies = store.Register(b.registry, "vocabularies", store.New(vocabularyKeyFn))
	b.vocabularyFilters = store.Register(b.registry, "vocabularyFilters", store.New(vocabularyFilterKeyFn))
	b.callAnalyticsJobs = store.Register(b.registry, "callAnalyticsJobs", store.New(callAnalyticsJobKeyFn))
	b.medicalScribeJobs = store.Register(b.registry, "medicalScribeJobs", store.New(medicalScribeJobKeyFn))
	b.medicalTranscriptionJobs = store.Register(
		b.registry, "medicalTranscriptionJobs", store.New(medicalTranscriptionJobKeyFn),
	)
}

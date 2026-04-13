package transcribe

// JobCount returns the number of transcription jobs stored in the backend.
// Exported for use in tests only.
func JobCount(b *InMemoryBackend) int {
	b.mu.RLock("JobCount")
	defer b.mu.RUnlock()

	return len(b.jobs)
}

// CallAnalyticsCategoryCount returns the number of Call Analytics categories.
// Exported for use in tests only.
func CallAnalyticsCategoryCount(b *InMemoryBackend) int {
	b.mu.RLock("CallAnalyticsCategoryCount")
	defer b.mu.RUnlock()

	return len(b.callAnalyticsCategories)
}

// LanguageModelCount returns the number of language models stored.
// Exported for use in tests only.
func LanguageModelCount(b *InMemoryBackend) int {
	b.mu.RLock("LanguageModelCount")
	defer b.mu.RUnlock()

	return len(b.languageModels)
}

// MedicalVocabularyCount returns the number of medical vocabularies stored.
// Exported for use in tests only.
func MedicalVocabularyCount(b *InMemoryBackend) int {
	b.mu.RLock("MedicalVocabularyCount")
	defer b.mu.RUnlock()

	return len(b.medicalVocabularies)
}

// VocabularyCount returns the number of custom vocabularies stored.
// Exported for use in tests only.
func VocabularyCount(b *InMemoryBackend) int {
	b.mu.RLock("VocabularyCount")
	defer b.mu.RUnlock()

	return len(b.vocabularies)
}

// VocabularyFilterCount returns the number of vocabulary filters stored.
// Exported for use in tests only.
func VocabularyFilterCount(b *InMemoryBackend) int {
	b.mu.RLock("VocabularyFilterCount")
	defer b.mu.RUnlock()

	return len(b.vocabularyFilters)
}

// CallAnalyticsJobCount returns the number of Call Analytics jobs stored.
// Exported for use in tests only.
func CallAnalyticsJobCount(b *InMemoryBackend) int {
	b.mu.RLock("CallAnalyticsJobCount")
	defer b.mu.RUnlock()

	return len(b.callAnalyticsJobs)
}

// MedicalScribeJobCount returns the number of Medical Scribe jobs stored.
// Exported for use in tests only.
func MedicalScribeJobCount(b *InMemoryBackend) int {
	b.mu.RLock("MedicalScribeJobCount")
	defer b.mu.RUnlock()

	return len(b.medicalScribeJobs)
}

// MedicalTranscriptionJobCount returns the number of Medical Transcription jobs stored.
// Exported for use in tests only.
func MedicalTranscriptionJobCount(b *InMemoryBackend) int {
	b.mu.RLock("MedicalTranscriptionJobCount")
	defer b.mu.RUnlock()

	return len(b.medicalTranscriptionJobs)
}

// HandlerOpsLen returns the number of operations in the handler dispatch map.
// Exported for use in tests only.
func HandlerOpsLen(h *Handler) int {
	return len(h.ops)
}

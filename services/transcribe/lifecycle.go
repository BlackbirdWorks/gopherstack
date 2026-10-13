package transcribe

import "time"

const vocabStatePending = "PENDING"

// SetLifecycleDelay sets how long new jobs report IN_PROGRESS, vocabularies PENDING and
// language models IN_PROGRESS before settling. Zero (the default) settles instantly.
func (b *InMemoryBackend) SetLifecycleDelay(d time.Duration) {
	b.mu.Lock("SetLifecycleDelay")
	defer b.mu.Unlock()

	b.lifecycleDelay = d
}

func (b *InMemoryBackend) settling(since time.Time) bool {
	return b.lifecycleDelay > 0 && time.Now().Before(since.Add(b.lifecycleDelay))
}

func (b *InMemoryBackend) observeTranscriptionJob(j *TranscriptionJob) {
	if j.JobStatus == jobStatusCompleted && b.settling(j.CreationTime) {
		j.JobStatus = jobStatusInProgress
		j.CompletionTime = time.Time{}
	}
}

func (b *InMemoryBackend) observeMedicalTranscriptionJob(j *MedicalTranscriptionJob) {
	if j.TranscriptionJobStatus == jobStatusCompleted && b.settling(j.CreationTime) {
		j.TranscriptionJobStatus = jobStatusInProgress
		j.CompletionTime = time.Time{}
	}
}

func (b *InMemoryBackend) observeCallAnalyticsJob(j *CallAnalyticsJob) {
	if j.CallAnalyticsJobStatus == jobStatusCompleted && b.settling(j.CreationTime) {
		j.CallAnalyticsJobStatus = jobStatusInProgress
		j.CompletionTime = time.Time{}
	}
}

func (b *InMemoryBackend) observeMedicalScribeJob(j *MedicalScribeJob) {
	if j.MedicalScribeJobStatus == jobStatusCompleted && b.settling(j.CreationTime) {
		j.MedicalScribeJobStatus = jobStatusInProgress
		j.CompletionTime = time.Time{}
	}
}

func (b *InMemoryBackend) observeVocabulary(v *Vocabulary) {
	if v.VocabularyState == vocabStateReady && b.settling(v.LastModifiedTime) {
		v.VocabularyState = vocabStatePending
		v.VocabularyFileURI = ""
	}
}

func (b *InMemoryBackend) observeMedicalVocabulary(v *MedicalVocabulary) {
	if v.VocabularyState == vocabStateReady && b.settling(v.LastModifiedTime) {
		v.VocabularyState = vocabStatePending
	}
}

func (b *InMemoryBackend) observeLanguageModel(m *LanguageModel) {
	if m.ModelStatus == modelStatusCompleted && b.settling(m.CreateTime) {
		m.ModelStatus = jobStatusInProgress
	}
}

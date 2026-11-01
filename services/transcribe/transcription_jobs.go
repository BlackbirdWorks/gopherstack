package transcribe

import (
	"fmt"
	"slices"
	"sort"
	"time"
)

// maxSpeakerLabelsMin is the minimum number of speakers for diarization.
const maxSpeakerLabelsMin = 2

// maxSpeakerLabelsMax is the maximum number of speakers for diarization.
const maxSpeakerLabelsMax = 10

// maxAlternativesMin is the minimum number of alternative transcripts.
const maxAlternativesMin = 2

// maxAlternativesMax is the maximum number of alternative transcripts.
const maxAlternativesMax = 10

// syntheticIdentifiedLanguageScore is the fixed score reported when language identification is enabled;
// the emulator cannot identify languages.
const syntheticIdentifiedLanguageScore = float32(0.97)

// supportedVocabularyFilterMethods returns the set of vocabulary filter methods.
func supportedVocabularyFilterMethods() []string {
	return []string{"remove", "mask", "tag"}
}

// supportedContentRedactionTypes returns the set of content redaction types.
func supportedContentRedactionTypes() []string { return []string{"PII"} }

// supportedRedactionOutputs returns the set of redaction output options.
func supportedRedactionOutputs() []string { return []string{roleRedacted, "redacted_and_unredacted"} }

// supportedSubtitleFormats returns the set of subtitle output formats.
func supportedSubtitleFormats() []string { return []string{"vtt", "srt"} }

// StartTranscriptionJob creates a transcription job; media is read and output written as the job settles.
func (b *InMemoryBackend) StartTranscriptionJob(input *TranscriptionJob) (*TranscriptionJob, error) {
	if err := validateTranscriptionJobInput(input); err != nil {
		return nil, err
	}

	b.mu.Lock("StartTranscriptionJob")
	defer b.mu.Unlock()

	if b.jobs.Has(input.JobName) {
		return nil, fmt.Errorf("%w: job %s already exists", ErrAlreadyExists, input.JobName)
	}

	job := *input
	job.CreationTime = b.now()
	job.JobStatus = jobStatusInProgress
	job.StartTime = job.CreationTime

	if input.JobExecutionSettings != nil && input.JobExecutionSettings.AllowDeferredExecution {
		job.JobStatus = jobStatusQueued
		job.StartTime = time.Time{}
	}

	b.jobs.Put(&job)
	b.recordResourceTagsLocked(b.resourceARN(resourceTypeTranscriptionJob, job.JobName), job.Tags)
	if job.JobStatus != jobStatusQueued {
		b.settleLocked(viewTranscription(&job))
	}

	return b.transcriptionCopyLocked(&job), nil
}

func (b *InMemoryBackend) transcriptionCopyLocked(job *TranscriptionJob) *TranscriptionJob {
	cp := *job
	cp.Tags = b.liveTagsLocked(b.resourceARN(resourceTypeTranscriptionJob, job.JobName))

	return &cp
}

func viewTranscription(j *TranscriptionJob) jobView {
	identify := j.IdentifyLanguage || j.IdentifyMultipleLanguages
	spec := jobSpec{
		Kind: kindTranscription, Name: j.JobName, Language: j.LanguageCode, MediaURI: j.Media.MediaFileURI,
		MediaFormat: j.MediaFormat, SampleRate: j.MediaSampleRateHertz,
		OutBucket: j.OutputBucketName, OutKey: j.OutputKey, Redaction: j.ContentRedaction != nil,
		Deferred: j.JobExecutionSettings != nil && j.JobExecutionSettings.AllowDeferredExecution,
	}

	if identify {
		spec.Language = resolveEffectiveLanguageCode(j.LanguageCode, j.LanguageOptions)
	}

	if j.Settings != nil {
		spec.ShowSpeakers = j.Settings.ShowSpeakerLabels
		spec.IdentifyChannels = j.Settings.ChannelIdentification
	}

	if j.Subtitles != nil {
		spec.SubtitleFormats, spec.SubtitleStart = j.Subtitles.Formats, j.Subtitles.OutputStartIndex
	}

	return jobView{
		spec: spec, created: j.CreationTime, status: &j.JobStatus, failure: &j.FailureReason,
		started: &j.StartTime, completed: &j.CompletionTime,
		complete: func(d delivered) {
			if identify {
				j.LanguageCode = spec.Language
				j.IdentifiedLanguageScore = syntheticIdentifiedLanguageScore
			}

			j.TranscriptFileURI, j.RedactedTranscriptURI = d.transcript, d.redacted
			j.ManagedToken, j.TranscriptJSON, j.ManagedOutputs = d.token, d.main, d.extra

			if j.Subtitles != nil && len(d.subtitles) > 0 {
				j.Subtitles.SubtitleFileURIs = d.subtitles
			}
		},
	}
}

// resolveEffectiveLanguageCode returns the best language code to use when language identification is on.
func resolveEffectiveLanguageCode(languageCode string, options []string) string {
	if languageCode != "" {
		return languageCode
	}

	if len(options) > 0 {
		return options[0]
	}

	return languageEnUS
}

// subtitlesInputFromOutput converts a SubtitlesOutput back to input for validation.
func subtitlesInputFromOutput(s *SubtitlesOutput) *SubtitlesInput {
	if s == nil {
		return nil
	}

	return &SubtitlesInput{Formats: s.Formats, OutputStartIndex: s.OutputStartIndex}
}

// GetTranscriptionJob returns a transcription job by name.
func (b *InMemoryBackend) GetTranscriptionJob(jobName string) (*TranscriptionJob, error) {
	b.mu.Lock("GetTranscriptionJob")
	defer b.mu.Unlock()

	job, ok := b.jobs.Get(jobName)
	if !ok {
		return nil, fmt.Errorf("%w: job %s not found", ErrNotFound, jobName)
	}

	b.settleLocked(viewTranscription(job))

	return b.transcriptionCopyLocked(job), nil
}

// ListTranscriptionJobs returns transcription jobs, optionally filtered by status and
// name substring, with pagination.
func (b *InMemoryBackend) ListTranscriptionJobs(
	statusFilter, nameContains, nextToken string, maxResults int32,
) ([]TranscriptionJob, string) {
	b.mu.Lock("ListTranscriptionJobs")
	defer b.mu.Unlock()

	all := make([]TranscriptionJob, 0, b.jobs.Len())
	for _, j := range b.jobs.All() {
		b.settleListLocked(viewTranscription(j))

		cp := *j

		if (statusFilter == "" || cp.JobStatus == statusFilter) && matchesNameContains(j.JobName, nameContains) {
			all = append(all, cp)
		}
	}

	sort.Slice(all, func(i, j int) bool { return all[i].JobName < all[j].JobName })

	return paginateList(all, nextToken, maxResults)
}

// DeleteTranscriptionJob removes a transcription job by name.
func (b *InMemoryBackend) DeleteTranscriptionJob(jobName string) error {
	if jobName == "" {
		return fmt.Errorf("%w: TranscriptionJobName is required", ErrValidation)
	}

	b.mu.Lock("DeleteTranscriptionJob")
	defer b.mu.Unlock()

	if !b.jobs.Delete(jobName) {
		return fmt.Errorf("%w: job %s not found", ErrNotFound, jobName)
	}

	b.forgetResourceTagsLocked(b.resourceARN(resourceTypeTranscriptionJob, jobName))

	return nil
}

func validateTranscriptionJobInput(input *TranscriptionJob) error {
	if err := validateJobName(input.JobName); err != nil {
		return err
	}

	if !input.IdentifyLanguage && !input.IdentifyMultipleLanguages && input.LanguageCode == "" {
		return fmt.Errorf(
			"%w: LanguageCode is required (or set IdentifyLanguage/IdentifyMultipleLanguages)",
			ErrValidation,
		)
	}

	if err := validateLanguageCode(input.LanguageCode); err != nil {
		return err
	}

	if err := validateMediaFormat(input.MediaFormat); err != nil {
		return err
	}

	if err := validateMediaSampleRateHertz(input.MediaSampleRateHertz); err != nil {
		return err
	}

	if err := validateSettings(input.Settings); err != nil {
		return err
	}

	if err := validateContentRedaction(input.ContentRedaction); err != nil {
		return err
	}

	if err := validateJobExecutionSettings(input.JobExecutionSettings); err != nil {
		return err
	}

	if err := validateSubtitles(subtitlesInputFromOutput(input.Subtitles)); err != nil {
		return err
	}

	if err := validateLanguageOptions(input.LanguageOptions); err != nil {
		return err
	}

	if err := validateLanguageIDSettings(input.LanguageIDSettings, input.IdentifyMultipleLanguages); err != nil {
		return err
	}

	return nil
}

// validateSpeakerLabels enforces the bidirectional requirement documented on
// Settings: ShowSpeakerLabels and MaxSpeakerLabels must be set together.
func validateSpeakerLabels(s *TranscriptionSettings) error {
	if s.MaxSpeakerLabels != 0 && !s.ShowSpeakerLabels {
		return fmt.Errorf("%w: ShowSpeakerLabels must be true when MaxSpeakerLabels is set", ErrValidation)
	}

	if s.ShowSpeakerLabels {
		if s.MaxSpeakerLabels == 0 {
			return fmt.Errorf("%w: MaxSpeakerLabels is required when ShowSpeakerLabels is true", ErrValidation)
		}

		if s.MaxSpeakerLabels < maxSpeakerLabelsMin || s.MaxSpeakerLabels > maxSpeakerLabelsMax {
			return fmt.Errorf("%w: MaxSpeakerLabels must be between %d and %d",
				ErrValidation, maxSpeakerLabelsMin, maxSpeakerLabelsMax)
		}
	}

	return nil
}

// validateAlternatives enforces the bidirectional requirement documented on
// Settings: ShowAlternatives and MaxAlternatives must be set together.
func validateAlternatives(s *TranscriptionSettings) error {
	if s.MaxAlternatives != 0 && !s.ShowAlternatives {
		return fmt.Errorf("%w: ShowAlternatives must be true when MaxAlternatives is set", ErrValidation)
	}

	if s.ShowAlternatives {
		if s.MaxAlternatives == 0 {
			return fmt.Errorf("%w: MaxAlternatives is required when ShowAlternatives is true", ErrValidation)
		}

		if s.MaxAlternatives < maxAlternativesMin || s.MaxAlternatives > maxAlternativesMax {
			return fmt.Errorf("%w: MaxAlternatives must be between %d and %d",
				ErrValidation, maxAlternativesMin, maxAlternativesMax)
		}
	}

	return nil
}

// validateSettings validates Settings fields when Settings is non-nil.
func validateSettings(s *TranscriptionSettings) error {
	if s == nil {
		return nil
	}

	if err := validateSpeakerLabels(s); err != nil {
		return err
	}

	if err := validateAlternatives(s); err != nil {
		return err
	}

	if s.VocabularyFilterMethod != "" &&
		!slices.Contains(supportedVocabularyFilterMethods(), s.VocabularyFilterMethod) {
		return fmt.Errorf("%w: VocabularyFilterMethod %q must be one of %v",
			ErrValidation, s.VocabularyFilterMethod, supportedVocabularyFilterMethods())
	}

	return nil
}

// validateContentRedaction validates ContentRedaction fields when non-nil.
func validateContentRedaction(cr *ContentRedaction) error {
	if cr == nil {
		return nil
	}

	if cr.RedactionType == "" {
		return fmt.Errorf("%w: ContentRedaction.RedactionType is required", ErrValidation)
	}

	if !slices.Contains(supportedContentRedactionTypes(), cr.RedactionType) {
		return fmt.Errorf("%w: ContentRedaction.RedactionType %q must be one of %v",
			ErrValidation, cr.RedactionType, supportedContentRedactionTypes())
	}

	if cr.RedactionOutput == "" {
		return fmt.Errorf("%w: ContentRedaction.RedactionOutput is required", ErrValidation)
	}

	if !slices.Contains(supportedRedactionOutputs(), cr.RedactionOutput) {
		return fmt.Errorf("%w: ContentRedaction.RedactionOutput %q must be one of %v",
			ErrValidation, cr.RedactionOutput, supportedRedactionOutputs())
	}

	return nil
}

// validateJobExecutionSettings enforces the role requirement for deferred jobs.
func validateJobExecutionSettings(settings *JobExecutionSettings) error {
	if settings == nil || !settings.AllowDeferredExecution {
		return nil
	}

	if settings.DataAccessRoleArn == "" {
		return fmt.Errorf(
			"%w: JobExecutionSettings.DataAccessRoleArn is required when AllowDeferredExecution is true",
			ErrValidation,
		)
	}

	return nil
}

// validateSubtitles validates Subtitles input fields when non-nil.
func validateSubtitles(s *SubtitlesInput) error {
	if s == nil {
		return nil
	}

	for _, f := range s.Formats {
		if !slices.Contains(supportedSubtitleFormats(), f) {
			return fmt.Errorf("%w: Subtitles.Formats contains unsupported format %q; must be vtt or srt",
				ErrValidation, f)
		}
	}

	return nil
}

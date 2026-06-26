package transcribe

// TranscriptionSettings represents the Settings field of a transcription job.
type TranscriptionSettings struct {
	VocabularyName         string `json:"VocabularyName,omitempty"`
	VocabularyFilterName   string `json:"VocabularyFilterName,omitempty"`
	VocabularyFilterMethod string `json:"VocabularyFilterMethod,omitempty"`
	MaxSpeakerLabels       int32  `json:"MaxSpeakerLabels,omitempty"`
	MaxAlternatives        int32  `json:"MaxAlternatives,omitempty"`
	ShowSpeakerLabels      bool   `json:"ShowSpeakerLabels,omitempty"`
	ChannelIdentification  bool   `json:"ChannelIdentification,omitempty"`
	ShowAlternatives       bool   `json:"ShowAlternatives,omitempty"`
}

// ContentRedaction represents content redaction settings for a transcription job.
type ContentRedaction struct {
	RedactionType   string   `json:"RedactionType"`
	RedactionOutput string   `json:"RedactionOutput,omitempty"`
	PiiEntityTypes  []string `json:"PiiEntityTypes,omitempty"`
}

// SubtitlesInput represents the subtitle generation settings.
type SubtitlesInput struct {
	Formats          []string `json:"Formats"`
	OutputStartIndex int32    `json:"OutputStartIndex,omitempty"`
}

// SubtitlesOutput represents the subtitle output returned by Get operations.
type SubtitlesOutput struct {
	Formats          []string `json:"Formats,omitempty"`
	SubtitleFileURIs []string `json:"SubtitleFileUris,omitempty"`
	OutputStartIndex int32    `json:"OutputStartIndex,omitempty"`
}

// ModelSettings holds the language model settings for a transcription job.
type ModelSettings struct {
	LanguageModelName string `json:"LanguageModelName,omitempty"`
}

// JobExecutionSettings controls deferred execution behavior.
type JobExecutionSettings struct {
	DataAccessRoleArn      string `json:"DataAccessRoleArn,omitempty"`
	AllowDeferredExecution bool   `json:"AllowDeferredExecution,omitempty"`
}

// ToxicityDetectionSettings represents a toxicity detection entry.
type ToxicityDetectionSettings struct {
	ToxicityCategories []string `json:"ToxicityCategories"`
}

// Media holds the media location for a job.
type Media struct {
	MediaFileURI         string `json:"MediaFileUri,omitempty"`
	RedactedMediaFileURI string `json:"RedactedMediaFileUri,omitempty"`
}

// LanguageIDSettings holds per-language-code identification settings.
type LanguageIDSettings struct {
	VocabularyName       string `json:"VocabularyName,omitempty"`
	VocabularyFilterName string `json:"VocabularyFilterName,omitempty"`
	LanguageModelName    string `json:"LanguageModelName,omitempty"`
}

// LanguageCodeItem represents an identified language with a score.
type LanguageCodeItem struct {
	LanguageCode      string  `json:"LanguageCode"`
	DurationInSeconds float32 `json:"DurationInSeconds"`
}

// InputDataConfig holds the S3 input configuration for custom language models.
type InputDataConfig struct {
	S3Uri             string `json:"S3Uri"`
	TuningDataS3Uri   string `json:"TuningDataS3Uri,omitempty"`
	DataAccessRoleArn string `json:"DataAccessRoleArn"`
}

// ChannelDefinition defines a channel in a call analytics job.
type ChannelDefinition struct {
	ParticipantRole string `json:"ParticipantRole"`
	ChannelID       int32  `json:"ChannelId"`
}

// CallAnalyticsSettings holds settings for a call analytics job.
type CallAnalyticsSettings struct {
	ContentRedaction       *ContentRedaction      `json:"ContentRedaction,omitempty"`
	Summarization          *SummarizationSettings `json:"Summarization,omitempty"`
	VocabularyName         string                 `json:"VocabularyName,omitempty"`
	VocabularyFilterName   string                 `json:"VocabularyFilterName,omitempty"`
	VocabularyFilterMethod string                 `json:"VocabularyFilterMethod,omitempty"`
	LanguageModelName      string                 `json:"LanguageModelName,omitempty"`
	LanguageOptions        []string               `json:"LanguageOptions,omitempty"`
}

// SummarizationSettings controls generative call summarization.
type SummarizationSettings struct {
	GenerateSummary bool `json:"GenerateSummary"`
}

// CallAnalyticsRule is a rule in a call analytics category.
type CallAnalyticsRule struct {
	NonTalkTimeFilter  *NonTalkTimeFilter  `json:"NonTalkTimeFilter,omitempty"`
	InterruptionFilter *InterruptionFilter `json:"InterruptionFilter,omitempty"`
	TranscriptFilter   *TranscriptFilter   `json:"TranscriptFilter,omitempty"`
	SentimentFilter    *SentimentFilter    `json:"SentimentFilter,omitempty"`
}

// NonTalkTimeFilter matches segments with no speech.
type NonTalkTimeFilter struct {
	ParticipantRole string `json:"ParticipantRole,omitempty"`
	Threshold       int64  `json:"Threshold,omitempty"`
	Negate          bool   `json:"Negate,omitempty"`
}

// InterruptionFilter matches interruptions by a participant.
type InterruptionFilter struct {
	ParticipantRole string `json:"ParticipantRole,omitempty"`
	Threshold       int64  `json:"Threshold,omitempty"`
	Negate          bool   `json:"Negate,omitempty"`
}

// TranscriptFilter matches specific phrases in the transcript.
type TranscriptFilter struct {
	TranscriptFilterType string   `json:"TranscriptFilterType"`
	ParticipantRole      string   `json:"ParticipantRole,omitempty"`
	Targets              []string `json:"Targets"`
	Negate               bool     `json:"Negate,omitempty"`
}

// SentimentFilter matches sentiment in the transcript.
type SentimentFilter struct {
	ParticipantRole string   `json:"ParticipantRole,omitempty"`
	Sentiments      []string `json:"Sentiments"`
	Negate          bool     `json:"Negate,omitempty"`
}

// MedicalTranscriptionSettings holds settings specific to medical transcription.
type MedicalTranscriptionSettings struct {
	VocabularyName        string `json:"VocabularyName,omitempty"`
	MaxSpeakerLabels      int32  `json:"MaxSpeakerLabels,omitempty"`
	MaxAlternatives       int32  `json:"MaxAlternatives,omitempty"`
	ShowSpeakerLabels     bool   `json:"ShowSpeakerLabels,omitempty"`
	ChannelIdentification bool   `json:"ChannelIdentification,omitempty"`
	ShowAlternatives      bool   `json:"ShowAlternatives,omitempty"`
}

// MedicalScribeSettings holds settings for a Medical Scribe job.
type MedicalScribeSettings struct {
	VocabularyName         string `json:"VocabularyName,omitempty"`
	VocabularyFilterName   string `json:"VocabularyFilterName,omitempty"`
	VocabularyFilterMethod string `json:"VocabularyFilterMethod,omitempty"`
	MaxSpeakerLabels       int32  `json:"MaxSpeakerLabels,omitempty"`
	ShowSpeakerLabels      bool   `json:"ShowSpeakerLabels,omitempty"`
	ChannelIdentification  bool   `json:"ChannelIdentification,omitempty"`
}

// MedicalScribeChannelDefinition defines a channel in a medical scribe job.
type MedicalScribeChannelDefinition struct {
	ParticipantRole string `json:"ParticipantRole"`
	ChannelID       int32  `json:"ChannelId"`
}

// ClinicalNoteGenerationSettings controls clinical note generation in medical scribe.
type ClinicalNoteGenerationSettings struct {
	NoteTemplate string `json:"NoteTemplate,omitempty"`
}

package transcribe

import "time"

// TranscriptionJob represents an Amazon Transcribe transcription job.
type TranscriptionJob struct {
	StartTime                 time.Time                     `json:"startTime"`
	CompletionTime            time.Time                     `json:"completionTime"`
	CreationTime              time.Time                     `json:"creationTime"`
	Tags                      map[string]string             `json:"tags,omitempty"`
	Subtitles                 *SubtitlesOutput              `json:"subtitles,omitempty"`
	ContentRedaction          *ContentRedaction             `json:"contentRedaction,omitempty"`
	ModelSettings             *ModelSettings                `json:"modelSettings,omitempty"`
	JobExecutionSettings      *JobExecutionSettings         `json:"jobExecutionSettings,omitempty"`
	Settings                  *TranscriptionSettings        `json:"settings,omitempty"`
	Media                     Media                         `json:"media"`
	LanguageCode              string                        `json:"languageCode"`
	JobStatus                 string                        `json:"jobStatus"`
	MediaFormat               string                        `json:"mediaFormat,omitempty"`
	OutputBucketName          string                        `json:"outputBucketName,omitempty"`
	OutputKey                 string                        `json:"outputKey,omitempty"`
	OutputEncryptionKMSKeyID  string                        `json:"outputEncryptionKMSKeyId,omitempty"`
	TranscriptText            string                        `json:"transcriptText"`
	JobName                   string                        `json:"jobName"`
	FailureReason             string                        `json:"failureReason,omitempty"`
	LanguageOptions           []string                      `json:"languageOptions,omitempty"`
	ToxicityDetection         []ToxicityDetectionSettings   `json:"toxicityDetection,omitempty"`
	LanguageCodes             []LanguageCodeItem            `json:"languageCodes,omitempty"`
	LanguageIDSettings        map[string]LanguageIDSettings `json:"languageIdSettings,omitempty"`
	TranscriptJSON            []byte                        `json:"-"`
	MediaSampleRateHertz      int32                         `json:"mediaSampleRateHertz,omitempty"`
	IdentifiedLanguageScore   float32                       `json:"identifiedLanguageScore,omitempty"`
	IdentifyLanguage          bool                          `json:"identifyLanguage,omitempty"`
	IdentifyMultipleLanguages bool                          `json:"identifyMultipleLanguages,omitempty"`
}

// CallAnalyticsCategory represents an Amazon Transcribe Call Analytics category.
type CallAnalyticsCategory struct {
	CreateTime     time.Time           `json:"createTime"`
	LastUpdateTime time.Time           `json:"lastUpdateTime"`
	Tags           map[string]string   `json:"tags,omitempty"`
	CategoryName   string              `json:"categoryName"`
	InputType      string              `json:"inputType"`
	Rules          []CallAnalyticsRule `json:"rules,omitempty"`
}

// LanguageModel represents a custom Amazon Transcribe language model.
type LanguageModel struct {
	CreateTime          time.Time         `json:"createTime"`
	LastModifiedTime    time.Time         `json:"lastModifiedTime"`
	InputDataConfig     *InputDataConfig  `json:"inputDataConfig,omitempty"`
	Tags                map[string]string `json:"tags,omitempty"`
	ModelName           string            `json:"modelName"`
	BaseModelName       string            `json:"baseModelName"`
	LanguageCode        string            `json:"languageCode"`
	ModelStatus         string            `json:"modelStatus"`
	FailureReason       string            `json:"failureReason,omitempty"`
	UpgradeAvailability bool              `json:"upgradeAvailability,omitempty"`
}

// MedicalVocabulary represents an Amazon Transcribe Medical custom vocabulary.
type MedicalVocabulary struct {
	LastModifiedTime  time.Time         `json:"lastModifiedTime"`
	Tags              map[string]string `json:"tags,omitempty"`
	VocabularyName    string            `json:"vocabularyName"`
	LanguageCode      string            `json:"languageCode"`
	VocabularyState   string            `json:"vocabularyState"`
	VocabularyFileURI string            `json:"vocabularyFileUri"`
	FailureReason     string            `json:"failureReason,omitempty"`
}

// Vocabulary represents an Amazon Transcribe custom vocabulary.
type Vocabulary struct {
	LastModifiedTime  time.Time         `json:"lastModifiedTime"`
	Tags              map[string]string `json:"tags,omitempty"`
	VocabularyName    string            `json:"vocabularyName"`
	LanguageCode      string            `json:"languageCode"`
	VocabularyState   string            `json:"vocabularyState"`
	VocabularyFileURI string            `json:"vocabularyFileUri,omitempty"`
	FailureReason     string            `json:"failureReason,omitempty"`
	Phrases           []string          `json:"phrases,omitempty"`
}

// VocabularyFilter represents an Amazon Transcribe custom vocabulary filter.
type VocabularyFilter struct {
	LastModifiedTime        time.Time         `json:"lastModifiedTime"`
	Tags                    map[string]string `json:"tags,omitempty"`
	VocabularyFilterName    string            `json:"vocabularyFilterName"`
	LanguageCode            string            `json:"languageCode"`
	VocabularyFilterFileURI string            `json:"vocabularyFilterFileUri,omitempty"`
	Words                   []string          `json:"words,omitempty"`
}

// CallAnalyticsJob represents an Amazon Transcribe Call Analytics job.
type CallAnalyticsJob struct {
	StartTime               time.Time              `json:"startTime"`
	CompletionTime          time.Time              `json:"completionTime"`
	CreationTime            time.Time              `json:"creationTime"`
	Tags                    map[string]string      `json:"tags,omitempty"`
	Settings                *CallAnalyticsSettings `json:"settings,omitempty"`
	Media                   Media                  `json:"media"`
	CallAnalyticsJobStatus  string                 `json:"callAnalyticsJobStatus"`
	LanguageCode            string                 `json:"languageCode"`
	MediaFormat             string                 `json:"mediaFormat,omitempty"`
	DataAccessRoleArn       string                 `json:"dataAccessRoleArn,omitempty"`
	FailureReason           string                 `json:"failureReason,omitempty"`
	CallAnalyticsJobName    string                 `json:"callAnalyticsJobName"`
	ChannelDefinitions      []ChannelDefinition    `json:"channelDefinitions,omitempty"`
	TranscriptJSON          []byte                 `json:"-"`
	IdentifiedLanguageScore float32                `json:"identifiedLanguageScore,omitempty"`
	MediaSampleRateHertz    int32                  `json:"mediaSampleRateHertz,omitempty"`
}

// MedicalScribeJob represents an Amazon Transcribe Medical Scribe job.
type MedicalScribeJob struct {
	StartTime              time.Time                        `json:"startTime"`
	CompletionTime         time.Time                        `json:"completionTime"`
	CreationTime           time.Time                        `json:"creationTime"`
	Tags                   map[string]string                `json:"tags,omitempty"`
	Settings               *MedicalScribeSettings           `json:"settings,omitempty"`
	Media                  Media                            `json:"media"`
	LanguageCode           string                           `json:"languageCode,omitempty"`
	DataAccessRoleArn      string                           `json:"dataAccessRoleArn,omitempty"`
	OutputBucketName       string                           `json:"outputBucketName,omitempty"`
	FailureReason          string                           `json:"failureReason,omitempty"`
	MedicalScribeJobStatus string                           `json:"medicalScribeJobStatus"`
	MedicalScribeJobName   string                           `json:"medicalScribeJobName"`
	ChannelDefinitions     []MedicalScribeChannelDefinition `json:"channelDefinitions,omitempty"`
}

// MedicalTranscriptionJob represents an Amazon Transcribe Medical transcription job.
type MedicalTranscriptionJob struct {
	CreationTime                     time.Time                     `json:"creationTime"`
	StartTime                        time.Time                     `json:"startTime"`
	CompletionTime                   time.Time                     `json:"completionTime"`
	Tags                             map[string]string             `json:"tags,omitempty"`
	Settings                         *MedicalTranscriptionSettings `json:"settings,omitempty"`
	Media                            Media                         `json:"media"`
	Type                             string                        `json:"type,omitempty"`
	Specialty                        string                        `json:"specialty,omitempty"`
	LanguageCode                     string                        `json:"languageCode"`
	MediaFormat                      string                        `json:"mediaFormat,omitempty"`
	FailureReason                    string                        `json:"failureReason,omitempty"`
	OutputBucketName                 string                        `json:"outputBucketName,omitempty"`
	OutputKey                        string                        `json:"outputKey,omitempty"`
	TranscriptionJobStatus           string                        `json:"transcriptionJobStatus"`
	MedicalContentIdentificationType string                        `json:"medicalContentIdentificationType,omitempty"`
	MedicalTranscriptionJobName      string                        `json:"medicalTranscriptionJobName"`
	TranscriptJSON                   []byte                        `json:"-"`
	MediaSampleRateHertz             int32                         `json:"mediaSampleRateHertz,omitempty"`
}

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
	ContentRedaction       *ContentRedaction             `json:"ContentRedaction,omitempty"`
	Summarization          *SummarizationSettings        `json:"Summarization,omitempty"`
	LanguageIDSettings     map[string]LanguageIDSettings `json:"LanguageIdSettings,omitempty"`
	VocabularyName         string                        `json:"VocabularyName,omitempty"`
	VocabularyFilterName   string                        `json:"VocabularyFilterName,omitempty"`
	VocabularyFilterMethod string                        `json:"VocabularyFilterMethod,omitempty"`
	LanguageModelName      string                        `json:"LanguageModelName,omitempty"`
	LanguageOptions        []string                      `json:"LanguageOptions,omitempty"`
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

// AbsoluteTimeRange specifies a time range in milliseconds within a call
// analytics rule filter.
type AbsoluteTimeRange struct {
	EndTime   *int64 `json:"EndTime,omitempty"`
	First     *int64 `json:"First,omitempty"`
	Last      *int64 `json:"Last,omitempty"`
	StartTime *int64 `json:"StartTime,omitempty"`
}

// RelativeTimeRange specifies a time range as a percentage of the media
// duration within a call analytics rule filter.
type RelativeTimeRange struct {
	EndPercentage   *int32 `json:"EndPercentage,omitempty"`
	First           *int32 `json:"First,omitempty"`
	Last            *int32 `json:"Last,omitempty"`
	StartPercentage *int32 `json:"StartPercentage,omitempty"`
}

// NonTalkTimeFilter matches segments with no speech.
//
// ParticipantRole is not a real member of AWS's NonTalkTimeFilter (unlike its
// three siblings below) - kept for backward compatibility, but unreachable by
// a real client since the real request/response type has no such field.
type NonTalkTimeFilter struct {
	AbsoluteTimeRange *AbsoluteTimeRange `json:"AbsoluteTimeRange,omitempty"`
	RelativeTimeRange *RelativeTimeRange `json:"RelativeTimeRange,omitempty"`
	ParticipantRole   string             `json:"ParticipantRole,omitempty"`
	Threshold         int64              `json:"Threshold,omitempty"`
	Negate            bool               `json:"Negate,omitempty"`
}

// InterruptionFilter matches interruptions by a participant.
type InterruptionFilter struct {
	AbsoluteTimeRange *AbsoluteTimeRange `json:"AbsoluteTimeRange,omitempty"`
	RelativeTimeRange *RelativeTimeRange `json:"RelativeTimeRange,omitempty"`
	ParticipantRole   string             `json:"ParticipantRole,omitempty"`
	Threshold         int64              `json:"Threshold,omitempty"`
	Negate            bool               `json:"Negate,omitempty"`
}

// TranscriptFilter matches specific phrases in the transcript.
type TranscriptFilter struct {
	AbsoluteTimeRange    *AbsoluteTimeRange `json:"AbsoluteTimeRange,omitempty"`
	RelativeTimeRange    *RelativeTimeRange `json:"RelativeTimeRange,omitempty"`
	TranscriptFilterType string             `json:"TranscriptFilterType"`
	ParticipantRole      string             `json:"ParticipantRole,omitempty"`
	Targets              []string           `json:"Targets"`
	Negate               bool               `json:"Negate,omitempty"`
}

// SentimentFilter matches sentiment in the transcript.
type SentimentFilter struct {
	AbsoluteTimeRange *AbsoluteTimeRange `json:"AbsoluteTimeRange,omitempty"`
	RelativeTimeRange *RelativeTimeRange `json:"RelativeTimeRange,omitempty"`
	ParticipantRole   string             `json:"ParticipantRole,omitempty"`
	Sentiments        []string           `json:"Sentiments"`
	Negate            bool               `json:"Negate,omitempty"`
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
	ClinicalNoteGenerationSettings *ClinicalNoteGenerationSettings `json:"ClinicalNoteGenerationSettings,omitempty"`
	VocabularyName                 string                          `json:"VocabularyName,omitempty"`
	VocabularyFilterName           string                          `json:"VocabularyFilterName,omitempty"`
	VocabularyFilterMethod         string                          `json:"VocabularyFilterMethod,omitempty"`
	MaxSpeakerLabels               int32                           `json:"MaxSpeakerLabels,omitempty"`
	ShowSpeakerLabels              bool                            `json:"ShowSpeakerLabels,omitempty"`
	ChannelIdentification          bool                            `json:"ChannelIdentification,omitempty"`
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

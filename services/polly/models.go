package polly

import "time"

// Tag is a Polly resource tag.
type Tag struct {
	Key   string `json:"Key"`
	Value string `json:"Value"`
}

// Lexicon stores pronunciation content and computed lexicon attributes.
type Lexicon struct {
	LastModified time.Time
	Name         string
	ARN          string
	Content      string
	Alphabet     string
	LanguageCode string
	LexemesCount int
	Size         int
}

// Voice describes a voice that can be selected for synthesis.
type Voice struct {
	AdditionalLanguageCodes []string `json:"AdditionalLanguageCodes,omitempty"`
	LanguageCode            string   `json:"LanguageCode"`
	LanguageName            string   `json:"LanguageName"`
	Gender                  string   `json:"Gender"`
	ID                      string   `json:"Id"`
	Name                    string   `json:"Name"`
	SupportedEngines        []string `json:"SupportedEngines"`
}

// SynthesisOptions contains common synchronous and asynchronous synthesis parameters.
type SynthesisOptions struct {
	Engine          string
	LanguageCode    string
	OutputFormat    string
	SampleRate      string
	Text            string
	TextType        string
	VoiceID         string
	LexiconNames    []string
	SpeechMarkTypes []string
}

// SpeechSynthesisTask represents an asynchronous synthesis task.
type SpeechSynthesisTask struct {
	CreationTime       time.Time
	TaskID             string
	TaskStatus         string
	TaskStatusReason   string
	OutputURI          string
	OutputS3BucketName string
	SNSRoleArn         string
	SNSTopicArn        string
	Options            SynthesisOptions
	polls              int
}

// SynthesizedSpeech is deterministic output from SynthesizeSpeech.
type SynthesizedSpeech struct {
	ContentType       string
	Data              []byte
	RequestCharacters int
}

// DescribeVoicesFilter limits voice responses.
type DescribeVoicesFilter struct {
	Engine                         string
	LanguageCode                   string
	Gender                         string
	IncludeAdditionalLanguageCodes bool
}

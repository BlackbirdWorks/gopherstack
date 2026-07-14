package polly

import (
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/collections"
	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

const (
	defaultSampleRateMP3 = "22050"
	defaultSampleRatePCM = "16000"
	defaultLanguageCode  = "en-US"
	defaultEngine        = "standard"
	engineNeural         = "neural"
	engineLongForm       = "long-form"
	engineGenerative     = "generative"
	outputFormatMP3      = "mp3"
	outputFormatOGG      = "ogg_vorbis"
	outputFormatOggOpus  = "ogg_opus"
	outputFormatPCM      = "pcm"
	outputFormatMulaw    = "mulaw"
	outputFormatAlaw     = "alaw"
	outputFormatJSON     = "json"
	textTypeText         = "text"
	textTypeSSML         = "ssml"
	genderFemale         = "Female"
	genderMale           = "Male"
	taskStatusScheduled  = "scheduled"
	taskStatusProgress   = "inProgress"
	taskStatusCompleted  = "completed"
	taskStatusFailed     = "failed"
	failedTaskMarker     = "[fail]"
	maxTaskPageSize      = 100

	maxSpeechTextLen  = 3000
	maxSpeechSSMLLen  = 6000
	maxTaskTextLen    = 100000
	maxLexiconNameLen = 20
	maxLexiconNames   = 5
	maxTagCount       = 50
	maxTagKeyLen      = 128
	maxTagValueLen    = 256

	// Speech-mark synthetic timing: roughly 80ms per character.
	msPerCharacter = 80

	// WAV/PCM container constants.
	defaultWAVSampleRate = 22050
	wavBitsPerByte       = 8
	wavHeaderMinusRIFF   = 36 // total header bytes minus the 8-byte RIFF chunk descriptor
	wavHeaderSize        = 44 // full RIFF/WAV header length
	wavPCMChunkSize      = 16 // PCM fmt subchunk size
	wavSilentDataLen     = 4  // two silent 16-bit samples

	// Headerless companded-audio (mulaw/alaw) silence byte values and sample count.
	mulawSilenceByte   = 0xFF
	alawSilenceByte    = 0xD5
	compandedSampleLen = 4
)

var (
	// ErrLexiconNotFound is returned when a requested lexicon is absent.
	ErrLexiconNotFound = errors.New("LexiconNotFoundException")
	// ErrTaskNotFound is returned when a requested synthesis task is absent.
	ErrTaskNotFound = errors.New("SynthesisTaskNotFoundException")
	// ErrValidation is returned when request parameters do not meet Polly constraints
	// that AWS models as a generic/unlisted validation failure.
	ErrValidation = errors.New("InvalidParameterValueException")
	// ErrResourceNotFound is returned when a tagged resource ARN is unknown.
	ErrResourceNotFound = errors.New("ResourceNotFoundException")
	// ErrTextLengthExceeded is returned when Text exceeds the format-specific length limit.
	ErrTextLengthExceeded = errors.New("TextLengthExceededException")
	// ErrInvalidSampleRate is returned when SampleRate is not valid for the requested OutputFormat.
	ErrInvalidSampleRate = errors.New("InvalidSampleRateException")
	// ErrEngineNotSupported is returned when the requested voice does not support the requested engine.
	ErrEngineNotSupported = errors.New("EngineNotSupportedException")
	// ErrLanguageNotSupported is returned when the requested voice does not support the requested language.
	ErrLanguageNotSupported = errors.New("LanguageNotSupportedException")
	// ErrMarksNotSupportedForFormat is returned when SpeechMarkTypes is requested with a non-json OutputFormat.
	ErrMarksNotSupportedForFormat = errors.New("MarksNotSupportedForFormatException")
	// ErrSsmlMarksNotSupportedForTextType is returned when the "ssml" speech mark type is
	// requested with a plain-text TextType.
	ErrSsmlMarksNotSupportedForTextType = errors.New("SsmlMarksNotSupportedForTextTypeException")
	// ErrInvalidNextToken is returned when a pagination token cannot be decoded.
	ErrInvalidNextToken = errors.New("InvalidNextTokenException")
	// ErrInvalidLexicon is returned when lexicon Content is not well-formed PLS lexicon XML.
	ErrInvalidLexicon = errors.New("InvalidLexiconException")
)

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

// InMemoryBackend stores Polly resources safely for concurrent requests.
//
// lexicons and tasks are *store.Table[T]-backed (see store_setup.go and
// pkgs/store's package doc); tags remains a plain map since its values are
// map[string]string, not *T, so there is nothing for store.Table to key on.
// voices is the static built-in voice catalogue, not a mutable resource
// collection.
type InMemoryBackend struct {
	lexicons  *store.Table[Lexicon]
	tasks     *store.Table[SpeechSynthesisTask]
	tags      map[string]map[string]string
	registry  *store.Registry
	accountID string
	region    string
	voices    []Voice
	mu        sync.RWMutex
}

// NewInMemoryBackend creates a Polly backend configured for default AWS identity.
func NewInMemoryBackend() *InMemoryBackend {
	return NewInMemoryBackendWithConfig(config.DefaultAccountID, config.DefaultRegion)
}

// NewInMemoryBackendWithConfig creates a Polly backend configured for account and region.
func NewInMemoryBackendWithConfig(accountID, region string) *InMemoryBackend {
	b := &InMemoryBackend{
		registry:  store.NewRegistry(),
		tags:      make(map[string]map[string]string),
		voices:    builtInVoices(),
		accountID: accountID,
		region:    region,
	}
	registerAllTables(b)

	return b
}

// Region returns configured AWS region.
func (b *InMemoryBackend) Region() string { return b.region }

// Reset clears stored resources.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.registry.ResetAll()
	b.tags = make(map[string]map[string]string)
}

// PutLexicon creates or replaces lexicon content.
func (b *InMemoryBackend) PutLexicon(name, content string) error {
	if err := validateLexicon(name, content); err != nil {
		return err
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	b.lexicons.Put(&Lexicon{
		Name:         name,
		ARN:          arn.Build("polly", b.region, b.accountID, "lexicon/"+name),
		Content:      content,
		Alphabet:     lexiconAttribute(content, "alphabet", "ipa"),
		LanguageCode: lexiconAttribute(content, "xml:lang", defaultLanguageCode),
		LexemesCount: strings.Count(content, "<lexeme>"),
		Size:         len(content),
		LastModified: time.Now().UTC(),
	})

	return nil
}

// GetLexicon returns named lexicon.
func (b *InMemoryBackend) GetLexicon(name string) (*Lexicon, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	lexicon, ok := b.lexicons.Get(name)
	if !ok {
		return nil, fmt.Errorf("%w: lexicon %q", ErrLexiconNotFound, name)
	}

	return cloneLexicon(lexicon), nil
}

// DeleteLexicon removes named lexicon.
func (b *InMemoryBackend) DeleteLexicon(name string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if !b.lexicons.Delete(name) {
		return fmt.Errorf("%w: lexicon %q", ErrLexiconNotFound, name)
	}

	return nil
}

// ListLexicons lists lexicons ordered by name.
func (b *InMemoryBackend) ListLexicons() []*Lexicon {
	b.mu.RLock()
	all := b.lexicons.All()
	out := make([]*Lexicon, 0, len(all))
	for _, lexicon := range all {
		out = append(out, cloneLexicon(lexicon))
	}
	b.mu.RUnlock()

	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })

	return out
}

// DescribeVoices lists built-in voices matching filters.
func (b *InMemoryBackend) DescribeVoices(filter DescribeVoicesFilter) ([]Voice, error) {
	if filter.Engine != "" && !slices.Contains(validEngines(), filter.Engine) {
		return nil, fmt.Errorf("%w: invalid Engine %q", ErrValidation, filter.Engine)
	}

	b.mu.RLock()
	defer b.mu.RUnlock()

	out := make([]Voice, 0, len(b.voices))
	for _, voice := range b.voices {
		if !matchesVoice(voice, filter) {
			continue
		}

		copyVoice := voice
		copyVoice.AdditionalLanguageCodes = slices.Clone(voice.AdditionalLanguageCodes)
		copyVoice.SupportedEngines = slices.Clone(voice.SupportedEngines)
		if !filter.IncludeAdditionalLanguageCodes {
			copyVoice.AdditionalLanguageCodes = nil
		}
		out = append(out, copyVoice)
	}

	return out, nil
}

// SynthesizeSpeech validates options and returns deterministic audio or speech-mark output.
func (b *InMemoryBackend) SynthesizeSpeech(options SynthesisOptions) (*SynthesizedSpeech, error) {
	limit := maxSpeechTextLen
	if options.TextType == textTypeSSML {
		limit = maxSpeechSSMLLen
	}
	if len(options.Text) > limit {
		return nil, fmt.Errorf("%w: text exceeds maximum length of %d characters", ErrTextLengthExceeded, limit)
	}

	normal, err := b.validateOptions(options)
	if err != nil {
		return nil, err
	}

	if normal.OutputFormat == outputFormatJSON {
		return &SynthesizedSpeech{
			Data:              speechMarks(normal),
			ContentType:       "application/x-json-stream",
			RequestCharacters: len(normal.Text),
		}, nil
	}

	data := syntheticAudioBytes(normal)

	return &SynthesizedSpeech{
		Data:              data,
		ContentType:       contentTypeForFormat(normal.OutputFormat),
		RequestCharacters: len(normal.Text),
	}, nil
}

// StartSpeechSynthesisTask creates scheduled asynchronous task.
func (b *InMemoryBackend) StartSpeechSynthesisTask(
	options SynthesisOptions,
	outputBucket, roleArn, topicArn string,
) (*SpeechSynthesisTask, error) {
	if outputBucket == "" {
		return nil, fmt.Errorf("%w: OutputS3BucketName is required", ErrValidation)
	}
	if len(options.Text) > maxTaskTextLen {
		return nil, fmt.Errorf(
			"%w: text exceeds maximum length of %d characters", ErrTextLengthExceeded, maxTaskTextLen,
		)
	}

	normal, err := b.validateOptions(options)
	if err != nil {
		return nil, err
	}

	id := uuid.NewString()
	task := &SpeechSynthesisTask{
		CreationTime:       time.Now().UTC(),
		TaskID:             id,
		TaskStatus:         taskStatusScheduled,
		OutputURI:          fmt.Sprintf("s3://%s/%s.%s", outputBucket, id, taskExtension(normal.OutputFormat)),
		OutputS3BucketName: outputBucket,
		SNSRoleArn:         roleArn,
		SNSTopicArn:        topicArn,
		Options:            normal,
	}

	b.mu.Lock()
	b.tasks.Put(task)
	b.tags[b.taskARN(id)] = make(map[string]string)
	b.mu.Unlock()

	return cloneTask(task), nil
}

// GetSpeechSynthesisTask retrieves task and advances simulated lifecycle.
func (b *InMemoryBackend) GetSpeechSynthesisTask(taskID string) (*SpeechSynthesisTask, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	task, ok := b.tasks.Get(taskID)
	if !ok {
		return nil, fmt.Errorf("%w: task %q", ErrTaskNotFound, taskID)
	}

	advanceTask(task)

	return cloneTask(task), nil
}

// ListSpeechSynthesisTasks lists tasks and advances lifecycle consistently with AWS polling.
func (b *InMemoryBackend) ListSpeechSynthesisTasks(
	status, token string,
	maxResults int,
) ([]*SpeechSynthesisTask, string, error) {
	if status != "" && !slices.Contains(validTaskStatuses(), status) {
		return nil, "", fmt.Errorf("%w: invalid Status %q", ErrValidation, status)
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	// Table.Snapshot returns tasks ordered by TaskID ascending, matching the
	// previous collections.SortedKeys(b.tasks) traversal order exactly.
	tasks := b.tasks.Snapshot()

	offset, err := parseToken(token, len(tasks))
	if err != nil {
		return nil, "", err
	}
	if maxResults <= 0 || maxResults > maxTaskPageSize {
		maxResults = maxTaskPageSize
	}

	out := make([]*SpeechSynthesisTask, 0, len(tasks))
	for _, task := range tasks[offset:] {
		advanceTask(task)
		if status == "" || task.TaskStatus == status {
			out = append(out, cloneTask(task))
		}
		if len(out) == maxResults {
			return out, encodeToken(offset + len(out)), nil
		}
	}

	return out, "", nil
}

// TagResource adds tags to known task ARN.
func (b *InMemoryBackend) TagResource(resourceArn string, tags []Tag) error {
	if err := validateTags(tags); err != nil {
		return err
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	current, ok := b.tags[resourceArn]
	if !ok {
		return fmt.Errorf("%w: resource %q not found", ErrResourceNotFound, resourceArn)
	}
	if len(current)+len(tags) > maxTagCount {
		return fmt.Errorf("%w: resource would exceed %d tag limit", ErrValidation, maxTagCount)
	}
	for _, tag := range tags {
		current[tag.Key] = tag.Value
	}

	return nil
}

// UntagResource removes tag keys from known task ARN.
func (b *InMemoryBackend) UntagResource(resourceArn string, keys []string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	current, ok := b.tags[resourceArn]
	if !ok {
		return fmt.Errorf("%w: resource %q not found", ErrResourceNotFound, resourceArn)
	}
	for _, key := range keys {
		delete(current, key)
	}

	return nil
}

// ListTagsForResource returns sorted task tags.
func (b *InMemoryBackend) ListTagsForResource(resourceArn string) ([]Tag, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	current, ok := b.tags[resourceArn]
	if !ok {
		return nil, fmt.Errorf("%w: resource %q not found", ErrResourceNotFound, resourceArn)
	}

	keys := collections.SortedKeys(current)

	out := make([]Tag, 0, len(keys))
	for _, key := range keys {
		out = append(out, Tag{Key: key, Value: current[key]})
	}

	return out, nil
}

func (b *InMemoryBackend) validateOptions(options SynthesisOptions) (SynthesisOptions, error) {
	options = defaultOptions(options)
	if options.Text == "" || options.VoiceID == "" {
		return options, fmt.Errorf("%w: Text and VoiceId are required", ErrValidation)
	}
	if len(options.LexiconNames) > maxLexiconNames {
		return options, fmt.Errorf("%w: LexiconNames must not exceed %d entries", ErrValidation, maxLexiconNames)
	}
	if !slices.Contains(validEngines(), options.Engine) {
		return options, fmt.Errorf("%w: invalid Engine %q", ErrValidation, options.Engine)
	}
	if !slices.Contains(validOutputFormats(), options.OutputFormat) {
		return options, fmt.Errorf("%w: invalid OutputFormat %q", ErrValidation, options.OutputFormat)
	}
	if !slices.Contains(validTextTypes(), options.TextType) {
		return options, fmt.Errorf("%w: invalid TextType %q", ErrValidation, options.TextType)
	}
	if !validSampleRate(options.OutputFormat, options.SampleRate) {
		return options, fmt.Errorf(
			"%w: invalid SampleRate %q for %s",
			ErrInvalidSampleRate,
			options.SampleRate,
			options.OutputFormat,
		)
	}
	if err := validateSpeechMarks(options); err != nil {
		return options, err
	}

	b.mu.RLock()
	defer b.mu.RUnlock()

	if err := b.checkVoiceSupport(options.VoiceID, options.Engine, options.LanguageCode); err != nil {
		return options, err
	}
	for _, name := range options.LexiconNames {
		if !b.lexicons.Has(name) {
			return options, fmt.Errorf("%w: lexicon %q", ErrLexiconNotFound, name)
		}
	}

	return options, nil
}

// checkVoiceSupport validates that voice id can render the requested engine and
// language, returning the AWS-accurate exception for the first constraint that
// fails: an unrecognized VoiceId, an engine the voice doesn't support
// (EngineNotSupportedException), or a language/dialect the voice doesn't speak
// (LanguageNotSupportedException).
func (b *InMemoryBackend) checkVoiceSupport(id, engine, languageCode string) error {
	for _, voice := range b.voices {
		if voice.ID != id {
			continue
		}
		if !slices.Contains(voice.SupportedEngines, engine) {
			return fmt.Errorf("%w: voice %q does not support engine %q", ErrEngineNotSupported, id, engine)
		}
		if languageCode == "" || voice.LanguageCode == languageCode ||
			slices.Contains(voice.AdditionalLanguageCodes, languageCode) {
			return nil
		}

		return fmt.Errorf("%w: voice %q does not support language %q", ErrLanguageNotSupported, id, languageCode)
	}

	return fmt.Errorf("%w: unknown VoiceId %q", ErrValidation, id)
}

func (b *InMemoryBackend) taskARN(id string) string {
	return arn.Build("polly", b.region, b.accountID, "synthesis-task/"+id)
}

// TaskARN returns resource ARN for tags on created task.
func (b *InMemoryBackend) TaskARN(taskID string) string { return b.taskARN(taskID) }

func validateLexicon(name, content string) error {
	if !validLexiconName(name) {
		return fmt.Errorf("%w: lexicon name must be 1-%d alphanumeric characters", ErrValidation, maxLexiconNameLen)
	}
	if content == "" || !strings.Contains(content, "<lexicon") {
		return fmt.Errorf("%w: Content must be PLS lexicon XML", ErrInvalidLexicon)
	}

	return nil
}

func validLexiconName(name string) bool {
	if name == "" || len(name) > maxLexiconNameLen {
		return false
	}
	for _, ch := range name {
		if (ch < 'A' || ch > 'Z') && (ch < 'a' || ch > 'z') && (ch < '0' || ch > '9') {
			return false
		}
	}

	return true
}

func lexiconAttribute(content, attr, fallback string) string {
	token := attr + `="`
	start := strings.Index(content, token)
	if start < 0 {
		return fallback
	}
	start += len(token)
	end := strings.IndexByte(content[start:], '"')
	if end < 0 {
		return fallback
	}

	return content[start : start+end]
}

func cloneLexicon(lexicon *Lexicon) *Lexicon {
	copyLexicon := *lexicon

	return &copyLexicon
}

func cloneTask(task *SpeechSynthesisTask) *SpeechSynthesisTask {
	copyTask := *task
	copyTask.Options.LexiconNames = slices.Clone(task.Options.LexiconNames)
	copyTask.Options.SpeechMarkTypes = slices.Clone(task.Options.SpeechMarkTypes)

	return &copyTask
}

func advanceTask(task *SpeechSynthesisTask) {
	switch task.TaskStatus {
	case taskStatusScheduled:
		task.TaskStatus = taskStatusProgress
	case taskStatusProgress:
		if strings.Contains(strings.ToLower(task.Options.Text), failedTaskMarker) {
			task.TaskStatus = taskStatusFailed
			task.TaskStatusReason = "Synthetic synthesis failure requested by text marker"
		} else {
			task.TaskStatus = taskStatusCompleted
		}
	}
	task.polls++
}

func matchesVoice(voice Voice, filter DescribeVoicesFilter) bool {
	if filter.Engine != "" && !slices.Contains(voice.SupportedEngines, filter.Engine) {
		return false
	}
	if filter.Gender != "" && voice.Gender != filter.Gender {
		return false
	}
	if filter.LanguageCode == "" || voice.LanguageCode == filter.LanguageCode {
		return true
	}

	return filter.IncludeAdditionalLanguageCodes && slices.Contains(voice.AdditionalLanguageCodes, filter.LanguageCode)
}

func defaultOptions(options SynthesisOptions) SynthesisOptions {
	if options.Engine == "" {
		options.Engine = defaultEngine
	}
	if options.TextType == "" {
		options.TextType = textTypeText
	}
	if options.OutputFormat == "" {
		options.OutputFormat = outputFormatMP3
	}
	if options.SampleRate == "" {
		switch {
		case options.OutputFormat == outputFormatOggOpus:
			options.SampleRate = "48000"
		case options.OutputFormat == outputFormatMulaw || options.OutputFormat == outputFormatAlaw:
			options.SampleRate = "8000"
		case options.OutputFormat == outputFormatPCM:
			options.SampleRate = defaultSampleRatePCM
		case options.Engine != defaultEngine:
			options.SampleRate = "24000"
		default:
			options.SampleRate = defaultSampleRateMP3
		}
	}

	return options
}

func validateSpeechMarks(options SynthesisOptions) error {
	for _, speechMark := range options.SpeechMarkTypes {
		if !slices.Contains(validSpeechMarkTypes(), speechMark) {
			return fmt.Errorf("%w: invalid SpeechMarkType %q", ErrValidation, speechMark)
		}
		// AWS: "SSML speech marks are not supported for plain text-type input."
		if speechMark == textTypeSSML && options.TextType != textTypeSSML {
			return fmt.Errorf(
				"%w: SpeechMarkTypes ssml requires TextType ssml", ErrSsmlMarksNotSupportedForTextType,
			)
		}
	}
	if len(options.SpeechMarkTypes) > 0 && options.OutputFormat != outputFormatJSON {
		return fmt.Errorf("%w: speech marks require json OutputFormat", ErrMarksNotSupportedForFormat)
	}
	if len(options.SpeechMarkTypes) == 0 && options.OutputFormat == outputFormatJSON {
		return fmt.Errorf("%w: json OutputFormat requires SpeechMarkTypes", ErrValidation)
	}

	return nil
}

func validateTags(tags []Tag) error {
	seen := make(map[string]bool, len(tags))
	for _, tag := range tags {
		if tag.Key == "" || len(tag.Key) > maxTagKeyLen || seen[tag.Key] {
			return fmt.Errorf(
				"%w: tag keys must be non-empty, unique, and at most %d characters",
				ErrValidation, maxTagKeyLen,
			)
		}
		if len(tag.Value) > maxTagValueLen {
			return fmt.Errorf("%w: tag value must be at most %d characters", ErrValidation, maxTagValueLen)
		}
		seen[tag.Key] = true
	}

	return nil
}

func validSampleRate(format, rate string) bool {
	rates := map[string][]string{
		outputFormatMP3:     {"8000", "16000", "22050", "24000", "44100", "48000"},
		outputFormatOGG:     {"8000", "16000", "22050", "24000", "44100", "48000"},
		outputFormatOggOpus: {"48000"},
		outputFormatPCM:     {"8000", "16000"},
		outputFormatMulaw:   {"8000"},
		outputFormatAlaw:    {"8000"},
		outputFormatJSON:    {"8000", "16000", "22050", "24000"},
	}

	return slices.Contains(rates[format], rate)
}

func contentTypeForFormat(format string) string {
	contentTypes := map[string]string{
		outputFormatMP3:     "audio/mpeg",
		outputFormatOGG:     "audio/ogg",
		outputFormatOggOpus: "audio/ogg",
		outputFormatPCM:     "audio/pcm",
		outputFormatMulaw:   "audio/mulaw",
		outputFormatAlaw:    "audio/alaw",
	}

	return contentTypes[format]
}

func taskExtension(format string) string {
	if format == outputFormatOGG || format == outputFormatOggOpus {
		return "ogg"
	}

	return format
}

// speechMarkItem is a timed speech mark entry used when building speech mark output.
type speechMarkItem struct {
	line string
	time int
}

// buildSentenceMarks returns one speechMarkItem per sentence in text, splitting
// on '.', '!' and '?' delimiters. Text with no sentence-ending punctuation is
// treated as a single sentence, matching AWS Polly behaviour.
func buildSentenceMarks(text string) []speechMarkItem {
	if text == "" {
		return nil
	}
	var out []speechMarkItem
	start := 0
	for i := range len(text) {
		ch := text[i]
		if ch != '.' && ch != '!' && ch != '?' {
			continue
		}
		end := i + 1
		if sentence := strings.TrimSpace(text[start:end]); sentence != "" {
			t := start * msPerCharacter
			out = append(out, speechMarkItem{
				time: t,
				line: fmt.Sprintf(`{"time":%d,"type":"sentence","start":%d,"end":%d,"value":%q}`,
					t, start, end, sentence),
			})
		}
		start = end
		for start < len(text) && (text[start] == ' ' || text[start] == '\n' ||
			text[start] == '\r' || text[start] == '\t') {
			start++
		}
	}
	if sentence := strings.TrimSpace(text[start:]); sentence != "" {
		t := start * msPerCharacter
		out = append(out, speechMarkItem{
			time: t,
			line: fmt.Sprintf(`{"time":%d,"type":"sentence","start":%d,"end":%d,"value":%q}`,
				t, start, len(text), sentence),
		})
	}

	return out
}

func speechMarks(options SynthesisOptions) []byte {
	var marks []speechMarkItem

	for _, typ := range options.SpeechMarkTypes {
		switch typ {
		case "sentence":
			marks = append(marks, buildSentenceMarks(options.Text)...)
		case textTypeSSML:
			marks = append(marks, speechMarkItem{
				time: 0,
				line: fmt.Sprintf(`{"time":0,"type":"ssml","start":0,"end":%d,"value":"<speak>"}`, len(options.Text)),
			})
		}
	}

	needWord := slices.Contains(options.SpeechMarkTypes, "word")
	needViseme := slices.Contains(options.SpeechMarkTypes, "viseme")
	if needWord || needViseme {
		offset := 0
		for word := range strings.FieldsSeq(options.Text) {
			start := strings.Index(options.Text[offset:], word) + offset
			end := start + len(word)
			timeMs := start * msPerCharacter
			if needWord {
				marks = append(marks, speechMarkItem{
					time: timeMs,
					line: fmt.Sprintf(`{"time":%d,"type":"word","start":%d,"end":%d,"value":%q}`,
						timeMs, start, end, word),
				})
			}
			if needViseme {
				marks = append(marks, speechMarkItem{
					time: timeMs,
					line: fmt.Sprintf(`{"time":%d,"type":"viseme","value":%q}`, timeMs, wordFirstViseme(word)),
				})
			}
			offset = end
		}
	}

	// Stable sort by time so sentence marks precede word marks at equal time positions.
	sort.SliceStable(marks, func(i, j int) bool { return marks[i].time < marks[j].time })

	lines := make([]string, 0, len(marks))
	for _, m := range marks {
		lines = append(lines, m.line)
	}

	if len(lines) == 0 {
		for _, typ := range options.SpeechMarkTypes {
			lines = append(lines, fmt.Sprintf(`{"time":0,"type":"%s","start":0,"end":%d,"value":%q}`,
				typ, len(options.Text), options.Text))
		}
	}

	return []byte(strings.Join(lines, "\n") + "\n")
}

// syntheticAudioBytes returns minimal but format-correct audio bytes for the given output format.
// PCM → RIFF/WAV container with one silent frame.
// MP3 → minimal MPEG-1 Layer 3 sync frame header.
// OGG/ogg_opus → OGG capture pattern + minimal Vorbis identification.
// mulaw/alaw → a few headerless companded silence samples.
func syntheticAudioBytes(opts SynthesisOptions) []byte {
	switch opts.OutputFormat {
	case outputFormatPCM:
		return minimalWAV(opts.SampleRate)
	case outputFormatMP3:
		return minimalMP3Frame()
	case outputFormatOGG, outputFormatOggOpus:
		return minimalOGG()
	case outputFormatMulaw, outputFormatAlaw:
		return minimalCompandedAudio(opts.OutputFormat)
	default:
		return minimalWAV(opts.SampleRate)
	}
}

// minimalCompandedAudio returns a few silent samples for headerless companded
// (mu-law/a-law) audio: AWS returns raw audio/mulaw or audio/alaw bytes with no
// RIFF/WAV container, unlike this backend's pcm output.
func minimalCompandedAudio(format string) []byte {
	silence := byte(mulawSilenceByte)
	if format == outputFormatAlaw {
		silence = alawSilenceByte
	}

	out := make([]byte, compandedSampleLen)
	for i := range out {
		out[i] = silence
	}

	return out
}

// minimalWAV returns a 46-byte RIFF/WAV file with two silent PCM samples.
func minimalWAV(sampleRateStr string) []byte {
	sampleRate := uint32(defaultWAVSampleRate)
	switch sampleRateStr {
	case "8000":
		sampleRate = 8000
	case "16000":
		sampleRate = 16000
	case "24000":
		sampleRate = 24000
	case "44100":
		sampleRate = 44100
	case "48000":
		sampleRate = 48000
	}
	const numChannels = uint16(1)
	const bitsPerSample = uint16(wavPCMChunkSize)
	const dataLen = uint32(wavSilentDataLen) // 2 silent 16-bit samples
	byteRate := sampleRate * uint32(numChannels) * uint32(bitsPerSample) / wavBitsPerByte
	blockAlign := numChannels * bitsPerSample / wavBitsPerByte
	fileSize := wavHeaderMinusRIFF + dataLen

	buf := make([]byte, 0, wavHeaderSize+dataLen)
	buf = append(buf, 'R', 'I', 'F', 'F')
	buf = binary.LittleEndian.AppendUint32(buf, fileSize)
	buf = append(buf, 'W', 'A', 'V', 'E')
	buf = append(buf, 'f', 'm', 't', ' ')
	buf = binary.LittleEndian.AppendUint32(buf, wavPCMChunkSize) // PCM chunk size
	buf = binary.LittleEndian.AppendUint16(buf, 1)               // PCM format
	buf = binary.LittleEndian.AppendUint16(buf, numChannels)
	buf = binary.LittleEndian.AppendUint32(buf, sampleRate)
	buf = binary.LittleEndian.AppendUint32(buf, byteRate)
	buf = binary.LittleEndian.AppendUint16(buf, blockAlign)
	buf = binary.LittleEndian.AppendUint16(buf, bitsPerSample)
	buf = append(buf, 'd', 'a', 't', 'a')
	buf = binary.LittleEndian.AppendUint32(buf, dataLen)
	buf = append(buf, 0, 0, 0, 0) // two silent 16-bit samples

	return buf
}

// minimalMP3Frame returns a minimal valid MPEG-1 Layer 3 frame header (silent frame).
// Frame sync: 0xFFE0 | layer(01) | bitrate(1001=128k) | samplerate(00=44100) | padding(0) | stereo(00).
func minimalMP3Frame() []byte {
	minimalMP3FrameBytes := []byte{
		0xFF, 0xFB, 0x90, 0x00, // sync + MPEG1 Layer3 128kbps 44100Hz stereo no-padding
		// 417 bytes of silence (128kbps frame at 44100 is 417 bytes)
	}
	const mp3FrameTotalLen = 417 // 128kbps frame at 44100Hz: 4-byte header + 413 bytes silence
	frame := make([]byte, mp3FrameTotalLen)
	copy(frame, minimalMP3FrameBytes)

	return frame
}

// minimalOGG returns the OGG capture pattern + minimal Vorbis identification page.
func minimalOGG() []byte {
	// OGG page header magic: "OggS" capture pattern
	return []byte{
		'O', 'g', 'g', 'S', // capture pattern
		0x00,                   // version
		0x02,                   // header type: beginning of stream
		0, 0, 0, 0, 0, 0, 0, 0, // granule position
		1, 0, 0, 0, // serial number
		0, 0, 0, 0, // page sequence
		0, 0, 0, 0, // checksum placeholder
		1,    // number of segments
		0x1E, // segment table: 30-byte vorbis id header
		// Vorbis identification header (minimal)
		0x01,                         // packet type: identification
		'v', 'o', 'r', 'b', 'i', 's', // codec id
		0, 0, 0, 0, // version
		0x01,                   // channels
		0x44, 0xAC, 0x00, 0x00, // sample rate 44100
		0, 0, 0, 0, // max bitrate
		0, 0, 0, 0, // nominal bitrate
		0, 0, 0, 0, // min bitrate
		0x01, // blocksize
	}
}

// encodeToken returns an opaque base64 cursor for pagination.
func encodeToken(n int) string {
	return base64.StdEncoding.EncodeToString([]byte(strconv.Itoa(n)))
}

func parseToken(token string, total int) (int, error) {
	if token == "" {
		return 0, nil
	}
	raw := token
	if decoded, err := base64.StdEncoding.DecodeString(token); err == nil {
		raw = string(decoded)
	}
	var offset int
	if _, err := fmt.Sscanf(raw, "%d", &offset); err != nil || offset < 0 || offset > total {
		return 0, fmt.Errorf("%w: invalid NextToken", ErrInvalidNextToken)
	}

	return offset, nil
}

func validEngines() []string {
	return []string{defaultEngine, engineNeural, engineLongForm, engineGenerative}
}

func validOutputFormats() []string {
	return []string{
		outputFormatMP3, outputFormatOGG, outputFormatOggOpus,
		outputFormatPCM, outputFormatMulaw, outputFormatAlaw, outputFormatJSON,
	}
}

func validTextTypes() []string { return []string{textTypeText, textTypeSSML} }

func validSpeechMarkTypes() []string { return []string{"sentence", textTypeSSML, "viseme", "word"} }

func validTaskStatuses() []string {
	return []string{taskStatusScheduled, taskStatusProgress, taskStatusCompleted, taskStatusFailed}
}

// wordFirstViseme returns an approximate AWS Polly viseme for the first character of word.
// AWS Polly visemes: p b m → "p", f v → "f", t d → "t", s z → "s",
// k g c q x → "k", n → "n", l → "l", r → "r", vowels → "@", etc.
func wordFirstViseme(word string) string {
	if word == "" {
		return "sil"
	}

	ch := word[0]
	if ch >= 'A' && ch <= 'Z' {
		ch += 32
	}

	if v := wordFirstVisemeVowelOrSibilant(ch); v != "" {
		return v
	}

	return wordFirstVisemeConsonant(ch)
}

func wordFirstVisemeVowelOrSibilant(ch byte) string {
	switch ch {
	case 'a', 'e', 'i', 'o', 'u', 'h':
		return "@"
	case 'p', 'b', 'm':
		return "p"
	case 'f', 'v':
		return "f"
	case 't', 'd':
		return "t"
	case 's', 'z':
		return "s"
	case 'j':
		return "S"
	}

	return ""
}

func wordFirstVisemeConsonant(ch byte) string {
	switch ch {
	case 'k', 'g', 'c', 'q', 'x':
		return "k"
	case 'n':
		return "n"
	case 'l':
		return "l"
	case 'r':
		return "r"
	case 'w', 'y':
		return "u"
	}

	return "p"
}

// Language name constants used in the built-in voice catalogue.
const (
	langAustrEnglish    = "Australian English"
	langBelgDutch       = "Belgian Dutch"
	langBelgFrench      = "Belgian French"
	langBrazPortuguese  = "Brazilian Portuguese"
	langBritEnglish     = "British English"
	langCanadFrench     = "Canadian French"
	langCastilSpanish   = "Castilian Spanish"
	langChinaMandarin   = "Chinese Mandarin"
	langDanish          = "Danish"
	langDutch           = "Dutch"
	langEuropPortuguese = "European Portuguese"
	langFrench          = "French"
	langGerman          = "German"
	langIndianEnglish   = "Indian English"
	langIrEnglish       = "Irish English"
	langItalian         = "Italian"
	langJapanese        = "Japanese"
	langKorean          = "Korean"
	langMexSpanish      = "Mexican Spanish"
	langNorwegian       = "Norwegian"
	langNZEnglish       = "New Zealand English"
	langPolish          = "Polish"
	langRussian         = "Russian"
	langSwedish         = "Swedish"
	langTurkish         = "Turkish"
	langUSEnglish       = "US English"
	langUSSpanish       = "US Spanish"
	langZAEnglish       = "South African English"
)

// Language code constants used in the built-in voice catalogue.
const (
	lcAustrEnglish    = "en-AU"
	lcBelgDutch       = "nl-BE"
	lcBelgFrench      = "fr-BE"
	lcBrazPortuguese  = "pt-BR"
	lcBritEnglish     = "en-GB"
	lcCanadFrench     = "fr-CA"
	lcCastilSpanish   = "es-ES"
	lcCatalan         = "ca-ES"
	lcChinaMandarin   = "cmn-CN"
	lcDanish          = "da-DK"
	lcDutch           = "nl-NL"
	lcEuropPortuguese = "pt-PT"
	lcFinnish         = "fi-FI"
	lcFrench          = "fr-FR"
	lcGerman          = "de-DE"
	lcAustrGerman     = "de-AT"
	lcHindi           = "hi-IN"
	lcIcelandic       = "is-IS"
	lcIndianEnglish   = "en-IN"
	lcIrEnglish       = "en-IE"
	lcItalian         = "it-IT"
	lcJapanese        = "ja-JP"
	lcKorean          = "ko-KR"
	lcMexSpanish      = "es-MX"
	lcNorwegian       = "nb-NO"
	lcNZEnglish       = "en-NZ"
	lcPolish          = "pl-PL"
	lcRomanian        = "ro-RO"
	lcRussian         = "ru-RU"
	lcSwedish         = "sv-SE"
	lcTurkish         = "tr-TR"
	lcUSSpanish       = "es-US"
	lcWelsh           = "cy-GB"
	lcZAEnglish       = "en-ZA"
)

const builtInVoicesCap = 90

func builtInVoices() []Voice {
	voices := make([]Voice, 0, builtInVoicesCap)
	voices = append(voices, builtInVoicesEnglishUS()...)
	voices = append(voices, builtInVoicesEnglishGlobal()...)
	voices = append(voices, builtInVoicesGermanic()...)
	voices = append(voices, builtInVoicesFrenchPortuguese()...)
	voices = append(voices, builtInVoicesSpanishItalian()...)
	voices = append(voices, builtInVoicesOther()...)

	return voices
}

func builtInVoicesEnglishUS() []Voice {
	std := defaultEngine
	neu := engineNeural
	lng := engineLongForm
	gen := engineGenerative

	return []Voice{
		{
			ID: "Ivy", Name: "Ivy", Gender: genderFemale,
			LanguageCode: defaultLanguageCode, LanguageName: langUSEnglish,
			SupportedEngines: []string{std, neu},
		},
		{
			ID: "Joanna", Name: "Joanna", Gender: genderFemale,
			LanguageCode: defaultLanguageCode, LanguageName: langUSEnglish,
			SupportedEngines: []string{std, neu, lng, gen},
		},
		{
			ID: "Kendra", Name: "Kendra", Gender: genderFemale,
			LanguageCode: defaultLanguageCode, LanguageName: langUSEnglish,
			SupportedEngines: []string{std, neu},
		},
		{
			ID: "Kimberly", Name: "Kimberly", Gender: genderFemale,
			LanguageCode: defaultLanguageCode, LanguageName: langUSEnglish,
			SupportedEngines: []string{std, neu},
		},
		{
			ID: "Salli", Name: "Salli", Gender: genderFemale,
			LanguageCode: defaultLanguageCode, LanguageName: langUSEnglish,
			SupportedEngines: []string{std, neu},
		},
		{
			ID: "Ruth", Name: "Ruth", Gender: genderFemale,
			LanguageCode: defaultLanguageCode, LanguageName: langUSEnglish,
			SupportedEngines: []string{lng, gen},
		},
		{
			ID: "Joey", Name: "Joey", Gender: genderMale,
			LanguageCode: defaultLanguageCode, LanguageName: langUSEnglish,
			SupportedEngines: []string{std, neu},
		},
		{
			ID: "Justin", Name: "Justin", Gender: genderMale,
			LanguageCode: defaultLanguageCode, LanguageName: langUSEnglish,
			SupportedEngines: []string{std, neu},
		},
		{
			ID: "Kevin", Name: "Kevin", Gender: genderMale,
			LanguageCode: defaultLanguageCode, LanguageName: langUSEnglish,
			SupportedEngines: []string{neu},
		},
		{
			ID: "Matthew", Name: "Matthew", Gender: genderMale,
			LanguageCode: defaultLanguageCode, LanguageName: langUSEnglish,
			SupportedEngines: []string{std, neu, lng, gen},
		},
		{
			ID: "Stephen", Name: "Stephen", Gender: genderMale,
			LanguageCode: defaultLanguageCode, LanguageName: langUSEnglish,
			SupportedEngines: []string{lng, gen},
		},
	}
}

func builtInVoicesEnglishGlobal() []Voice {
	std := defaultEngine
	neu := engineNeural
	gen := engineGenerative

	return []Voice{
		// English (AU)
		{
			ID: "Nicole", Name: "Nicole", Gender: genderFemale,
			LanguageCode: lcAustrEnglish, LanguageName: langAustrEnglish,
			SupportedEngines: []string{std},
		},
		{
			ID: "Olivia", Name: "Olivia", Gender: genderFemale,
			LanguageCode: lcAustrEnglish, LanguageName: langAustrEnglish,
			SupportedEngines: []string{neu},
		},
		{
			ID: "Russell", Name: "Russell", Gender: genderMale,
			LanguageCode: lcAustrEnglish, LanguageName: langAustrEnglish,
			SupportedEngines: []string{std},
		},
		// English (GB)
		{
			ID: "Amy", Name: "Amy", Gender: genderFemale,
			LanguageCode: lcBritEnglish, LanguageName: langBritEnglish,
			SupportedEngines: []string{std, neu, gen},
		},
		{
			ID: "Emma", Name: "Emma", Gender: genderFemale,
			LanguageCode: lcBritEnglish, LanguageName: langBritEnglish,
			SupportedEngines: []string{std, neu},
		},
		{
			ID: "Brian", Name: "Brian", Gender: genderMale,
			LanguageCode: lcBritEnglish, LanguageName: langBritEnglish,
			SupportedEngines: []string{std, neu},
		},
		{
			ID: "Arthur", Name: "Arthur", Gender: genderMale,
			LanguageCode: lcBritEnglish, LanguageName: langBritEnglish,
			SupportedEngines: []string{neu},
		},
		// English (IN)
		{
			ID: "Aditi", Name: "Aditi", Gender: genderFemale,
			LanguageCode: lcIndianEnglish, LanguageName: langIndianEnglish,
			AdditionalLanguageCodes: []string{lcHindi},
			SupportedEngines:        []string{std},
		},
		{
			ID: "Kajal", Name: "Kajal", Gender: genderFemale,
			LanguageCode: lcIndianEnglish, LanguageName: langIndianEnglish,
			AdditionalLanguageCodes: []string{lcHindi},
			SupportedEngines:        []string{neu},
		},
		{
			ID: "Raveena", Name: "Raveena", Gender: genderFemale,
			LanguageCode: lcIndianEnglish, LanguageName: langIndianEnglish,
			SupportedEngines: []string{std},
		},
		// English (IE)
		{
			ID: "Niamh", Name: "Niamh", Gender: genderFemale,
			LanguageCode: lcIrEnglish, LanguageName: langIrEnglish,
			SupportedEngines: []string{neu},
		},
		// English (NZ)
		{
			ID: "Aria", Name: "Aria", Gender: genderFemale,
			LanguageCode: lcNZEnglish, LanguageName: langNZEnglish,
			SupportedEngines: []string{neu},
		},
		// English (ZA)
		{
			ID: "Ayanda", Name: "Ayanda", Gender: genderFemale,
			LanguageCode: lcZAEnglish, LanguageName: langZAEnglish,
			SupportedEngines: []string{neu},
		},
		// Welsh
		{
			ID: "Gwyneth", Name: "Gwyneth", Gender: genderFemale,
			LanguageCode: lcWelsh, LanguageName: "Welsh",
			SupportedEngines: []string{std},
		},
	}
}

func builtInVoicesGermanic() []Voice {
	std := defaultEngine
	neu := engineNeural

	return []Voice{
		// Danish
		{
			ID: "Naja", Name: "Naja", Gender: genderFemale,
			LanguageCode: lcDanish, LanguageName: langDanish,
			SupportedEngines: []string{std},
		},
		{
			ID: "Mads", Name: "Mads", Gender: genderMale,
			LanguageCode: lcDanish, LanguageName: langDanish,
			SupportedEngines: []string{std},
		},
		{
			ID: "Sofie", Name: "Sofie", Gender: genderFemale,
			LanguageCode: lcDanish, LanguageName: langDanish,
			SupportedEngines: []string{neu},
		},
		// Dutch (NL)
		{
			ID: "Lotte", Name: "Lotte", Gender: genderFemale,
			LanguageCode: lcDutch, LanguageName: langDutch,
			SupportedEngines: []string{std, neu},
		},
		{
			ID: "Ruben", Name: "Ruben", Gender: genderMale,
			LanguageCode: lcDutch, LanguageName: langDutch,
			SupportedEngines: []string{std},
		},
		{
			ID: "Laura", Name: "Laura", Gender: genderFemale,
			LanguageCode: lcDutch, LanguageName: langDutch,
			SupportedEngines: []string{neu},
		},
		// Dutch (BE)
		{
			ID: "Lisa", Name: "Lisa", Gender: genderFemale,
			LanguageCode: lcBelgDutch, LanguageName: langBelgDutch,
			SupportedEngines: []string{neu},
		},
		// Finnish
		{
			ID: "Suvi", Name: "Suvi", Gender: genderFemale,
			LanguageCode: lcFinnish, LanguageName: "Finnish",
			SupportedEngines: []string{neu},
		},
		// German (AT)
		{
			ID: "Hannah", Name: "Hannah", Gender: genderFemale,
			LanguageCode: lcAustrGerman, LanguageName: "Austrian German",
			SupportedEngines: []string{neu},
		},
		// German (DE)
		{
			ID: "Marlene", Name: "Marlene", Gender: genderFemale,
			LanguageCode: lcGerman, LanguageName: langGerman,
			SupportedEngines: []string{std},
		},
		{
			ID: "Vicki", Name: "Vicki", Gender: genderFemale,
			LanguageCode: lcGerman, LanguageName: langGerman,
			SupportedEngines: []string{std, neu},
		},
		{
			ID: "Hans", Name: "Hans", Gender: genderMale,
			LanguageCode: lcGerman, LanguageName: langGerman,
			SupportedEngines: []string{std},
		},
		{
			ID: "Daniel", Name: "Daniel", Gender: genderMale,
			LanguageCode: lcGerman, LanguageName: langGerman,
			SupportedEngines: []string{neu},
		},
		// Icelandic
		{
			ID: "Dora", Name: "Dóra", Gender: genderFemale,
			LanguageCode: lcIcelandic, LanguageName: "Icelandic",
			SupportedEngines: []string{std},
		},
		{
			ID: "Karl", Name: "Karl", Gender: genderMale,
			LanguageCode: lcIcelandic, LanguageName: "Icelandic",
			SupportedEngines: []string{std},
		},
		// Norwegian
		{
			ID: "Liv", Name: "Liv", Gender: genderFemale,
			LanguageCode: lcNorwegian, LanguageName: langNorwegian,
			SupportedEngines: []string{std},
		},
		{
			ID: "Ida", Name: "Ida", Gender: genderFemale,
			LanguageCode: lcNorwegian, LanguageName: langNorwegian,
			SupportedEngines: []string{neu},
		},
		// Swedish
		{
			ID: "Astrid", Name: "Astrid", Gender: genderFemale,
			LanguageCode: lcSwedish, LanguageName: langSwedish,
			SupportedEngines: []string{std},
		},
		{
			ID: "Elin", Name: "Elin", Gender: genderFemale,
			LanguageCode: lcSwedish, LanguageName: langSwedish,
			SupportedEngines: []string{neu},
		},
	}
}

func builtInVoicesFrenchPortuguese() []Voice {
	std := defaultEngine
	neu := engineNeural

	return []Voice{
		// French (BE)
		{
			ID: "Isabelle", Name: "Isabelle", Gender: genderFemale,
			LanguageCode: lcBelgFrench, LanguageName: langBelgFrench,
			SupportedEngines: []string{neu},
		},
		// French (CA)
		{
			ID: "Chantal", Name: "Chantal", Gender: genderFemale,
			LanguageCode: lcCanadFrench, LanguageName: langCanadFrench,
			SupportedEngines: []string{std},
		},
		{
			ID: "Gabrielle", Name: "Gabrielle", Gender: genderFemale,
			LanguageCode: lcCanadFrench, LanguageName: langCanadFrench,
			SupportedEngines: []string{neu},
		},
		{
			ID: "Liam", Name: "Liam", Gender: genderMale,
			LanguageCode: lcCanadFrench, LanguageName: langCanadFrench,
			SupportedEngines: []string{neu},
		},
		// French (FR)
		{
			ID: "Celine", Name: "Céline", Gender: genderFemale,
			LanguageCode: lcFrench, LanguageName: langFrench,
			SupportedEngines: []string{std},
		},
		{
			ID: "Lea", Name: "Léa", Gender: genderFemale,
			LanguageCode: lcFrench, LanguageName: langFrench,
			SupportedEngines: []string{std, neu},
		},
		{
			ID: "Mathieu", Name: "Mathieu", Gender: genderMale,
			LanguageCode: lcFrench, LanguageName: langFrench,
			SupportedEngines: []string{std},
		},
		{
			ID: "Remi", Name: "Rémi", Gender: genderMale,
			LanguageCode: lcFrench, LanguageName: langFrench,
			SupportedEngines: []string{neu},
		},
		// Portuguese (BR)
		{
			ID: "Camila", Name: "Camila", Gender: genderFemale,
			LanguageCode: lcBrazPortuguese, LanguageName: langBrazPortuguese,
			SupportedEngines: []string{std, neu},
		},
		{
			ID: "Vitoria", Name: "Vitória", Gender: genderFemale,
			LanguageCode: lcBrazPortuguese, LanguageName: langBrazPortuguese,
			SupportedEngines: []string{std, neu},
		},
		{
			ID: "Ricardo", Name: "Ricardo", Gender: genderMale,
			LanguageCode: lcBrazPortuguese, LanguageName: langBrazPortuguese,
			SupportedEngines: []string{std},
		},
		{
			ID: "Thiago", Name: "Thiago", Gender: genderMale,
			LanguageCode: lcBrazPortuguese, LanguageName: langBrazPortuguese,
			SupportedEngines: []string{neu},
		},
		// Portuguese (PT)
		{
			ID: "Ines", Name: "Inês", Gender: genderFemale,
			LanguageCode: lcEuropPortuguese, LanguageName: langEuropPortuguese,
			SupportedEngines: []string{std, neu},
		},
		{
			ID: "Cristiano", Name: "Cristiano", Gender: genderMale,
			LanguageCode: lcEuropPortuguese, LanguageName: langEuropPortuguese,
			SupportedEngines: []string{std},
		},
		// Romanian
		{
			ID: "Carmen", Name: "Carmen", Gender: genderFemale,
			LanguageCode: lcRomanian, LanguageName: "Romanian",
			SupportedEngines: []string{std},
		},
	}
}

func builtInVoicesSpanishItalian() []Voice {
	std := defaultEngine
	neu := engineNeural

	return []Voice{
		// Italian
		{
			ID: "Carla", Name: "Carla", Gender: genderFemale,
			LanguageCode: lcItalian, LanguageName: langItalian,
			SupportedEngines: []string{std},
		},
		{
			ID: "Bianca", Name: "Bianca", Gender: genderFemale,
			LanguageCode: lcItalian, LanguageName: langItalian,
			SupportedEngines: []string{std, neu},
		},
		{
			ID: "Giorgio", Name: "Giorgio", Gender: genderMale,
			LanguageCode: lcItalian, LanguageName: langItalian,
			SupportedEngines: []string{std},
		},
		{
			ID: "Adriano", Name: "Adriano", Gender: genderMale,
			LanguageCode: lcItalian, LanguageName: langItalian,
			SupportedEngines: []string{neu},
		},
		// Spanish (ES)
		{
			ID: "Conchita", Name: "Conchita", Gender: genderFemale,
			LanguageCode: lcCastilSpanish, LanguageName: langCastilSpanish,
			SupportedEngines: []string{std},
		},
		{
			ID: "Lucia", Name: "Lucia", Gender: genderFemale,
			LanguageCode: lcCastilSpanish, LanguageName: langCastilSpanish,
			SupportedEngines: []string{std, neu},
		},
		{
			ID: "Enrique", Name: "Enrique", Gender: genderMale,
			LanguageCode: lcCastilSpanish, LanguageName: langCastilSpanish,
			SupportedEngines: []string{std},
		},
		{
			ID: "Sergio", Name: "Sergio", Gender: genderMale,
			LanguageCode: lcCastilSpanish, LanguageName: langCastilSpanish,
			SupportedEngines: []string{neu},
		},
		// Spanish (MX)
		{
			ID: "Mia", Name: "Mia", Gender: genderFemale,
			LanguageCode: lcMexSpanish, LanguageName: langMexSpanish,
			SupportedEngines: []string{std, neu},
		},
		// Spanish (US)
		{
			ID: "Lupe", Name: "Lupe", Gender: genderFemale,
			LanguageCode: lcUSSpanish, LanguageName: langUSSpanish,
			SupportedEngines: []string{std, neu},
		},
		{
			ID: "Penelope", Name: "Penélope", Gender: genderFemale,
			LanguageCode: lcUSSpanish, LanguageName: langUSSpanish,
			SupportedEngines: []string{std},
		},
		{
			ID: "Miguel", Name: "Miguel", Gender: genderMale,
			LanguageCode: lcUSSpanish, LanguageName: langUSSpanish,
			SupportedEngines: []string{std},
		},
		{
			ID: "Pedro", Name: "Pedro", Gender: genderMale,
			LanguageCode: lcUSSpanish, LanguageName: langUSSpanish,
			SupportedEngines: []string{neu},
		},
	}
}

func builtInVoicesOther() []Voice {
	std := defaultEngine
	neu := engineNeural

	return []Voice{
		// Catalan
		{
			ID: "Arlet", Name: "Arlet", Gender: genderFemale,
			LanguageCode: lcCatalan, LanguageName: "Catalan",
			SupportedEngines: []string{neu},
		},
		// Chinese Mandarin
		{
			ID: "Zhiyu", Name: "Zhiyu", Gender: genderFemale,
			LanguageCode: lcChinaMandarin, LanguageName: langChinaMandarin,
			SupportedEngines: []string{std, neu},
		},
		// Japanese
		{
			ID: "Mizuki", Name: "Mizuki", Gender: genderFemale,
			LanguageCode: lcJapanese, LanguageName: langJapanese,
			SupportedEngines: []string{std},
		},
		{
			ID: "Kazuha", Name: "Kazuha", Gender: genderFemale,
			LanguageCode: lcJapanese, LanguageName: langJapanese,
			SupportedEngines: []string{neu},
		},
		{
			ID: "Takumi", Name: "Takumi", Gender: genderMale,
			LanguageCode: lcJapanese, LanguageName: langJapanese,
			SupportedEngines: []string{std, neu},
		},
		// Korean
		{
			ID: "Seoyeon", Name: "Seoyeon", Gender: genderFemale,
			LanguageCode: lcKorean, LanguageName: langKorean,
			SupportedEngines: []string{std, neu},
		},
		// Polish
		{
			ID: "Ewa", Name: "Ewa", Gender: genderFemale,
			LanguageCode: lcPolish, LanguageName: langPolish,
			SupportedEngines: []string{std},
		},
		{
			ID: "Maja", Name: "Maja", Gender: genderFemale,
			LanguageCode: lcPolish, LanguageName: langPolish,
			SupportedEngines: []string{std},
		},
		{
			ID: "Ola", Name: "Ola", Gender: genderFemale,
			LanguageCode: lcPolish, LanguageName: langPolish,
			SupportedEngines: []string{neu},
		},
		{
			ID: "Jacek", Name: "Jacek", Gender: genderMale,
			LanguageCode: lcPolish, LanguageName: langPolish,
			SupportedEngines: []string{std},
		},
		{
			ID: "Jan", Name: "Jan", Gender: genderMale,
			LanguageCode: lcPolish, LanguageName: langPolish,
			SupportedEngines: []string{std},
		},
		// Russian
		{
			ID: "Tatyana", Name: "Tatyana", Gender: genderFemale,
			LanguageCode: lcRussian, LanguageName: langRussian,
			SupportedEngines: []string{std},
		},
		{
			ID: "Maxim", Name: "Maxim", Gender: genderMale,
			LanguageCode: lcRussian, LanguageName: langRussian,
			SupportedEngines: []string{std},
		},
		// Turkish
		{
			ID: "Filiz", Name: "Filiz", Gender: genderFemale,
			LanguageCode: lcTurkish, LanguageName: langTurkish,
			SupportedEngines: []string{std},
		},
		{
			ID: "Burcu", Name: "Burcu", Gender: genderFemale,
			LanguageCode: lcTurkish, LanguageName: langTurkish,
			SupportedEngines: []string{neu},
		},
	}
}

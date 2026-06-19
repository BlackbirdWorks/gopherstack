package polly

import (
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
	"github.com/blackbirdworks/gopherstack/pkgs/config"
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
	outputFormatPCM      = "pcm"
	outputFormatJSON     = "json"
	textTypeText         = "text"
	textTypeSSML         = "ssml"
	genderFemale         = "Female"
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
)

var (
	// ErrLexiconNotFound is returned when a requested lexicon is absent.
	ErrLexiconNotFound = errors.New("LexiconNotFoundException")
	// ErrTaskNotFound is returned when a requested synthesis task is absent.
	ErrTaskNotFound = errors.New("SynthesisTaskNotFoundException")
	// ErrValidation is returned when request parameters do not meet Polly constraints.
	ErrValidation = errors.New("InvalidParameterValueException")
	// ErrResourceNotFound is returned when a tagged resource ARN is unknown.
	ErrResourceNotFound = errors.New("ResourceNotFoundException")
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
type InMemoryBackend struct {
	lexicons  map[string]*Lexicon
	tasks     map[string]*SpeechSynthesisTask
	tags      map[string]map[string]string
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
	return &InMemoryBackend{
		lexicons:  make(map[string]*Lexicon),
		tasks:     make(map[string]*SpeechSynthesisTask),
		tags:      make(map[string]map[string]string),
		voices:    builtInVoices(),
		accountID: accountID,
		region:    region,
	}
}

// Region returns configured AWS region.
func (b *InMemoryBackend) Region() string { return b.region }

// Reset clears stored resources.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.lexicons = make(map[string]*Lexicon)
	b.tasks = make(map[string]*SpeechSynthesisTask)
	b.tags = make(map[string]map[string]string)
}

// PutLexicon creates or replaces lexicon content.
func (b *InMemoryBackend) PutLexicon(name, content string) error {
	if err := validateLexicon(name, content); err != nil {
		return err
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	b.lexicons[name] = &Lexicon{
		Name:         name,
		ARN:          arn.Build("polly", b.region, b.accountID, "lexicon/"+name),
		Content:      content,
		Alphabet:     lexiconAttribute(content, "alphabet", "ipa"),
		LanguageCode: lexiconAttribute(content, "xml:lang", defaultLanguageCode),
		LexemesCount: strings.Count(content, "<lexeme>"),
		Size:         len(content),
		LastModified: time.Now().UTC(),
	}

	return nil
}

// GetLexicon returns named lexicon.
func (b *InMemoryBackend) GetLexicon(name string) (*Lexicon, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	lexicon, ok := b.lexicons[name]
	if !ok {
		return nil, fmt.Errorf("%w: lexicon %q", ErrLexiconNotFound, name)
	}

	return cloneLexicon(lexicon), nil
}

// DeleteLexicon removes named lexicon.
func (b *InMemoryBackend) DeleteLexicon(name string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, ok := b.lexicons[name]; !ok {
		return fmt.Errorf("%w: lexicon %q", ErrLexiconNotFound, name)
	}

	delete(b.lexicons, name)

	return nil
}

// ListLexicons lists lexicons ordered by name.
func (b *InMemoryBackend) ListLexicons() []*Lexicon {
	b.mu.RLock()
	defer b.mu.RUnlock()

	out := make([]*Lexicon, 0, len(b.lexicons))
	for _, lexicon := range b.lexicons {
		out = append(out, cloneLexicon(lexicon))
	}

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
		return nil, fmt.Errorf("%w: text exceeds maximum length of %d characters", ErrValidation, limit)
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
		return nil, fmt.Errorf("%w: text exceeds maximum length of %d characters", ErrValidation, maxTaskTextLen)
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
	b.tasks[id] = task
	b.tags[b.taskARN(id)] = make(map[string]string)
	b.mu.Unlock()

	return cloneTask(task), nil
}

// GetSpeechSynthesisTask retrieves task and advances simulated lifecycle.
func (b *InMemoryBackend) GetSpeechSynthesisTask(taskID string) (*SpeechSynthesisTask, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	task, ok := b.tasks[taskID]
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

	keys := make([]string, 0, len(b.tasks))
	for key := range b.tasks {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	offset, err := parseToken(token, len(keys))
	if err != nil {
		return nil, "", err
	}
	if maxResults <= 0 || maxResults > maxTaskPageSize {
		maxResults = maxTaskPageSize
	}

	out := make([]*SpeechSynthesisTask, 0, len(keys))
	for _, key := range keys[offset:] {
		task := b.tasks[key]
		advanceTask(task)
		if status == "" || task.TaskStatus == status {
			out = append(out, cloneTask(task))
		}
		if len(out) == maxResults {
			return out, strconv.Itoa(offset + len(out)), nil
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

	keys := make([]string, 0, len(current))
	for key := range current {
		keys = append(keys, key)
	}
	sort.Strings(keys)

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
			ErrValidation,
			options.SampleRate,
			options.OutputFormat,
		)
	}
	if err := validateSpeechMarks(options); err != nil {
		return options, err
	}

	b.mu.RLock()
	defer b.mu.RUnlock()

	if !b.voiceSupports(options.VoiceID, options.Engine, options.LanguageCode) {
		return options, fmt.Errorf(
			"%w: voice %q does not support requested language/engine",
			ErrValidation,
			options.VoiceID,
		)
	}
	for _, name := range options.LexiconNames {
		if _, ok := b.lexicons[name]; !ok {
			return options, fmt.Errorf("%w: lexicon %q", ErrLexiconNotFound, name)
		}
	}

	return options, nil
}

func (b *InMemoryBackend) voiceSupports(id, engine, languageCode string) bool {
	for _, voice := range b.voices {
		if voice.ID != id || !slices.Contains(voice.SupportedEngines, engine) {
			continue
		}
		if languageCode == "" || voice.LanguageCode == languageCode ||
			slices.Contains(voice.AdditionalLanguageCodes, languageCode) {
			return true
		}
	}

	return false
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
		return fmt.Errorf("%w: Content must be PLS lexicon XML", ErrValidation)
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
	}
	if len(options.SpeechMarkTypes) > 0 && options.OutputFormat != outputFormatJSON {
		return fmt.Errorf("%w: speech marks require json OutputFormat", ErrValidation)
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
		outputFormatMP3:  {"8000", "16000", "22050", "24000", "44100", "48000"},
		outputFormatOGG:  {"8000", "16000", "22050", "24000", "44100", "48000"},
		outputFormatPCM:  {"8000", "16000"},
		outputFormatJSON: {"8000", "16000", "22050", "24000"},
	}

	return slices.Contains(rates[format], rate)
}

func contentTypeForFormat(format string) string {
	contentTypes := map[string]string{
		outputFormatMP3: "audio/mpeg",
		outputFormatOGG: "audio/ogg",
		outputFormatPCM: "audio/pcm",
	}

	return contentTypes[format]
}

func taskExtension(format string) string {
	if format == outputFormatOGG {
		return "ogg"
	}

	return format
}

func speechMarks(options SynthesisOptions) []byte {
	lines := make([]string, 0, len(options.SpeechMarkTypes))
	offset := 0
	for _, word := range strings.Fields(options.Text) {
		start := strings.Index(options.Text[offset:], word) + offset
		end := start + len(word)
		timeMs := offset * 80 // ~80ms per character as rough timing
		for _, mark := range options.SpeechMarkTypes {
			switch mark {
			case "word":
				lines = append(lines, fmt.Sprintf(`{"time":%d,"type":"word","start":%d,"end":%d,"value":%q}`,
					timeMs, start, end, word))
			case "sentence":
				lines = append(lines, fmt.Sprintf(`{"time":0,"type":"sentence","start":0,"end":%d,"value":%q}`,
					len(options.Text), options.Text))
			case textTypeSSML:
				lines = append(lines, fmt.Sprintf(`{"time":0,"type":"ssml","start":0,"end":%d,"value":"<speak>"}`,
					len(options.Text)))
			case "viseme":
				lines = append(lines, fmt.Sprintf(`{"time":%d,"type":"viseme","value":"p"}`, timeMs))
			}
		}
		offset = end
	}
	if len(lines) == 0 {
		for _, mark := range options.SpeechMarkTypes {
			lines = append(lines, fmt.Sprintf(`{"time":0,"type":"%s","start":0,"end":%d,"value":%q}`,
				mark, len(options.Text), options.Text))
		}
	}

	return []byte(strings.Join(lines, "\n") + "\n")
}

// syntheticAudioBytes returns minimal but format-correct audio bytes for the given output format.
// PCM → RIFF/WAV container with one silent frame.
// MP3 → minimal MPEG-1 Layer 3 sync frame header.
// OGG → OGG capture pattern + minimal Vorbis identification.
func syntheticAudioBytes(opts SynthesisOptions) []byte {
	switch opts.OutputFormat {
	case outputFormatPCM:
		return minimalWAV(opts.SampleRate)
	case outputFormatMP3:
		return minimalMP3Frame()
	case outputFormatOGG:
		return minimalOGG()
	default:
		return minimalWAV(opts.SampleRate)
	}
}

// minimalWAV returns a 46-byte RIFF/WAV file with two silent PCM samples.
func minimalWAV(sampleRateStr string) []byte {
	sampleRate := uint32(22050)
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
	const bitsPerSample = uint16(16)
	const dataLen = uint32(4) // 2 silent 16-bit samples
	byteRate := sampleRate * uint32(numChannels) * uint32(bitsPerSample) / 8
	blockAlign := numChannels * bitsPerSample / 8
	fileSize := 36 + dataLen

	buf := make([]byte, 0, 44+dataLen)
	buf = append(buf, 'R', 'I', 'F', 'F')
	buf = binary.LittleEndian.AppendUint32(buf, fileSize)
	buf = append(buf, 'W', 'A', 'V', 'E')
	buf = append(buf, 'f', 'm', 't', ' ')
	buf = binary.LittleEndian.AppendUint32(buf, 16) // PCM chunk size
	buf = binary.LittleEndian.AppendUint16(buf, 1)  // PCM format
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
	frame := make([]byte, 4+413) // header + silence
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

func parseToken(token string, total int) (int, error) {
	if token == "" {
		return 0, nil
	}
	var offset int
	if _, err := fmt.Sscanf(token, "%d", &offset); err != nil || offset < 0 || offset > total {
		return 0, fmt.Errorf("%w: invalid NextToken", ErrValidation)
	}

	return offset, nil
}

func validEngines() []string {
	return []string{defaultEngine, engineNeural, engineLongForm, engineGenerative}
}

func validOutputFormats() []string {
	return []string{outputFormatMP3, outputFormatOGG, outputFormatPCM, outputFormatJSON}
}

func validTextTypes() []string { return []string{textTypeText, textTypeSSML} }

func validSpeechMarkTypes() []string { return []string{"sentence", textTypeSSML, "viseme", "word"} }

func validTaskStatuses() []string {
	return []string{taskStatusScheduled, taskStatusProgress, taskStatusCompleted, taskStatusFailed}
}

func builtInVoices() []Voice {
	return []Voice{
		{
			ID: "Joanna", Name: "Joanna", Gender: genderFemale, LanguageCode: "en-US",
			LanguageName:     "US English",
			SupportedEngines: []string{defaultEngine, engineNeural, engineLongForm, engineGenerative},
		},
		{
			ID: "Matthew", Name: "Matthew", Gender: "Male", LanguageCode: "en-US",
			LanguageName:     "US English",
			SupportedEngines: []string{defaultEngine, engineNeural, engineLongForm, engineGenerative},
		},
		{
			ID: "Aditi", Name: "Aditi", Gender: genderFemale, LanguageCode: "en-IN",
			LanguageName: "Indian English", AdditionalLanguageCodes: []string{"hi-IN"},
			SupportedEngines: []string{defaultEngine},
		},
		{
			ID: "Amy", Name: "Amy", Gender: genderFemale, LanguageCode: "en-GB",
			LanguageName: "British English", SupportedEngines: []string{defaultEngine, engineNeural},
		},
	}
}

package polly

import (
	"errors"
	"fmt"
	"maps"
	"slices"
	"sort"
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
	taskStatusScheduled  = "scheduled"
	taskStatusProgress   = "inProgress"
	taskStatusCompleted  = "completed"
	taskStatusFailed     = "failed"
	failedTaskMarker     = "[fail]"
	maxTaskPageSize      = 100
)

var (
	// ErrNotFound is returned when a requested Polly resource is absent.
	ErrNotFound = errors.New("NotFoundException")
	// ErrValidation is returned when request parameters do not meet Polly constraints.
	ErrValidation = errors.New("InvalidParameterValueException")
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
	LexiconNames    []string
	OutputFormat    string
	SampleRate      string
	SpeechMarkTypes []string
	Text            string
	TextType        string
	VoiceID         string
}

// SpeechSynthesisTask represents an asynchronous synthesis task.
type SpeechSynthesisTask struct {
	CreationTime       time.Time
	Options            SynthesisOptions
	TaskID             string
	TaskStatus         string
	TaskStatusReason   string
	OutputURI          string
	OutputS3BucketName string
	SNSRoleArn         string
	SNSTopicArn        string
	polls              int
}

// SynthesizedSpeech is deterministic output from SynthesizeSpeech.
type SynthesizedSpeech struct {
	Data              []byte
	ContentType       string
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
	voices    []Voice
	accountID string
	region    string
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
		return nil, fmt.Errorf("%w: lexicon %q", ErrNotFound, name)
	}

	return cloneLexicon(lexicon), nil
}

// DeleteLexicon removes named lexicon.
func (b *InMemoryBackend) DeleteLexicon(name string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, ok := b.lexicons[name]; !ok {
		return fmt.Errorf("%w: lexicon %q", ErrNotFound, name)
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
	normal, err := b.validateOptions(options)
	if err != nil {
		return nil, err
	}

	if normal.OutputFormat == "json" {
		return &SynthesizedSpeech{
			Data:              speechMarks(normal),
			ContentType:       "application/x-json-stream",
			RequestCharacters: len(normal.Text),
		}, nil
	}

	data := []byte(
		fmt.Sprintf("POLLY:%s:%s:%s:%s", normal.OutputFormat, normal.SampleRate, normal.VoiceID, normal.Text),
	)

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
		return nil, fmt.Errorf("%w: task %q", ErrNotFound, taskID)
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
			return out, fmt.Sprintf("%d", offset+len(out)), nil
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
		return fmt.Errorf("%w: resource %q", ErrNotFound, resourceArn)
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
		return fmt.Errorf("%w: resource %q", ErrNotFound, resourceArn)
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
		return nil, fmt.Errorf("%w: resource %q", ErrNotFound, resourceArn)
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
			return options, fmt.Errorf("%w: lexicon %q", ErrNotFound, name)
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
	if name == "" || strings.ContainsAny(name, " /") {
		return fmt.Errorf("%w: invalid lexicon name", ErrValidation)
	}
	if content == "" || !strings.Contains(content, "<lexicon") {
		return fmt.Errorf("%w: Content must be PLS lexicon XML", ErrValidation)
	}

	return nil
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
		options.TextType = "text"
	}
	if options.OutputFormat == "" {
		options.OutputFormat = "mp3"
	}
	if options.SampleRate == "" {
		if options.OutputFormat == "pcm" {
			options.SampleRate = defaultSampleRatePCM
		} else if options.Engine != defaultEngine {
			options.SampleRate = "24000"
		} else {
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
	if len(options.SpeechMarkTypes) > 0 && options.OutputFormat != "json" {
		return fmt.Errorf("%w: speech marks require json OutputFormat", ErrValidation)
	}
	if len(options.SpeechMarkTypes) == 0 && options.OutputFormat == "json" {
		return fmt.Errorf("%w: json OutputFormat requires SpeechMarkTypes", ErrValidation)
	}

	return nil
}

func validateTags(tags []Tag) error {
	seen := make(map[string]bool, len(tags))
	for _, tag := range tags {
		if tag.Key == "" || seen[tag.Key] {
			return fmt.Errorf("%w: tag keys must be non-empty and unique", ErrValidation)
		}
		seen[tag.Key] = true
	}

	return nil
}

func validSampleRate(format, rate string) bool {
	rates := map[string][]string{
		"mp3":        {"8000", "16000", "22050", "24000", "44100", "48000"},
		"ogg_vorbis": {"8000", "16000", "22050", "24000", "44100", "48000"},
		"pcm":        {"8000", "16000"},
		"json":       {"8000", "16000", "22050", "24000"},
	}

	return slices.Contains(rates[format], rate)
}

func contentTypeForFormat(format string) string {
	contentTypes := map[string]string{
		"mp3":        "audio/mpeg",
		"ogg_vorbis": "audio/ogg",
		"pcm":        "audio/pcm",
	}

	return contentTypes[format]
}

func taskExtension(format string) string {
	if format == "ogg_vorbis" {
		return "ogg"
	}

	return format
}

func speechMarks(options SynthesisOptions) []byte {
	lines := make([]string, 0, len(options.SpeechMarkTypes))
	for _, mark := range options.SpeechMarkTypes {
		lines = append(lines, fmt.Sprintf(`{"time":0,"type":"%s","start":0,"end":%d,"value":%q}`,
			mark, len(options.Text), options.Text))
	}

	return []byte(strings.Join(lines, "\n") + "\n")
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

func validEngines() []string { return []string{"standard", "neural", "long-form", "generative"} }

func validOutputFormats() []string { return []string{"mp3", "ogg_vorbis", "pcm", "json"} }

func validTextTypes() []string { return []string{"text", "ssml"} }

func validSpeechMarkTypes() []string { return []string{"sentence", "ssml", "viseme", "word"} }

func validTaskStatuses() []string {
	return []string{taskStatusScheduled, taskStatusProgress, taskStatusCompleted, taskStatusFailed}
}

func builtInVoices() []Voice {
	return []Voice{
		{
			ID: "Joanna", Name: "Joanna", Gender: "Female", LanguageCode: "en-US",
			LanguageName: "US English", SupportedEngines: []string{"standard", "neural", "long-form", "generative"},
		},
		{
			ID: "Matthew", Name: "Matthew", Gender: "Male", LanguageCode: "en-US",
			LanguageName: "US English", SupportedEngines: []string{"standard", "neural", "long-form", "generative"},
		},
		{
			ID: "Aditi", Name: "Aditi", Gender: "Female", LanguageCode: "en-IN",
			LanguageName: "Indian English", AdditionalLanguageCodes: []string{"hi-IN"},
			SupportedEngines: []string{"standard"},
		},
		{
			ID: "Amy", Name: "Amy", Gender: "Female", LanguageCode: "en-GB",
			LanguageName: "British English", SupportedEngines: []string{"standard", "neural"},
		},
	}
}

func tagsMap(tags []Tag) map[string]string {
	out := make(map[string]string, len(tags))
	for _, tag := range tags {
		out[tag.Key] = tag.Value
	}

	return out
}

func copyTags(tags map[string]string) map[string]string {
	out := make(map[string]string, len(tags))
	maps.Copy(out, tags)

	return out
}

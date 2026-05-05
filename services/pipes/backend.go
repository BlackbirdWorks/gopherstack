package pipes

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"maps"
	"regexp"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

const (
	stateRunning      = "RUNNING"
	stateStopped      = "STOPPED"
	stateStarting     = "STARTING"
	stateStopping     = "STOPPING"
	stateCreateFailed = "CREATE_FAILED"
	stateUpdateFailed = "UPDATE_FAILED"
	stateDeleteFailed = "DELETE_FAILED"

	maxPipeNameLen  = 64
	maxTagKeyLen    = 128
	maxTagValueLen  = 256
	maxTagsPerPipe  = 50
	maxPipesPerAcct = 1000

	// nextTokenSep separates cursor values in pagination tokens.
	nextTokenSep = "\x00"
)

var (
	ErrNotFound      = awserr.New("NotFoundException", awserr.ErrNotFound)
	ErrAlreadyExists = awserr.New("ConflictException", awserr.ErrConflict)
	ErrValidation    = awserr.New("ValidationException", awserr.ErrInvalidParameter)

	pipeNameRE = regexp.MustCompile(`^[a-zA-Z0-9_-]+$`)
)

// FilterCriteria holds event filter patterns applied before forwarding to the target.
type FilterCriteria struct {
	Filters []Filter `json:"Filters,omitempty"`
}

// Filter is a single JSON-pattern filter.
type Filter struct {
	Pattern string `json:"Pattern,omitempty"`
}

// SQSSourceParameters holds SQS-specific source configuration.
type SQSSourceParameters struct {
	BatchSize                      int `json:"BatchSize,omitempty"`
	MaximumBatchingWindowInSeconds int `json:"MaximumBatchingWindowInSeconds,omitempty"`
}

// SourceParameters holds source-specific configuration.
type SourceParameters struct {
	FilterCriteria     *FilterCriteria      `json:"FilterCriteria,omitempty"`
	SqsQueueParameters *SQSSourceParameters `json:"SqsQueueParameters,omitempty"`
}

// LambdaFunctionParameters holds Lambda-specific target configuration.
type LambdaFunctionParameters struct {
	InvocationType string `json:"InvocationType,omitempty"`
}

// TargetParameters holds target-specific configuration.
type TargetParameters struct {
	LambdaFunctionParameters *LambdaFunctionParameters `json:"LambdaFunctionParameters,omitempty"`
	InputTemplate            string                    `json:"InputTemplate,omitempty"`
}

// DeadLetterConfig identifies the DLQ for failed pipe events.
type DeadLetterConfig struct {
	Arn string `json:"Arn,omitempty"`
}

// CloudwatchLogsLogDestination is a CloudWatch Logs target.
type CloudwatchLogsLogDestination struct {
	LogGroupArn string `json:"LogGroupArn,omitempty"`
}

// LogDestination wraps possible log destination types.
type LogDestination struct {
	CloudwatchLogsLogDestination *CloudwatchLogsLogDestination `json:"CloudwatchLogsLogDestination,omitempty"`
}

// LogConfiguration controls pipe execution logging.
type LogConfiguration struct {
	Level                string           `json:"Level,omitempty"`
	Destinations         []LogDestination `json:"Destinations,omitempty"`
	IncludeExecutionData []string         `json:"IncludeExecutionData,omitempty"`
}

// Pipe represents an EventBridge Pipe.
type Pipe struct {
	SourceParameters *SourceParameters `json:"sourceParameters,omitempty"`
	TargetParameters *TargetParameters `json:"targetParameters,omitempty"`
	DeadLetterConfig *DeadLetterConfig `json:"deadLetterConfig,omitempty"`
	LogConfiguration *LogConfiguration `json:"logConfiguration,omitempty"`
	LastModifiedTime time.Time         `json:"lastModifiedTime"`
	CreationTime     time.Time         `json:"creationTime"`
	Tags             map[string]string `json:"tags,omitempty"`
	Description      string            `json:"description,omitempty"`
	Enrichment       string            `json:"enrichment,omitempty"`
	Source           string            `json:"source"`
	Target           string            `json:"target"`
	RoleARN          string            `json:"roleArn"`
	StateReason      string            `json:"stateReason,omitempty"`
	DesiredState     string            `json:"desiredState"`
	CurrentState     string            `json:"currentState"`
	AccountID        string            `json:"accountID"`
	Region           string            `json:"region"`
	ARN              string            `json:"arn"`
	Name             string            `json:"name"`
}

func (p *Pipe) effectiveBatchSize() int {
	if p.SourceParameters != nil &&
		p.SourceParameters.SqsQueueParameters != nil &&
		p.SourceParameters.SqsQueueParameters.BatchSize > 0 {
		return p.SourceParameters.SqsQueueParameters.BatchSize
	}

	return pipeDefaultBatchSize
}

func clonePipe(p *Pipe) *Pipe {
	cp := *p
	cp.Tags = maps.Clone(p.Tags)
	if p.SourceParameters != nil {
		sp := *p.SourceParameters
		if p.SourceParameters.FilterCriteria != nil {
			fc := *p.SourceParameters.FilterCriteria
			fc.Filters = append([]Filter(nil), p.SourceParameters.FilterCriteria.Filters...)
			sp.FilterCriteria = &fc
		}
		if p.SourceParameters.SqsQueueParameters != nil {
			sqsp := *p.SourceParameters.SqsQueueParameters
			sp.SqsQueueParameters = &sqsp
		}
		cp.SourceParameters = &sp
	}
	if p.TargetParameters != nil {
		tp := *p.TargetParameters
		if p.TargetParameters.LambdaFunctionParameters != nil {
			lfp := *p.TargetParameters.LambdaFunctionParameters
			tp.LambdaFunctionParameters = &lfp
		}
		cp.TargetParameters = &tp
	}
	if p.DeadLetterConfig != nil {
		dlc := *p.DeadLetterConfig
		cp.DeadLetterConfig = &dlc
	}
	if p.LogConfiguration != nil {
		lc := *p.LogConfiguration
		lc.Destinations = append([]LogDestination(nil), p.LogConfiguration.Destinations...)
		lc.IncludeExecutionData = append([]string(nil), p.LogConfiguration.IncludeExecutionData...)
		cp.LogConfiguration = &lc
	}

	return &cp
}

// InMemoryBackend is the in-memory store for pipes.
type InMemoryBackend struct {
	pipes        map[string]*Pipe
	pipeARNIndex map[string]string
	mu           *lockmetrics.RWMutex
	accountID    string
	region       string
}

func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	return &InMemoryBackend{
		pipes:        make(map[string]*Pipe),
		pipeARNIndex: make(map[string]string),
		accountID:    accountID,
		region:       region,
		mu:           lockmetrics.New("pipes"),
	}
}

func (b *InMemoryBackend) Region() string { return b.region }

// CreatePipeInput holds the full set of fields for pipe creation.
type CreatePipeInput struct {
	Tags             map[string]string
	SourceParameters *SourceParameters
	TargetParameters *TargetParameters
	DeadLetterConfig *DeadLetterConfig
	LogConfiguration *LogConfiguration
	Name             string
	RoleARN          string
	Source           string
	Target           string
	Description      string
	Enrichment       string
	DesiredState     string
}

func (b *InMemoryBackend) CreatePipe(in CreatePipeInput) (*Pipe, error) {
	if err := validatePipeName(in.Name); err != nil {
		return nil, err
	}
	if err := validateDesiredState(in.DesiredState); err != nil {
		return nil, err
	}
	if in.Source == "" {
		return nil, fmt.Errorf("%w: Source is required", ErrValidation)
	}
	if in.Target == "" {
		return nil, fmt.Errorf("%w: Target is required", ErrValidation)
	}
	if err := validateTags(in.Tags); err != nil {
		return nil, err
	}

	b.mu.Lock("CreatePipe")
	defer b.mu.Unlock()

	if len(b.pipes) >= maxPipesPerAcct {
		return nil, fmt.Errorf(
			"%w: account has reached the maximum number of pipes (%d)",
			ErrValidation,
			maxPipesPerAcct,
		)
	}
	if _, ok := b.pipes[in.Name]; ok {
		return nil, fmt.Errorf("%w: pipe %s already exists", ErrAlreadyExists, in.Name)
	}
	if in.DesiredState == "" {
		in.DesiredState = stateRunning
	}

	now := time.Now()
	pipeARN := arn.Build("pipes", b.region, b.accountID, "pipe/"+in.Name)
	p := &Pipe{
		Name: in.Name, ARN: pipeARN, RoleARN: in.RoleARN,
		Source: in.Source, Target: in.Target, Description: in.Description,
		Enrichment: in.Enrichment, DesiredState: in.DesiredState, CurrentState: in.DesiredState,
		AccountID: b.accountID, Region: b.region,
		CreationTime: now, LastModifiedTime: now,
		Tags:             mergeTags(nil, in.Tags),
		SourceParameters: in.SourceParameters, TargetParameters: in.TargetParameters,
		DeadLetterConfig: in.DeadLetterConfig, LogConfiguration: in.LogConfiguration,
	}
	b.pipes[in.Name] = p
	b.pipeARNIndex[pipeARN] = in.Name

	return clonePipe(p), nil
}

func (b *InMemoryBackend) GetPipe(name string) (*Pipe, error) {
	b.mu.RLock("GetPipe")
	defer b.mu.RUnlock()
	p, ok := b.pipes[name]
	if !ok {
		return nil, fmt.Errorf("%w: pipe %s not found", ErrNotFound, name)
	}

	return clonePipe(p), nil
}

// ListPipesFilter holds optional query parameters for ListPipes.
type ListPipesFilter struct {
	NamePrefix   string
	DesiredState string
	CurrentState string
	SourcePrefix string
	TargetPrefix string
	NextToken    string
	Limit        int
}

// ListPipesResult holds the paginated result of a ListPipes call.
type ListPipesResult struct {
	NextToken string
	Pipes     []*Pipe
}

func (b *InMemoryBackend) ListPipes(f ListPipesFilter) ListPipesResult {
	b.mu.RLock("ListPipes")
	defer b.mu.RUnlock()

	limit := f.Limit
	if limit <= 0 || limit > 1000 {
		limit = 1000
	}

	names := b.sortedPipeNames()
	startIdx := b.resolveStartIndex(names, f.NextToken)
	result, lastIncluded := b.collectMatchingPipes(names, startIdx, limit, f)
	nextToken := b.buildNextToken(names, startIdx, len(result), limit, lastIncluded, f)

	return ListPipesResult{Pipes: result, NextToken: nextToken}
}

func (b *InMemoryBackend) sortedPipeNames() []string {
	names := make([]string, 0, len(b.pipes))
	for name := range b.pipes {
		names = append(names, name)
	}
	for i := 0; i < len(names); i++ {
		for j := i + 1; j < len(names); j++ {
			if names[j] < names[i] {
				names[i], names[j] = names[j], names[i]
			}
		}
	}

	return names
}

func (b *InMemoryBackend) resolveStartIndex(names []string, nextToken string) int {
	if nextToken == "" {
		return 0
	}
	decoded, err := base64.StdEncoding.DecodeString(nextToken)
	if err != nil {
		return 0
	}
	cursor := strings.TrimSuffix(string(decoded), nextTokenSep)
	startIdx := len(names)
	for i, n := range names {
		if n > cursor {
			startIdx = i

			break
		}
	}

	return startIdx
}

func (b *InMemoryBackend) collectMatchingPipes(
	names []string, startIdx, limit int, f ListPipesFilter,
) ([]*Pipe, string) {
	var result []*Pipe
	var lastIncluded string
	for i := startIdx; i < len(names); i++ {
		if len(result) >= limit {
			break
		}
		p := b.pipes[names[i]]
		if !matchesFilter(p, f) {
			continue
		}
		result = append(result, clonePipe(p))
		lastIncluded = p.Name
	}

	return result, lastIncluded
}

func (b *InMemoryBackend) buildNextToken(
	names []string, startIdx, resultLen, limit int, lastIncluded string, f ListPipesFilter,
) string {
	if resultLen < limit || lastIncluded == "" {
		return ""
	}
	for i := startIdx + resultLen; i < len(names); i++ {
		if matchesFilter(b.pipes[names[i]], f) {
			return base64.StdEncoding.EncodeToString([]byte(lastIncluded + nextTokenSep))
		}
	}

	return ""
}

func matchesFilter(p *Pipe, f ListPipesFilter) bool {
	if f.NamePrefix != "" && !strings.HasPrefix(p.Name, f.NamePrefix) {
		return false
	}
	if f.DesiredState != "" && p.DesiredState != f.DesiredState {
		return false
	}
	if f.CurrentState != "" && p.CurrentState != f.CurrentState {
		return false
	}
	if f.SourcePrefix != "" && !strings.HasPrefix(p.Source, f.SourcePrefix) {
		return false
	}
	if f.TargetPrefix != "" && !strings.HasPrefix(p.Target, f.TargetPrefix) {
		return false
	}

	return true
}

// UpdatePipeInput holds the fields that can be updated on an existing pipe.
type UpdatePipeInput struct {
	SourceParameters *SourceParameters
	TargetParameters *TargetParameters
	DeadLetterConfig *DeadLetterConfig
	LogConfiguration *LogConfiguration
	RoleARN          string
	Target           string
	Description      string
	Enrichment       string
	DesiredState     string
}

func (b *InMemoryBackend) UpdatePipe(name string, in UpdatePipeInput) (*Pipe, error) {
	if err := validateDesiredState(in.DesiredState); err != nil {
		return nil, err
	}
	b.mu.Lock("UpdatePipe")
	defer b.mu.Unlock()
	p, ok := b.pipes[name]
	if !ok {
		return nil, fmt.Errorf("%w: pipe %s not found", ErrNotFound, name)
	}
	if in.RoleARN != "" {
		p.RoleARN = in.RoleARN
	}
	if in.Target != "" {
		p.Target = in.Target
	}
	if in.DesiredState != "" {
		p.DesiredState = in.DesiredState
	}
	if in.Enrichment != "" {
		p.Enrichment = in.Enrichment
	}
	p.Description = in.Description
	if in.SourceParameters != nil {
		p.SourceParameters = in.SourceParameters
	}
	if in.TargetParameters != nil {
		p.TargetParameters = in.TargetParameters
	}
	if in.DeadLetterConfig != nil {
		p.DeadLetterConfig = in.DeadLetterConfig
	}
	if in.LogConfiguration != nil {
		p.LogConfiguration = in.LogConfiguration
	}
	p.LastModifiedTime = time.Now()

	return clonePipe(p), nil
}

func (b *InMemoryBackend) DeletePipe(name string) error {
	b.mu.Lock("DeletePipe")
	defer b.mu.Unlock()
	p, ok := b.pipes[name]
	if !ok {
		return fmt.Errorf("%w: pipe %s not found", ErrNotFound, name)
	}
	delete(b.pipeARNIndex, p.ARN)
	delete(b.pipes, name)

	return nil
}

func (b *InMemoryBackend) StartPipe(name string) (*Pipe, error) {
	b.mu.Lock("StartPipe")
	defer b.mu.Unlock()
	p, ok := b.pipes[name]
	if !ok {
		return nil, fmt.Errorf("%w: pipe %s not found", ErrNotFound, name)
	}
	if p.DesiredState == stateRunning && p.CurrentState == stateRunning {
		return nil, fmt.Errorf("%w: pipe %s is already in RUNNING state", ErrValidation, name)
	}
	p.DesiredState = stateRunning
	p.CurrentState = stateRunning
	p.StateReason = ""
	p.LastModifiedTime = time.Now()

	return clonePipe(p), nil
}

func (b *InMemoryBackend) StopPipe(name string) (*Pipe, error) {
	b.mu.Lock("StopPipe")
	defer b.mu.Unlock()
	p, ok := b.pipes[name]
	if !ok {
		return nil, fmt.Errorf("%w: pipe %s not found", ErrNotFound, name)
	}
	if p.DesiredState == stateStopped && p.CurrentState == stateStopped {
		return nil, fmt.Errorf("%w: pipe %s is already in STOPPED state", ErrValidation, name)
	}
	p.DesiredState = stateStopped
	p.CurrentState = stateStopped
	p.StateReason = ""
	p.LastModifiedTime = time.Now()

	return clonePipe(p), nil
}

// MarkPipeFailed updates a pipe to a failed state with a reason message.
func (b *InMemoryBackend) MarkPipeFailed(name, state, reason string) {
	b.mu.Lock("MarkPipeFailed")
	defer b.mu.Unlock()
	p, ok := b.pipes[name]
	if !ok {
		return
	}
	p.CurrentState = state
	p.StateReason = reason
	p.LastModifiedTime = time.Now()
}

func (b *InMemoryBackend) TagResource(resourceARN string, kv map[string]string) error {
	if err := validateTags(kv); err != nil {
		return err
	}
	b.mu.Lock("TagResource")
	defer b.mu.Unlock()
	name, ok := b.pipeARNIndex[resourceARN]
	if !ok {
		return fmt.Errorf("%w: resource %s not found", ErrNotFound, resourceARN)
	}
	p := b.pipes[name]
	merged := mergeTags(p.Tags, kv)
	if len(merged) > maxTagsPerPipe {
		return fmt.Errorf("%w: pipe would exceed %d tags limit", ErrValidation, maxTagsPerPipe)
	}
	p.Tags = merged

	return nil
}

func (b *InMemoryBackend) UntagResource(resourceARN string, keys []string) error {
	b.mu.Lock("UntagResource")
	defer b.mu.Unlock()
	name, ok := b.pipeARNIndex[resourceARN]
	if !ok {
		return fmt.Errorf("%w: resource %s not found", ErrNotFound, resourceARN)
	}
	p := b.pipes[name]
	for _, k := range keys {
		delete(p.Tags, k)
	}

	return nil
}

func (b *InMemoryBackend) ListTagsForResource(resourceARN string) (map[string]string, error) {
	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()
	name, ok := b.pipeARNIndex[resourceARN]
	if !ok {
		return nil, fmt.Errorf("%w: resource %s not found", ErrNotFound, resourceARN)
	}
	p := b.pipes[name]
	result := make(map[string]string, len(p.Tags))
	maps.Copy(result, p.Tags)

	return result, nil
}

func mergeTags(existing, incoming map[string]string) map[string]string {
	result := make(map[string]string, len(existing)+len(incoming))
	maps.Copy(result, existing)
	maps.Copy(result, incoming)

	return result
}

func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()
	b.pipes = make(map[string]*Pipe)
	b.pipeARNIndex = make(map[string]string)
}

func validatePipeName(name string) error {
	if name == "" {
		return fmt.Errorf("%w: pipe name must not be empty", ErrValidation)
	}
	if len(name) > maxPipeNameLen {
		return fmt.Errorf(
			"%w: pipe name exceeds maximum length of %d characters",
			ErrValidation,
			maxPipeNameLen,
		)
	}
	if !pipeNameRE.MatchString(name) {
		return fmt.Errorf(
			"%w: pipe name %q contains invalid characters (allowed: a-z, A-Z, 0-9, -, _)",
			ErrValidation,
			name,
		)
	}

	return nil
}

func validateDesiredState(state string) error {
	if state == "" || state == stateRunning || state == stateStopped {
		return nil
	}

	return fmt.Errorf("%w: DesiredState must be RUNNING or STOPPED, got %q", ErrValidation, state)
}

func validateTags(tags map[string]string) error {
	for k, v := range tags {
		if len(k) == 0 {
			return fmt.Errorf("%w: tag key must not be empty", ErrValidation)
		}
		if len(k) > maxTagKeyLen {
			return fmt.Errorf(
				"%w: tag key %q exceeds maximum length of %d",
				ErrValidation,
				k,
				maxTagKeyLen,
			)
		}
		if len(v) > maxTagValueLen {
			return fmt.Errorf(
				"%w: tag value for key %q exceeds maximum length of %d",
				ErrValidation,
				k,
				maxTagValueLen,
			)
		}
	}

	return nil
}

func (b *InMemoryBackend) Snapshot() []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()
	type snap struct {
		Pipes     map[string]*Pipe `json:"pipes"`
		AccountID string           `json:"accountID"`
		Region    string           `json:"region"`
	}
	s := snap{Pipes: b.pipes, AccountID: b.accountID, Region: b.region}
	data, err := json.Marshal(s)
	if err != nil {
		return nil
	}

	return data
}

func (b *InMemoryBackend) Restore(data []byte) error {
	type snap struct {
		Pipes     map[string]*Pipe `json:"pipes"`
		AccountID string           `json:"accountID"`
		Region    string           `json:"region"`
	}
	var s snap
	if err := json.Unmarshal(data, &s); err != nil {
		return err
	}
	b.mu.Lock("Restore")
	defer b.mu.Unlock()
	if s.Pipes == nil {
		s.Pipes = make(map[string]*Pipe)
	}
	b.pipes = s.Pipes
	b.accountID = s.AccountID
	b.region = s.Region
	b.pipeARNIndex = make(map[string]string, len(b.pipes))
	for name, p := range b.pipes {
		b.pipeARNIndex[p.ARN] = name
	}

	return nil
}

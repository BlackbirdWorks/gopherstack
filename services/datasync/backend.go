package datasync

import (
	"encoding/json"
	"fmt"
	"maps"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

const (
	invalidRequestType      = "InvalidRequestException"
	resourceNotFoundType    = "ResourceNotFoundException"
	conflictExceptionType   = "ResourceExistsException"

	agentStatusOnline  = "ONLINE"
	taskStatusAvailable = "AVAILABLE"

	executionStatusLaunching   = "LAUNCHING"
	executionStatusSuccess     = "SUCCESS"

	defaultMaxResults = 100
)

var (
	// ErrNotFound is returned when a resource does not exist.
	ErrNotFound = awserr.New(resourceNotFoundType, awserr.ErrNotFound)
	// ErrAlreadyExists is returned when a resource already exists.
	ErrAlreadyExists = awserr.New(conflictExceptionType, awserr.ErrAlreadyExists)
	// ErrInvalidParameter is returned for invalid input.
	ErrInvalidParameter = awserr.New(invalidRequestType, awserr.ErrInvalidParameter)
)

// storedAgent holds an agent with all fields.
// CreationTime is first so its non-pointer prefix (wall, ext) reduces GC pointer bytes.
type storedAgent struct {
	CreationTime time.Time         `json:"creationTime"`
	Tags         map[string]string `json:"tags"`
	AgentArn     string            `json:"agentArn"`
	Name         string            `json:"name"`
	Status       string            `json:"status"`
	EndpointType string            `json:"endpointType"`
}

func (a *storedAgent) toAgent() Agent {
	return Agent{
		AgentArn:     a.AgentArn,
		Name:         a.Name,
		Status:       a.Status,
		EndpointType: a.EndpointType,
		CreationTime: a.CreationTime,
		Tags:         a.Tags,
	}
}

// storedLocation holds a location with all fields.
// CreationTime is first so its non-pointer prefix (wall, ext) reduces GC pointer bytes.
type storedLocation struct {
	CreationTime   time.Time         `json:"creationTime"`
	S3Config       *storedS3Config   `json:"s3Config,omitempty"`
	Tags           map[string]string `json:"tags"`
	LocationArn    string            `json:"locationArn"`
	LocationUri    string            `json:"locationUri"`
	S3BucketArn    string            `json:"s3BucketArn,omitempty"`
	Subdirectory   string            `json:"subdirectory,omitempty"`
	S3StorageClass string            `json:"s3StorageClass,omitempty"`
	LocationType   string            `json:"locationType"`
}

type storedS3Config struct {
	BucketAccessRoleArn string `json:"bucketAccessRoleArn"`
}

func (l *storedLocation) toLocation() Location {
	return Location{
		LocationArn:  l.LocationArn,
		LocationUri:  l.LocationUri,
		CreationTime: l.CreationTime,
	}
}

func (l *storedLocation) toLocationS3() LocationS3 {
	loc := LocationS3{
		LocationArn:    l.LocationArn,
		LocationUri:    l.LocationUri,
		S3BucketArn:    l.S3BucketArn,
		Subdirectory:   l.Subdirectory,
		S3StorageClass: l.S3StorageClass,
		CreationTime:   l.CreationTime,
	}
	if l.S3Config != nil {
		loc.S3Config = S3Config{BucketAccessRoleArn: l.S3Config.BucketAccessRoleArn}
	}

	return loc
}

// storedTask holds a task with all fields.
// CreationTime is first so its non-pointer prefix (wall, ext) reduces GC pointer bytes.
type storedTask struct {
	CreationTime            time.Time         `json:"creationTime"`
	Tags                    map[string]string `json:"tags"`
	TaskArn                 string            `json:"taskArn"`
	Name                    string            `json:"name"`
	Status                  string            `json:"status"`
	SourceLocationArn       string            `json:"sourceLocationArn"`
	DestinationLocationArn  string            `json:"destinationLocationArn"`
	CloudWatchLogGroupArn   string            `json:"cloudWatchLogGroupArn,omitempty"`
	CurrentTaskExecutionArn string            `json:"currentTaskExecutionArn,omitempty"`
}

func (t *storedTask) toTask() Task {
	return Task{
		TaskArn:                 t.TaskArn,
		Name:                    t.Name,
		Status:                  t.Status,
		SourceLocationArn:       t.SourceLocationArn,
		DestinationLocationArn:  t.DestinationLocationArn,
		CloudWatchLogGroupArn:   t.CloudWatchLogGroupArn,
		CurrentTaskExecutionArn: t.CurrentTaskExecutionArn,
		CreationTime:            t.CreationTime,
		Tags:                    t.Tags,
	}
}

// storedTaskExecution holds a task execution with all fields.
// StartTime is first so its non-pointer prefix (wall, ext) reduces GC pointer bytes.
type storedTaskExecution struct {
	StartTime                time.Time `json:"startTime"`
	TaskExecutionArn         string    `json:"taskExecutionArn"`
	Status                   string    `json:"status"`
	EstimatedFilesToTransfer int64     `json:"estimatedFilesToTransfer"`
	EstimatedBytesToTransfer int64     `json:"estimatedBytesToTransfer"`
	FilesTransferred         int64     `json:"filesTransferred"`
	BytesTransferred         int64     `json:"bytesTransferred"`
}

func (e *storedTaskExecution) toTaskExecution() TaskExecution {
	return TaskExecution{
		TaskExecutionArn:         e.TaskExecutionArn,
		Status:                   e.Status,
		StartTime:                e.StartTime,
		EstimatedFilesToTransfer: e.EstimatedFilesToTransfer,
		EstimatedBytesToTransfer: e.EstimatedBytesToTransfer,
		FilesTransferred:         e.FilesTransferred,
		BytesTransferred:         e.BytesTransferred,
	}
}

// snapshot holds serializable backend state.
type snapshot struct {
	Agents     map[string]*storedAgent                   `json:"agents"`
	Locations  map[string]*storedLocation                `json:"locations"`
	Tasks      map[string]*storedTask                    `json:"tasks"`
	Executions map[string]map[string]*storedTaskExecution `json:"executions"` // taskArn → executionArn → execution
	Tags       map[string]map[string]string              `json:"tags"`
}

// InMemoryBackend implements StorageBackend using in-memory maps.
type InMemoryBackend struct {
	mu         *lockmetrics.RWMutex
	agents     map[string]*storedAgent                    // agentArn → agent
	locations  map[string]*storedLocation                 // locationArn → location
	tasks      map[string]*storedTask                     // taskArn → task
	executions map[string]map[string]*storedTaskExecution  // taskArn → executionArn → execution
	tags       map[string]map[string]string               // resourceArn → tags
	accountID  string
	region     string
}

// NewInMemoryBackend constructs a new InMemoryBackend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	return &InMemoryBackend{
		mu:         lockmetrics.New("datasync"),
		accountID:  accountID,
		region:     region,
		agents:     make(map[string]*storedAgent),
		locations:  make(map[string]*storedLocation),
		tasks:      make(map[string]*storedTask),
		executions: make(map[string]map[string]*storedTaskExecution),
		tags:       make(map[string]map[string]string),
	}
}

func (b *InMemoryBackend) agentARN(id string) string {
	return arn.Build("datasync", b.region, b.accountID, "agent/"+id)
}

func (b *InMemoryBackend) locationARN(id string) string {
	return arn.Build("datasync", b.region, b.accountID, "location/"+id)
}

func (b *InMemoryBackend) taskARN(id string) string {
	return arn.Build("datasync", b.region, b.accountID, "task/"+id)
}

func (b *InMemoryBackend) executionARN(taskArn, id string) string {
	// Extract task resource portion: task/<task-id>
	parts := strings.SplitN(taskArn, ":task/", 2)
	if len(parts) == 2 {
		return arn.Build("datasync", b.region, b.accountID, fmt.Sprintf("task/%s/execution/%s", parts[1], id))
	}

	return arn.Build("datasync", b.region, b.accountID, "task/unknown/execution/"+id)
}

func newID() string {
	return strings.ReplaceAll(uuid.NewString(), "-", "")[:16]
}

// CreateAgent creates a new DataSync agent.
func (b *InMemoryBackend) CreateAgent(name, _ string, tags map[string]string) (*Agent, error) {
	b.mu.Lock("CreateAgent")
	defer b.mu.Unlock()

	id := newID()
	agentArn := b.agentARN(id)
	now := time.Now().UTC()

	agentTags := make(map[string]string)
	maps.Copy(agentTags, tags)

	a := &storedAgent{
		AgentArn:     agentArn,
		Name:         name,
		Status:       agentStatusOnline,
		EndpointType: "PUBLIC",
		CreationTime: now,
		Tags:         agentTags,
	}
	b.agents[agentArn] = a

	if len(agentTags) > 0 {
		b.tags[agentArn] = make(map[string]string)
		maps.Copy(b.tags[agentArn], agentTags)
	}

	cp := a.toAgent()

	return &cp, nil
}

// DescribeAgent returns agent details.
func (b *InMemoryBackend) DescribeAgent(agentArn string) (*Agent, error) {
	b.mu.RLock("DescribeAgent")
	defer b.mu.RUnlock()

	a, ok := b.agents[agentArn]
	if !ok {
		return nil, ErrNotFound
	}

	cp := a.toAgent()

	return &cp, nil
}

// UpdateAgent updates the agent's name.
func (b *InMemoryBackend) UpdateAgent(agentArn, name string) error {
	b.mu.Lock("UpdateAgent")
	defer b.mu.Unlock()

	a, ok := b.agents[agentArn]
	if !ok {
		return ErrNotFound
	}

	a.Name = name

	return nil
}

// DeleteAgent deletes an agent.
func (b *InMemoryBackend) DeleteAgent(agentArn string) error {
	b.mu.Lock("DeleteAgent")
	defer b.mu.Unlock()

	if _, ok := b.agents[agentArn]; !ok {
		return ErrNotFound
	}

	delete(b.agents, agentArn)
	delete(b.tags, agentArn)

	return nil
}

// ListAgents returns agents, sorted by ARN.
func (b *InMemoryBackend) ListAgents(maxResults int32, nextToken string) ([]*AgentListEntry, string, error) {
	b.mu.RLock("ListAgents")
	defer b.mu.RUnlock()

	arns := make([]string, 0, len(b.agents))
	for a := range b.agents {
		arns = append(arns, a)
	}
	sort.Strings(arns)

	all := make([]*AgentListEntry, 0, len(arns))
	for _, a := range arns {
		ag := b.agents[a]
		all = append(all, &AgentListEntry{
			AgentArn: ag.AgentArn,
			Name:     ag.Name,
			Status:   ag.Status,
		})
	}

	limit := int(maxResults)
	pg := page.New(all, nextToken, limit, defaultMaxResults)

	return pg.Data, pg.Next, nil
}

// CreateLocationS3 creates a new S3 location.
func (b *InMemoryBackend) CreateLocationS3(subdirectory, s3BucketArn, s3StorageClass string, s3Config S3Config, tags map[string]string) (*Location, error) {
	b.mu.Lock("CreateLocationS3")
	defer b.mu.Unlock()

	id := newID()
	locationArn := b.locationARN(id)
	now := time.Now().UTC()

	// Build S3 URI: s3://<bucket-name>/<subdirectory>
	bucketName := extractBucketName(s3BucketArn)
	sub := strings.TrimPrefix(subdirectory, "/")
	locationUri := fmt.Sprintf("s3://%s/%s", bucketName, sub)

	locationTags := make(map[string]string)
	maps.Copy(locationTags, tags)

	storedCfg := &storedS3Config{BucketAccessRoleArn: s3Config.BucketAccessRoleArn}

	l := &storedLocation{
		LocationArn:    locationArn,
		LocationUri:    locationUri,
		S3BucketArn:    s3BucketArn,
		Subdirectory:   subdirectory,
		S3StorageClass: s3StorageClass,
		S3Config:       storedCfg,
		LocationType:   "S3",
		CreationTime:   now,
		Tags:           locationTags,
	}
	b.locations[locationArn] = l

	if len(locationTags) > 0 {
		b.tags[locationArn] = make(map[string]string)
		maps.Copy(b.tags[locationArn], locationTags)
	}

	cp := l.toLocation()

	return &cp, nil
}

// DescribeLocationS3 returns S3 location details.
func (b *InMemoryBackend) DescribeLocationS3(locationArn string) (*LocationS3, error) {
	b.mu.RLock("DescribeLocationS3")
	defer b.mu.RUnlock()

	l, ok := b.locations[locationArn]
	if !ok {
		return nil, ErrNotFound
	}

	if l.LocationType != "S3" {
		return nil, ErrNotFound
	}

	cp := l.toLocationS3()

	return &cp, nil
}

// DeleteLocation deletes a location.
func (b *InMemoryBackend) DeleteLocation(locationArn string) error {
	b.mu.Lock("DeleteLocation")
	defer b.mu.Unlock()

	if _, ok := b.locations[locationArn]; !ok {
		return ErrNotFound
	}

	delete(b.locations, locationArn)
	delete(b.tags, locationArn)

	return nil
}

// ListLocations returns locations, sorted by ARN.
func (b *InMemoryBackend) ListLocations(maxResults int32, nextToken string) ([]*LocationListEntry, string, error) {
	b.mu.RLock("ListLocations")
	defer b.mu.RUnlock()

	arns := make([]string, 0, len(b.locations))
	for a := range b.locations {
		arns = append(arns, a)
	}
	sort.Strings(arns)

	all := make([]*LocationListEntry, 0, len(arns))
	for _, a := range arns {
		l := b.locations[a]
		all = append(all, &LocationListEntry{
			LocationArn:  l.LocationArn,
			LocationUri:  l.LocationUri,
			CreationTime: l.CreationTime,
		})
	}

	limit := int(maxResults)
	pg := page.New(all, nextToken, limit, defaultMaxResults)

	return pg.Data, pg.Next, nil
}

// CreateTask creates a new DataSync task.
func (b *InMemoryBackend) CreateTask(sourceLocationArn, destinationLocationArn, name, cloudWatchLogGroupArn string, tags map[string]string) (*Task, error) {
	b.mu.Lock("CreateTask")
	defer b.mu.Unlock()

	if _, ok := b.locations[sourceLocationArn]; !ok {
		return nil, fmt.Errorf("source location %s not found: %w", sourceLocationArn, ErrInvalidParameter)
	}

	if _, ok := b.locations[destinationLocationArn]; !ok {
		return nil, fmt.Errorf("destination location %s not found: %w", destinationLocationArn, ErrInvalidParameter)
	}

	id := newID()
	taskArn := b.taskARN(id)
	now := time.Now().UTC()

	taskTags := make(map[string]string)
	maps.Copy(taskTags, tags)

	t := &storedTask{
		TaskArn:                taskArn,
		Name:                   name,
		Status:                 taskStatusAvailable,
		SourceLocationArn:      sourceLocationArn,
		DestinationLocationArn: destinationLocationArn,
		CloudWatchLogGroupArn:  cloudWatchLogGroupArn,
		CreationTime:           now,
		Tags:                   taskTags,
	}
	b.tasks[taskArn] = t

	if len(taskTags) > 0 {
		b.tags[taskArn] = make(map[string]string)
		maps.Copy(b.tags[taskArn], taskTags)
	}

	cp := t.toTask()

	return &cp, nil
}

// DescribeTask returns task details.
func (b *InMemoryBackend) DescribeTask(taskArn string) (*Task, error) {
	b.mu.RLock("DescribeTask")
	defer b.mu.RUnlock()

	t, ok := b.tasks[taskArn]
	if !ok {
		return nil, ErrNotFound
	}

	cp := t.toTask()

	return &cp, nil
}

// UpdateTask updates the task's name and CloudWatch log group.
func (b *InMemoryBackend) UpdateTask(taskArn, name, cloudWatchLogGroupArn string) error {
	b.mu.Lock("UpdateTask")
	defer b.mu.Unlock()

	t, ok := b.tasks[taskArn]
	if !ok {
		return ErrNotFound
	}

	if name != "" {
		t.Name = name
	}

	t.CloudWatchLogGroupArn = cloudWatchLogGroupArn

	return nil
}

// DeleteTask deletes a task.
func (b *InMemoryBackend) DeleteTask(taskArn string) error {
	b.mu.Lock("DeleteTask")
	defer b.mu.Unlock()

	if _, ok := b.tasks[taskArn]; !ok {
		return ErrNotFound
	}

	delete(b.tasks, taskArn)
	delete(b.executions, taskArn)
	delete(b.tags, taskArn)

	return nil
}

// ListTasks returns tasks, sorted by ARN.
func (b *InMemoryBackend) ListTasks(maxResults int32, nextToken string) ([]*TaskListEntry, string, error) {
	b.mu.RLock("ListTasks")
	defer b.mu.RUnlock()

	arns := make([]string, 0, len(b.tasks))
	for a := range b.tasks {
		arns = append(arns, a)
	}
	sort.Strings(arns)

	all := make([]*TaskListEntry, 0, len(arns))
	for _, a := range arns {
		t := b.tasks[a]
		all = append(all, &TaskListEntry{
			TaskArn: t.TaskArn,
			Name:    t.Name,
			Status:  t.Status,
		})
	}

	limit := int(maxResults)
	pg := page.New(all, nextToken, limit, defaultMaxResults)

	return pg.Data, pg.Next, nil
}

// StartTaskExecution starts a new task execution.
func (b *InMemoryBackend) StartTaskExecution(taskArn string) (*TaskExecution, error) {
	b.mu.Lock("StartTaskExecution")
	defer b.mu.Unlock()

	t, ok := b.tasks[taskArn]
	if !ok {
		return nil, ErrNotFound
	}

	id := newID()
	execArn := b.executionARN(taskArn, id)
	now := time.Now().UTC()

	e := &storedTaskExecution{
		TaskExecutionArn: execArn,
		Status:           executionStatusLaunching,
		StartTime:        now,
	}

	if b.executions[taskArn] == nil {
		b.executions[taskArn] = make(map[string]*storedTaskExecution)
	}

	b.executions[taskArn][execArn] = e
	t.CurrentTaskExecutionArn = execArn

	cp := e.toTaskExecution()

	return &cp, nil
}

// CancelTaskExecution cancels a running task execution.
func (b *InMemoryBackend) CancelTaskExecution(taskExecutionArn string) error {
	b.mu.Lock("CancelTaskExecution")
	defer b.mu.Unlock()

	taskArn := extractTaskArnFromExecution(taskExecutionArn)
	if taskArn == "" {
		return ErrNotFound
	}

	execMap, ok := b.executions[taskArn]
	if !ok {
		return ErrNotFound
	}

	if _, ok = execMap[taskExecutionArn]; !ok {
		return ErrNotFound
	}

	delete(execMap, taskExecutionArn)

	if t, ok := b.tasks[taskArn]; ok && t.CurrentTaskExecutionArn == taskExecutionArn {
		t.CurrentTaskExecutionArn = ""
	}

	return nil
}

// DescribeTaskExecution returns task execution details.
func (b *InMemoryBackend) DescribeTaskExecution(taskExecutionArn string) (*TaskExecution, error) {
	b.mu.RLock("DescribeTaskExecution")
	defer b.mu.RUnlock()

	taskArn := extractTaskArnFromExecution(taskExecutionArn)
	if taskArn == "" {
		return nil, ErrNotFound
	}

	execMap, ok := b.executions[taskArn]
	if !ok {
		return nil, ErrNotFound
	}

	e, ok := execMap[taskExecutionArn]
	if !ok {
		return nil, ErrNotFound
	}

	cp := e.toTaskExecution()

	return &cp, nil
}

// ListTaskExecutions returns executions for a task, sorted by ARN.
func (b *InMemoryBackend) ListTaskExecutions(taskArn string, maxResults int32, nextToken string) ([]*TaskExecutionListEntry, string, error) {
	b.mu.RLock("ListTaskExecutions")
	defer b.mu.RUnlock()

	execMap := b.executions[taskArn]
	execArns := make([]string, 0, len(execMap))
	for a := range execMap {
		execArns = append(execArns, a)
	}
	sort.Strings(execArns)

	all := make([]*TaskExecutionListEntry, 0, len(execArns))
	for _, a := range execArns {
		e := execMap[a]
		all = append(all, &TaskExecutionListEntry{
			TaskExecutionArn: e.TaskExecutionArn,
			Status:           e.Status,
		})
	}

	limit := int(maxResults)
	pg := page.New(all, nextToken, limit, defaultMaxResults)

	return pg.Data, pg.Next, nil
}

// TagResource adds or updates tags on a resource.
func (b *InMemoryBackend) TagResource(resourceArn string, tags map[string]string) error {
	b.mu.Lock("TagResource")
	defer b.mu.Unlock()

	if !b.isKnownResource(resourceArn) {
		return ErrNotFound
	}

	if b.tags[resourceArn] == nil {
		b.tags[resourceArn] = make(map[string]string)
	}
	maps.Copy(b.tags[resourceArn], tags)

	return nil
}

// UntagResource removes tags from a resource.
func (b *InMemoryBackend) UntagResource(resourceArn string, keys []string) error {
	b.mu.Lock("UntagResource")
	defer b.mu.Unlock()

	if !b.isKnownResource(resourceArn) {
		return ErrNotFound
	}

	for _, k := range keys {
		delete(b.tags[resourceArn], k)
	}

	return nil
}

// ListTagsForResource returns tags for a resource with pagination.
func (b *InMemoryBackend) ListTagsForResource(resourceArn string, maxResults int32, nextToken string) (map[string]string, string, error) {
	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()

	if !b.isKnownResource(resourceArn) {
		return nil, "", ErrNotFound
	}

	// Build sorted key list for stable pagination.
	tagMap := b.tags[resourceArn]
	keys := make([]string, 0, len(tagMap))
	for k := range tagMap {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	type tagEntry struct {
		key   string
		value string
	}

	all := make([]tagEntry, 0, len(keys))
	for _, k := range keys {
		all = append(all, tagEntry{k, tagMap[k]})
	}

	limit := int(maxResults)
	pg := page.New(all, nextToken, limit, defaultMaxResults)

	result := make(map[string]string, len(pg.Data))
	for _, e := range pg.Data {
		result[e.key] = e.value
	}

	return result, pg.Next, nil
}

// isKnownResource returns true if the ARN corresponds to a known agent, location, or task.
// Must be called with at least a read lock held.
func (b *InMemoryBackend) isKnownResource(a string) bool {
	if _, ok := b.agents[a]; ok {
		return true
	}

	if _, ok := b.locations[a]; ok {
		return true
	}

	if _, ok := b.tasks[a]; ok {
		return true
	}

	return false
}

// AccountID returns the account ID.
func (b *InMemoryBackend) AccountID() string { return b.accountID }

// Region returns the region.
func (b *InMemoryBackend) Region() string { return b.region }

// Reset clears all state.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	b.agents = make(map[string]*storedAgent)
	b.locations = make(map[string]*storedLocation)
	b.tasks = make(map[string]*storedTask)
	b.executions = make(map[string]map[string]*storedTaskExecution)
	b.tags = make(map[string]map[string]string)
}

// Snapshot serializes the backend state to JSON.
func (b *InMemoryBackend) Snapshot() []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	data, _ := json.Marshal(snapshot{
		Agents:     b.agents,
		Locations:  b.locations,
		Tasks:      b.tasks,
		Executions: b.executions,
		Tags:       b.tags,
	})

	return data
}

// Restore deserializes backend state from a snapshot.
func (b *InMemoryBackend) Restore(data []byte) error {
	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	var snap snapshot
	if err := json.Unmarshal(data, &snap); err != nil {
		return err
	}

	if snap.Agents != nil {
		b.agents = snap.Agents
	} else {
		b.agents = make(map[string]*storedAgent)
	}

	if snap.Locations != nil {
		b.locations = snap.Locations
	} else {
		b.locations = make(map[string]*storedLocation)
	}

	if snap.Tasks != nil {
		b.tasks = snap.Tasks
	} else {
		b.tasks = make(map[string]*storedTask)
	}

	if snap.Executions != nil {
		b.executions = snap.Executions
	} else {
		b.executions = make(map[string]map[string]*storedTaskExecution)
	}

	if snap.Tags != nil {
		b.tags = snap.Tags
	} else {
		b.tags = make(map[string]map[string]string)
	}

	return nil
}

// extractBucketName extracts the bucket name from an S3 ARN.
// Format: arn:aws:s3:::bucket-name or arn:aws:s3:::bucket-name/prefix
func extractBucketName(s3BucketArn string) string {
	// S3 ARNs: arn:aws:s3:::bucket-name
	parts := strings.SplitN(s3BucketArn, ":::", 2)
	if len(parts) == 2 {
		name := strings.SplitN(parts[1], "/", 2)[0]
		if name != "" {
			return name
		}
	}

	return "unknown-bucket"
}

// extractTaskArnFromExecution extracts the task ARN from a task execution ARN.
// Execution ARN format: arn:aws:datasync:region:account:task/<task-id>/execution/<exec-id>
func extractTaskArnFromExecution(execArn string) string {
	// Find /execution/ suffix and strip it.
	idx := strings.LastIndex(execArn, "/execution/")
	if idx < 0 {
		return ""
	}

	return execArn[:idx]
}

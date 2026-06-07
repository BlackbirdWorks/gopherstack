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
	invalidRequestType    = "InvalidRequestException"
	resourceNotFoundType  = "ResourceNotFoundException"
	conflictExceptionType = "ResourceExistsException"

	agentStatusOnline   = "ONLINE"
	taskStatusAvailable = "AVAILABLE"

	executionStatusLaunching = "LAUNCHING"
	executionStatusSuccess   = "SUCCESS"

	defaultMaxResults = 100

	arnSplitParts = 2
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
	CreationTime   time.Time                  `json:"creationTime"`
	S3Config       *storedS3Config            `json:"s3Config,omitempty"`
	AzureBlob      *storedAzureBlobConfig     `json:"azureBlob,omitempty"`
	Efs            *storedEfsConfig           `json:"efs,omitempty"`
	FsxLustre      *storedFsxLustreConfig     `json:"fsxLustre,omitempty"`
	FsxOntap       *storedFsxOntapConfig      `json:"fsxOntap,omitempty"`
	FsxOpenZfs     *storedFsxOpenZfsConfig    `json:"fsxOpenZfs,omitempty"`
	FsxWindows     *storedFsxWindowsConfig    `json:"fsxWindows,omitempty"`
	Hdfs           *storedHdfsConfig          `json:"hdfs,omitempty"`
	Nfs            *storedNfsConfig           `json:"nfs,omitempty"`
	ObjectStorage  *storedObjectStorageConfig `json:"objectStorage,omitempty"`
	Smb            *storedSmbConfig           `json:"smb,omitempty"`
	Tags           map[string]string          `json:"tags"`
	LocationArn    string                     `json:"locationArn"`
	LocationURI    string                     `json:"locationUri"`
	S3BucketArn    string                     `json:"s3BucketArn,omitempty"`
	Subdirectory   string                     `json:"subdirectory,omitempty"`
	S3StorageClass string                     `json:"s3StorageClass,omitempty"`
	LocationType   string                     `json:"locationType"`
}

type storedS3Config struct {
	BucketAccessRoleArn string `json:"bucketAccessRoleArn"`
}

// --- Type-specific location config stored types ---

type storedAzureBlobConfig struct {
	SasToken     string   `json:"sasToken,omitempty"`
	AgentArns    []string `json:"agentArns,omitempty"`
	ContainerURL string   `json:"containerUrl"`
	BlobType     string   `json:"blobType,omitempty"`
	AccessTier   string   `json:"accessTier,omitempty"`
}

type storedEfsEc2Config struct {
	SecurityGroupArns []string `json:"securityGroupArns"`
	SubnetArn         string   `json:"subnetArn"`
}

type storedEfsConfig struct {
	Ec2Config               *storedEfsEc2Config `json:"ec2Config,omitempty"`
	EfsFilesystemArn        string              `json:"efsFilesystemArn"`
	AccessPointArn          string              `json:"accessPointArn,omitempty"`
	FileSystemAccessRoleArn string              `json:"fileSystemAccessRoleArn,omitempty"`
	InTransitEncryption     string              `json:"inTransitEncryption,omitempty"`
}

type storedFsxLustreConfig struct {
	SecurityGroupArns []string `json:"securityGroupArns,omitempty"`
	FsxFilesystemArn  string   `json:"fsxFilesystemArn"`
}

type storedFsxMountOptions struct {
	Version string `json:"version,omitempty"`
}

type storedFsxNfsProtocol struct {
	MountOptions *storedFsxMountOptions `json:"mountOptions,omitempty"`
}

type storedFsxSmbProtocol struct {
	MountOptions *storedFsxMountOptions `json:"mountOptions,omitempty"`
	Domain       string                 `json:"domain,omitempty"`
	Password     string                 `json:"password,omitempty"`
	User         string                 `json:"user,omitempty"`
}

type storedFsxProtocol struct {
	NFS *storedFsxNfsProtocol `json:"nfs,omitempty"`
	SMB *storedFsxSmbProtocol `json:"smb,omitempty"`
}

type storedFsxOntapConfig struct {
	Protocol                 *storedFsxProtocol `json:"protocol,omitempty"`
	SecurityGroupArns        []string           `json:"securityGroupArns,omitempty"`
	StorageVirtualMachineArn string             `json:"storageVirtualMachineArn"`
}

type storedFsxOpenZfsConfig struct {
	Protocol          *storedFsxProtocol `json:"protocol,omitempty"`
	SecurityGroupArns []string           `json:"securityGroupArns,omitempty"`
	FsxFilesystemArn  string             `json:"fsxFilesystemArn"`
}

type storedFsxWindowsConfig struct {
	SecurityGroupArns []string `json:"securityGroupArns,omitempty"`
	FsxFilesystemArn  string   `json:"fsxFilesystemArn"`
	Domain            string   `json:"domain,omitempty"`
	User              string   `json:"user,omitempty"`
	Password          string   `json:"password,omitempty"`
}

type storedHdfsNameNode struct {
	Hostname string `json:"hostname"`
	Port     int32  `json:"port"`
}

type storedQopConfig struct {
	DataTransferProtection string `json:"dataTransferProtection,omitempty"`
	RpcProtection          string `json:"rpcProtection,omitempty"`
}

type storedHdfsConfig struct {
	QopConfiguration   *storedQopConfig     `json:"qopConfiguration,omitempty"`
	NameNodes          []storedHdfsNameNode `json:"nameNodes"`
	AgentArns          []string             `json:"agentArns,omitempty"`
	KerberosPrincipal  string               `json:"kerberosPrincipal,omitempty"`
	KerberosKeytab     string               `json:"kerberosKeytab,omitempty"`
	KerberosKrb5Conf   string               `json:"kerberosKrb5Conf,omitempty"`
	KmsKeyProviderURI  string               `json:"kmsKeyProviderUri,omitempty"`
	AuthenticationType string               `json:"authenticationType,omitempty"`
	SimpleUser         string               `json:"simpleUser,omitempty"`
	BlockSize          int64                `json:"blockSize,omitempty"`
	ReplicationFactor  int32                `json:"replicationFactor,omitempty"`
}

type storedMountOptions struct {
	Version string `json:"version,omitempty"`
}

type storedNfsConfig struct {
	MountOptions   *storedMountOptions `json:"mountOptions,omitempty"`
	AgentArns      []string            `json:"agentArns,omitempty"`
	ServerHostname string              `json:"serverHostname"`
}

type storedObjectStorageConfig struct {
	AgentArns      []string `json:"agentArns,omitempty"`
	ServerHostname string   `json:"serverHostname"`
	BucketName     string   `json:"bucketName"`
	AccessKey      string   `json:"accessKey,omitempty"`
	SecretKey      string   `json:"secretKey,omitempty"`
	ServerProtocol string   `json:"serverProtocol,omitempty"`
	ServerPort     int32    `json:"serverPort,omitempty"`
}

type storedSmbConfig struct {
	MountOptions   *storedMountOptions `json:"mountOptions,omitempty"`
	AgentArns      []string            `json:"agentArns,omitempty"`
	ServerHostname string              `json:"serverHostname"`
	Domain         string              `json:"domain,omitempty"`
	User           string              `json:"user,omitempty"`
	Password       string              `json:"password,omitempty"`
}

func (l *storedLocation) toLocation() Location {
	return Location{
		LocationArn:  l.LocationArn,
		LocationURI:  l.LocationURI,
		CreationTime: l.CreationTime,
	}
}

func (l *storedLocation) toLocationS3() LocationS3 {
	loc := LocationS3{
		LocationArn:    l.LocationArn,
		LocationURI:    l.LocationURI,
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
	Agents     map[string]*storedAgent                    `json:"agents"`
	Locations  map[string]*storedLocation                 `json:"locations"`
	Tasks      map[string]*storedTask                     `json:"tasks"`
	Executions map[string]map[string]*storedTaskExecution `json:"executions"` // taskArn → executionArn → execution
	Tags       map[string]map[string]string               `json:"tags"`
}

// InMemoryBackend implements StorageBackend using in-memory maps.
type InMemoryBackend struct {
	mu         *lockmetrics.RWMutex
	agents     map[string]*storedAgent                    // agentArn → agent
	locations  map[string]*storedLocation                 // locationArn → location
	tasks      map[string]*storedTask                     // taskArn → task
	executions map[string]map[string]*storedTaskExecution // taskArn → executionArn → execution
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
	parts := strings.SplitN(taskArn, ":task/", arnSplitParts)
	if len(parts) == arnSplitParts {
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
func (b *InMemoryBackend) CreateLocationS3(
	subdirectory, s3BucketArn, s3StorageClass string,
	s3Config S3Config,
	tags map[string]string,
) (*Location, error) {
	b.mu.Lock("CreateLocationS3")
	defer b.mu.Unlock()

	id := newID()
	locationArn := b.locationARN(id)
	now := time.Now().UTC()

	// Build S3 URI: s3://<bucket-name>/<subdirectory>
	bucketName := extractBucketName(s3BucketArn)
	sub := strings.TrimPrefix(subdirectory, "/")
	locationURI := fmt.Sprintf("s3://%s/%s", bucketName, sub)

	locationTags := make(map[string]string)
	maps.Copy(locationTags, tags)

	storedCfg := &storedS3Config{BucketAccessRoleArn: s3Config.BucketAccessRoleArn}

	l := &storedLocation{
		LocationArn:    locationArn,
		LocationURI:    locationURI,
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
			LocationURI:  l.LocationURI,
			CreationTime: l.CreationTime,
		})
	}

	limit := int(maxResults)
	pg := page.New(all, nextToken, limit, defaultMaxResults)

	return pg.Data, pg.Next, nil
}

// CreateTask creates a new DataSync task.
func (b *InMemoryBackend) CreateTask(
	sourceLocationArn, destinationLocationArn, name, cloudWatchLogGroupArn string,
	tags map[string]string,
) (*Task, error) {
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

	if t, found := b.tasks[taskArn]; found && t.CurrentTaskExecutionArn == taskExecutionArn {
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
func (b *InMemoryBackend) ListTaskExecutions(
	taskArn string,
	maxResults int32,
	nextToken string,
) ([]*TaskExecutionListEntry, string, error) {
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
func (b *InMemoryBackend) ListTagsForResource(
	resourceArn string,
	maxResults int32,
	nextToken string,
) (map[string]string, string, error) {
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

// UpdateLocationS3 updates an S3 location's subdirectory, storage class, and S3 config.
func (b *InMemoryBackend) UpdateLocationS3(locationArn, subdirectory, s3StorageClass string, s3Config S3Config) error {
	b.mu.Lock("UpdateLocationS3")
	defer b.mu.Unlock()

	l, ok := b.locations[locationArn]
	if !ok || l.LocationType != "S3" {
		return ErrNotFound
	}

	if subdirectory != "" {
		l.Subdirectory = subdirectory
		bucketName := extractBucketName(l.S3BucketArn)
		sub := strings.TrimPrefix(subdirectory, "/")
		l.LocationURI = fmt.Sprintf("s3://%s/%s", bucketName, sub)
	}

	l.S3StorageClass = s3StorageClass
	l.S3Config = &storedS3Config{BucketAccessRoleArn: s3Config.BucketAccessRoleArn}

	return nil
}

// UpdateTaskExecution updates a task execution (no-op: options are advisory only).
func (b *InMemoryBackend) UpdateTaskExecution(taskExecutionArn string) error {
	b.mu.RLock("UpdateTaskExecution")
	defer b.mu.RUnlock()

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

	return nil
}

// --- AzureBlob ---

func (b *InMemoryBackend) CreateLocationAzureBlob(
	containerURL, subdirectory, blobType, accessTier string,
	sasConfig *SasConfiguration,
	agentArns []string,
	tags map[string]string,
) (*Location, error) {
	b.mu.Lock("CreateLocationAzureBlob")
	defer b.mu.Unlock()

	id := newID()
	locationArn := b.locationARN(id)
	now := time.Now().UTC()

	sub := strings.TrimPrefix(subdirectory, "/")
	locationURI := fmt.Sprintf("azure-blob://%s/%s", containerURL, sub)

	locationTags := make(map[string]string)
	maps.Copy(locationTags, tags)

	cfg := &storedAzureBlobConfig{
		ContainerURL: containerURL,
		BlobType:     blobType,
		AccessTier:   accessTier,
		AgentArns:    agentArns,
	}
	if sasConfig != nil {
		cfg.SasToken = sasConfig.Token
	}

	l := &storedLocation{
		LocationArn:  locationArn,
		LocationURI:  locationURI,
		Subdirectory: subdirectory,
		LocationType: "AZURE_BLOB",
		CreationTime: now,
		Tags:         locationTags,
		AzureBlob:    cfg,
	}
	b.locations[locationArn] = l

	if len(locationTags) > 0 {
		b.tags[locationArn] = make(map[string]string)
		maps.Copy(b.tags[locationArn], locationTags)
	}

	cp := l.toLocation()

	return &cp, nil
}

func (b *InMemoryBackend) DescribeLocationAzureBlob(locationArn string) (*LocationAzureBlob, error) {
	b.mu.RLock("DescribeLocationAzureBlob")
	defer b.mu.RUnlock()

	l, ok := b.locations[locationArn]
	if !ok || l.LocationType != "AZURE_BLOB" {
		return nil, ErrNotFound
	}

	out := &LocationAzureBlob{
		LocationArn:  l.LocationArn,
		LocationURI:  l.LocationURI,
		Subdirectory: l.Subdirectory,
		CreationTime: l.CreationTime,
	}

	if l.AzureBlob != nil {
		out.ContainerURL = l.AzureBlob.ContainerURL
		out.BlobType = l.AzureBlob.BlobType
		out.AccessTier = l.AzureBlob.AccessTier
		out.AgentArns = l.AzureBlob.AgentArns

		if l.AzureBlob.SasToken != "" {
			out.SasConfiguration = &SasConfiguration{Token: l.AzureBlob.SasToken}
		}
	}

	return out, nil
}

func (b *InMemoryBackend) UpdateLocationAzureBlob(
	locationArn, containerURL, subdirectory, blobType, accessTier string,
	sasConfig *SasConfiguration,
	agentArns []string,
) error {
	b.mu.Lock("UpdateLocationAzureBlob")
	defer b.mu.Unlock()

	l, ok := b.locations[locationArn]
	if !ok || l.LocationType != "AZURE_BLOB" {
		return ErrNotFound
	}

	if containerURL != "" {
		l.AzureBlob.ContainerURL = containerURL
	}

	if subdirectory != "" {
		l.Subdirectory = subdirectory
		sub := strings.TrimPrefix(subdirectory, "/")
		cu := l.AzureBlob.ContainerURL
		l.LocationURI = fmt.Sprintf("azure-blob://%s/%s", cu, sub)
	}

	if blobType != "" {
		l.AzureBlob.BlobType = blobType
	}

	if accessTier != "" {
		l.AzureBlob.AccessTier = accessTier
	}

	if sasConfig != nil {
		l.AzureBlob.SasToken = sasConfig.Token
	}

	if agentArns != nil {
		l.AzureBlob.AgentArns = agentArns
	}

	return nil
}

// --- EFS ---

func (b *InMemoryBackend) CreateLocationEfs(
	efsFilesystemArn, subdirectory, accessPointArn, fileSystemAccessRoleArn, inTransitEncryption string,
	ec2Config *Ec2Config,
	tags map[string]string,
) (*Location, error) {
	b.mu.Lock("CreateLocationEfs")
	defer b.mu.Unlock()

	id := newID()
	locationArn := b.locationARN(id)
	now := time.Now().UTC()

	// EFS URI: efs://<filesystem-id>/<subdirectory>
	fsID := efsFilesystemArn
	if idx := strings.LastIndex(efsFilesystemArn, "/"); idx >= 0 {
		fsID = efsFilesystemArn[idx+1:]
	}

	sub := strings.TrimPrefix(subdirectory, "/")
	locationURI := fmt.Sprintf("efs://%s/%s", fsID, sub)

	locationTags := make(map[string]string)
	maps.Copy(locationTags, tags)

	cfg := &storedEfsConfig{
		EfsFilesystemArn:        efsFilesystemArn,
		AccessPointArn:          accessPointArn,
		FileSystemAccessRoleArn: fileSystemAccessRoleArn,
		InTransitEncryption:     inTransitEncryption,
	}
	if ec2Config != nil {
		cfg.Ec2Config = &storedEfsEc2Config{
			SubnetArn:         ec2Config.SubnetArn,
			SecurityGroupArns: ec2Config.SecurityGroupArns,
		}
	}

	l := &storedLocation{
		LocationArn:  locationArn,
		LocationURI:  locationURI,
		Subdirectory: subdirectory,
		LocationType: "EFS",
		CreationTime: now,
		Tags:         locationTags,
		Efs:          cfg,
	}
	b.locations[locationArn] = l

	if len(locationTags) > 0 {
		b.tags[locationArn] = make(map[string]string)
		maps.Copy(b.tags[locationArn], locationTags)
	}

	cp := l.toLocation()

	return &cp, nil
}

func (b *InMemoryBackend) DescribeLocationEfs(locationArn string) (*LocationEfs, error) {
	b.mu.RLock("DescribeLocationEfs")
	defer b.mu.RUnlock()

	l, ok := b.locations[locationArn]
	if !ok || l.LocationType != "EFS" {
		return nil, ErrNotFound
	}

	out := &LocationEfs{
		LocationArn:  l.LocationArn,
		LocationURI:  l.LocationURI,
		Subdirectory: l.Subdirectory,
		CreationTime: l.CreationTime,
	}

	if l.Efs != nil {
		out.EfsFilesystemArn = l.Efs.EfsFilesystemArn
		out.AccessPointArn = l.Efs.AccessPointArn
		out.FileSystemAccessRoleArn = l.Efs.FileSystemAccessRoleArn
		out.InTransitEncryption = l.Efs.InTransitEncryption

		if l.Efs.Ec2Config != nil {
			out.Ec2Config = &Ec2Config{
				SubnetArn:         l.Efs.Ec2Config.SubnetArn,
				SecurityGroupArns: l.Efs.Ec2Config.SecurityGroupArns,
			}
		}
	}

	return out, nil
}

func (b *InMemoryBackend) UpdateLocationEfs(
	locationArn, subdirectory, accessPointArn, fileSystemAccessRoleArn, inTransitEncryption string,
	ec2Config *Ec2Config,
) error {
	b.mu.Lock("UpdateLocationEfs")
	defer b.mu.Unlock()

	l, ok := b.locations[locationArn]
	if !ok || l.LocationType != "EFS" {
		return ErrNotFound
	}

	if subdirectory != "" {
		l.Subdirectory = subdirectory
		fsID := l.Efs.EfsFilesystemArn
		if idx := strings.LastIndex(fsID, "/"); idx >= 0 {
			fsID = fsID[idx+1:]
		}

		sub := strings.TrimPrefix(subdirectory, "/")
		l.LocationURI = fmt.Sprintf("efs://%s/%s", fsID, sub)
	}

	if l.Efs == nil {
		l.Efs = &storedEfsConfig{}
	}

	if accessPointArn != "" {
		l.Efs.AccessPointArn = accessPointArn
	}

	if fileSystemAccessRoleArn != "" {
		l.Efs.FileSystemAccessRoleArn = fileSystemAccessRoleArn
	}

	if inTransitEncryption != "" {
		l.Efs.InTransitEncryption = inTransitEncryption
	}

	if ec2Config != nil {
		l.Efs.Ec2Config = &storedEfsEc2Config{
			SubnetArn:         ec2Config.SubnetArn,
			SecurityGroupArns: ec2Config.SecurityGroupArns,
		}
	}

	return nil
}

// --- FsxLustre ---

func (b *InMemoryBackend) CreateLocationFsxLustre(
	fsxFilesystemArn, subdirectory string,
	securityGroupArns []string,
	tags map[string]string,
) (*Location, error) {
	b.mu.Lock("CreateLocationFsxLustre")
	defer b.mu.Unlock()

	id := newID()
	locationArn := b.locationARN(id)
	now := time.Now().UTC()

	sub := strings.TrimPrefix(subdirectory, "/")
	locationURI := fmt.Sprintf("lustre://%s/%s", fsxFilesystemArn, sub)

	locationTags := make(map[string]string)
	maps.Copy(locationTags, tags)

	l := &storedLocation{
		LocationArn:  locationArn,
		LocationURI:  locationURI,
		Subdirectory: subdirectory,
		LocationType: "FSX_LUSTRE",
		CreationTime: now,
		Tags:         locationTags,
		FsxLustre: &storedFsxLustreConfig{
			FsxFilesystemArn:  fsxFilesystemArn,
			SecurityGroupArns: securityGroupArns,
		},
	}
	b.locations[locationArn] = l

	if len(locationTags) > 0 {
		b.tags[locationArn] = make(map[string]string)
		maps.Copy(b.tags[locationArn], locationTags)
	}

	cp := l.toLocation()

	return &cp, nil
}

func (b *InMemoryBackend) DescribeLocationFsxLustre(locationArn string) (*LocationFsxLustre, error) {
	b.mu.RLock("DescribeLocationFsxLustre")
	defer b.mu.RUnlock()

	l, ok := b.locations[locationArn]
	if !ok || l.LocationType != "FSX_LUSTRE" {
		return nil, ErrNotFound
	}

	out := &LocationFsxLustre{
		LocationArn:  l.LocationArn,
		LocationURI:  l.LocationURI,
		Subdirectory: l.Subdirectory,
		CreationTime: l.CreationTime,
	}

	if l.FsxLustre != nil {
		out.FsxFilesystemArn = l.FsxLustre.FsxFilesystemArn
		out.SecurityGroupArns = l.FsxLustre.SecurityGroupArns
	}

	return out, nil
}

func (b *InMemoryBackend) UpdateLocationFsxLustre(locationArn, subdirectory string) error {
	b.mu.Lock("UpdateLocationFsxLustre")
	defer b.mu.Unlock()

	l, ok := b.locations[locationArn]
	if !ok || l.LocationType != "FSX_LUSTRE" {
		return ErrNotFound
	}

	if subdirectory != "" {
		l.Subdirectory = subdirectory
		fsArn := ""
		if l.FsxLustre != nil {
			fsArn = l.FsxLustre.FsxFilesystemArn
		}

		sub := strings.TrimPrefix(subdirectory, "/")
		l.LocationURI = fmt.Sprintf("lustre://%s/%s", fsArn, sub)
	}

	return nil
}

// --- FsxOntap ---

func toStoredFsxProtocol(p *FsxProtocol) *storedFsxProtocol {
	if p == nil {
		return nil
	}

	sp := &storedFsxProtocol{}

	if p.NFS != nil {
		sp.NFS = &storedFsxNfsProtocol{}
		if p.NFS.MountOptions != nil {
			sp.NFS.MountOptions = &storedFsxMountOptions{Version: p.NFS.MountOptions.Version}
		}
	}

	if p.SMB != nil {
		sp.SMB = &storedFsxSmbProtocol{
			Domain:   p.SMB.Domain,
			Password: p.SMB.Password,
			User:     p.SMB.User,
		}
		if p.SMB.MountOptions != nil {
			sp.SMB.MountOptions = &storedFsxMountOptions{Version: p.SMB.MountOptions.Version}
		}
	}

	return sp
}

func fromStoredFsxProtocol(sp *storedFsxProtocol) *FsxProtocol {
	if sp == nil {
		return nil
	}

	p := &FsxProtocol{}

	if sp.NFS != nil {
		p.NFS = &FsxNfsProtocol{}
		if sp.NFS.MountOptions != nil {
			p.NFS.MountOptions = &MountOptions{Version: sp.NFS.MountOptions.Version}
		}
	}

	if sp.SMB != nil {
		p.SMB = &FsxSmbProtocol{
			Domain:   sp.SMB.Domain,
			Password: sp.SMB.Password,
			User:     sp.SMB.User,
		}
		if sp.SMB.MountOptions != nil {
			p.SMB.MountOptions = &MountOptions{Version: sp.SMB.MountOptions.Version}
		}
	}

	return p
}

func (b *InMemoryBackend) CreateLocationFsxOntap(
	storageVirtualMachineArn, subdirectory string,
	protocol *FsxProtocol,
	securityGroupArns []string,
	tags map[string]string,
) (*Location, error) {
	b.mu.Lock("CreateLocationFsxOntap")
	defer b.mu.Unlock()

	id := newID()
	locationArn := b.locationARN(id)
	now := time.Now().UTC()

	sub := strings.TrimPrefix(subdirectory, "/")
	locationURI := fmt.Sprintf("ontap://%s/%s", storageVirtualMachineArn, sub)

	locationTags := make(map[string]string)
	maps.Copy(locationTags, tags)

	l := &storedLocation{
		LocationArn:  locationArn,
		LocationURI:  locationURI,
		Subdirectory: subdirectory,
		LocationType: "FSX_ONTAP",
		CreationTime: now,
		Tags:         locationTags,
		FsxOntap: &storedFsxOntapConfig{
			StorageVirtualMachineArn: storageVirtualMachineArn,
			SecurityGroupArns:        securityGroupArns,
			Protocol:                 toStoredFsxProtocol(protocol),
		},
	}
	b.locations[locationArn] = l

	if len(locationTags) > 0 {
		b.tags[locationArn] = make(map[string]string)
		maps.Copy(b.tags[locationArn], locationTags)
	}

	cp := l.toLocation()

	return &cp, nil
}

func (b *InMemoryBackend) DescribeLocationFsxOntap(locationArn string) (*LocationFsxOntap, error) {
	b.mu.RLock("DescribeLocationFsxOntap")
	defer b.mu.RUnlock()

	l, ok := b.locations[locationArn]
	if !ok || l.LocationType != "FSX_ONTAP" {
		return nil, ErrNotFound
	}

	out := &LocationFsxOntap{
		LocationArn:  l.LocationArn,
		LocationURI:  l.LocationURI,
		Subdirectory: l.Subdirectory,
		CreationTime: l.CreationTime,
	}

	if l.FsxOntap != nil {
		out.StorageVirtualMachineArn = l.FsxOntap.StorageVirtualMachineArn
		out.SecurityGroupArns = l.FsxOntap.SecurityGroupArns
		out.Protocol = fromStoredFsxProtocol(l.FsxOntap.Protocol)
	}

	return out, nil
}

func (b *InMemoryBackend) UpdateLocationFsxOntap(locationArn, subdirectory string, protocol *FsxProtocol) error {
	b.mu.Lock("UpdateLocationFsxOntap")
	defer b.mu.Unlock()

	l, ok := b.locations[locationArn]
	if !ok || l.LocationType != "FSX_ONTAP" {
		return ErrNotFound
	}

	if subdirectory != "" {
		l.Subdirectory = subdirectory
		svm := ""
		if l.FsxOntap != nil {
			svm = l.FsxOntap.StorageVirtualMachineArn
		}

		sub := strings.TrimPrefix(subdirectory, "/")
		l.LocationURI = fmt.Sprintf("ontap://%s/%s", svm, sub)
	}

	if protocol != nil && l.FsxOntap != nil {
		l.FsxOntap.Protocol = toStoredFsxProtocol(protocol)
	}

	return nil
}

// --- FsxOpenZfs ---

func (b *InMemoryBackend) CreateLocationFsxOpenZfs(
	fsxFilesystemArn, subdirectory string,
	protocol *FsxProtocol,
	securityGroupArns []string,
	tags map[string]string,
) (*Location, error) {
	b.mu.Lock("CreateLocationFsxOpenZfs")
	defer b.mu.Unlock()

	id := newID()
	locationArn := b.locationARN(id)
	now := time.Now().UTC()

	sub := strings.TrimPrefix(subdirectory, "/")
	locationURI := fmt.Sprintf("openzfs://%s/%s", fsxFilesystemArn, sub)

	locationTags := make(map[string]string)
	maps.Copy(locationTags, tags)

	l := &storedLocation{
		LocationArn:  locationArn,
		LocationURI:  locationURI,
		Subdirectory: subdirectory,
		LocationType: "FSX_OPENZFS",
		CreationTime: now,
		Tags:         locationTags,
		FsxOpenZfs: &storedFsxOpenZfsConfig{
			FsxFilesystemArn:  fsxFilesystemArn,
			SecurityGroupArns: securityGroupArns,
			Protocol:          toStoredFsxProtocol(protocol),
		},
	}
	b.locations[locationArn] = l

	if len(locationTags) > 0 {
		b.tags[locationArn] = make(map[string]string)
		maps.Copy(b.tags[locationArn], locationTags)
	}

	cp := l.toLocation()

	return &cp, nil
}

func (b *InMemoryBackend) DescribeLocationFsxOpenZfs(locationArn string) (*LocationFsxOpenZfs, error) {
	b.mu.RLock("DescribeLocationFsxOpenZfs")
	defer b.mu.RUnlock()

	l, ok := b.locations[locationArn]
	if !ok || l.LocationType != "FSX_OPENZFS" {
		return nil, ErrNotFound
	}

	out := &LocationFsxOpenZfs{
		LocationArn:  l.LocationArn,
		LocationURI:  l.LocationURI,
		Subdirectory: l.Subdirectory,
		CreationTime: l.CreationTime,
	}

	if l.FsxOpenZfs != nil {
		out.FsxFilesystemArn = l.FsxOpenZfs.FsxFilesystemArn
		out.SecurityGroupArns = l.FsxOpenZfs.SecurityGroupArns
		out.Protocol = fromStoredFsxProtocol(l.FsxOpenZfs.Protocol)
	}

	return out, nil
}

func (b *InMemoryBackend) UpdateLocationFsxOpenZfs(locationArn, subdirectory string, protocol *FsxProtocol) error {
	b.mu.Lock("UpdateLocationFsxOpenZfs")
	defer b.mu.Unlock()

	l, ok := b.locations[locationArn]
	if !ok || l.LocationType != "FSX_OPENZFS" {
		return ErrNotFound
	}

	if subdirectory != "" {
		l.Subdirectory = subdirectory
		fsArn := ""
		if l.FsxOpenZfs != nil {
			fsArn = l.FsxOpenZfs.FsxFilesystemArn
		}

		sub := strings.TrimPrefix(subdirectory, "/")
		l.LocationURI = fmt.Sprintf("openzfs://%s/%s", fsArn, sub)
	}

	if protocol != nil && l.FsxOpenZfs != nil {
		l.FsxOpenZfs.Protocol = toStoredFsxProtocol(protocol)
	}

	return nil
}

// --- FsxWindows ---

func (b *InMemoryBackend) CreateLocationFsxWindows(
	fsxFilesystemArn, subdirectory, domain, user, password string,
	securityGroupArns []string,
	tags map[string]string,
) (*Location, error) {
	b.mu.Lock("CreateLocationFsxWindows")
	defer b.mu.Unlock()

	id := newID()
	locationArn := b.locationARN(id)
	now := time.Now().UTC()

	sub := strings.TrimPrefix(subdirectory, "/")
	locationURI := fmt.Sprintf("smb://%s/%s", fsxFilesystemArn, sub)

	locationTags := make(map[string]string)
	maps.Copy(locationTags, tags)

	l := &storedLocation{
		LocationArn:  locationArn,
		LocationURI:  locationURI,
		Subdirectory: subdirectory,
		LocationType: "FSX_WINDOWS",
		CreationTime: now,
		Tags:         locationTags,
		FsxWindows: &storedFsxWindowsConfig{
			FsxFilesystemArn:  fsxFilesystemArn,
			Domain:            domain,
			User:              user,
			Password:          password,
			SecurityGroupArns: securityGroupArns,
		},
	}
	b.locations[locationArn] = l

	if len(locationTags) > 0 {
		b.tags[locationArn] = make(map[string]string)
		maps.Copy(b.tags[locationArn], locationTags)
	}

	cp := l.toLocation()

	return &cp, nil
}

func (b *InMemoryBackend) DescribeLocationFsxWindows(locationArn string) (*LocationFsxWindows, error) {
	b.mu.RLock("DescribeLocationFsxWindows")
	defer b.mu.RUnlock()

	l, ok := b.locations[locationArn]
	if !ok || l.LocationType != "FSX_WINDOWS" {
		return nil, ErrNotFound
	}

	out := &LocationFsxWindows{
		LocationArn:  l.LocationArn,
		LocationURI:  l.LocationURI,
		Subdirectory: l.Subdirectory,
		CreationTime: l.CreationTime,
	}

	if l.FsxWindows != nil {
		out.FsxFilesystemArn = l.FsxWindows.FsxFilesystemArn
		out.Domain = l.FsxWindows.Domain
		out.User = l.FsxWindows.User
		out.SecurityGroupArns = l.FsxWindows.SecurityGroupArns
	}

	return out, nil
}

func (b *InMemoryBackend) UpdateLocationFsxWindows(locationArn, subdirectory, domain, user, password string) error {
	b.mu.Lock("UpdateLocationFsxWindows")
	defer b.mu.Unlock()

	l, ok := b.locations[locationArn]
	if !ok || l.LocationType != "FSX_WINDOWS" {
		return ErrNotFound
	}

	if l.FsxWindows == nil {
		l.FsxWindows = &storedFsxWindowsConfig{}
	}

	if subdirectory != "" {
		l.Subdirectory = subdirectory
		sub := strings.TrimPrefix(subdirectory, "/")
		l.LocationURI = fmt.Sprintf("smb://%s/%s", l.FsxWindows.FsxFilesystemArn, sub)
	}

	if domain != "" {
		l.FsxWindows.Domain = domain
	}

	if user != "" {
		l.FsxWindows.User = user
	}

	if password != "" {
		l.FsxWindows.Password = password
	}

	return nil
}

// --- HDFS ---

func (b *InMemoryBackend) CreateLocationHdfs(
	subdirectory, authenticationType, simpleUser string,
	kerberosPrincipal, kerberosKeytab, kerberosKrb5Conf, kmsKeyProviderURI string,
	nameNodes []HdfsNameNode,
	blockSize int64,
	replicationFactor int32,
	qopConfig *QopConfiguration,
	agentArns []string,
	tags map[string]string,
) (*Location, error) {
	b.mu.Lock("CreateLocationHdfs")
	defer b.mu.Unlock()

	id := newID()
	locationArn := b.locationARN(id)
	now := time.Now().UTC()

	host := "hdfs"
	if len(nameNodes) > 0 {
		host = fmt.Sprintf("%s:%d", nameNodes[0].Hostname, nameNodes[0].Port)
	}

	sub := strings.TrimPrefix(subdirectory, "/")
	locationURI := fmt.Sprintf("hdfs://%s/%s", host, sub)

	locationTags := make(map[string]string)
	maps.Copy(locationTags, tags)

	storedNodes := make([]storedHdfsNameNode, len(nameNodes))
	for i, n := range nameNodes {
		storedNodes[i] = storedHdfsNameNode{Hostname: n.Hostname, Port: n.Port}
	}

	cfg := &storedHdfsConfig{
		NameNodes:          storedNodes,
		AuthenticationType: authenticationType,
		SimpleUser:         simpleUser,
		KerberosPrincipal:  kerberosPrincipal,
		KerberosKeytab:     kerberosKeytab,
		KerberosKrb5Conf:   kerberosKrb5Conf,
		KmsKeyProviderURI:  kmsKeyProviderURI,
		BlockSize:          blockSize,
		ReplicationFactor:  replicationFactor,
		AgentArns:          agentArns,
	}

	if qopConfig != nil {
		cfg.QopConfiguration = &storedQopConfig{
			DataTransferProtection: qopConfig.DataTransferProtection,
			RpcProtection:          qopConfig.RpcProtection,
		}
	}

	l := &storedLocation{
		LocationArn:  locationArn,
		LocationURI:  locationURI,
		Subdirectory: subdirectory,
		LocationType: "HDFS",
		CreationTime: now,
		Tags:         locationTags,
		Hdfs:         cfg,
	}
	b.locations[locationArn] = l

	if len(locationTags) > 0 {
		b.tags[locationArn] = make(map[string]string)
		maps.Copy(b.tags[locationArn], locationTags)
	}

	cp := l.toLocation()

	return &cp, nil
}

func (b *InMemoryBackend) DescribeLocationHdfs(locationArn string) (*LocationHdfs, error) {
	b.mu.RLock("DescribeLocationHdfs")
	defer b.mu.RUnlock()

	l, ok := b.locations[locationArn]
	if !ok || l.LocationType != "HDFS" {
		return nil, ErrNotFound
	}

	out := &LocationHdfs{
		LocationArn:  l.LocationArn,
		LocationURI:  l.LocationURI,
		Subdirectory: l.Subdirectory,
		CreationTime: l.CreationTime,
	}

	if l.Hdfs != nil {
		out.AuthenticationType = l.Hdfs.AuthenticationType
		out.SimpleUser = l.Hdfs.SimpleUser
		out.KerberosPrincipal = l.Hdfs.KerberosPrincipal
		out.KmsKeyProviderURI = l.Hdfs.KmsKeyProviderURI
		out.BlockSize = l.Hdfs.BlockSize
		out.ReplicationFactor = l.Hdfs.ReplicationFactor
		out.AgentArns = l.Hdfs.AgentArns

		nodes := make([]HdfsNameNode, len(l.Hdfs.NameNodes))
		for i, n := range l.Hdfs.NameNodes {
			nodes[i] = HdfsNameNode{Hostname: n.Hostname, Port: n.Port}
		}

		out.NameNodes = nodes

		if l.Hdfs.QopConfiguration != nil {
			out.QopConfiguration = &QopConfiguration{
				DataTransferProtection: l.Hdfs.QopConfiguration.DataTransferProtection,
				RpcProtection:          l.Hdfs.QopConfiguration.RpcProtection,
			}
		}
	}

	return out, nil
}

func (b *InMemoryBackend) UpdateLocationHdfs(
	locationArn, subdirectory, authenticationType, simpleUser string,
	kerberosPrincipal, kerberosKeytab, kerberosKrb5Conf, kmsKeyProviderURI string,
	nameNodes []HdfsNameNode,
	blockSize int64,
	replicationFactor int32,
	qopConfig *QopConfiguration,
	agentArns []string,
) error {
	b.mu.Lock("UpdateLocationHdfs")
	defer b.mu.Unlock()

	l, ok := b.locations[locationArn]
	if !ok || l.LocationType != "HDFS" {
		return ErrNotFound
	}

	if l.Hdfs == nil {
		l.Hdfs = &storedHdfsConfig{}
	}

	if subdirectory != "" {
		l.Subdirectory = subdirectory
		host := "hdfs"
		if len(l.Hdfs.NameNodes) > 0 {
			host = fmt.Sprintf("%s:%d", l.Hdfs.NameNodes[0].Hostname, l.Hdfs.NameNodes[0].Port)
		}

		sub := strings.TrimPrefix(subdirectory, "/")
		l.LocationURI = fmt.Sprintf("hdfs://%s/%s", host, sub)
	}

	if len(nameNodes) > 0 {
		storedNodes := make([]storedHdfsNameNode, len(nameNodes))
		for i, n := range nameNodes {
			storedNodes[i] = storedHdfsNameNode{Hostname: n.Hostname, Port: n.Port}
		}

		l.Hdfs.NameNodes = storedNodes
	}

	if authenticationType != "" {
		l.Hdfs.AuthenticationType = authenticationType
	}

	if simpleUser != "" {
		l.Hdfs.SimpleUser = simpleUser
	}

	if kerberosPrincipal != "" {
		l.Hdfs.KerberosPrincipal = kerberosPrincipal
	}

	if kerberosKeytab != "" {
		l.Hdfs.KerberosKeytab = kerberosKeytab
	}

	if kerberosKrb5Conf != "" {
		l.Hdfs.KerberosKrb5Conf = kerberosKrb5Conf
	}

	if kmsKeyProviderURI != "" {
		l.Hdfs.KmsKeyProviderURI = kmsKeyProviderURI
	}

	if blockSize > 0 {
		l.Hdfs.BlockSize = blockSize
	}

	if replicationFactor > 0 {
		l.Hdfs.ReplicationFactor = replicationFactor
	}

	if agentArns != nil {
		l.Hdfs.AgentArns = agentArns
	}

	if qopConfig != nil {
		l.Hdfs.QopConfiguration = &storedQopConfig{
			DataTransferProtection: qopConfig.DataTransferProtection,
			RpcProtection:          qopConfig.RpcProtection,
		}
	}

	return nil
}

// --- NFS ---

func (b *InMemoryBackend) CreateLocationNfs(
	serverHostname, subdirectory string,
	mountOptions *MountOptions,
	agentArns []string,
	tags map[string]string,
) (*Location, error) {
	b.mu.Lock("CreateLocationNfs")
	defer b.mu.Unlock()

	id := newID()
	locationArn := b.locationARN(id)
	now := time.Now().UTC()

	sub := strings.TrimPrefix(subdirectory, "/")
	locationURI := fmt.Sprintf("nfs://%s/%s", serverHostname, sub)

	locationTags := make(map[string]string)
	maps.Copy(locationTags, tags)

	cfg := &storedNfsConfig{
		ServerHostname: serverHostname,
		AgentArns:      agentArns,
	}

	if mountOptions != nil {
		cfg.MountOptions = &storedMountOptions{Version: mountOptions.Version}
	}

	l := &storedLocation{
		LocationArn:  locationArn,
		LocationURI:  locationURI,
		Subdirectory: subdirectory,
		LocationType: "NFS",
		CreationTime: now,
		Tags:         locationTags,
		Nfs:          cfg,
	}
	b.locations[locationArn] = l

	if len(locationTags) > 0 {
		b.tags[locationArn] = make(map[string]string)
		maps.Copy(b.tags[locationArn], locationTags)
	}

	cp := l.toLocation()

	return &cp, nil
}

func (b *InMemoryBackend) DescribeLocationNfs(locationArn string) (*LocationNfs, error) {
	b.mu.RLock("DescribeLocationNfs")
	defer b.mu.RUnlock()

	l, ok := b.locations[locationArn]
	if !ok || l.LocationType != "NFS" {
		return nil, ErrNotFound
	}

	out := &LocationNfs{
		LocationArn:  l.LocationArn,
		LocationURI:  l.LocationURI,
		Subdirectory: l.Subdirectory,
		CreationTime: l.CreationTime,
	}

	if l.Nfs != nil {
		out.ServerHostname = l.Nfs.ServerHostname
		out.AgentArns = l.Nfs.AgentArns

		if l.Nfs.MountOptions != nil {
			out.MountOptions = &MountOptions{Version: l.Nfs.MountOptions.Version}
		}
	}

	return out, nil
}

func (b *InMemoryBackend) UpdateLocationNfs(
	locationArn, subdirectory string,
	mountOptions *MountOptions,
	agentArns []string,
) error {
	b.mu.Lock("UpdateLocationNfs")
	defer b.mu.Unlock()

	l, ok := b.locations[locationArn]
	if !ok || l.LocationType != "NFS" {
		return ErrNotFound
	}

	if l.Nfs == nil {
		l.Nfs = &storedNfsConfig{}
	}

	if subdirectory != "" {
		l.Subdirectory = subdirectory
		sub := strings.TrimPrefix(subdirectory, "/")
		l.LocationURI = fmt.Sprintf("nfs://%s/%s", l.Nfs.ServerHostname, sub)
	}

	if mountOptions != nil {
		l.Nfs.MountOptions = &storedMountOptions{Version: mountOptions.Version}
	}

	if agentArns != nil {
		l.Nfs.AgentArns = agentArns
	}

	return nil
}

// --- ObjectStorage ---

func (b *InMemoryBackend) CreateLocationObjectStorage(
	serverHostname, serverProtocol, bucketName, subdirectory, accessKey, secretKey string,
	serverPort int32,
	agentArns []string,
	tags map[string]string,
) (*Location, error) {
	b.mu.Lock("CreateLocationObjectStorage")
	defer b.mu.Unlock()

	id := newID()
	locationArn := b.locationARN(id)
	now := time.Now().UTC()

	sub := strings.TrimPrefix(subdirectory, "/")
	locationURI := fmt.Sprintf("object-storage://%s/%s/%s", serverHostname, bucketName, sub)

	locationTags := make(map[string]string)
	maps.Copy(locationTags, tags)

	l := &storedLocation{
		LocationArn:  locationArn,
		LocationURI:  locationURI,
		Subdirectory: subdirectory,
		LocationType: "OBJECT_STORAGE",
		CreationTime: now,
		Tags:         locationTags,
		ObjectStorage: &storedObjectStorageConfig{
			ServerHostname: serverHostname,
			ServerProtocol: serverProtocol,
			BucketName:     bucketName,
			AccessKey:      accessKey,
			SecretKey:      secretKey,
			ServerPort:     serverPort,
			AgentArns:      agentArns,
		},
	}
	b.locations[locationArn] = l

	if len(locationTags) > 0 {
		b.tags[locationArn] = make(map[string]string)
		maps.Copy(b.tags[locationArn], locationTags)
	}

	cp := l.toLocation()

	return &cp, nil
}

func (b *InMemoryBackend) DescribeLocationObjectStorage(locationArn string) (*LocationObjectStorage, error) {
	b.mu.RLock("DescribeLocationObjectStorage")
	defer b.mu.RUnlock()

	l, ok := b.locations[locationArn]
	if !ok || l.LocationType != "OBJECT_STORAGE" {
		return nil, ErrNotFound
	}

	out := &LocationObjectStorage{
		LocationArn:  l.LocationArn,
		LocationURI:  l.LocationURI,
		Subdirectory: l.Subdirectory,
		CreationTime: l.CreationTime,
	}

	if l.ObjectStorage != nil {
		out.ServerHostname = l.ObjectStorage.ServerHostname
		out.ServerProtocol = l.ObjectStorage.ServerProtocol
		out.BucketName = l.ObjectStorage.BucketName
		out.AccessKey = l.ObjectStorage.AccessKey
		out.ServerPort = l.ObjectStorage.ServerPort
		out.AgentArns = l.ObjectStorage.AgentArns
	}

	return out, nil
}

func (b *InMemoryBackend) UpdateLocationObjectStorage(
	locationArn, serverProtocol, subdirectory, accessKey, secretKey string,
	serverPort int32,
	agentArns []string,
) error {
	b.mu.Lock("UpdateLocationObjectStorage")
	defer b.mu.Unlock()

	l, ok := b.locations[locationArn]
	if !ok || l.LocationType != "OBJECT_STORAGE" {
		return ErrNotFound
	}

	if l.ObjectStorage == nil {
		l.ObjectStorage = &storedObjectStorageConfig{}
	}

	if subdirectory != "" {
		l.Subdirectory = subdirectory
		sub := strings.TrimPrefix(subdirectory, "/")
		l.LocationURI = fmt.Sprintf(
			"object-storage://%s/%s/%s",
			l.ObjectStorage.ServerHostname,
			l.ObjectStorage.BucketName,
			sub,
		)
	}

	if serverProtocol != "" {
		l.ObjectStorage.ServerProtocol = serverProtocol
	}

	if accessKey != "" {
		l.ObjectStorage.AccessKey = accessKey
	}

	if secretKey != "" {
		l.ObjectStorage.SecretKey = secretKey
	}

	if serverPort > 0 {
		l.ObjectStorage.ServerPort = serverPort
	}

	if agentArns != nil {
		l.ObjectStorage.AgentArns = agentArns
	}

	return nil
}

// --- SMB ---

func (b *InMemoryBackend) CreateLocationSmb(
	serverHostname, subdirectory, domain, user, password string,
	mountOptions *MountOptions,
	agentArns []string,
	tags map[string]string,
) (*Location, error) {
	b.mu.Lock("CreateLocationSmb")
	defer b.mu.Unlock()

	id := newID()
	locationArn := b.locationARN(id)
	now := time.Now().UTC()

	sub := strings.TrimPrefix(subdirectory, "/")
	locationURI := fmt.Sprintf("smb://%s/%s", serverHostname, sub)

	locationTags := make(map[string]string)
	maps.Copy(locationTags, tags)

	cfg := &storedSmbConfig{
		ServerHostname: serverHostname,
		Domain:         domain,
		User:           user,
		Password:       password,
		AgentArns:      agentArns,
	}

	if mountOptions != nil {
		cfg.MountOptions = &storedMountOptions{Version: mountOptions.Version}
	}

	l := &storedLocation{
		LocationArn:  locationArn,
		LocationURI:  locationURI,
		Subdirectory: subdirectory,
		LocationType: "SMB",
		CreationTime: now,
		Tags:         locationTags,
		Smb:          cfg,
	}
	b.locations[locationArn] = l

	if len(locationTags) > 0 {
		b.tags[locationArn] = make(map[string]string)
		maps.Copy(b.tags[locationArn], locationTags)
	}

	cp := l.toLocation()

	return &cp, nil
}

func (b *InMemoryBackend) DescribeLocationSmb(locationArn string) (*LocationSmb, error) {
	b.mu.RLock("DescribeLocationSmb")
	defer b.mu.RUnlock()

	l, ok := b.locations[locationArn]
	if !ok || l.LocationType != "SMB" {
		return nil, ErrNotFound
	}

	out := &LocationSmb{
		LocationArn:  l.LocationArn,
		LocationURI:  l.LocationURI,
		Subdirectory: l.Subdirectory,
		CreationTime: l.CreationTime,
	}

	if l.Smb != nil {
		out.ServerHostname = l.Smb.ServerHostname
		out.Domain = l.Smb.Domain
		out.User = l.Smb.User
		out.AgentArns = l.Smb.AgentArns

		if l.Smb.MountOptions != nil {
			out.MountOptions = &MountOptions{Version: l.Smb.MountOptions.Version}
		}
	}

	return out, nil
}

func (b *InMemoryBackend) UpdateLocationSmb(
	locationArn, subdirectory, domain, user, password string,
	mountOptions *MountOptions,
	agentArns []string,
) error {
	b.mu.Lock("UpdateLocationSmb")
	defer b.mu.Unlock()

	l, ok := b.locations[locationArn]
	if !ok || l.LocationType != "SMB" {
		return ErrNotFound
	}

	if l.Smb == nil {
		l.Smb = &storedSmbConfig{}
	}

	if subdirectory != "" {
		l.Subdirectory = subdirectory
		sub := strings.TrimPrefix(subdirectory, "/")
		l.LocationURI = fmt.Sprintf("smb://%s/%s", l.Smb.ServerHostname, sub)
	}

	if domain != "" {
		l.Smb.Domain = domain
	}

	if user != "" {
		l.Smb.User = user
	}

	if password != "" {
		l.Smb.Password = password
	}

	if mountOptions != nil {
		l.Smb.MountOptions = &storedMountOptions{Version: mountOptions.Version}
	}

	if agentArns != nil {
		l.Smb.AgentArns = agentArns
	}

	return nil
}

// extractBucketName extracts the bucket name from an S3 ARN.
// Format: arn:aws:s3:::bucket-name or arn:aws:s3:::bucket-name/prefix.
func extractBucketName(s3BucketArn string) string {
	// S3 ARNs: arn:aws:s3:::bucket-name
	parts := strings.SplitN(s3BucketArn, ":::", arnSplitParts)
	if len(parts) == arnSplitParts {
		name := strings.SplitN(parts[1], "/", arnSplitParts)[0]
		if name != "" {
			return name
		}
	}

	return "unknown-bucket"
}

// extractTaskArnFromExecution extracts the task ARN from a task execution ARN.
// Execution ARN format: arn:aws:datasync:region:account:task/<task-id>/execution/<exec-id>.
func extractTaskArnFromExecution(execArn string) string {
	// Find /execution/ suffix and strip it.
	idx := strings.LastIndex(execArn, "/execution/")
	if idx < 0 {
		return ""
	}

	return execArn[:idx]
}

package datasync

import "time"

// StorageBackend is the interface for DataSync storage operations.
type StorageBackend interface {
	// Agent operations
	CreateAgent(name string, activationKey string, tags map[string]string) (*Agent, error)
	DescribeAgent(agentArn string) (*Agent, error)
	UpdateAgent(agentArn, name string) error
	DeleteAgent(agentArn string) error
	ListAgents(maxResults int32, nextToken string) ([]*AgentListEntry, string, error)

	// Location operations (S3)
	CreateLocationS3(
		subdirectory, s3BucketArn, s3StorageClass string,
		s3Config S3Config,
		tags map[string]string,
	) (*Location, error)
	DescribeLocationS3(locationArn string) (*LocationS3, error)
	DeleteLocation(locationArn string) error
	ListLocations(maxResults int32, nextToken string) ([]*LocationListEntry, string, error)

	// Task operations
	CreateTask(
		sourceLocationArn, destinationLocationArn, name, cloudWatchLogGroupArn string,
		tags map[string]string,
	) (*Task, error)
	DescribeTask(taskArn string) (*Task, error)
	UpdateTask(taskArn, name, cloudWatchLogGroupArn string) error
	DeleteTask(taskArn string) error
	ListTasks(maxResults int32, nextToken string) ([]*TaskListEntry, string, error)

	// Task execution operations
	StartTaskExecution(taskArn string) (*TaskExecution, error)
	CancelTaskExecution(taskExecutionArn string) error
	DescribeTaskExecution(taskExecutionArn string) (*TaskExecution, error)
	ListTaskExecutions(taskArn string, maxResults int32, nextToken string) ([]*TaskExecutionListEntry, string, error)

	// Tag operations
	TagResource(resourceArn string, tags map[string]string) error
	UntagResource(resourceArn string, keys []string) error
	ListTagsForResource(resourceArn string, maxResults int32, nextToken string) (map[string]string, string, error)

	AccountID() string
	Region() string
	Reset()
	Snapshot() []byte
	Restore(data []byte) error
}

// Agent represents a DataSync agent.
// CreationTime is first: time.Time's non-pointer prefix reduces GC pointer bytes.
type Agent struct {
	CreationTime time.Time
	Tags         map[string]string
	AgentArn     string
	Name         string
	Status       string
	EndpointType string
}

// AgentListEntry is an agent entry in a list response.
type AgentListEntry struct {
	AgentArn string
	Name     string
	Status   string
}

// S3Config holds IAM role configuration for an S3 location.
type S3Config struct {
	BucketAccessRoleArn string
}

// Location is a generic DataSync location.
// CreationTime is first: time.Time's non-pointer prefix reduces GC pointer bytes.
type Location struct {
	CreationTime time.Time
	LocationArn  string
	LocationURI  string
}

// LocationS3 is a DataSync S3 location with full details.
// CreationTime is first: time.Time's non-pointer prefix reduces GC pointer bytes.
type LocationS3 struct {
	CreationTime   time.Time
	S3Config       S3Config
	LocationArn    string
	LocationURI    string
	S3BucketArn    string
	Subdirectory   string
	S3StorageClass string
}

// LocationListEntry is a location entry in a list response.
type LocationListEntry struct {
	CreationTime time.Time
	LocationArn  string
	LocationURI  string
}

// Task represents a DataSync transfer task.
// CreationTime is first: time.Time's non-pointer prefix reduces GC pointer bytes.
type Task struct {
	CreationTime            time.Time
	Tags                    map[string]string
	TaskArn                 string
	Name                    string
	Status                  string
	SourceLocationArn       string
	DestinationLocationArn  string
	CloudWatchLogGroupArn   string
	CurrentTaskExecutionArn string
}

// TaskListEntry is a task entry in a list response.
type TaskListEntry struct {
	TaskArn string
	Name    string
	Status  string
}

// TaskExecution represents a DataSync task execution.
// StartTime is first: time.Time's non-pointer prefix reduces GC pointer bytes.
type TaskExecution struct {
	StartTime                time.Time
	TaskExecutionArn         string
	Status                   string
	EstimatedFilesToTransfer int64
	EstimatedBytesToTransfer int64
	FilesTransferred         int64
	BytesTransferred         int64
}

// TaskExecutionListEntry is a task execution entry in a list response.
type TaskExecutionListEntry struct {
	TaskExecutionArn string
	Status           string
}

var _ StorageBackend = (*InMemoryBackend)(nil)

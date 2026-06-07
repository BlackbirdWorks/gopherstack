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

	// Location operations (S3 update)
	UpdateLocationS3(locationArn, subdirectory, s3StorageClass string, s3Config S3Config) error

	// Task execution update
	UpdateTaskExecution(taskExecutionArn string) error

	// Location operations (Azure Blob)
	CreateLocationAzureBlob(
		containerURL, subdirectory, blobType, accessTier string,
		sasConfig *SasConfiguration,
		agentArns []string,
		tags map[string]string,
	) (*Location, error)
	DescribeLocationAzureBlob(locationArn string) (*LocationAzureBlob, error)
	UpdateLocationAzureBlob(
		locationArn, containerURL, subdirectory, blobType, accessTier string,
		sasConfig *SasConfiguration,
		agentArns []string,
	) error

	// Location operations (EFS)
	CreateLocationEfs(
		efsFilesystemArn, subdirectory, accessPointArn, fileSystemAccessRoleArn, inTransitEncryption string,
		ec2Config *Ec2Config,
		tags map[string]string,
	) (*Location, error)
	DescribeLocationEfs(locationArn string) (*LocationEfs, error)
	UpdateLocationEfs(
		locationArn, subdirectory, accessPointArn, fileSystemAccessRoleArn, inTransitEncryption string,
		ec2Config *Ec2Config,
	) error

	// Location operations (FSx Lustre)
	CreateLocationFsxLustre(
		fsxFilesystemArn, subdirectory string,
		securityGroupArns []string,
		tags map[string]string,
	) (*Location, error)
	DescribeLocationFsxLustre(locationArn string) (*LocationFsxLustre, error)
	UpdateLocationFsxLustre(locationArn, subdirectory string) error

	// Location operations (FSx ONTAP)
	CreateLocationFsxOntap(
		storageVirtualMachineArn, subdirectory string,
		protocol *FsxProtocol,
		securityGroupArns []string,
		tags map[string]string,
	) (*Location, error)
	DescribeLocationFsxOntap(locationArn string) (*LocationFsxOntap, error)
	UpdateLocationFsxOntap(locationArn, subdirectory string, protocol *FsxProtocol) error

	// Location operations (FSx OpenZFS)
	CreateLocationFsxOpenZfs(
		fsxFilesystemArn, subdirectory string,
		protocol *FsxProtocol,
		securityGroupArns []string,
		tags map[string]string,
	) (*Location, error)
	DescribeLocationFsxOpenZfs(locationArn string) (*LocationFsxOpenZfs, error)
	UpdateLocationFsxOpenZfs(locationArn, subdirectory string, protocol *FsxProtocol) error

	// Location operations (FSx Windows)
	CreateLocationFsxWindows(
		fsxFilesystemArn, subdirectory, domain, user, password string,
		securityGroupArns []string,
		tags map[string]string,
	) (*Location, error)
	DescribeLocationFsxWindows(locationArn string) (*LocationFsxWindows, error)
	UpdateLocationFsxWindows(locationArn, subdirectory, domain, user, password string) error

	// Location operations (HDFS)
	CreateLocationHdfs(
		subdirectory, authenticationType, simpleUser string,
		kerberosPrincipal, kerberosKeytab, kerberosKrb5Conf, kmsKeyProviderURI string,
		nameNodes []HdfsNameNode,
		blockSize int64,
		replicationFactor int32,
		qopConfig *QopConfiguration,
		agentArns []string,
		tags map[string]string,
	) (*Location, error)
	DescribeLocationHdfs(locationArn string) (*LocationHdfs, error)
	UpdateLocationHdfs(
		locationArn, subdirectory, authenticationType, simpleUser string,
		kerberosPrincipal, kerberosKeytab, kerberosKrb5Conf, kmsKeyProviderURI string,
		nameNodes []HdfsNameNode,
		blockSize int64,
		replicationFactor int32,
		qopConfig *QopConfiguration,
		agentArns []string,
	) error

	// Location operations (NFS)
	CreateLocationNfs(
		serverHostname, subdirectory string,
		mountOptions *MountOptions,
		agentArns []string,
		tags map[string]string,
	) (*Location, error)
	DescribeLocationNfs(locationArn string) (*LocationNfs, error)
	UpdateLocationNfs(locationArn, subdirectory string, mountOptions *MountOptions, agentArns []string) error

	// Location operations (Object Storage)
	CreateLocationObjectStorage(
		serverHostname, serverProtocol, bucketName, subdirectory, accessKey, secretKey string,
		serverPort int32,
		agentArns []string,
		tags map[string]string,
	) (*Location, error)
	DescribeLocationObjectStorage(locationArn string) (*LocationObjectStorage, error)
	UpdateLocationObjectStorage(
		locationArn, serverProtocol, subdirectory, accessKey, secretKey string,
		serverPort int32,
		agentArns []string,
	) error

	// Location operations (SMB)
	CreateLocationSmb(
		serverHostname, subdirectory, domain, user, password string,
		mountOptions *MountOptions,
		agentArns []string,
		tags map[string]string,
	) (*Location, error)
	DescribeLocationSmb(locationArn string) (*LocationSmb, error)
	UpdateLocationSmb(
		locationArn, subdirectory, domain, user, password string,
		mountOptions *MountOptions,
		agentArns []string,
	) error

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

// SasConfiguration holds Azure Blob SAS token configuration.
type SasConfiguration struct {
	Token string
}

// LocationAzureBlob is a DataSync Azure Blob location with full details.
// CreationTime is first: time.Time's non-pointer prefix reduces GC pointer bytes.
type LocationAzureBlob struct {
	CreationTime     time.Time
	SasConfiguration *SasConfiguration
	AgentArns        []string
	LocationArn      string
	LocationURI      string
	ContainerURL     string
	BlobType         string
	AccessTier       string
	Subdirectory     string
}

// Ec2Config holds EC2 configuration for an EFS location.
type Ec2Config struct {
	SecurityGroupArns []string
	SubnetArn         string
}

// LocationEfs is a DataSync EFS location with full details.
// CreationTime is first: time.Time's non-pointer prefix reduces GC pointer bytes.
type LocationEfs struct {
	CreationTime            time.Time
	Ec2Config               *Ec2Config
	LocationArn             string
	LocationURI             string
	EfsFilesystemArn        string
	AccessPointArn          string
	FileSystemAccessRoleArn string
	InTransitEncryption     string
	Subdirectory            string
}

// LocationFsxLustre is a DataSync FSx Lustre location with full details.
// CreationTime is first: time.Time's non-pointer prefix reduces GC pointer bytes.
type LocationFsxLustre struct {
	CreationTime      time.Time
	SecurityGroupArns []string
	LocationArn       string
	LocationURI       string
	FsxFilesystemArn  string
	Subdirectory      string
}

// MountOptions holds NFS/SMB mount version.
type MountOptions struct {
	Version string
}

// FsxNfsProtocol holds FSx NFS protocol configuration.
type FsxNfsProtocol struct {
	MountOptions *MountOptions
}

// FsxSmbProtocol holds FSx SMB protocol configuration.
type FsxSmbProtocol struct {
	MountOptions *MountOptions
	Domain       string
	Password     string
	User         string
}

// FsxProtocol holds FSx protocol configuration (NFS or SMB).
type FsxProtocol struct {
	NFS *FsxNfsProtocol
	SMB *FsxSmbProtocol
}

// LocationFsxOntap is a DataSync FSx ONTAP location with full details.
// CreationTime is first: time.Time's non-pointer prefix reduces GC pointer bytes.
type LocationFsxOntap struct {
	CreationTime             time.Time
	Protocol                 *FsxProtocol
	SecurityGroupArns        []string
	LocationArn              string
	LocationURI              string
	StorageVirtualMachineArn string
	Subdirectory             string
}

// LocationFsxOpenZfs is a DataSync FSx OpenZFS location with full details.
// CreationTime is first: time.Time's non-pointer prefix reduces GC pointer bytes.
type LocationFsxOpenZfs struct {
	CreationTime      time.Time
	Protocol          *FsxProtocol
	SecurityGroupArns []string
	LocationArn       string
	LocationURI       string
	FsxFilesystemArn  string
	Subdirectory      string
}

// LocationFsxWindows is a DataSync FSx Windows location with full details.
// CreationTime is first: time.Time's non-pointer prefix reduces GC pointer bytes.
type LocationFsxWindows struct {
	CreationTime      time.Time
	SecurityGroupArns []string
	LocationArn       string
	LocationURI       string
	FsxFilesystemArn  string
	Domain            string
	User              string
	Subdirectory      string
}

// HdfsNameNode is an HDFS name node endpoint.
type HdfsNameNode struct {
	Hostname string
	Port     int32
}

// QopConfiguration holds HDFS QOP configuration.
type QopConfiguration struct {
	DataTransferProtection string
	RPCProtection          string
}

// LocationHdfs is a DataSync HDFS location with full details.
// CreationTime is first: time.Time's non-pointer prefix reduces GC pointer bytes.
type LocationHdfs struct {
	CreationTime       time.Time
	QopConfiguration   *QopConfiguration
	NameNodes          []HdfsNameNode
	AgentArns          []string
	LocationArn        string
	LocationURI        string
	AuthenticationType string
	SimpleUser         string
	KerberosPrincipal  string
	KmsKeyProviderURI  string
	Subdirectory       string
	BlockSize          int64
	ReplicationFactor  int32
}

// LocationNfs is a DataSync NFS location with full details.
// CreationTime is first: time.Time's non-pointer prefix reduces GC pointer bytes.
type LocationNfs struct {
	CreationTime   time.Time
	MountOptions   *MountOptions
	AgentArns      []string
	LocationArn    string
	LocationURI    string
	ServerHostname string
	Subdirectory   string
}

// LocationObjectStorage is a DataSync object storage location with full details.
// CreationTime is first: time.Time's non-pointer prefix reduces GC pointer bytes.
type LocationObjectStorage struct {
	CreationTime   time.Time
	AgentArns      []string
	LocationArn    string
	LocationURI    string
	ServerHostname string
	ServerProtocol string
	BucketName     string
	AccessKey      string
	Subdirectory   string
	ServerPort     int32
}

// LocationSmb is a DataSync SMB location with full details.
// CreationTime is first: time.Time's non-pointer prefix reduces GC pointer bytes.
type LocationSmb struct {
	CreationTime   time.Time
	MountOptions   *MountOptions
	AgentArns      []string
	LocationArn    string
	LocationURI    string
	ServerHostname string
	Domain         string
	User           string
	Subdirectory   string
}

var _ StorageBackend = (*InMemoryBackend)(nil)

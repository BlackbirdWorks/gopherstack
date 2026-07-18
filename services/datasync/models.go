package datasync

import (
	"maps"
	"time"
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
	ContainerURL string   `json:"containerUrl"`
	BlobType     string   `json:"blobType,omitempty"`
	AccessTier   string   `json:"accessTier,omitempty"`
	AgentArns    []string `json:"agentArns,omitempty"`
}

type storedEfsEc2Config struct {
	SubnetArn         string   `json:"subnetArn"`
	SecurityGroupArns []string `json:"securityGroupArns"`
}

type storedEfsConfig struct {
	Ec2Config               *storedEfsEc2Config `json:"ec2Config,omitempty"`
	EfsFilesystemArn        string              `json:"efsFilesystemArn"`
	AccessPointArn          string              `json:"accessPointArn,omitempty"`
	FileSystemAccessRoleArn string              `json:"fileSystemAccessRoleArn,omitempty"`
	InTransitEncryption     string              `json:"inTransitEncryption,omitempty"`
}

type storedFsxLustreConfig struct {
	FsxFilesystemArn  string   `json:"fsxFilesystemArn"`
	SecurityGroupArns []string `json:"securityGroupArns,omitempty"`
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
	StorageVirtualMachineArn string             `json:"storageVirtualMachineArn"`
	SecurityGroupArns        []string           `json:"securityGroupArns,omitempty"`
}

type storedFsxOpenZfsConfig struct {
	Protocol          *storedFsxProtocol `json:"protocol,omitempty"`
	FsxFilesystemArn  string             `json:"fsxFilesystemArn"`
	SecurityGroupArns []string           `json:"securityGroupArns,omitempty"`
}

type storedFsxWindowsConfig struct {
	FsxFilesystemArn  string   `json:"fsxFilesystemArn"`
	Domain            string   `json:"domain,omitempty"`
	User              string   `json:"user,omitempty"`
	Password          string   `json:"password,omitempty"`
	SecurityGroupArns []string `json:"securityGroupArns,omitempty"`
}

type storedHdfsNameNode struct {
	Hostname string `json:"hostname"`
	Port     int32  `json:"port"`
}

type storedQopConfig struct {
	DataTransferProtection string `json:"dataTransferProtection,omitempty"`
	RPCProtection          string `json:"rpcProtection,omitempty"`
}

type storedHdfsConfig struct {
	QopConfiguration   *storedQopConfig     `json:"qopConfiguration,omitempty"`
	KerberosPrincipal  string               `json:"kerberosPrincipal,omitempty"`
	KerberosKeytab     string               `json:"kerberosKeytab,omitempty"`
	KerberosKrb5Conf   string               `json:"kerberosKrb5Conf,omitempty"`
	KmsKeyProviderURI  string               `json:"kmsKeyProviderUri,omitempty"`
	AuthenticationType string               `json:"authenticationType,omitempty"`
	SimpleUser         string               `json:"simpleUser,omitempty"`
	NameNodes          []storedHdfsNameNode `json:"nameNodes"`
	AgentArns          []string             `json:"agentArns,omitempty"`
	BlockSize          int64                `json:"blockSize,omitempty"`
	ReplicationFactor  int32                `json:"replicationFactor,omitempty"`
}

type storedMountOptions struct {
	Version string `json:"version,omitempty"`
}

type storedNfsConfig struct {
	MountOptions   *storedMountOptions `json:"mountOptions,omitempty"`
	ServerHostname string              `json:"serverHostname"`
	AgentArns      []string            `json:"agentArns,omitempty"`
}

type storedObjectStorageConfig struct {
	ServerHostname string   `json:"serverHostname"`
	BucketName     string   `json:"bucketName"`
	AccessKey      string   `json:"accessKey,omitempty"`
	SecretKey      string   `json:"secretKey,omitempty"`
	ServerProtocol string   `json:"serverProtocol,omitempty"`
	AgentArns      []string `json:"agentArns,omitempty"`
	ServerPort     int32    `json:"serverPort,omitempty"`
}

type storedSmbConfig struct {
	MountOptions   *storedMountOptions `json:"mountOptions,omitempty"`
	ServerHostname string              `json:"serverHostname"`
	Domain         string              `json:"domain,omitempty"`
	User           string              `json:"user,omitempty"`
	Password       string              `json:"password,omitempty"`
	AgentArns      []string            `json:"agentArns,omitempty"`
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
	StartTime                time.Time      `json:"startTime"`
	Options                  map[string]any `json:"options,omitempty"`
	TaskExecutionArn         string         `json:"taskExecutionArn"`
	Status                   string         `json:"status"`
	EstimatedFilesToTransfer int64          `json:"estimatedFilesToTransfer"`
	EstimatedBytesToTransfer int64          `json:"estimatedBytesToTransfer"`
	FilesTransferred         int64          `json:"filesTransferred"`
	BytesTransferred         int64          `json:"bytesTransferred"`
}

func (e *storedTaskExecution) toTaskExecution() TaskExecution {
	return TaskExecution{
		TaskExecutionArn:         e.TaskExecutionArn,
		Status:                   e.Status,
		StartTime:                e.StartTime,
		Options:                  maps.Clone(e.Options),
		EstimatedFilesToTransfer: e.EstimatedFilesToTransfer,
		EstimatedBytesToTransfer: e.EstimatedBytesToTransfer,
		FilesTransferred:         e.FilesTransferred,
		BytesTransferred:         e.BytesTransferred,
	}
}

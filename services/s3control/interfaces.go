package s3control

// StorageBackend defines the interface for S3 Control backend implementations.
// All mutating methods must be safe for concurrent use.
type StorageBackend interface {
	PutPublicAccessBlock(cfg PublicAccessBlock)
	GetPublicAccessBlock(accountID string) (*PublicAccessBlock, error)
	DeletePublicAccessBlock(accountID string) error
	ListAll() []PublicAccessBlock

	AssociateAccessGrantsIdentityCenter(accountID, identityCenterArn string)
	CreateAccessGrantsInstance(accountID, identityCenterArn string) *AccessGrantsInstance
	CreateAccessGrant(
		accountID, locationID, granteeType, granteeIdentifier, permission, applicationArn string,
	) (*AccessGrant, error)
	CreateAccessGrantsLocation(accountID, locationScope, iamRoleArn string) *AccessGrantsLocation
	CreateAccessPoint(accountID, name, bucket string) *AccessPoint
	GetAccessPoint(accountID, name string) (*AccessPoint, error)
	DeleteAccessPoint(accountID, name string) error
	ListAccessPoints(accountID string) []*AccessPoint
	PutAccessPointPolicy(accountID, name, policy string) error
	GetAccessPointPolicy(accountID, name string) (string, error)
	DeleteAccessPointPolicy(accountID, name string) error
	CreateAccessPointForObjectLambda(accountID, name string) *ObjectLambdaAccessPoint
	CreateBucket(accountID, bucketName string) *OutpostsBucket
	CreateJob(accountID, roleArn string, priority int32) (*BatchJob, error)
	GetJob(accountID, jobID string) (*BatchJob, error)
	ListJobs(accountID string) []*BatchJob
	UpdateJobPriority(accountID, jobID string, priority int32) (*BatchJob, error)
	UpdateJobStatus(accountID, jobID, status string) (*BatchJob, error)
	CreateMultiRegionAccessPoint(accountID, name, clientToken string) *MultiRegionAccessPointRequest
	GetMultiRegionAccessPoint(accountID, name string) (*MultiRegionAccessPoint, error)
	DeleteMultiRegionAccessPoint(accountID, name string) error
	ListMultiRegionAccessPoints(accountID string) []*MultiRegionAccessPoint
	PutMultiRegionAccessPointPolicy(accountID, name, policy string) error
	CreateStorageLensGroup(accountID, name string) *StorageLensGroup

	Reset()
	AccountID() string
	Region() string

	Snapshot() []byte
	Restore(data []byte) error
}

// Verify interface is satisfied at compile time.
var _ StorageBackend = (*InMemoryBackend)(nil)

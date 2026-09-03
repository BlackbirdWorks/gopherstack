package s3control

import "context"

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
	GetAccessPointPolicyStatus(accountID, name string) (bool, error)
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

	Snapshot(ctx context.Context) []byte
	Restore(ctx context.Context, data []byte) error

	// ---- batch1 ----

	// Job tagging
	PutJobTagging(accountID, jobID string, tags TagSet) error
	GetJobTagging(accountID, jobID string) (TagSet, error)
	DeleteJobTagging(accountID, jobID string) error

	// Access Grants Instance
	GetAccessGrantsInstance(accountID string) (*AccessGrantsInstance, error)
	DeleteAccessGrantsInstance(accountID string) error
	GetAccessGrantsInstanceResourcePolicy(accountID string) (string, error)
	PutAccessGrantsInstanceResourcePolicy(accountID, policy string)
	DeleteAccessGrantsInstanceResourcePolicy(accountID string)
	DissociateAccessGrantsIdentityCenter(accountID string)
	GetAccessGrantsInstanceForPrefix(accountID, prefix string) (*AccessGrantsInstance, error)

	// Access Grants CRUD
	GetAccessGrant(accountID, grantID string) (*AccessGrant, error)
	DeleteAccessGrant(accountID, grantID string) error
	ListAccessGrants(accountID string, filter AccessGrantsFilter) []*AccessGrant
	ListCallerAccessGrants(accountID, grantScope string) []*AccessGrant
	GetAccessGrantsLocation(accountID, locationID string) (*AccessGrantsLocation, error)
	DeleteAccessGrantsLocation(accountID, locationID string) error
	UpdateAccessGrantsLocation(accountID, locationID, iamRoleArn string) (*AccessGrantsLocation, error)
	ListAccessGrantsLocations(accountID string) []*AccessGrantsLocation
	GetDataAccess(accountID, target, permission string) (string, error)

	// Access Point Scope
	GetAccessPointScope(accountID, name string) (string, error)
	PutAccessPointScope(accountID, name, scope string) error
	DeleteAccessPointScope(accountID, name string) error
	ListAccessPointsForDirectoryBuckets(accountID string) []*AccessPoint

	// Object Lambda Access Points
	GetAccessPointForObjectLambda(accountID, name string) (*ObjectLambdaAccessPoint, error)
	DeleteAccessPointForObjectLambda(accountID, name string) error
	ListAccessPointsForObjectLambda(accountID string) []*ObjectLambdaAccessPoint
	GetAccessPointPolicyForObjectLambda(accountID, name string) (string, error)
	PutAccessPointPolicyForObjectLambda(accountID, name, policy string) error
	DeleteAccessPointPolicyForObjectLambda(accountID, name string) error
	GetAccessPointPolicyStatusForObjectLambda(accountID, name string) (bool, error)
	GetAccessPointConfigurationForObjectLambda(accountID, name string) (string, error)
	PutAccessPointConfigurationForObjectLambda(accountID, name, config string) error

	// Outposts Bucket. Unlike every other resource family in this interface,
	// these are NOT accountID-keyed -- see the CreateBucket doc comment in
	// bucket.go (gopherstack-eje5): the real CreateBucketInput carries no
	// AccountId at all, so bucket identity and its sub-resource state are
	// keyed by bucketName alone.
	GetBucket(bucketName string) (*OutpostsBucket, error)
	DeleteBucket(bucketName string) error
	GetBucketLifecycleConfiguration(bucketName string) (string, error)
	PutBucketLifecycleConfiguration(bucketName, lifecycleConfig string) error
	DeleteBucketLifecycleConfiguration(bucketName string) error
	GetBucketPolicy(bucketName string) (string, error)
	PutBucketPolicy(bucketName, policy string) error
	DeleteBucketPolicy(bucketName string) error
	GetBucketTagging(bucketName string) (TagSet, error)
	PutBucketTagging(bucketName string, tags TagSet) error
	DeleteBucketTagging(bucketName string) error
	GetBucketVersioning(bucketName string) (string, error)
	PutBucketVersioning(bucketName, status string) error
	ListRegionalBuckets() []*OutpostsBucket

	// MRAP
	DescribeMultiRegionAccessPointOperation(accountID, requestToken string) (*MultiRegionAccessPointRequest, error)
	GetMultiRegionAccessPointPolicy(accountID, name string) (string, error)
	GetMultiRegionAccessPointPolicyStatus(accountID, name string) (bool, error)
	GetMultiRegionAccessPointRoutes(accountID, mrap string) (string, error)

	// ---- batch2 ----

	// Bucket Replication. Same bucketName-only keying as the Outposts Bucket
	// group above.
	GetBucketReplication(bucketName string) (string, error)
	PutBucketReplication(bucketName, config string) error
	DeleteBucketReplication(bucketName string) error

	// MRAP routes (submit)
	SubmitMultiRegionAccessPointRoutes(accountID, mrap, routes string) error

	// Storage Lens Configuration
	GetStorageLensConfiguration(accountID, configName string) (string, error)
	PutStorageLensConfiguration(accountID, configName, config string) error
	DeleteStorageLensConfiguration(accountID, configName string) error
	GetStorageLensConfigurationTagging(accountID, configName string) (TagSet, error)
	PutStorageLensConfigurationTagging(accountID, configName string, tags TagSet) error
	DeleteStorageLensConfigurationTagging(accountID, configName string) error
	ListStorageLensConfigurations(accountID string) []string

	// Storage Lens Groups (additional CRUD)
	GetStorageLensGroup(accountID, name string) (*StorageLensGroup, error)
	UpdateStorageLensGroup(accountID, name string) (*StorageLensGroup, error)
	DeleteStorageLensGroup(accountID, name string) error
	ListStorageLensGroups(accountID string) []*StorageLensGroup

	// Resource Tags
	ListTagsForResource(arn string) map[string]string
	TagResource(arn string, tags map[string]string)
	UntagResource(arn string, tagKeys []string)

	// ---- batch3 ----

	// AccessPoint VPC configuration
	SetAccessPointVpcConfig(accountID, name, vpcID, bucketAccountID string) error

	// AccessPoint per-AP public access block. NOT separate AWS operations --
	// aws-sdk-go-v2/service/s3control has no Get/Put/DeleteAccessPoint
	// PublicAccessBlock ops. These back the PublicAccessBlockConfiguration
	// field that travels inline on CreateAccessPoint/GetAccessPoint instead
	// (see handler_access_points.go); do not route them as standalone ops.
	GetAccessPointPublicAccessBlock(accountID, name string) (*PublicAccessBlock, error)
	PutAccessPointPublicAccessBlock(accountID, name string, cfg PublicAccessBlock) error
	DeleteAccessPointPublicAccessBlock(accountID, name string) error

	// BatchJob extended fields
	UpdateJobDetails(accountID, jobID, description, manifest, operation, report string, confirmationRequired bool) error
	UpdateJobStatusValidated(accountID, jobID, requestedStatus, statusUpdateReason string) (*BatchJob, error)

	// MRAP regions
	SetMRAPRegions(accountID, name string, regions []string) error

	// StorageLensGroup filter
	UpdateStorageLensGroupFilter(accountID, name, filter string) error
}

// Verify interface is satisfied at compile time.
var _ StorageBackend = (*InMemoryBackend)(nil)

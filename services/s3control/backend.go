package s3control

import (
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

const (
	// defaultAccessGrantsInstanceID is the fixed ID for the single Access Grants instance per account.
	defaultAccessGrantsInstanceID = "default"

	// jobStatusNew is the initial status for a newly created batch job.
	jobStatusNew = "New"
)

var (
	// ErrNotFound is returned when public access block config is not found.
	ErrNotFound = awserr.New("NoSuchPublicAccessBlockConfiguration", awserr.ErrNotFound)

	// ErrAlreadyExists is returned when a resource already exists.
	ErrAlreadyExists = awserr.New("BucketAlreadyExists", awserr.ErrAlreadyExists)
)

// PublicAccessBlock represents the S3 Control public access block configuration.
type PublicAccessBlock struct {
	AccountID             string `json:"accountID"`
	BlockPublicAcls       bool   `json:"blockPublicAcls"`
	IgnorePublicAcls      bool   `json:"ignorePublicAcls"`
	BlockPublicPolicy     bool   `json:"blockPublicPolicy"`
	RestrictPublicBuckets bool   `json:"restrictPublicBuckets"`
}

// AccessGrantsInstance represents an S3 Access Grants instance.
type AccessGrantsInstance struct {
	AccountID                    string `json:"accountID"`
	AccessGrantsInstanceArn      string `json:"accessGrantsInstanceArn"`
	AccessGrantsInstanceID       string `json:"accessGrantsInstanceID"`
	IdentityCenterArn            string `json:"identityCenterArn"`
	IdentityCenterInstanceArn    string `json:"identityCenterInstanceArn"`
	IdentityCenterApplicationArn string `json:"identityCenterApplicationArn"`
	CreatedAt                    string `json:"createdAt"`
}

// AccessGrant represents an S3 Access Grants grant.
type AccessGrant struct {
	AccountID              string `json:"accountID"`
	AccessGrantID          string `json:"accessGrantID"`
	AccessGrantArn         string `json:"accessGrantArn"`
	AccessGrantsLocationID string `json:"accessGrantsLocationID"`
	GrantScope             string `json:"grantScope"`
	Permission             string `json:"permission"`
	GranteeType            string `json:"granteeType"`
	GranteeIdentifier      string `json:"granteeIdentifier"`
	ApplicationArn         string `json:"applicationArn"`
	CreatedAt              string `json:"createdAt"`
}

// AccessGrantsLocation represents an S3 Access Grants location.
type AccessGrantsLocation struct {
	AccountID               string `json:"accountID"`
	AccessGrantsLocationID  string `json:"accessGrantsLocationID"`
	AccessGrantsLocationArn string `json:"accessGrantsLocationArn"`
	LocationScope           string `json:"locationScope"`
	IAMRoleArn              string `json:"iamRoleArn"`
	CreatedAt               string `json:"createdAt"`
}

// AccessPoint represents an S3 access point.
type AccessPoint struct {
	AccountID      string `json:"accountID"`
	Name           string `json:"name"`
	Bucket         string `json:"bucket"`
	AccessPointArn string `json:"accessPointArn"`
	Alias          string `json:"alias"`
}

// ObjectLambdaAccessPoint represents an S3 Object Lambda access point.
type ObjectLambdaAccessPoint struct {
	AccountID                  string `json:"accountID"`
	Name                       string `json:"name"`
	ObjectLambdaAccessPointArn string `json:"objectLambdaAccessPointArn"`
}

// OutpostsBucket represents an S3 Outposts bucket.
type OutpostsBucket struct {
	AccountID string `json:"accountID"`
	Name      string `json:"name"`
	BucketArn string `json:"bucketArn"`
	Location  string `json:"location"`
}

// BatchJob represents an S3 Batch Operations job.
type BatchJob struct {
	AccountID string `json:"accountID"`
	JobID     string `json:"jobID"`
	JobArn    string `json:"jobArn"`
	RoleArn   string `json:"roleArn"`
	Status    string `json:"status"`
	Priority  int32  `json:"priority"`
}

// MultiRegionAccessPointRequest represents an MRAP async request.
type MultiRegionAccessPointRequest struct {
	AccountID       string `json:"accountID"`
	RequestTokenARN string `json:"requestTokenARN"`
	Name            string `json:"name"`
}

// StorageLensGroup represents an S3 Storage Lens group.
type StorageLensGroup struct {
	AccountID           string `json:"accountID"`
	Name                string `json:"name"`
	StorageLensGroupArn string `json:"storageLensGroupArn"`
}

// InMemoryBackend is the in-memory store for S3 Control resources.
type InMemoryBackend struct {
	configs                  map[string]*PublicAccessBlock
	accessGrantsInstances    map[string]*AccessGrantsInstance
	accessGrants             map[string]*AccessGrant
	accessGrantsLocations    map[string]*AccessGrantsLocation
	accessPoints             map[string]*AccessPoint
	objectLambdaAccessPoints map[string]*ObjectLambdaAccessPoint
	outpostsBuckets          map[string]*OutpostsBucket
	batchJobs                map[string]*BatchJob
	mrapRequests             map[string]*MultiRegionAccessPointRequest
	storageLensGroups        map[string]*StorageLensGroup
	mu                       *lockmetrics.RWMutex
	nextID                   int64
}

// NewInMemoryBackend creates a new InMemoryBackend.
func NewInMemoryBackend() *InMemoryBackend {
	return &InMemoryBackend{
		configs:                  make(map[string]*PublicAccessBlock),
		accessGrantsInstances:    make(map[string]*AccessGrantsInstance),
		accessGrants:             make(map[string]*AccessGrant),
		accessGrantsLocations:    make(map[string]*AccessGrantsLocation),
		accessPoints:             make(map[string]*AccessPoint),
		objectLambdaAccessPoints: make(map[string]*ObjectLambdaAccessPoint),
		outpostsBuckets:          make(map[string]*OutpostsBucket),
		batchJobs:                make(map[string]*BatchJob),
		mrapRequests:             make(map[string]*MultiRegionAccessPointRequest),
		storageLensGroups:        make(map[string]*StorageLensGroup),
		mu:                       lockmetrics.New("s3control"),
	}
}

// newID generates a new unique ID string using an internal counter (must be called under lock).
func (b *InMemoryBackend) newID(prefix string) string {
	b.nextID++

	return fmt.Sprintf("%s-%d", prefix, b.nextID)
}

// PutPublicAccessBlock creates or updates the public access block configuration for an account.
func (b *InMemoryBackend) PutPublicAccessBlock(cfg PublicAccessBlock) {
	b.mu.Lock("PutPublicAccessBlock")
	defer b.mu.Unlock()

	cp := cfg
	b.configs[cfg.AccountID] = &cp
}

// GetPublicAccessBlock retrieves the public access block configuration for an account.
func (b *InMemoryBackend) GetPublicAccessBlock(accountID string) (*PublicAccessBlock, error) {
	b.mu.RLock("GetPublicAccessBlock")
	defer b.mu.RUnlock()

	cfg, ok := b.configs[accountID]
	if !ok {
		return nil, ErrNotFound
	}

	cp := *cfg

	return &cp, nil
}

// ListAll returns all stored public access block configurations.
func (b *InMemoryBackend) ListAll() []PublicAccessBlock {
	b.mu.RLock("ListAll")
	defer b.mu.RUnlock()

	out := make([]PublicAccessBlock, 0, len(b.configs))
	for _, cfg := range b.configs {
		out = append(out, *cfg)
	}

	return out
}

// DeletePublicAccessBlock deletes the public access block configuration for an account.
func (b *InMemoryBackend) DeletePublicAccessBlock(accountID string) error {
	b.mu.Lock("DeletePublicAccessBlock")
	defer b.mu.Unlock()

	if _, ok := b.configs[accountID]; !ok {
		return ErrNotFound
	}

	delete(b.configs, accountID)

	return nil
}

// AssociateAccessGrantsIdentityCenter associates an IAM Identity Center instance with an S3 Access Grants instance.
func (b *InMemoryBackend) AssociateAccessGrantsIdentityCenter(accountID, identityCenterArn string) {
	b.mu.Lock("AssociateAccessGrantsIdentityCenter")
	defer b.mu.Unlock()

	inst, ok := b.accessGrantsInstances[accountID]
	if !ok {
		inst = &AccessGrantsInstance{
			AccountID:              accountID,
			AccessGrantsInstanceID: defaultAccessGrantsInstanceID,
			AccessGrantsInstanceArn: fmt.Sprintf(
				"arn:aws:s3:%s:%s:access-grants/default",
				config.DefaultRegion,
				accountID,
			),
		}
		b.accessGrantsInstances[accountID] = inst
	}

	inst.IdentityCenterArn = identityCenterArn
	inst.IdentityCenterInstanceArn = identityCenterArn
}

// CreateAccessGrantsInstance creates an S3 Access Grants instance for an account.
func (b *InMemoryBackend) CreateAccessGrantsInstance(accountID, identityCenterArn string) *AccessGrantsInstance {
	b.mu.Lock("CreateAccessGrantsInstance")
	defer b.mu.Unlock()

	inst := &AccessGrantsInstance{
		AccountID:              accountID,
		AccessGrantsInstanceID: defaultAccessGrantsInstanceID,
		AccessGrantsInstanceArn: fmt.Sprintf(
			"arn:aws:s3:%s:%s:access-grants/default",
			config.DefaultRegion,
			accountID,
		),
		IdentityCenterArn:         identityCenterArn,
		IdentityCenterInstanceArn: identityCenterArn,
	}
	b.accessGrantsInstances[accountID] = inst

	cp := *inst

	return &cp
}

// CreateAccessGrant creates an access grant for an account.
func (b *InMemoryBackend) CreateAccessGrant(
	accountID, locationID, granteeType, granteeIdentifier, permission, applicationArn string,
) *AccessGrant {
	b.mu.Lock("CreateAccessGrant")
	defer b.mu.Unlock()

	id := b.newID("grant")
	arn := fmt.Sprintf("arn:aws:s3:%s:%s:access-grants/default/grant/%s", config.DefaultRegion, accountID, id)

	grant := &AccessGrant{
		AccountID:              accountID,
		AccessGrantID:          id,
		AccessGrantArn:         arn,
		AccessGrantsLocationID: locationID,
		GrantScope:             fmt.Sprintf("s3://%s/*", locationID),
		Permission:             permission,
		GranteeType:            granteeType,
		GranteeIdentifier:      granteeIdentifier,
		ApplicationArn:         applicationArn,
	}
	b.accessGrants[accountID+":"+id] = grant

	cp := *grant

	return &cp
}

// CreateAccessGrantsLocation creates an Access Grants location.
func (b *InMemoryBackend) CreateAccessGrantsLocation(
	accountID, locationScope, iamRoleArn string,
) *AccessGrantsLocation {
	b.mu.Lock("CreateAccessGrantsLocation")
	defer b.mu.Unlock()

	id := b.newID("location")
	arn := fmt.Sprintf("arn:aws:s3:%s:%s:access-grants/default/location/%s", config.DefaultRegion, accountID, id)

	loc := &AccessGrantsLocation{
		AccountID:               accountID,
		AccessGrantsLocationID:  id,
		AccessGrantsLocationArn: arn,
		LocationScope:           locationScope,
		IAMRoleArn:              iamRoleArn,
	}
	b.accessGrantsLocations[accountID+":"+id] = loc

	cp := *loc

	return &cp
}

// CreateAccessPoint creates an S3 access point.
func (b *InMemoryBackend) CreateAccessPoint(accountID, name, bucket string) *AccessPoint {
	b.mu.Lock("CreateAccessPoint")
	defer b.mu.Unlock()

	arn := fmt.Sprintf("arn:aws:s3:%s:%s:accesspoint/%s", config.DefaultRegion, accountID, name)
	alias := fmt.Sprintf("%s-%s-s3alias", name, accountID[:8])

	ap := &AccessPoint{
		AccountID:      accountID,
		Name:           name,
		Bucket:         bucket,
		AccessPointArn: arn,
		Alias:          alias,
	}
	b.accessPoints[accountID+":"+name] = ap

	cp := *ap

	return &cp
}

// CreateAccessPointForObjectLambda creates an Object Lambda access point.
func (b *InMemoryBackend) CreateAccessPointForObjectLambda(accountID, name string) *ObjectLambdaAccessPoint {
	b.mu.Lock("CreateAccessPointForObjectLambda")
	defer b.mu.Unlock()

	arn := fmt.Sprintf("arn:aws:s3-object-lambda:%s:%s:accesspoint/%s", config.DefaultRegion, accountID, name)

	ap := &ObjectLambdaAccessPoint{
		AccountID:                  accountID,
		Name:                       name,
		ObjectLambdaAccessPointArn: arn,
	}
	b.objectLambdaAccessPoints[accountID+":"+name] = ap

	cp := *ap

	return &cp
}

// CreateBucket creates an S3 Outposts bucket.
func (b *InMemoryBackend) CreateBucket(accountID, bucketName string) *OutpostsBucket {
	b.mu.Lock("CreateBucket")
	defer b.mu.Unlock()

	arn := fmt.Sprintf(
		"arn:aws:s3-outposts:%s:%s:outpost/op-00000000/bucket/%s",
		config.DefaultRegion,
		accountID,
		bucketName,
	)

	bkt := &OutpostsBucket{
		AccountID: accountID,
		Name:      bucketName,
		BucketArn: arn,
		Location:  fmt.Sprintf("/%s", bucketName),
	}
	b.outpostsBuckets[accountID+":"+bucketName] = bkt

	cp := *bkt

	return &cp
}

// CreateJob creates an S3 Batch Operations job.
func (b *InMemoryBackend) CreateJob(accountID, roleArn string, priority int32) *BatchJob {
	b.mu.Lock("CreateJob")
	defer b.mu.Unlock()

	id := b.newID("job")
	arn := fmt.Sprintf("arn:aws:s3:%s:%s:job/%s", config.DefaultRegion, accountID, id)

	job := &BatchJob{
		AccountID: accountID,
		JobID:     id,
		JobArn:    arn,
		RoleArn:   roleArn,
		Priority:  priority,
		Status:    jobStatusNew,
	}
	b.batchJobs[accountID+":"+id] = job

	cp := *job

	return &cp
}

// CreateMultiRegionAccessPoint creates an async MRAP request.
func (b *InMemoryBackend) CreateMultiRegionAccessPoint(
	accountID, name, _ string,
) *MultiRegionAccessPointRequest {
	b.mu.Lock("CreateMultiRegionAccessPoint")
	defer b.mu.Unlock()

	token := b.newID("mrap-token")
	tokenARN := fmt.Sprintf("arn:aws:s3::%s:async-request/mrap/create/%s", accountID, token)

	req := &MultiRegionAccessPointRequest{
		AccountID:       accountID,
		RequestTokenARN: tokenARN,
		Name:            name,
	}
	b.mrapRequests[accountID+":"+token] = req

	cp := *req

	return &cp
}

// CreateStorageLensGroup creates an S3 Storage Lens group.
func (b *InMemoryBackend) CreateStorageLensGroup(accountID, name string) *StorageLensGroup {
	b.mu.Lock("CreateStorageLensGroup")
	defer b.mu.Unlock()

	arn := fmt.Sprintf("arn:aws:s3:%s:%s:storage-lens-group/%s", config.DefaultRegion, accountID, name)

	grp := &StorageLensGroup{
		AccountID:           accountID,
		Name:                name,
		StorageLensGroupArn: arn,
	}
	b.storageLensGroups[accountID+":"+name] = grp

	cp := *grp

	return &cp
}

package s3control

import (
	"fmt"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

const (
	// defaultAccessGrantsInstanceID is the fixed ID for the single Access Grants instance per account.
	defaultAccessGrantsInstanceID = "default"

	// jobStatusNew is the initial status for a newly created batch job.
	jobStatusNew = "New"

	// aliasAccountIDMaxLen is the max characters of accountID used in access point aliases.
	aliasAccountIDMaxLen = 8

	// ARN format constants.
	arnFmtAccessGrantsInstance = "arn:aws:s3:%s:%s:access-grants/default"
	arnFmtAccessGrant          = "arn:aws:s3:%s:%s:access-grants/default/grant/%s"
	arnFmtAccessGrantsLocation = "arn:aws:s3:%s:%s:access-grants/default/location/%s"
	arnFmtAccessPoint          = "arn:aws:s3:%s:%s:accesspoint/%s"
	arnFmtObjectLambda         = "arn:aws:s3-object-lambda:%s:%s:accesspoint/%s"
	arnFmtOutpostsBucket       = "arn:aws:s3-outposts:%s:%s:outpost/op-00000000/bucket/%s"
	arnFmtJob                  = "arn:aws:s3:%s:%s:job/%s"
	// arnFmtMRAPToken is the ARN for MRAP async request tokens; gosec false positive (not a credential).
	arnFmtMRAPToken        = "arn:aws:s3::%s:async-request/mrap/create/%s" //nolint:gosec // ARN format, not a credential
	arnFmtStorageLensGroup = "arn:aws:s3:%s:%s:storage-lens-group/%s"
)

var (
	// ErrNotFound is returned when public access block config is not found.
	ErrNotFound = awserr.New("NoSuchPublicAccessBlockConfiguration", awserr.ErrNotFound)

	// ErrAlreadyExists is returned when a resource already exists.
	ErrAlreadyExists = awserr.New("BucketAlreadyExists", awserr.ErrAlreadyExists)

	// ErrValidation is returned when a required parameter is missing or invalid.
	ErrValidation = awserr.New("BadRequestException", awserr.ErrInvalidParameter)
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
	AccountID       string `json:"accountID"`
	Name            string `json:"name"`
	Bucket          string `json:"bucket"`
	AccessPointArn  string `json:"accessPointArn"`
	Alias           string `json:"alias"`
	VpcID           string `json:"vpcID,omitempty"`
	BucketAccountId string `json:"bucketAccountID,omitempty"`
	NetworkOrigin   string `json:"networkOrigin"`
	CreationDate    string `json:"creationDate,omitempty"`
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
	AccountID            string `json:"accountID"`
	JobID                string `json:"jobID"`
	JobArn               string `json:"jobArn"`
	RoleArn              string `json:"roleArn"`
	Status               string `json:"status"`
	Priority             int32  `json:"priority"`
	Description          string `json:"description,omitempty"`
	Manifest             string `json:"manifest,omitempty"`
	Operation            string `json:"operation,omitempty"`
	Report               string `json:"report,omitempty"`
	ConfirmationRequired bool   `json:"confirmationRequired,omitempty"`
	CreationTime         string `json:"creationTime,omitempty"`
	TerminationDate      string `json:"terminationDate,omitempty"`
	StatusUpdateReason   string `json:"statusUpdateReason,omitempty"`
}

// MultiRegionAccessPointRequest represents an MRAP async request.
type MultiRegionAccessPointRequest struct {
	AccountID       string `json:"accountID"`
	RequestTokenARN string `json:"requestTokenARN"`
	Name            string `json:"name"`
}

// MultiRegionAccessPoint represents a stored MRAP instance.
type MultiRegionAccessPoint struct {
	AccountID string   `json:"accountID"`
	Name      string   `json:"name"`
	Alias     string   `json:"alias"`
	Status    string   `json:"status"`
	Policy    string   `json:"policy,omitempty"`
	Regions   []string `json:"regions,omitempty"`
	CreatedAt string   `json:"createdAt,omitempty"`
}

// StorageLensGroup represents an S3 Storage Lens group.
type StorageLensGroup struct {
	AccountID           string `json:"accountID"`
	Name                string `json:"name"`
	StorageLensGroupArn string `json:"storageLensGroupArn"`
	Filter              string `json:"filter,omitempty"`
	CreatedAt           string `json:"createdAt,omitempty"`
}

// InMemoryBackend is the in-memory store for S3 Control resources.
type InMemoryBackend struct {
	outpostsBuckets          map[string]*OutpostsBucket
	mu                       *lockmetrics.RWMutex
	accessGrants             map[string]*AccessGrant
	accessGrantsLocations    map[string]*AccessGrantsLocation
	accessPoints             map[string]*AccessPoint
	accessPointPolicies      map[string]string
	objectLambdaAccessPoints map[string]*ObjectLambdaAccessPoint
	mrapRequests             map[string]*MultiRegionAccessPointRequest
	mraps                    map[string]*MultiRegionAccessPoint
	batchJobs                map[string]*BatchJob
	accessGrantsInstances    map[string]*AccessGrantsInstance
	storageLensGroups        map[string]*StorageLensGroup
	configs                  map[string]*PublicAccessBlock
	// batch1 additions
	jobTags                      map[string]TagSet
	accessGrantsInstancePolicies map[string]string
	accessPointScopes            map[string]string
	objectLambdaAPPolicies       map[string]string
	objectLambdaAPConfigs        map[string]string
	bucketPolicies               map[string]string
	bucketTagging                map[string]TagSet
	bucketLifecycle              map[string]string
	bucketVersioning             map[string]string
	mrapPolicies                 map[string]string
	mrapRoutes                   map[string]string
	// batch2 additions
	bucketReplication     map[string]string            // accountID:bucketName → replication config XML
	storageLensConfigs    map[string]string            // accountID:configName → config XML
	storageLensConfigTags map[string]TagSet            // accountID:configName → tags
	resourceTags          map[string]map[string]string // ARN → tag key → tag value
	// batch3 additions
	accessPointPABs map[string]*PublicAccessBlock // accountID:apName → per-AP public access block
	accountID       string
	region          string
	nextID          int64
}

// NewInMemoryBackend creates a new InMemoryBackend with default config values.
func NewInMemoryBackend() *InMemoryBackend {
	return NewInMemoryBackendWithConfig(config.DefaultAccountID, config.DefaultRegion)
}

// NewInMemoryBackendWithConfig creates a new InMemoryBackend with explicit config values.
func NewInMemoryBackendWithConfig(accountID, region string) *InMemoryBackend {
	return &InMemoryBackend{
		configs:                      make(map[string]*PublicAccessBlock),
		accessGrantsInstances:        make(map[string]*AccessGrantsInstance),
		accessGrants:                 make(map[string]*AccessGrant),
		accessGrantsLocations:        make(map[string]*AccessGrantsLocation),
		accessPoints:                 make(map[string]*AccessPoint),
		accessPointPolicies:          make(map[string]string),
		objectLambdaAccessPoints:     make(map[string]*ObjectLambdaAccessPoint),
		outpostsBuckets:              make(map[string]*OutpostsBucket),
		batchJobs:                    make(map[string]*BatchJob),
		mrapRequests:                 make(map[string]*MultiRegionAccessPointRequest),
		mraps:                        make(map[string]*MultiRegionAccessPoint),
		storageLensGroups:            make(map[string]*StorageLensGroup),
		jobTags:                      make(map[string]TagSet),
		accessGrantsInstancePolicies: make(map[string]string),
		accessPointScopes:            make(map[string]string),
		objectLambdaAPPolicies:       make(map[string]string),
		objectLambdaAPConfigs:        make(map[string]string),
		bucketPolicies:               make(map[string]string),
		bucketTagging:                make(map[string]TagSet),
		bucketLifecycle:              make(map[string]string),
		bucketVersioning:             make(map[string]string),
		mrapPolicies:                 make(map[string]string),
		mrapRoutes:                   make(map[string]string),
		bucketReplication:            make(map[string]string),
		storageLensConfigs:           make(map[string]string),
		storageLensConfigTags:        make(map[string]TagSet),
		resourceTags:                 make(map[string]map[string]string),
		accessPointPABs:              make(map[string]*PublicAccessBlock),
		mu:                           lockmetrics.New("s3control"),
		accountID:                    accountID,
		region:                       region,
	}
}

// AccountID returns the AWS account ID configured for this backend.
func (b *InMemoryBackend) AccountID() string { return b.accountID }

// Region returns the AWS region configured for this backend.
func (b *InMemoryBackend) Region() string { return b.region }

// Reset clears all stored resources without recreating the backend.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	b.configs = make(map[string]*PublicAccessBlock)
	b.accessGrantsInstances = make(map[string]*AccessGrantsInstance)
	b.accessGrants = make(map[string]*AccessGrant)
	b.accessGrantsLocations = make(map[string]*AccessGrantsLocation)
	b.accessPoints = make(map[string]*AccessPoint)
	b.accessPointPolicies = make(map[string]string)
	b.objectLambdaAccessPoints = make(map[string]*ObjectLambdaAccessPoint)
	b.outpostsBuckets = make(map[string]*OutpostsBucket)
	b.batchJobs = make(map[string]*BatchJob)
	b.mrapRequests = make(map[string]*MultiRegionAccessPointRequest)
	b.mraps = make(map[string]*MultiRegionAccessPoint)
	b.storageLensGroups = make(map[string]*StorageLensGroup)
	b.jobTags = make(map[string]TagSet)
	b.accessGrantsInstancePolicies = make(map[string]string)
	b.accessPointScopes = make(map[string]string)
	b.objectLambdaAPPolicies = make(map[string]string)
	b.objectLambdaAPConfigs = make(map[string]string)
	b.bucketPolicies = make(map[string]string)
	b.bucketTagging = make(map[string]TagSet)
	b.bucketLifecycle = make(map[string]string)
	b.bucketVersioning = make(map[string]string)
	b.mrapPolicies = make(map[string]string)
	b.mrapRoutes = make(map[string]string)
	b.bucketReplication = make(map[string]string)
	b.storageLensConfigs = make(map[string]string)
	b.storageLensConfigTags = make(map[string]TagSet)
	b.resourceTags = make(map[string]map[string]string)
	b.accessPointPABs = make(map[string]*PublicAccessBlock)
	b.nextID = 0
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
			AccountID:               accountID,
			AccessGrantsInstanceID:  defaultAccessGrantsInstanceID,
			AccessGrantsInstanceArn: fmt.Sprintf(arnFmtAccessGrantsInstance, b.region, accountID),
			CreatedAt:               nowRFC3339(),
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
		AccountID:                 accountID,
		AccessGrantsInstanceID:    defaultAccessGrantsInstanceID,
		AccessGrantsInstanceArn:   fmt.Sprintf(arnFmtAccessGrantsInstance, b.region, accountID),
		IdentityCenterArn:         identityCenterArn,
		IdentityCenterInstanceArn: identityCenterArn,
		CreatedAt:                 nowRFC3339(),
	}
	b.accessGrantsInstances[accountID] = inst

	cp := *inst

	return &cp
}

// CreateAccessGrant creates an access grant for an account.
// Returns ErrValidation if permission is empty.
func (b *InMemoryBackend) CreateAccessGrant(
	accountID, locationID, granteeType, granteeIdentifier, permission, applicationArn string,
) (*AccessGrant, error) {
	if permission == "" {
		return nil, fmt.Errorf("permission is required: %w", ErrValidation)
	}

	b.mu.Lock("CreateAccessGrant")
	defer b.mu.Unlock()

	id := b.newID("grant")
	arn := fmt.Sprintf(arnFmtAccessGrant, b.region, accountID, id)

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
		CreatedAt:              nowRFC3339(),
	}
	b.accessGrants[accountID+":"+id] = grant

	cp := *grant

	return &cp, nil
}

// CreateAccessGrantsLocation creates an Access Grants location.
func (b *InMemoryBackend) CreateAccessGrantsLocation(
	accountID, locationScope, iamRoleArn string,
) *AccessGrantsLocation {
	b.mu.Lock("CreateAccessGrantsLocation")
	defer b.mu.Unlock()

	id := b.newID("location")
	arn := fmt.Sprintf(arnFmtAccessGrantsLocation, b.region, accountID, id)

	loc := &AccessGrantsLocation{
		AccountID:               accountID,
		AccessGrantsLocationID:  id,
		AccessGrantsLocationArn: arn,
		LocationScope:           locationScope,
		IAMRoleArn:              iamRoleArn,
		CreatedAt:               nowRFC3339(),
	}
	b.accessGrantsLocations[accountID+":"+id] = loc

	cp := *loc

	return &cp
}

// CreateAccessPoint creates an S3 access point.
func (b *InMemoryBackend) CreateAccessPoint(accountID, name, bucket string) *AccessPoint {
	b.mu.Lock("CreateAccessPoint")
	defer b.mu.Unlock()

	arn := fmt.Sprintf(arnFmtAccessPoint, b.region, accountID, name)

	// Guard against short accountIDs to prevent panics on slice operations.
	aliasPrefix := accountID
	if len(aliasPrefix) > aliasAccountIDMaxLen {
		aliasPrefix = aliasPrefix[:aliasAccountIDMaxLen]
	}

	alias := fmt.Sprintf("%s-%s-s3alias", name, aliasPrefix)

	ap := &AccessPoint{
		AccountID:      accountID,
		Name:           name,
		Bucket:         bucket,
		AccessPointArn: arn,
		Alias:          alias,
		NetworkOrigin:  "Internet",
		CreationDate:   nowRFC3339(),
	}
	b.accessPoints[accountID+":"+name] = ap

	cp := *ap

	return &cp
}

// SetAccessPointVpcConfig sets VPC configuration fields on an existing access point.
// NetworkOrigin is set to "VPC" when vpcID is non-empty, else "Internet".
// Alias is cleared for VPC access points (AWS does not emit an alias for VPC APs).
func (b *InMemoryBackend) SetAccessPointVpcConfig(accountID, name, vpcID, bucketAccountId string) error {
	b.mu.Lock("SetAccessPointVpcConfig")
	defer b.mu.Unlock()

	ap, ok := b.accessPoints[accountID+":"+name]
	if !ok {
		return ErrNotFound
	}

	ap.VpcID = vpcID
	ap.BucketAccountId = bucketAccountId

	if vpcID != "" {
		ap.NetworkOrigin = "VPC"
		ap.Alias = ""
	} else {
		ap.NetworkOrigin = "Internet"
	}

	return nil
}

// GetAccessPoint retrieves an S3 access point by name.
func (b *InMemoryBackend) GetAccessPoint(accountID, name string) (*AccessPoint, error) {
	b.mu.RLock("GetAccessPoint")
	defer b.mu.RUnlock()

	ap, ok := b.accessPoints[accountID+":"+name]
	if !ok {
		return nil, ErrNotFound
	}

	cp := *ap

	return &cp, nil
}

// DeleteAccessPoint removes an S3 access point.
func (b *InMemoryBackend) DeleteAccessPoint(accountID, name string) error {
	b.mu.Lock("DeleteAccessPoint")
	defer b.mu.Unlock()

	key := accountID + ":" + name
	if _, ok := b.accessPoints[key]; !ok {
		return ErrNotFound
	}

	delete(b.accessPoints, key)
	delete(b.accessPointPolicies, key)

	return nil
}

// ListAccessPoints returns all access points for an account.
func (b *InMemoryBackend) ListAccessPoints(accountID string) []*AccessPoint {
	b.mu.RLock("ListAccessPoints")
	defer b.mu.RUnlock()

	prefix := accountID + ":"
	var out []*AccessPoint

	for k, v := range b.accessPoints {
		if strings.HasPrefix(k, prefix) {
			cp := *v
			out = append(out, &cp)
		}
	}

	return out
}

// PutAccessPointPolicy stores a policy for an access point.
func (b *InMemoryBackend) PutAccessPointPolicy(accountID, name, policy string) error {
	b.mu.Lock("PutAccessPointPolicy")
	defer b.mu.Unlock()

	key := accountID + ":" + name
	if _, ok := b.accessPoints[key]; !ok {
		return ErrNotFound
	}

	b.accessPointPolicies[key] = policy

	return nil
}

// GetAccessPointPolicy retrieves the policy for an access point.
func (b *InMemoryBackend) GetAccessPointPolicy(accountID, name string) (string, error) {
	b.mu.RLock("GetAccessPointPolicy")
	defer b.mu.RUnlock()

	key := accountID + ":" + name
	if _, ok := b.accessPoints[key]; !ok {
		return "", ErrNotFound
	}

	policy, ok := b.accessPointPolicies[key]
	if !ok {
		return "", ErrNotFound
	}

	return policy, nil
}

// DeleteAccessPointPolicy removes the policy for an access point.
func (b *InMemoryBackend) DeleteAccessPointPolicy(accountID, name string) error {
	b.mu.Lock("DeleteAccessPointPolicy")
	defer b.mu.Unlock()

	key := accountID + ":" + name
	if _, ok := b.accessPoints[key]; !ok {
		return ErrNotFound
	}

	delete(b.accessPointPolicies, key)

	return nil
}

// CreateAccessPointForObjectLambda creates an Object Lambda access point.
func (b *InMemoryBackend) CreateAccessPointForObjectLambda(accountID, name string) *ObjectLambdaAccessPoint {
	b.mu.Lock("CreateAccessPointForObjectLambda")
	defer b.mu.Unlock()

	arn := fmt.Sprintf(arnFmtObjectLambda, b.region, accountID, name)

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

	arn := fmt.Sprintf(arnFmtOutpostsBucket, b.region, accountID, bucketName)

	bkt := &OutpostsBucket{
		AccountID: accountID,
		Name:      bucketName,
		BucketArn: arn,
		Location:  "/" + bucketName,
	}
	b.outpostsBuckets[accountID+":"+bucketName] = bkt

	cp := *bkt

	return &cp
}

// CreateJob creates an S3 Batch Operations job.
// Returns ErrValidation if roleArn is empty.
func (b *InMemoryBackend) CreateJob(accountID, roleArn string, priority int32) (*BatchJob, error) {
	if roleArn == "" {
		return nil, fmt.Errorf("roleArn is required: %w", ErrValidation)
	}

	b.mu.Lock("CreateJob")
	defer b.mu.Unlock()

	id := b.newID("job")
	arn := fmt.Sprintf(arnFmtJob, b.region, accountID, id)

	job := &BatchJob{
		AccountID:    accountID,
		JobID:        id,
		JobArn:       arn,
		RoleArn:      roleArn,
		Priority:     priority,
		Status:       jobStatusNew,
		CreationTime: nowRFC3339(),
	}
	b.batchJobs[accountID+":"+id] = job

	cp := *job

	return &cp, nil
}

// UpdateJobDetails persists the extended fields from a CreateJob request to an existing job.
func (b *InMemoryBackend) UpdateJobDetails(
	accountID, jobID, description, manifest, operation, report string,
	confirmationRequired bool,
) error {
	b.mu.Lock("UpdateJobDetails")
	defer b.mu.Unlock()

	job, ok := b.batchJobs[accountID+":"+jobID]
	if !ok {
		return ErrNotFound
	}

	job.Description = description
	job.Manifest = manifest
	job.Operation = operation
	job.Report = report
	job.ConfirmationRequired = confirmationRequired

	return nil
}

// CreateMultiRegionAccessPoint creates an async MRAP request and stores the MRAP instance.
func (b *InMemoryBackend) CreateMultiRegionAccessPoint(
	accountID, name, _ string,
) *MultiRegionAccessPointRequest {
	b.mu.Lock("CreateMultiRegionAccessPoint")
	defer b.mu.Unlock()

	token := b.newID("mrap-token")
	tokenARN := fmt.Sprintf(arnFmtMRAPToken, accountID, token)

	req := &MultiRegionAccessPointRequest{
		AccountID:       accountID,
		RequestTokenARN: tokenARN,
		Name:            name,
	}
	b.mrapRequests[accountID+":"+token] = req

	// Also store the real MRAP object so GetMultiRegionAccessPoint can retrieve it.
	alias := fmt.Sprintf("%s.mrap.accesspoint.s3-global.amazonaws.com", name)
	mrap := &MultiRegionAccessPoint{
		AccountID: accountID,
		Name:      name,
		Alias:     alias,
		Status:    "READY",
		CreatedAt: nowRFC3339(),
	}
	b.mraps[accountID+":"+name] = mrap

	cp := *req

	return &cp
}

// SetMRAPRegions stores the bucket-region list for an MRAP.
func (b *InMemoryBackend) SetMRAPRegions(accountID, name string, regions []string) error {
	b.mu.Lock("SetMRAPRegions")
	defer b.mu.Unlock()

	mrap, ok := b.mraps[accountID+":"+name]
	if !ok {
		return ErrNotFound
	}

	cp := make([]string, len(regions))
	copy(cp, regions)
	mrap.Regions = cp

	return nil
}

// GetJob retrieves a batch job by ID.
func (b *InMemoryBackend) GetJob(accountID, jobID string) (*BatchJob, error) {
	b.mu.RLock("GetJob")
	defer b.mu.RUnlock()

	job, ok := b.batchJobs[accountID+":"+jobID]
	if !ok {
		return nil, ErrNotFound
	}

	cp := *job

	return &cp, nil
}

// ListJobs returns all batch jobs for an account.
func (b *InMemoryBackend) ListJobs(accountID string) []*BatchJob {
	b.mu.RLock("ListJobs")
	defer b.mu.RUnlock()

	prefix := accountID + ":"
	var out []*BatchJob

	for k, v := range b.batchJobs {
		if strings.HasPrefix(k, prefix) {
			cp := *v
			out = append(out, &cp)
		}
	}

	return out
}

// UpdateJobPriority changes the priority of a batch job.
func (b *InMemoryBackend) UpdateJobPriority(accountID, jobID string, priority int32) (*BatchJob, error) {
	b.mu.Lock("UpdateJobPriority")
	defer b.mu.Unlock()

	job, ok := b.batchJobs[accountID+":"+jobID]
	if !ok {
		return nil, ErrNotFound
	}

	job.Priority = priority
	cp := *job

	return &cp, nil
}

// UpdateJobStatus changes the status of a batch job.
func (b *InMemoryBackend) UpdateJobStatus(accountID, jobID, status string) (*BatchJob, error) {
	b.mu.Lock("UpdateJobStatus")
	defer b.mu.Unlock()

	job, ok := b.batchJobs[accountID+":"+jobID]
	if !ok {
		return nil, ErrNotFound
	}

	job.Status = status
	cp := *job

	return &cp, nil
}

// GetMultiRegionAccessPoint retrieves an MRAP by name.
func (b *InMemoryBackend) GetMultiRegionAccessPoint(accountID, name string) (*MultiRegionAccessPoint, error) {
	b.mu.RLock("GetMultiRegionAccessPoint")
	defer b.mu.RUnlock()

	mrap, ok := b.mraps[accountID+":"+name]
	if !ok {
		return nil, ErrNotFound
	}

	cp := *mrap

	return &cp, nil
}

// DeleteMultiRegionAccessPoint removes an MRAP.
func (b *InMemoryBackend) DeleteMultiRegionAccessPoint(accountID, name string) error {
	b.mu.Lock("DeleteMultiRegionAccessPoint")
	defer b.mu.Unlock()

	key := accountID + ":" + name
	if _, ok := b.mraps[key]; !ok {
		return ErrNotFound
	}

	delete(b.mraps, key)

	return nil
}

// ListMultiRegionAccessPoints returns all MRAPs for an account.
func (b *InMemoryBackend) ListMultiRegionAccessPoints(accountID string) []*MultiRegionAccessPoint {
	b.mu.RLock("ListMultiRegionAccessPoints")
	defer b.mu.RUnlock()

	prefix := accountID + ":"
	var out []*MultiRegionAccessPoint

	for k, v := range b.mraps {
		if strings.HasPrefix(k, prefix) {
			cp := *v
			out = append(out, &cp)
		}
	}

	return out
}

// PutMultiRegionAccessPointPolicy stores a policy for an MRAP.
func (b *InMemoryBackend) PutMultiRegionAccessPointPolicy(accountID, name, policy string) error {
	b.mu.Lock("PutMultiRegionAccessPointPolicy")
	defer b.mu.Unlock()

	key := accountID + ":" + name
	mrap, ok := b.mraps[key]
	if !ok {
		return ErrNotFound
	}

	mrap.Policy = policy

	return nil
}

// CreateStorageLensGroup creates an S3 Storage Lens group.
func (b *InMemoryBackend) CreateStorageLensGroup(accountID, name string) *StorageLensGroup {
	b.mu.Lock("CreateStorageLensGroup")
	defer b.mu.Unlock()

	arn := fmt.Sprintf(arnFmtStorageLensGroup, b.region, accountID, name)

	grp := &StorageLensGroup{
		AccountID:           accountID,
		Name:                name,
		StorageLensGroupArn: arn,
		CreatedAt:           nowRFC3339(),
	}
	b.storageLensGroups[accountID+":"+name] = grp

	cp := *grp

	return &cp
}

// UpdateStorageLensGroupFilter stores the filter XML for an existing Storage Lens group.
func (b *InMemoryBackend) UpdateStorageLensGroupFilter(accountID, name, filter string) error {
	b.mu.Lock("UpdateStorageLensGroupFilter")
	defer b.mu.Unlock()

	grp, ok := b.storageLensGroups[accountID+":"+name]
	if !ok {
		return errStorageLensGroupNotFound
	}

	grp.Filter = filter

	return nil
}

// nowRFC3339 returns the current UTC time formatted as RFC3339.
func nowRFC3339() string {
	return time.Now().UTC().Format(time.RFC3339)
}

// --- Seed helpers for testing ---

// AddPublicAccessBlockInternal stores a public access block directly, for seeding test data.
func (b *InMemoryBackend) AddPublicAccessBlockInternal(accountID string, block *PublicAccessBlock) {
	b.mu.Lock("AddPublicAccessBlockInternal")
	defer b.mu.Unlock()

	cp := *block
	b.configs[accountID] = &cp
}

// AddAccessGrantsInstanceInternal creates an access grants instance directly, for seeding test data.
func (b *InMemoryBackend) AddAccessGrantsInstanceInternal(accountID, identityCenterArn string) *AccessGrantsInstance {
	return b.CreateAccessGrantsInstance(accountID, identityCenterArn)
}

// AddAccessGrantInternal creates an access grant directly, for seeding test data.
func (b *InMemoryBackend) AddAccessGrantInternal(
	accountID, locationID, granteeType, granteeIdentifier, permission string,
) *AccessGrant {
	grant, _ := b.CreateAccessGrant(accountID, locationID, granteeType, granteeIdentifier, permission, "")

	return grant
}

// AddAccessPointInternal creates an access point directly, for seeding test data.
func (b *InMemoryBackend) AddAccessPointInternal(accountID, name, bucket string) *AccessPoint {
	return b.CreateAccessPoint(accountID, name, bucket)
}

// AddBatchJobInternal creates a batch job directly, for seeding test data.
func (b *InMemoryBackend) AddBatchJobInternal(accountID, roleArn string, priority int32) *BatchJob {
	job, _ := b.CreateJob(accountID, roleArn, priority)

	return job
}

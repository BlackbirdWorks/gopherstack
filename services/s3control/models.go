package s3control

// TagSet represents a key-value tag map.
type TagSet map[string]string

// PublicAccessBlock represents the S3 Control public access block configuration.
//
// This type backs two distinct store.Table instances (see store_setup.go):
// the account-level "configs" table (keyed by AccountID alone) and the
// per-access-point "accessPointPABs" table (keyed by AccountID+APName).
type PublicAccessBlock struct {
	AccountID string `json:"accountID"`
	// APName is the access point name this value is keyed by in the
	// accessPointPABs Table (see store_setup.go); it is always empty for
	// entries in the account-level configs Table. Tagged json:"-" because
	// accessPointPABs is a "dirty" table -- persistence.go round-trips it
	// through a dedicated DTO that carries APName as a real JSON field, so
	// it survives the round trip despite being excluded here. It must never
	// change after the value is created (store.Table's keyFn purity
	// requirement).
	APName                string `json:"-"`
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
	BucketAccountID string `json:"bucketAccountID,omitempty"`
	NetworkOrigin   string `json:"networkOrigin"`
	CreationDate    string `json:"creationDate,omitempty"`
}

// ObjectLambdaAccessPointAlias represents an alias for an Object Lambda access point.
type ObjectLambdaAccessPointAlias struct {
	Status string `json:"status,omitempty"`
	Value  string `json:"value,omitempty"`
}

// ObjectLambdaAccessPoint represents an S3 Object Lambda access point.
type ObjectLambdaAccessPoint struct {
	Alias                      *ObjectLambdaAccessPointAlias `json:"alias,omitempty"`
	AccountID                  string                        `json:"accountID"`
	Name                       string                        `json:"name"`
	ObjectLambdaAccessPointArn string                        `json:"objectLambdaAccessPointArn"`
}

// cloneObjectLambdaAccessPoint returns a deep copy of an ObjectLambdaAccessPoint.
func cloneObjectLambdaAccessPoint(src *ObjectLambdaAccessPoint) *ObjectLambdaAccessPoint {
	if src == nil {
		return nil
	}

	cp := *src
	if src.Alias != nil {
		aliasCopy := *src.Alias
		cp.Alias = &aliasCopy
	}

	return &cp
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
	Description          string `json:"description,omitempty"`
	TerminationDate      string `json:"terminationDate,omitempty"`
	JobArn               string `json:"jobArn"`
	RoleArn              string `json:"roleArn"`
	Status               string `json:"status"`
	StatusUpdateReason   string `json:"statusUpdateReason,omitempty"`
	Operation            string `json:"operation,omitempty"`
	AccountID            string `json:"accountID"`
	JobID                string `json:"jobID"`
	Report               string `json:"report,omitempty"`
	Manifest             string `json:"manifest,omitempty"`
	CreationTime         string `json:"creationTime,omitempty"`
	Priority             int32  `json:"priority"`
	ConfirmationRequired bool   `json:"confirmationRequired,omitempty"`
}

// MultiRegionAccessPointRequest represents an MRAP async request.
type MultiRegionAccessPointRequest struct {
	AccountID string `json:"accountID"`
	// Token is the bare request token this value is keyed by in the
	// mrapRequests Table (see store_setup.go); RequestTokenARN embeds it but
	// as a full ARN, not a value store.Table's keyFn can use directly.
	// Tagged json:"-" because mrapRequests is a "dirty" table --
	// persistence.go round-trips it through a dedicated DTO that carries
	// Token as a real JSON field, so it survives the round trip despite
	// being excluded here. It must never change after the value is created
	// (store.Table's keyFn purity requirement).
	Token           string `json:"-"`
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
	CreatedAt string   `json:"createdAt,omitempty"`
	Regions   []string `json:"regions,omitempty"`
}

// StorageLensGroup represents an S3 Storage Lens group.
type StorageLensGroup struct {
	AccountID           string `json:"accountID"`
	Name                string `json:"name"`
	StorageLensGroupArn string `json:"storageLensGroupArn"`
	Filter              string `json:"filter,omitempty"`
	CreatedAt           string `json:"createdAt,omitempty"`
}

package ociobjectstorage

import "time"

// Storage tiers, versioning modes and public-access types as spelled on the wire.
const (
	tierStandard = "Standard"
	tierIA       = "InfrequentAccess"
	tierArchive  = "Archive"

	versioningDisabled  = "Disabled"
	versioningEnabled   = "Enabled"
	versioningSuspended = "Suspended"

	accessNone     = "NoPublicAccess"
	accessRead     = "ObjectRead"
	accessReadNoLs = "ObjectReadWithoutList"

	archivalArchived  = "Archived"
	archivalRestoring = "Restoring"
	archivalRestored  = "Restored"

	nullVersion = "null"
)

// ObjectMeta is the user-settable metadata of an object.
type ObjectMeta struct {
	Metadata           map[string]string `json:"metadata,omitempty"`
	ContentType        string            `json:"contentType,omitempty"`
	ContentLanguage    string            `json:"contentLanguage,omitempty"`
	ContentEncoding    string            `json:"contentEncoding,omitempty"`
	ContentDisposition string            `json:"contentDisposition,omitempty"`
	CacheControl       string            `json:"cacheControl,omitempty"`
	StorageTier        string            `json:"storageTier,omitempty"`
}

// Object is one stored version of an object (or a delete marker).
type Object struct {
	TimeCreated    time.Time  `json:"timeCreated"`
	TimeModified   time.Time  `json:"timeModified"`
	RestoreReadyAt time.Time  `json:"restoreReadyAt"`
	RestoreExpiry  time.Time  `json:"restoreExpiry"`
	TimeArchived   time.Time  `json:"timeArchived,omitzero"`
	Checksum       Checksum   `json:"checksum,omitzero"`
	Meta           ObjectMeta `json:"meta"`
	Name           string     `json:"name"`
	VersionID      string     `json:"versionId"`
	Etag           string     `json:"etag"`
	MD5            string     `json:"md5"`
	MultipartMD5   string     `json:"multipartMd5,omitempty"`
	SseKeySha256   string     `json:"sseKeySha256,omitempty"`
	KmsKeyID       string     `json:"kmsKeyId,omitempty"`
	Data           []byte     `json:"data"`
	IsDeleteMarker bool       `json:"isDeleteMarker"`
}

// Part is one uploaded multipart part.
type Part struct {
	Etag        string `json:"etag"`
	MD5         string `json:"md5"`
	ChecksumAlg string `json:"checksumAlg,omitempty"`
	Checksum    string `json:"checksum,omitempty"`
	Data        []byte `json:"data"`
}

// Upload is an in-flight multipart upload.
type Upload struct {
	TimeCreated time.Time    `json:"timeCreated"`
	Parts       map[int]Part `json:"parts"`
	ID          string       `json:"id"`
	Object      string       `json:"object"`
	Meta        ObjectMeta   `json:"meta"`
	ChecksumAlg string       `json:"checksumAlg,omitempty"`
	SseKeySha   string       `json:"sseKeySha256,omitempty"`
	KmsKeyID    string       `json:"kmsKeyId,omitempty"`
}

// PAR is a pre-authenticated request.
type PAR struct {
	TimeCreated         time.Time `json:"timeCreated"`
	TimeExpires         time.Time `json:"timeExpires"`
	ID                  string    `json:"id"`
	Name                string    `json:"name"`
	Token               string    `json:"token"`
	AccessType          string    `json:"accessType"`
	ObjectName          string    `json:"objectName"`
	BucketListingAction string    `json:"bucketListingAction"`
}

// LifecycleRule is one object lifecycle rule.
type LifecycleRule struct {
	ObjectNameFilter *NameFilter `json:"objectNameFilter,omitempty"`
	Target           *string     `json:"target,omitempty"`
	Name             string      `json:"name"`
	Action           string      `json:"action"`
	TimeUnit         string      `json:"timeUnit"`
	TimeAmount       int64       `json:"timeAmount"`
	IsEnabled        bool        `json:"isEnabled"`
}

// NameFilter restricts a lifecycle rule to matching object names.
type NameFilter struct {
	InclusionPatterns []string `json:"inclusionPatterns,omitempty"`
	ExclusionPatterns []string `json:"exclusionPatterns,omitempty"`
	InclusionPrefixes []string `json:"inclusionPrefixes,omitempty"`
}

// Lifecycle is a bucket's lifecycle policy.
type Lifecycle struct {
	TimeCreated time.Time       `json:"timeCreated"`
	Items       []LifecycleRule `json:"items"`
}

// Retention is a bucket retention rule.
type Retention struct {
	TimeCreated    time.Time  `json:"timeCreated"`
	TimeModified   time.Time  `json:"timeModified"`
	TimeRuleLocked *time.Time `json:"timeRuleLocked,omitempty"`
	ID             string     `json:"id"`
	DisplayName    string     `json:"displayName"`
	Etag           string     `json:"etag"`
	Unit           string     `json:"unit,omitempty"`
	Amount         int64      `json:"amount"`
	HasDuration    bool       `json:"hasDuration"`
}

// Bucket is a bucket with all its contents.
type Bucket struct {
	TimeCreated         time.Time                 `json:"timeCreated"`
	DefinedTags         map[string]map[string]any `json:"definedTags,omitempty"`
	FreeformTags        map[string]string         `json:"freeformTags,omitempty"`
	Metadata            map[string]string         `json:"metadata,omitempty"`
	Objects             map[string][]*Object      `json:"objects"`
	Uploads             map[string]*Upload        `json:"uploads"`
	PARs                map[string]*PAR           `json:"pars"`
	Retention           map[string]*Retention     `json:"retention"`
	Lifecycle           *Lifecycle                `json:"lifecycle,omitempty"`
	Replication         *ReplicationPolicy        `json:"replication,omitempty"`
	ReplicationSource   *ReplicationSource        `json:"replicationSource,omitempty"`
	Name                string                    `json:"name"`
	Versioning          string                    `json:"versioning"`
	CompartmentID       string                    `json:"compartmentId"`
	CreatedBy           string                    `json:"createdBy"`
	Etag                string                    `json:"etag"`
	PublicAccessType    string                    `json:"publicAccessType"`
	StorageTier         string                    `json:"storageTier"`
	LifecycleEtag       string                    `json:"lifecycleEtag,omitempty"`
	AutoTiering         string                    `json:"autoTiering,omitempty"`
	KmsKeyID            string                    `json:"kmsKeyId,omitempty"`
	ID                  string                    `json:"id"`
	ReplQueue           []ReplTask                `json:"replQueue,omitempty"`
	ObjectEventsEnabled bool                      `json:"objectEventsEnabled"`
	IsBucketKeyEnabled  bool                      `json:"isBucketKeyEnabled"`
}

// Work request states as spelled on the wire.
const (
	wrAccepted   = "ACCEPTED"
	wrInProgress = "IN_PROGRESS"
	wrCompleted  = "COMPLETED"
	wrFailed     = "FAILED"
	wrCanceled   = "CANCELED"
)

// PendingCopy is a cross-region copy waiting for the injectable clock to reach its due time.
type PendingCopy struct {
	SourceNS     string    `json:"sourceNs"`
	SourceBucket string    `json:"sourceBucket"`
	VersionID    string    `json:"versionId"`
	DueAt        time.Time `json:"dueAt"`
	Input        CopyInput `json:"input"`
	SseKeySha256 string    `json:"sseKeySha256,omitempty"`
	KmsKeyID     string    `json:"kmsKeyId,omitempty"`
}

// WorkRequest records an asynchronous operation (copy, reencrypt).
type WorkRequest struct {
	TimeAccepted  time.Time    `json:"timeAccepted"`
	TimeFinished  time.Time    `json:"timeFinished"`
	Pending       *PendingCopy `json:"pending,omitempty"`
	ID            string       `json:"id"`
	OperationType string       `json:"operationType"`
	Status        string       `json:"status"`
	CompartmentID string       `json:"compartmentId"`
	EntityURI     string       `json:"entityUri"`
	Message       string       `json:"message,omitempty"`
}

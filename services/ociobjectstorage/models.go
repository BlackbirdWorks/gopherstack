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
	Meta           ObjectMeta `json:"meta"`
	Name           string     `json:"name"`
	VersionID      string     `json:"versionId"`
	Etag           string     `json:"etag"`
	MD5            string     `json:"md5"`
	MultipartMD5   string     `json:"multipartMd5,omitempty"`
	Data           []byte     `json:"data"`
	IsDeleteMarker bool       `json:"isDeleteMarker"`
}

// Part is one uploaded multipart part.
type Part struct {
	Etag string `json:"etag"`
	MD5  string `json:"md5"`
	Data []byte `json:"data"`
}

// Upload is an in-flight multipart upload.
type Upload struct {
	TimeCreated time.Time    `json:"timeCreated"`
	Parts       map[int]Part `json:"parts"`
	ID          string       `json:"id"`
	Object      string       `json:"object"`
	Meta        ObjectMeta   `json:"meta"`
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
	Name                string                    `json:"name"`
	CompartmentID       string                    `json:"compartmentId"`
	CreatedBy           string                    `json:"createdBy"`
	Etag                string                    `json:"etag"`
	PublicAccessType    string                    `json:"publicAccessType"`
	StorageTier         string                    `json:"storageTier"`
	Versioning          string                    `json:"versioning"`
	AutoTiering         string                    `json:"autoTiering,omitempty"`
	KmsKeyID            string                    `json:"kmsKeyId,omitempty"`
	ID                  string                    `json:"id"`
	LifecycleEtag       string                    `json:"lifecycleEtag,omitempty"`
	ObjectEventsEnabled bool                      `json:"objectEventsEnabled"`
	IsBucketKeyEnabled  bool                      `json:"isBucketKeyEnabled"`
}

// WorkRequest records an asynchronous operation (copy); the emulator completes it synchronously.
type WorkRequest struct {
	TimeAccepted  time.Time `json:"timeAccepted"`
	TimeFinished  time.Time `json:"timeFinished"`
	ID            string    `json:"id"`
	OperationType string    `json:"operationType"`
	Status        string    `json:"status"`
	CompartmentID string    `json:"compartmentId"`
	EntityURI     string    `json:"entityUri"`
}

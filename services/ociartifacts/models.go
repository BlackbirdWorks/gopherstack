package ociartifacts

import "time"

// Lifecycle states.
const (
	stateAvailable = "AVAILABLE"
	stateDeleted   = "DELETED"
)

// Readme formats.
const (
	readmeMarkdown = "TEXT_MARKDOWN"
	readmePlain    = "TEXT_PLAIN"
)

// Tags are the shared freeform and defined tag sets of a resource.
type Tags struct {
	FreeformTags map[string]string         `json:"freeformTags,omitempty"`
	DefinedTags  map[string]map[string]any `json:"definedTags,omitempty"`
}

func (t *Tags) merge(in Tags) {
	if in.FreeformTags != nil {
		t.FreeformTags = in.FreeformTags
	}

	if in.DefinedTags != nil {
		t.DefinedTags = in.DefinedTags
	}
}

func (t *Tags) free() map[string]string {
	if t.FreeformTags == nil {
		return map[string]string{}
	}

	return t.FreeformTags
}

func (t *Tags) defined() map[string]map[string]any {
	if t.DefinedTags == nil {
		return map[string]map[string]any{}
	}

	return t.DefinedTags
}

// Readme is a container repository's README.
type Readme struct {
	Content string `json:"content"`
	Format  string `json:"format"`
}

// Repo is a container repository.
type Repo struct {
	TimeCreated    time.Time  `json:"timeCreated"`
	TimeLastPushed *time.Time `json:"timeLastPushed,omitempty"`
	Readme         *Readme    `json:"readme,omitempty"`
	Tags           Tags       `json:"tags"`
	ID             string     `json:"id"`
	Name           string     `json:"name"`
	CompartmentID  string     `json:"compartmentId"`
	CreatedBy      string     `json:"createdBy"`
	Etag           string     `json:"etag"`
	Seq            int64      `json:"seq"`
	IsImmutable    bool       `json:"isImmutable"`
	IsPublic       bool       `json:"isPublic"`
}

// Version is a tag of an image.
type Version struct {
	TimeCreated time.Time `json:"timeCreated"`
	Version     string    `json:"version"`
	CreatedBy   string    `json:"createdBy"`
}

// Layer is one blob of an image.
type Layer struct {
	TimeCreated time.Time `json:"timeCreated"`
	Digest      string    `json:"digest"`
	Size        int64     `json:"size"`
}

// Image is a manifest pushed to a container repository.
type Image struct {
	TimeCreated    time.Time  `json:"timeCreated"`
	TimeLastPulled *time.Time `json:"timeLastPulled,omitempty"`
	Tags           Tags       `json:"tags"`
	ID             string     `json:"id"`
	RepoID         string     `json:"repoId"`
	Digest         string     `json:"digest"`
	CreatedBy      string     `json:"createdBy"`
	State          string     `json:"state"`
	LastVersion    string     `json:"lastVersion,omitempty"`
	Etag           string     `json:"etag"`
	Versions       []Version  `json:"versions"`
	Layers         []Layer    `json:"layers"`
	LayersSize     int64      `json:"layersSize"`
	PullCount      int64      `json:"pullCount"`
	Seq            int64      `json:"seq"`
	ManifestSize   int        `json:"manifestSize"`
}

// Signature is a container image signature.
type Signature struct {
	TimeCreated     time.Time `json:"timeCreated"`
	Tags            Tags      `json:"tags"`
	ID              string    `json:"id"`
	ImageID         string    `json:"imageId"`
	CompartmentID   string    `json:"compartmentId"`
	CreatedBy       string    `json:"createdBy"`
	KmsKeyID        string    `json:"kmsKeyId"`
	KmsKeyVersionID string    `json:"kmsKeyVersionId"`
	Message         string    `json:"message"`
	Signature       string    `json:"signature"`
	Algorithm       string    `json:"signingAlgorithm"`
	Etag            string    `json:"etag"`
	Seq             int64     `json:"seq"`
}

// GenericRepo is a generic artifacts repository.
type GenericRepo struct {
	TimeCreated   time.Time `json:"timeCreated"`
	Tags          Tags      `json:"tags"`
	ID            string    `json:"id"`
	Name          string    `json:"name"`
	Description   string    `json:"description"`
	CompartmentID string    `json:"compartmentId"`
	Etag          string    `json:"etag"`
	Seq           int64     `json:"seq"`
	IsImmutable   bool      `json:"isImmutable"`
}

// GenericArtifact is one uploaded generic artifact version.
type GenericArtifact struct {
	TimeCreated time.Time `json:"timeCreated"`
	Tags        Tags      `json:"tags"`
	ID          string    `json:"id"`
	RepoID      string    `json:"repoId"`
	Path        string    `json:"path"`
	Version     string    `json:"version"`
	Sha256      string    `json:"sha256"`
	Etag        string    `json:"etag"`
	Content     []byte    `json:"content"`
	Seq         int64     `json:"seq"`
}

type readmeWire struct {
	Content string `json:"content"`
	Format  string `json:"format"`
}

type repoWire struct {
	TimeCreated       time.Time                 `json:"timeCreated"`
	TimeLastPushed    *time.Time                `json:"timeLastPushed,omitempty"`
	Readme            *readmeWire               `json:"readme,omitempty"`
	FreeformTags      map[string]string         `json:"freeformTags"`
	DefinedTags       map[string]map[string]any `json:"definedTags"`
	SystemTags        map[string]map[string]any `json:"systemTags"`
	CompartmentID     string                    `json:"compartmentId"`
	CreatedBy         string                    `json:"createdBy"`
	DisplayName       string                    `json:"displayName"`
	ID                string                    `json:"id"`
	LifecycleState    string                    `json:"lifecycleState"`
	Namespace         string                    `json:"namespace"`
	LayersSizeInBytes int64                     `json:"layersSizeInBytes"`
	BillableSizeInGBs int64                     `json:"billableSizeInGBs"`
	ImageCount        int                       `json:"imageCount"`
	LayerCount        int                       `json:"layerCount"`
	IsImmutable       bool                      `json:"isImmutable"`
	IsPublic          bool                      `json:"isPublic"`
}

type repoSummaryWire struct {
	TimeCreated       time.Time                 `json:"timeCreated"`
	FreeformTags      map[string]string         `json:"freeformTags"`
	DefinedTags       map[string]map[string]any `json:"definedTags"`
	SystemTags        map[string]map[string]any `json:"systemTags"`
	CompartmentID     string                    `json:"compartmentId"`
	DisplayName       string                    `json:"displayName"`
	ID                string                    `json:"id"`
	LifecycleState    string                    `json:"lifecycleState"`
	Namespace         string                    `json:"namespace"`
	LayersSizeInBytes int64                     `json:"layersSizeInBytes"`
	BillableSizeInGBs int64                     `json:"billableSizeInGBs"`
	ImageCount        int                       `json:"imageCount"`
	LayerCount        int                       `json:"layerCount"`
	IsPublic          bool                      `json:"isPublic"`
}

type repoCollectionWire struct {
	Items               []repoSummaryWire `json:"items"`
	LayersSizeInBytes   int64             `json:"layersSizeInBytes"`
	ImageCount          int               `json:"imageCount"`
	LayerCount          int               `json:"layerCount"`
	RemainingItemsCount int               `json:"remainingItemsCount"`
	RepositoryCount     int               `json:"repositoryCount"`
}

type versionWire struct {
	TimeCreated time.Time `json:"timeCreated"`
	CreatedBy   string    `json:"createdBy"`
	Version     string    `json:"version"`
}

type layerWire struct {
	TimeCreated time.Time `json:"timeCreated"`
	Digest      string    `json:"digest"`
	SizeInBytes int64     `json:"sizeInBytes"`
}

type imageWire struct {
	TimeCreated         time.Time                 `json:"timeCreated"`
	TimeLastPulled      *time.Time                `json:"timeLastPulled,omitempty"`
	FreeformTags        map[string]string         `json:"freeformTags"`
	DefinedTags         map[string]map[string]any `json:"definedTags"`
	SystemTags          map[string]map[string]any `json:"systemTags"`
	CompartmentID       string                    `json:"compartmentId"`
	CreatedBy           string                    `json:"createdBy"`
	Digest              string                    `json:"digest"`
	DisplayName         string                    `json:"displayName"`
	ID                  string                    `json:"id"`
	LifecycleState      string                    `json:"lifecycleState"`
	RepositoryID        string                    `json:"repositoryId"`
	RepositoryName      string                    `json:"repositoryName"`
	Version             *string                   `json:"version,omitempty"`
	Layers              []layerWire               `json:"layers"`
	Versions            []versionWire             `json:"versions"`
	LayersSizeInBytes   int64                     `json:"layersSizeInBytes"`
	PullCount           int64                     `json:"pullCount"`
	ManifestSizeInBytes int                       `json:"manifestSizeInBytes"`
}

type imageSummaryWire struct {
	TimeCreated    time.Time                 `json:"timeCreated"`
	FreeformTags   map[string]string         `json:"freeformTags"`
	DefinedTags    map[string]map[string]any `json:"definedTags"`
	SystemTags     map[string]map[string]any `json:"systemTags"`
	Version        *string                   `json:"version,omitempty"`
	CompartmentID  string                    `json:"compartmentId"`
	Digest         string                    `json:"digest"`
	DisplayName    string                    `json:"displayName"`
	ID             string                    `json:"id"`
	LifecycleState string                    `json:"lifecycleState"`
	RepositoryID   string                    `json:"repositoryId"`
	RepositoryName string                    `json:"repositoryName"`
}

type imageCollectionWire struct {
	Items               []imageSummaryWire `json:"items"`
	RemainingItemsCount int                `json:"remainingItemsCount"`
}

type signatureWire struct {
	TimeCreated      time.Time                 `json:"timeCreated"`
	FreeformTags     map[string]string         `json:"freeformTags"`
	DefinedTags      map[string]map[string]any `json:"definedTags"`
	SystemTags       map[string]map[string]any `json:"systemTags"`
	CompartmentID    string                    `json:"compartmentId"`
	CreatedBy        string                    `json:"createdBy,omitempty"`
	DisplayName      string                    `json:"displayName"`
	ID               string                    `json:"id"`
	ImageID          string                    `json:"imageId"`
	KmsKeyID         string                    `json:"kmsKeyId"`
	KmsKeyVersionID  string                    `json:"kmsKeyVersionId"`
	Message          string                    `json:"message"`
	Signature        string                    `json:"signature"`
	SigningAlgorithm string                    `json:"signingAlgorithm"`
	LifecycleState   string                    `json:"lifecycleState"`
}

type signatureCollectionWire struct {
	Items               []signatureWire `json:"items"`
	RemainingItemsCount int             `json:"remainingItemsCount"`
}

type configWire struct {
	Namespace                      string `json:"namespace"`
	IsRepositoryCreatedOnFirstPush bool   `json:"isRepositoryCreatedOnFirstPush"`
}

type genericRepoWire struct {
	TimeCreated    time.Time                 `json:"timeCreated"`
	FreeformTags   map[string]string         `json:"freeformTags"`
	DefinedTags    map[string]map[string]any `json:"definedTags"`
	ID             string                    `json:"id"`
	DisplayName    string                    `json:"displayName"`
	CompartmentID  string                    `json:"compartmentId"`
	Description    string                    `json:"description"`
	LifecycleState string                    `json:"lifecycleState"`
	RepositoryType string                    `json:"repositoryType"`
	IsImmutable    bool                      `json:"isImmutable"`
}

type genericRepoCollectionWire struct {
	Items []genericRepoWire `json:"items"`
}

type genericArtifactWire struct {
	TimeCreated    time.Time                 `json:"timeCreated"`
	FreeformTags   map[string]string         `json:"freeformTags"`
	DefinedTags    map[string]map[string]any `json:"definedTags"`
	ID             string                    `json:"id"`
	DisplayName    string                    `json:"displayName"`
	CompartmentID  string                    `json:"compartmentId"`
	RepositoryID   string                    `json:"repositoryId"`
	ArtifactPath   string                    `json:"artifactPath"`
	Version        string                    `json:"version"`
	Sha256         string                    `json:"sha256"`
	LifecycleState string                    `json:"lifecycleState"`
	SizeInBytes    int64                     `json:"sizeInBytes"`
}

type genericArtifactCollectionWire struct {
	Items []genericArtifactWire `json:"items"`
}

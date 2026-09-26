package ecrpublic

import "time"

// Repository is the domain model for a public ECR repository.
type Repository struct {
	CreatedAt      time.Time
	Tags           map[string]string
	RepositoryName string
	RepositoryArn  string
	RegistryID     string
	RepositoryURI  string
	PolicyText     string
	CatalogData    CatalogData
	HasPolicy      bool
}

// CatalogData is the publicly-visible Gallery metadata for a repository.
type CatalogData struct {
	AboutText            string
	Description          string
	UsageText            string
	LogoImageBlob        []byte
	Architectures        []string
	OperatingSystems     []string
	MarketplaceCertified bool
}

// RegistryCatalogData is the account-wide Gallery display metadata.
type RegistryCatalogData struct {
	DisplayName string
}

// Image is the domain model for a pushed image manifest, keyed by
// (repository, digest) via imageTableKey.
type Image struct {
	ImagePushedAt          time.Time
	RepositoryName         string
	ImageDigest            string
	ImageManifest          string
	ImageManifestMediaType string
	ArtifactMediaType      string
	RegistryID             string
	ImageSizeInBytes       int64
}

// tagBinding records which digest a tag currently points to, and when the
// binding was created (surfaced by DescribeImageTags).
type tagBinding struct {
	CreatedAt time.Time
	Digest    string
}

// layerUploadState tracks an in-progress InitiateLayerUpload session. Never
// persisted: AWS does not guarantee in-flight uploads survive a restart.
type layerUploadState struct {
	CreatedAt      time.Time
	RepositoryName string
	Data           []byte
	PartSizes      []int64
	Size           int64
}

// ImageIdentifier identifies an image by digest, tag, or both.
type ImageIdentifier struct {
	ImageDigest string
	ImageTag    string
}

// ImageDetail is a computed, request-time view of an Image annotated with its
// current tags; it is not itself persisted state.
type ImageDetail struct {
	ImagePushedAt          time.Time
	ArtifactMediaType      string
	ImageDigest            string
	ImageManifestMediaType string
	RegistryID             string
	RepositoryName         string
	ImageTags              []string
	ImageSizeInBytes       int64
}

// ImageTagDetail is a computed, request-time view of a single tag binding.
type ImageTagDetail struct {
	CreatedAt              time.Time
	ImagePushedAt          time.Time
	ImageTag               string
	ArtifactMediaType      string
	ImageDigest            string
	ImageManifestMediaType string
	ImageSizeInBytes       int64
}

// ImageFailure describes an image that BatchDeleteImage could not process.
type ImageFailure struct {
	ImageID       ImageIdentifier
	FailureCode   string
	FailureReason string
}

// LayerInfo describes an available image layer.
type LayerInfo struct {
	LayerDigest       string
	LayerAvailability string
	MediaType         string
	LayerSize         int64
}

// LayerFailure describes a layer digest BatchCheckLayerAvailability could not find.
type LayerFailure struct {
	LayerDigest   string
	FailureCode   string
	FailureReason string
}

// RegistryAliasInfo describes one alias of a public registry.
type RegistryAliasInfo struct {
	Name                 string
	Status               string
	DefaultRegistryAlias bool
	PrimaryRegistryAlias bool
}

// RegistryInfo describes a public registry, computed from backend state.
type RegistryInfo struct {
	RegistryArn string
	RegistryID  string
	RegistryURI string
	Aliases     []RegistryAliasInfo
	Verified    bool
}

// PutImageRequest carries the fields PutImage needs from the wire request.
type PutImageRequest struct {
	ImageDigest            string
	ImageManifest          string
	ImageManifestMediaType string
	ImageTag               string
}

package ecrpublic

// Wire DTOs for the Amazon ECR Public JSON-RPC (awsjson1.1) API. Field names
// and shapes are verified against the pinned aws-sdk-go-v2/service/ecrpublic
// v1.47.1 request_snapshot/ and response_snapshot/ fixtures (that SDK version
// generates via smithy schemas rather than serializers.go/deserializers.go,
// so the snapshot fixtures -- exact captured wire JSON per operation -- are
// the authoritative source here instead of a serializer function body).
//
// Tag.Key/Tag.Value are capitalized on the wire (unlike every other field in
// this API, which is lowerCamelCase) -- confirmed by
// request_snapshot/CreateRepository.request.snap: `{"Key":"...","Value":"..."}`.
import "github.com/blackbirdworks/gopherstack/pkgs/awstime"

// TagWire mirrors types.Tag.
type TagWire struct {
	Key   string `json:"Key"`
	Value string `json:"Value"`
}

// RepositoryWire mirrors types.Repository.
type RepositoryWire struct {
	RegistryID     string  `json:"registryId,omitempty"`
	RepositoryArn  string  `json:"repositoryArn,omitempty"`
	RepositoryName string  `json:"repositoryName,omitempty"`
	RepositoryURI  string  `json:"repositoryUri,omitempty"`
	CreatedAt      float64 `json:"createdAt,omitempty"`
}

// CatalogDataInputWire mirrors types.RepositoryCatalogDataInput.
type CatalogDataInputWire struct {
	AboutText        *string  `json:"aboutText,omitempty"`
	Description      *string  `json:"description,omitempty"`
	UsageText        *string  `json:"usageText,omitempty"`
	LogoImageBlob    []byte   `json:"logoImageBlob,omitempty"`
	Architectures    []string `json:"architectures,omitempty"`
	OperatingSystems []string `json:"operatingSystems,omitempty"`
}

// CatalogDataWire mirrors types.RepositoryCatalogData.
type CatalogDataWire struct {
	AboutText            string   `json:"aboutText,omitempty"`
	Description          string   `json:"description,omitempty"`
	LogoURL              string   `json:"logoUrl,omitempty"`
	UsageText            string   `json:"usageText,omitempty"`
	Architectures        []string `json:"architectures,omitempty"`
	OperatingSystems     []string `json:"operatingSystems,omitempty"`
	MarketplaceCertified bool     `json:"marketplaceCertified,omitempty"`
}

// RegistryAliasWire mirrors types.RegistryAlias.
type RegistryAliasWire struct {
	Name                 string `json:"name"`
	Status               string `json:"status"`
	DefaultRegistryAlias bool   `json:"defaultRegistryAlias"`
	PrimaryRegistryAlias bool   `json:"primaryRegistryAlias"`
}

// RegistryWire mirrors types.Registry.
type RegistryWire struct {
	RegistryArn string              `json:"registryArn"`
	RegistryID  string              `json:"registryId"`
	RegistryURI string              `json:"registryUri"`
	Aliases     []RegistryAliasWire `json:"aliases"`
	Verified    bool                `json:"verified"`
}

// RegistryCatalogDataWire mirrors types.RegistryCatalogData.
type RegistryCatalogDataWire struct {
	DisplayName string `json:"displayName,omitempty"`
}

// AuthorizationDataWire mirrors types.AuthorizationData.
type AuthorizationDataWire struct {
	AuthorizationToken string  `json:"authorizationToken,omitempty"`
	ExpiresAt          float64 `json:"expiresAt,omitempty"`
}

// ImageIdentifierWire mirrors types.ImageIdentifier.
type ImageIdentifierWire struct {
	ImageDigest string `json:"imageDigest,omitempty"`
	ImageTag    string `json:"imageTag,omitempty"`
}

// ImageDetailWire mirrors types.ImageDetail.
type ImageDetailWire struct {
	ArtifactMediaType      string   `json:"artifactMediaType,omitempty"`
	ImageDigest            string   `json:"imageDigest,omitempty"`
	ImageManifestMediaType string   `json:"imageManifestMediaType,omitempty"`
	RegistryID             string   `json:"registryId,omitempty"`
	RepositoryName         string   `json:"repositoryName,omitempty"`
	ImageTags              []string `json:"imageTags,omitempty"`
	ImagePushedAt          float64  `json:"imagePushedAt,omitempty"`
	ImageSizeInBytes       int64    `json:"imageSizeInBytes,omitempty"`
}

// ReferencedImageDetailWire mirrors types.ReferencedImageDetail.
type ReferencedImageDetailWire struct {
	ArtifactMediaType      string  `json:"artifactMediaType,omitempty"`
	ImageDigest            string  `json:"imageDigest,omitempty"`
	ImageManifestMediaType string  `json:"imageManifestMediaType,omitempty"`
	ImagePushedAt          float64 `json:"imagePushedAt,omitempty"`
	ImageSizeInBytes       int64   `json:"imageSizeInBytes,omitempty"`
}

// ImageTagDetailWire mirrors types.ImageTagDetail.
type ImageTagDetailWire struct {
	ImageDetail *ReferencedImageDetailWire `json:"imageDetail,omitempty"`
	ImageTag    string                     `json:"imageTag,omitempty"`
	CreatedAt   float64                    `json:"createdAt,omitempty"`
}

// ImageWire mirrors types.Image.
type ImageWire struct {
	ImageID                ImageIdentifierWire `json:"imageId"`
	ImageManifest          string              `json:"imageManifest,omitempty"`
	ImageManifestMediaType string              `json:"imageManifestMediaType,omitempty"`
	RegistryID             string              `json:"registryId,omitempty"`
	RepositoryName         string              `json:"repositoryName,omitempty"`
}

// LayerWire mirrors types.Layer.
type LayerWire struct {
	LayerAvailability string `json:"layerAvailability,omitempty"`
	LayerDigest       string `json:"layerDigest,omitempty"`
	MediaType         string `json:"mediaType,omitempty"`
	LayerSize         int64  `json:"layerSize,omitempty"`
}

// LayerFailureWire mirrors types.LayerFailure.
type LayerFailureWire struct {
	FailureCode   string `json:"failureCode,omitempty"`
	FailureReason string `json:"failureReason,omitempty"`
	LayerDigest   string `json:"layerDigest,omitempty"`
}

// ImageFailureWire mirrors types.ImageFailure.
type ImageFailureWire struct {
	FailureCode   string              `json:"failureCode,omitempty"`
	FailureReason string              `json:"failureReason,omitempty"`
	ImageID       ImageIdentifierWire `json:"imageId"`
}

func tagsToWire(tags map[string]string) []TagWire {
	out := make([]TagWire, 0, len(tags))
	for k, v := range tags {
		out = append(out, TagWire{Key: k, Value: v})
	}

	return out
}

func tagsFromWire(tags []TagWire) map[string]string {
	out := make(map[string]string, len(tags))
	for _, t := range tags {
		out[t.Key] = t.Value
	}

	return out
}

func toRepositoryWire(r *Repository) RepositoryWire {
	return RepositoryWire{
		CreatedAt:      awstime.Epoch(r.CreatedAt),
		RegistryID:     r.RegistryID,
		RepositoryArn:  r.RepositoryArn,
		RepositoryName: r.RepositoryName,
		RepositoryURI:  r.RepositoryURI,
	}
}

func toCatalogDataInput(w *CatalogDataInputWire) *CatalogData {
	if w == nil {
		return &CatalogData{}
	}

	cd := &CatalogData{
		Architectures:    w.Architectures,
		OperatingSystems: w.OperatingSystems,
		LogoImageBlob:    w.LogoImageBlob,
	}

	if w.AboutText != nil {
		cd.AboutText = *w.AboutText
	}

	if w.Description != nil {
		cd.Description = *w.Description
	}

	if w.UsageText != nil {
		cd.UsageText = *w.UsageText
	}

	return cd
}

func toCatalogDataWire(cd *CatalogData) CatalogDataWire {
	w := CatalogDataWire{
		AboutText:            cd.AboutText,
		Architectures:        cd.Architectures,
		Description:          cd.Description,
		OperatingSystems:     cd.OperatingSystems,
		UsageText:            cd.UsageText,
		MarketplaceCertified: cd.MarketplaceCertified,
	}

	if len(cd.LogoImageBlob) > 0 {
		w.LogoURL = "https://" + repositoryURIHost + "/logos/" + shortHash(string(cd.LogoImageBlob)) + ".png"
	}

	return w
}

func toImageIdentifierWire(id ImageIdentifier) ImageIdentifierWire {
	return ImageIdentifierWire(id)
}

func imageIdentifierFromWire(w ImageIdentifierWire) ImageIdentifier {
	return ImageIdentifier(w)
}

func toImageDetailWire(d ImageDetail) ImageDetailWire {
	return ImageDetailWire{
		ArtifactMediaType:      d.ArtifactMediaType,
		ImageDigest:            d.ImageDigest,
		ImageManifestMediaType: d.ImageManifestMediaType,
		ImagePushedAt:          awstime.Epoch(d.ImagePushedAt),
		ImageSizeInBytes:       d.ImageSizeInBytes,
		ImageTags:              d.ImageTags,
		RegistryID:             d.RegistryID,
		RepositoryName:         d.RepositoryName,
	}
}

func toImageTagDetailWire(d ImageTagDetail) ImageTagDetailWire {
	return ImageTagDetailWire{
		CreatedAt: awstime.Epoch(d.CreatedAt),
		ImageDetail: &ReferencedImageDetailWire{
			ArtifactMediaType:      d.ArtifactMediaType,
			ImageDigest:            d.ImageDigest,
			ImageManifestMediaType: d.ImageManifestMediaType,
			ImagePushedAt:          awstime.Epoch(d.ImagePushedAt),
			ImageSizeInBytes:       d.ImageSizeInBytes,
		},
		ImageTag: d.ImageTag,
	}
}

func toImageFailureWire(f ImageFailure) ImageFailureWire {
	return ImageFailureWire{
		FailureCode:   f.FailureCode,
		FailureReason: f.FailureReason,
		ImageID:       toImageIdentifierWire(f.ImageID),
	}
}

func toLayerWire(l LayerInfo) LayerWire {
	return LayerWire{
		LayerAvailability: l.LayerAvailability,
		LayerDigest:       l.LayerDigest,
		LayerSize:         l.LayerSize,
		MediaType:         l.MediaType,
	}
}

func toLayerFailureWire(f LayerFailure) LayerFailureWire {
	return LayerFailureWire{
		FailureCode:   f.FailureCode,
		FailureReason: f.FailureReason,
		LayerDigest:   f.LayerDigest,
	}
}

func toRegistryWire(info RegistryInfo) RegistryWire {
	aliases := make([]RegistryAliasWire, 0, len(info.Aliases))
	for _, a := range info.Aliases {
		aliases = append(aliases, RegistryAliasWire(a))
	}

	return RegistryWire{
		Aliases:     aliases,
		RegistryArn: info.RegistryArn,
		RegistryID:  info.RegistryID,
		RegistryURI: info.RegistryURI,
		Verified:    info.Verified,
	}
}

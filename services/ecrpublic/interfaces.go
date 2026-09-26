package ecrpublic

import "context"

// Backend is the interface for the Amazon ECR Public backend.
type Backend interface {
	AccountID() string

	CreateRepository(name string, catalogData *CatalogData, tags map[string]string) (*Repository, error)
	DescribeRepositories(registryID string, names []string) ([]*Repository, error)
	DeleteRepository(registryID, name string, force bool) (*Repository, error)

	GetRepositoryCatalogData(registryID, name string) (*CatalogData, error)
	PutRepositoryCatalogData(registryID, name string, catalogData *CatalogData) (*CatalogData, error)

	GetRepositoryPolicy(registryID, name string) (string, error)
	SetRepositoryPolicy(registryID, name, policyText string) (string, error)
	DeleteRepositoryPolicy(registryID, name string) (string, error)

	TagResource(resourceARN string, tags map[string]string) error
	UntagResource(resourceARN string, tagKeys []string) error
	ListTagsForResource(resourceARN string) (map[string]string, error)

	DescribeRegistries() ([]RegistryInfo, error)
	GetRegistryCatalogData() (RegistryCatalogData, error)
	PutRegistryCatalogData(displayName string) (RegistryCatalogData, error)
	GetAuthorizationToken(ctx context.Context) (string, int64, error)

	DescribeImages(registryID, repositoryName string, imageIDs []ImageIdentifier) ([]ImageDetail, error)
	DescribeImageTags(registryID, repositoryName string) ([]ImageTagDetail, error)
	PutImage(registryID, repositoryName string, req PutImageRequest) (*Image, error)
	BatchDeleteImage(
		registryID, repositoryName string, imageIDs []ImageIdentifier,
	) ([]ImageIdentifier, []ImageFailure, error)

	BatchCheckLayerAvailability(
		registryID, repositoryName string, layerDigests []string,
	) ([]LayerInfo, []LayerFailure, error)
	InitiateLayerUpload(registryID, repositoryName string) (uploadID string, partSize int64, err error)
	UploadLayerPart(
		registryID, repositoryName, uploadID string, firstByte, lastByte int64, blob []byte,
	) (lastByteReceived int64, err error)
	CompleteLayerUpload(
		registryID, repositoryName, uploadID string, layerDigests []string,
	) (digest string, err error)

	Reset()
}

// Compile-time assertion that InMemoryBackend implements Backend.
var _ Backend = (*InMemoryBackend)(nil)

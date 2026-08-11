package datasync

import (
	"fmt"
	"maps"
	"strings"
	"time"
)

// --- AzureBlob ---

func (b *InMemoryBackend) CreateLocationAzureBlob(
	containerURL, subdirectory, blobType, accessTier, authenticationType string,
	sasConfig *SasConfiguration,
	agentArns []string,
	tags map[string]string,
	secretConfig SecretConfig,
) (*Location, error) {
	b.mu.Lock("CreateLocationAzureBlob")
	defer b.mu.Unlock()

	if err := b.validateAgentArns(agentArns); err != nil {
		return nil, err
	}

	id := newID()
	locationArn := b.locationARN(id)
	now := time.Now().UTC()

	sub := strings.TrimPrefix(subdirectory, "/")
	locationURI := fmt.Sprintf("azure-blob://%s/%s", containerURL, sub)

	locationTags := make(map[string]string)
	maps.Copy(locationTags, tags)

	cfg := &storedAzureBlobConfig{
		ContainerURL:       containerURL,
		BlobType:           blobType,
		AccessTier:         accessTier,
		AuthenticationType: authenticationType,
		AgentArns:          agentArns,
		CmkSecretConfig:    toStoredCmkSecretConfig(secretConfig.Cmk),
		CustomSecretConfig: toStoredCustomSecretConfig(secretConfig.Custom),
	}
	if sasConfig != nil {
		cfg.SasToken = sasConfig.Token
	}

	l := &storedLocation{
		LocationArn:  locationArn,
		LocationURI:  locationURI,
		Subdirectory: subdirectory,
		LocationType: locationTypeAzureBlob,
		CreationTime: now,
		Tags:         locationTags,
		AzureBlob:    cfg,
	}
	b.locations.Put(l)

	if len(locationTags) > 0 {
		b.tags[locationArn] = make(map[string]string)
		maps.Copy(b.tags[locationArn], locationTags)
	}

	cp := l.toLocation()

	return &cp, nil
}

func (b *InMemoryBackend) DescribeLocationAzureBlob(locationArn string) (*LocationAzureBlob, error) {
	b.mu.RLock("DescribeLocationAzureBlob")
	defer b.mu.RUnlock()

	l, ok := b.locations.Get(locationArn)
	if !ok || l.LocationType != locationTypeAzureBlob {
		return nil, ErrNotFound
	}

	out := &LocationAzureBlob{
		LocationArn:  l.LocationArn,
		LocationURI:  l.LocationURI,
		Subdirectory: l.Subdirectory,
		CreationTime: l.CreationTime,
	}

	if l.AzureBlob != nil {
		out.ContainerURL = l.AzureBlob.ContainerURL
		out.BlobType = l.AzureBlob.BlobType
		out.AccessTier = l.AzureBlob.AccessTier
		out.AuthenticationType = l.AzureBlob.AuthenticationType
		out.AgentArns = l.AzureBlob.AgentArns

		if l.AzureBlob.SasToken != "" {
			out.SasConfiguration = &SasConfiguration{Token: l.AzureBlob.SasToken}
		}

		out.CmkSecretConfig = fromStoredCmkSecretConfig(l.AzureBlob.CmkSecretConfig)
		out.CustomSecretConfig = fromStoredCustomSecretConfig(l.AzureBlob.CustomSecretConfig)
	}

	return out, nil
}

func (b *InMemoryBackend) UpdateLocationAzureBlob(
	locationArn, containerURL, subdirectory, blobType, accessTier, authenticationType string,
	sasConfig *SasConfiguration,
	agentArns []string,
	secretConfig SecretConfig,
) error {
	b.mu.Lock("UpdateLocationAzureBlob")
	defer b.mu.Unlock()

	l, ok := b.locations.Get(locationArn)
	if !ok || l.LocationType != locationTypeAzureBlob {
		return ErrNotFound
	}

	if err := b.validateAgentArns(agentArns); err != nil {
		return err
	}

	if containerURL != "" {
		l.AzureBlob.ContainerURL = containerURL
	}

	if subdirectory != "" {
		l.Subdirectory = subdirectory
		sub := strings.TrimPrefix(subdirectory, "/")
		cu := l.AzureBlob.ContainerURL
		l.LocationURI = fmt.Sprintf("azure-blob://%s/%s", cu, sub)
	}

	if blobType != "" {
		l.AzureBlob.BlobType = blobType
	}

	if accessTier != "" {
		l.AzureBlob.AccessTier = accessTier
	}

	if authenticationType != "" {
		l.AzureBlob.AuthenticationType = authenticationType
	}

	if sasConfig != nil {
		l.AzureBlob.SasToken = sasConfig.Token
	}

	if secretConfig.Cmk != nil {
		l.AzureBlob.CmkSecretConfig = toStoredCmkSecretConfig(secretConfig.Cmk)
	}

	if secretConfig.Custom != nil {
		l.AzureBlob.CustomSecretConfig = toStoredCustomSecretConfig(secretConfig.Custom)
	}

	if agentArns != nil {
		l.AzureBlob.AgentArns = agentArns
	}

	return nil
}

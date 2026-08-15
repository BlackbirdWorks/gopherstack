package datasync

import (
	"fmt"
	"maps"
	"strings"
	"time"
)

// --- ObjectStorage ---

func (b *InMemoryBackend) CreateLocationObjectStorage(
	serverHostname, serverProtocol, bucketName, subdirectory, accessKey, secretKey string,
	serverPort int32,
	agentArns []string,
	tags map[string]string,
	secretConfig SecretConfig,
) (*Location, error) {
	b.mu.Lock("CreateLocationObjectStorage")
	defer b.mu.Unlock()

	if err := b.validateAgentArns(agentArns); err != nil {
		return nil, err
	}

	id := newID()
	locationArn := b.locationARN(id)
	now := time.Now().UTC()

	sub := strings.TrimPrefix(subdirectory, "/")
	locationURI := fmt.Sprintf("object-storage://%s/%s/%s", serverHostname, bucketName, sub)

	locationTags := make(map[string]string)
	maps.Copy(locationTags, tags)

	l := &storedLocation{
		LocationArn:  locationArn,
		LocationURI:  locationURI,
		Subdirectory: subdirectory,
		LocationType: locationTypeObjectStorage,
		CreationTime: now,
		Tags:         locationTags,
		ObjectStorage: &storedObjectStorageConfig{
			ServerHostname:     serverHostname,
			ServerProtocol:     serverProtocol,
			BucketName:         bucketName,
			AccessKey:          accessKey,
			SecretKey:          secretKey,
			ServerPort:         serverPort,
			AgentArns:          agentArns,
			CmkSecretConfig:    toStoredCmkSecretConfig(secretConfig.Cmk),
			CustomSecretConfig: toStoredCustomSecretConfig(secretConfig.Custom),
		},
	}
	b.locations.Put(l)

	if len(locationTags) > 0 {
		b.tags[locationArn] = make(map[string]string)
		maps.Copy(b.tags[locationArn], locationTags)
	}

	cp := l.toLocation()

	return &cp, nil
}

func (b *InMemoryBackend) DescribeLocationObjectStorage(locationArn string) (*LocationObjectStorage, error) {
	b.mu.RLock("DescribeLocationObjectStorage")
	defer b.mu.RUnlock()

	l, ok := b.locations.Get(locationArn)
	if !ok || l.LocationType != locationTypeObjectStorage {
		return nil, ErrNotFound
	}

	out := &LocationObjectStorage{
		LocationArn:  l.LocationArn,
		LocationURI:  l.LocationURI,
		Subdirectory: l.Subdirectory,
		CreationTime: l.CreationTime,
	}

	if l.ObjectStorage != nil {
		out.ServerHostname = l.ObjectStorage.ServerHostname
		out.ServerProtocol = l.ObjectStorage.ServerProtocol
		out.BucketName = l.ObjectStorage.BucketName
		out.AccessKey = l.ObjectStorage.AccessKey
		out.ServerPort = l.ObjectStorage.ServerPort
		out.AgentArns = l.ObjectStorage.AgentArns
		out.CmkSecretConfig = fromStoredCmkSecretConfig(l.ObjectStorage.CmkSecretConfig)
		out.CustomSecretConfig = fromStoredCustomSecretConfig(l.ObjectStorage.CustomSecretConfig)
	}

	return out, nil
}

func (b *InMemoryBackend) UpdateLocationObjectStorage(
	locationArn, serverHostname, serverProtocol, subdirectory, accessKey, secretKey string,
	serverPort int32,
	agentArns []string,
	secretConfig SecretConfig,
) error {
	b.mu.Lock("UpdateLocationObjectStorage")
	defer b.mu.Unlock()

	l, ok := b.locations.Get(locationArn)
	if !ok || l.LocationType != locationTypeObjectStorage {
		return ErrNotFound
	}

	if err := b.validateAgentArns(agentArns); err != nil {
		return err
	}

	if l.ObjectStorage == nil {
		l.ObjectStorage = &storedObjectStorageConfig{}
	}

	if serverHostname != "" {
		l.ObjectStorage.ServerHostname = serverHostname
	}

	if subdirectory != "" {
		l.Subdirectory = subdirectory
	}

	if serverHostname != "" || subdirectory != "" {
		sub := strings.TrimPrefix(l.Subdirectory, "/")
		l.LocationURI = fmt.Sprintf(
			"object-storage://%s/%s/%s",
			l.ObjectStorage.ServerHostname,
			l.ObjectStorage.BucketName,
			sub,
		)
	}

	updateObjectStorageFields(l.ObjectStorage, serverProtocol, accessKey, secretKey, serverPort, agentArns)
	updateObjectStorageSecretConfig(l.ObjectStorage, secretConfig)

	return nil
}

func updateObjectStorageFields(
	cfg *storedObjectStorageConfig,
	serverProtocol, accessKey, secretKey string,
	serverPort int32,
	agentArns []string,
) {
	if serverProtocol != "" {
		cfg.ServerProtocol = serverProtocol
	}

	if accessKey != "" {
		cfg.AccessKey = accessKey
	}

	if secretKey != "" {
		cfg.SecretKey = secretKey
	}

	if serverPort > 0 {
		cfg.ServerPort = serverPort
	}

	if agentArns != nil {
		cfg.AgentArns = agentArns
	}
}

func updateObjectStorageSecretConfig(cfg *storedObjectStorageConfig, secretConfig SecretConfig) {
	if secretConfig.Cmk != nil {
		cfg.CmkSecretConfig = toStoredCmkSecretConfig(secretConfig.Cmk)
	}

	if secretConfig.Custom != nil {
		cfg.CustomSecretConfig = toStoredCustomSecretConfig(secretConfig.Custom)
	}
}

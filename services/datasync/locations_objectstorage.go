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
) (*Location, error) {
	b.mu.Lock("CreateLocationObjectStorage")
	defer b.mu.Unlock()

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
			ServerHostname: serverHostname,
			ServerProtocol: serverProtocol,
			BucketName:     bucketName,
			AccessKey:      accessKey,
			SecretKey:      secretKey,
			ServerPort:     serverPort,
			AgentArns:      agentArns,
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
	}

	return out, nil
}

func (b *InMemoryBackend) UpdateLocationObjectStorage(
	locationArn, serverProtocol, subdirectory, accessKey, secretKey string,
	serverPort int32,
	agentArns []string,
) error {
	b.mu.Lock("UpdateLocationObjectStorage")
	defer b.mu.Unlock()

	l, ok := b.locations.Get(locationArn)
	if !ok || l.LocationType != locationTypeObjectStorage {
		return ErrNotFound
	}

	if l.ObjectStorage == nil {
		l.ObjectStorage = &storedObjectStorageConfig{}
	}

	if subdirectory != "" {
		l.Subdirectory = subdirectory
		sub := strings.TrimPrefix(subdirectory, "/")
		l.LocationURI = fmt.Sprintf(
			"object-storage://%s/%s/%s",
			l.ObjectStorage.ServerHostname,
			l.ObjectStorage.BucketName,
			sub,
		)
	}

	if serverProtocol != "" {
		l.ObjectStorage.ServerProtocol = serverProtocol
	}

	if accessKey != "" {
		l.ObjectStorage.AccessKey = accessKey
	}

	if secretKey != "" {
		l.ObjectStorage.SecretKey = secretKey
	}

	if serverPort > 0 {
		l.ObjectStorage.ServerPort = serverPort
	}

	if agentArns != nil {
		l.ObjectStorage.AgentArns = agentArns
	}

	return nil
}

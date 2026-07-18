package datasync

import (
	"fmt"
	"maps"
	"strings"
	"time"
)

// --- FsxOpenZfs ---

func (b *InMemoryBackend) CreateLocationFsxOpenZfs(
	fsxFilesystemArn, subdirectory string,
	protocol *FsxProtocol,
	securityGroupArns []string,
	tags map[string]string,
) (*Location, error) {
	b.mu.Lock("CreateLocationFsxOpenZfs")
	defer b.mu.Unlock()

	id := newID()
	locationArn := b.locationARN(id)
	now := time.Now().UTC()

	sub := strings.TrimPrefix(subdirectory, "/")
	// AWS uses the "fsxz://" scheme for FSx OpenZFS location URIs (e.g.
	// "fsxz://us-west-2.fs-1234567890abcdef02/fsx/folderA/folder" per the
	// DescribeLocationFsxOpenZfs API docs), not "openzfs://".
	locationURI := fmt.Sprintf("fsxz://%s/%s", fsxFilesystemArn, sub)

	locationTags := make(map[string]string)
	maps.Copy(locationTags, tags)

	l := &storedLocation{
		LocationArn:  locationArn,
		LocationURI:  locationURI,
		Subdirectory: subdirectory,
		LocationType: locationTypeFsxOpenZfs,
		CreationTime: now,
		Tags:         locationTags,
		FsxOpenZfs: &storedFsxOpenZfsConfig{
			FsxFilesystemArn:  fsxFilesystemArn,
			SecurityGroupArns: securityGroupArns,
			Protocol:          toStoredFsxProtocol(protocol),
		},
	}

	cp := b.storeLocation(l)

	return &cp, nil
}

func (b *InMemoryBackend) DescribeLocationFsxOpenZfs(locationArn string) (*LocationFsxOpenZfs, error) {
	b.mu.RLock("DescribeLocationFsxOpenZfs")
	defer b.mu.RUnlock()

	l, ok := b.locations.Get(locationArn)
	if !ok || l.LocationType != locationTypeFsxOpenZfs {
		return nil, ErrNotFound
	}

	out := &LocationFsxOpenZfs{
		LocationArn:  l.LocationArn,
		LocationURI:  l.LocationURI,
		Subdirectory: l.Subdirectory,
		CreationTime: l.CreationTime,
	}

	if l.FsxOpenZfs != nil {
		out.FsxFilesystemArn = l.FsxOpenZfs.FsxFilesystemArn
		out.SecurityGroupArns = l.FsxOpenZfs.SecurityGroupArns
		out.Protocol = fromStoredFsxProtocol(l.FsxOpenZfs.Protocol)
	}

	return out, nil
}

func (b *InMemoryBackend) UpdateLocationFsxOpenZfs(locationArn, subdirectory string, protocol *FsxProtocol) error {
	b.mu.Lock("UpdateLocationFsxOpenZfs")
	defer b.mu.Unlock()

	l, ok := b.locations.Get(locationArn)
	if !ok || l.LocationType != locationTypeFsxOpenZfs {
		return ErrNotFound
	}

	if subdirectory != "" {
		l.Subdirectory = subdirectory
		fsArn := ""
		if l.FsxOpenZfs != nil {
			fsArn = l.FsxOpenZfs.FsxFilesystemArn
		}

		sub := strings.TrimPrefix(subdirectory, "/")
		l.LocationURI = fmt.Sprintf("fsxz://%s/%s", fsArn, sub)
	}

	if protocol != nil && l.FsxOpenZfs != nil {
		l.FsxOpenZfs.Protocol = toStoredFsxProtocol(protocol)
	}

	return nil
}

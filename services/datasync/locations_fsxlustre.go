package datasync

import (
	"fmt"
	"maps"
	"strings"
	"time"
)

// --- FsxLustre ---

func (b *InMemoryBackend) CreateLocationFsxLustre(
	fsxFilesystemArn, subdirectory string,
	securityGroupArns []string,
	tags map[string]string,
) (*Location, error) {
	b.mu.Lock("CreateLocationFsxLustre")
	defer b.mu.Unlock()

	id := newID()
	locationArn := b.locationARN(id)
	now := time.Now().UTC()

	sub := strings.TrimPrefix(subdirectory, "/")
	locationURI := fmt.Sprintf("%s%s/%s", fsxLustreURIScheme, fsxFilesystemArn, sub)

	locationTags := make(map[string]string)
	maps.Copy(locationTags, tags)

	l := &storedLocation{
		LocationArn:  locationArn,
		LocationURI:  locationURI,
		Subdirectory: subdirectory,
		LocationType: locationTypeFsxLustre,
		CreationTime: now,
		Tags:         locationTags,
		FsxLustre: &storedFsxLustreConfig{
			FsxFilesystemArn:  fsxFilesystemArn,
			SecurityGroupArns: securityGroupArns,
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

func (b *InMemoryBackend) DescribeLocationFsxLustre(locationArn string) (*LocationFsxLustre, error) {
	b.mu.RLock("DescribeLocationFsxLustre")
	defer b.mu.RUnlock()

	l, ok := b.locations.Get(locationArn)
	if !ok || l.LocationType != locationTypeFsxLustre {
		return nil, ErrNotFound
	}

	out := &LocationFsxLustre{
		LocationArn:  l.LocationArn,
		LocationURI:  l.LocationURI,
		Subdirectory: l.Subdirectory,
		CreationTime: l.CreationTime,
	}

	if l.FsxLustre != nil {
		out.FsxFilesystemArn = l.FsxLustre.FsxFilesystemArn
		out.SecurityGroupArns = l.FsxLustre.SecurityGroupArns
	}

	return out, nil
}

func (b *InMemoryBackend) UpdateLocationFsxLustre(locationArn, subdirectory string) error {
	b.mu.Lock("UpdateLocationFsxLustre")
	defer b.mu.Unlock()

	l, ok := b.locations.Get(locationArn)
	if !ok || l.LocationType != locationTypeFsxLustre {
		return ErrNotFound
	}

	if subdirectory != "" {
		l.Subdirectory = subdirectory
		fsArn := ""
		if l.FsxLustre != nil {
			fsArn = l.FsxLustre.FsxFilesystemArn
		}

		sub := strings.TrimPrefix(subdirectory, "/")
		l.LocationURI = fmt.Sprintf("%s%s/%s", fsxLustreURIScheme, fsArn, sub)
	}

	return nil
}

package datasync

import (
	"fmt"
	"maps"
	"strings"
	"time"
)

// --- FsxWindows ---

func (b *InMemoryBackend) CreateLocationFsxWindows(
	fsxFilesystemArn, subdirectory, domain, user, password string,
	securityGroupArns []string,
	tags map[string]string,
) (*Location, error) {
	b.mu.Lock("CreateLocationFsxWindows")
	defer b.mu.Unlock()

	id := newID()
	locationArn := b.locationARN(id)
	now := time.Now().UTC()

	sub := strings.TrimPrefix(subdirectory, "/")
	locationURI := fmt.Sprintf("smb://%s/%s", fsxFilesystemArn, sub)

	locationTags := make(map[string]string)
	maps.Copy(locationTags, tags)

	l := &storedLocation{
		LocationArn:  locationArn,
		LocationURI:  locationURI,
		Subdirectory: subdirectory,
		LocationType: locationTypeFsxWindows,
		CreationTime: now,
		Tags:         locationTags,
		FsxWindows: &storedFsxWindowsConfig{
			FsxFilesystemArn:  fsxFilesystemArn,
			Domain:            domain,
			User:              user,
			Password:          password,
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

func (b *InMemoryBackend) DescribeLocationFsxWindows(locationArn string) (*LocationFsxWindows, error) {
	b.mu.RLock("DescribeLocationFsxWindows")
	defer b.mu.RUnlock()

	l, ok := b.locations.Get(locationArn)
	if !ok || l.LocationType != locationTypeFsxWindows {
		return nil, ErrNotFound
	}

	out := &LocationFsxWindows{
		LocationArn:  l.LocationArn,
		LocationURI:  l.LocationURI,
		Subdirectory: l.Subdirectory,
		CreationTime: l.CreationTime,
	}

	if l.FsxWindows != nil {
		out.FsxFilesystemArn = l.FsxWindows.FsxFilesystemArn
		out.Domain = l.FsxWindows.Domain
		out.User = l.FsxWindows.User
		out.SecurityGroupArns = l.FsxWindows.SecurityGroupArns
	}

	return out, nil
}

func (b *InMemoryBackend) UpdateLocationFsxWindows(locationArn, subdirectory, domain, user, password string) error {
	b.mu.Lock("UpdateLocationFsxWindows")
	defer b.mu.Unlock()

	l, ok := b.locations.Get(locationArn)
	if !ok || l.LocationType != locationTypeFsxWindows {
		return ErrNotFound
	}

	if l.FsxWindows == nil {
		l.FsxWindows = &storedFsxWindowsConfig{}
	}

	if subdirectory != "" {
		l.Subdirectory = subdirectory
		sub := strings.TrimPrefix(subdirectory, "/")
		l.LocationURI = fmt.Sprintf("smb://%s/%s", l.FsxWindows.FsxFilesystemArn, sub)
	}

	if domain != "" {
		l.FsxWindows.Domain = domain
	}

	if user != "" {
		l.FsxWindows.User = user
	}

	if password != "" {
		l.FsxWindows.Password = password
	}

	return nil
}

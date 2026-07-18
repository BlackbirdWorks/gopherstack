package datasync

import (
	"fmt"
	"maps"
	"strings"
	"time"
)

// --- SMB ---

func (b *InMemoryBackend) CreateLocationSmb(
	serverHostname, subdirectory, domain, user, password string,
	mountOptions *MountOptions,
	agentArns []string,
	tags map[string]string,
) (*Location, error) {
	b.mu.Lock("CreateLocationSmb")
	defer b.mu.Unlock()

	id := newID()
	locationArn := b.locationARN(id)
	now := time.Now().UTC()

	sub := strings.TrimPrefix(subdirectory, "/")
	locationURI := fmt.Sprintf("smb://%s/%s", serverHostname, sub)

	locationTags := make(map[string]string)
	maps.Copy(locationTags, tags)

	cfg := &storedSmbConfig{
		ServerHostname: serverHostname,
		Domain:         domain,
		User:           user,
		Password:       password,
		AgentArns:      agentArns,
	}

	if mountOptions != nil {
		cfg.MountOptions = &storedMountOptions{Version: mountOptions.Version}
	}

	l := &storedLocation{
		LocationArn:  locationArn,
		LocationURI:  locationURI,
		Subdirectory: subdirectory,
		LocationType: locationTypeSMB,
		CreationTime: now,
		Tags:         locationTags,
		Smb:          cfg,
	}
	b.locations.Put(l)

	if len(locationTags) > 0 {
		b.tags[locationArn] = make(map[string]string)
		maps.Copy(b.tags[locationArn], locationTags)
	}

	cp := l.toLocation()

	return &cp, nil
}

func (b *InMemoryBackend) DescribeLocationSmb(locationArn string) (*LocationSmb, error) {
	b.mu.RLock("DescribeLocationSmb")
	defer b.mu.RUnlock()

	l, ok := b.locations.Get(locationArn)
	if !ok || l.LocationType != locationTypeSMB {
		return nil, ErrNotFound
	}

	out := &LocationSmb{
		LocationArn:  l.LocationArn,
		LocationURI:  l.LocationURI,
		Subdirectory: l.Subdirectory,
		CreationTime: l.CreationTime,
	}

	if l.Smb != nil {
		out.ServerHostname = l.Smb.ServerHostname
		out.Domain = l.Smb.Domain
		out.User = l.Smb.User
		out.AgentArns = l.Smb.AgentArns

		if l.Smb.MountOptions != nil {
			out.MountOptions = &MountOptions{Version: l.Smb.MountOptions.Version}
		}
	}

	return out, nil
}

func (b *InMemoryBackend) UpdateLocationSmb(
	locationArn, subdirectory, domain, user, password string,
	mountOptions *MountOptions,
	agentArns []string,
) error {
	b.mu.Lock("UpdateLocationSmb")
	defer b.mu.Unlock()

	l, ok := b.locations.Get(locationArn)
	if !ok || l.LocationType != locationTypeSMB {
		return ErrNotFound
	}

	if l.Smb == nil {
		l.Smb = &storedSmbConfig{}
	}

	if subdirectory != "" {
		l.Subdirectory = subdirectory
		sub := strings.TrimPrefix(subdirectory, "/")
		l.LocationURI = fmt.Sprintf("smb://%s/%s", l.Smb.ServerHostname, sub)
	}

	if domain != "" {
		l.Smb.Domain = domain
	}

	if user != "" {
		l.Smb.User = user
	}

	if password != "" {
		l.Smb.Password = password
	}

	if mountOptions != nil {
		l.Smb.MountOptions = &storedMountOptions{Version: mountOptions.Version}
	}

	if agentArns != nil {
		l.Smb.AgentArns = agentArns
	}

	return nil
}

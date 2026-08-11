package datasync

import (
	"fmt"
	"maps"
	"strings"
	"time"
)

// --- NFS ---

func (b *InMemoryBackend) CreateLocationNfs(
	serverHostname, subdirectory string,
	mountOptions *MountOptions,
	agentArns []string,
	tags map[string]string,
) (*Location, error) {
	b.mu.Lock("CreateLocationNfs")
	defer b.mu.Unlock()

	if err := b.validateAgentArns(agentArns); err != nil {
		return nil, err
	}

	id := newID()
	locationArn := b.locationARN(id)
	now := time.Now().UTC()

	sub := strings.TrimPrefix(subdirectory, "/")
	locationURI := fmt.Sprintf("nfs://%s/%s", serverHostname, sub)

	locationTags := make(map[string]string)
	maps.Copy(locationTags, tags)

	cfg := &storedNfsConfig{
		ServerHostname: serverHostname,
		AgentArns:      agentArns,
	}

	if mountOptions != nil {
		cfg.MountOptions = &storedMountOptions{Version: mountOptions.Version}
	}

	l := &storedLocation{
		LocationArn:  locationArn,
		LocationURI:  locationURI,
		Subdirectory: subdirectory,
		LocationType: locationTypeNFS,
		CreationTime: now,
		Tags:         locationTags,
		Nfs:          cfg,
	}
	b.locations.Put(l)

	if len(locationTags) > 0 {
		b.tags[locationArn] = make(map[string]string)
		maps.Copy(b.tags[locationArn], locationTags)
	}

	cp := l.toLocation()

	return &cp, nil
}

func (b *InMemoryBackend) DescribeLocationNfs(locationArn string) (*LocationNfs, error) {
	b.mu.RLock("DescribeLocationNfs")
	defer b.mu.RUnlock()

	l, ok := b.locations.Get(locationArn)
	if !ok || l.LocationType != locationTypeNFS {
		return nil, ErrNotFound
	}

	out := &LocationNfs{
		LocationArn:  l.LocationArn,
		LocationURI:  l.LocationURI,
		Subdirectory: l.Subdirectory,
		CreationTime: l.CreationTime,
	}

	if l.Nfs != nil {
		out.ServerHostname = l.Nfs.ServerHostname
		out.AgentArns = l.Nfs.AgentArns

		if l.Nfs.MountOptions != nil {
			out.MountOptions = &MountOptions{Version: l.Nfs.MountOptions.Version}
		}
	}

	return out, nil
}

func (b *InMemoryBackend) UpdateLocationNfs(
	locationArn, subdirectory string,
	mountOptions *MountOptions,
	agentArns []string,
) error {
	b.mu.Lock("UpdateLocationNfs")
	defer b.mu.Unlock()

	l, ok := b.locations.Get(locationArn)
	if !ok || l.LocationType != locationTypeNFS {
		return ErrNotFound
	}

	if err := b.validateAgentArns(agentArns); err != nil {
		return err
	}

	if l.Nfs == nil {
		l.Nfs = &storedNfsConfig{}
	}

	if subdirectory != "" {
		l.Subdirectory = subdirectory
		sub := strings.TrimPrefix(subdirectory, "/")
		l.LocationURI = fmt.Sprintf("nfs://%s/%s", l.Nfs.ServerHostname, sub)
	}

	if mountOptions != nil {
		l.Nfs.MountOptions = &storedMountOptions{Version: mountOptions.Version}
	}

	if agentArns != nil {
		l.Nfs.AgentArns = agentArns
	}

	return nil
}

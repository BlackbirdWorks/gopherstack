package datasync

import (
	"fmt"
	"maps"
	"strings"
	"time"
)

// --- EFS ---

func (b *InMemoryBackend) CreateLocationEfs(
	efsFilesystemArn, subdirectory, accessPointArn, fileSystemAccessRoleArn, inTransitEncryption string,
	ec2Config *Ec2Config,
	tags map[string]string,
) (*Location, error) {
	b.mu.Lock("CreateLocationEfs")
	defer b.mu.Unlock()

	id := newID()
	locationArn := b.locationARN(id)
	now := time.Now().UTC()

	// EFS URI: efs://<filesystem-id>/<subdirectory>
	fsID := efsFilesystemArn
	if idx := strings.LastIndex(efsFilesystemArn, "/"); idx >= 0 {
		fsID = efsFilesystemArn[idx+1:]
	}

	sub := strings.TrimPrefix(subdirectory, "/")
	locationURI := fmt.Sprintf("efs://%s/%s", fsID, sub)

	locationTags := make(map[string]string)
	maps.Copy(locationTags, tags)

	cfg := &storedEfsConfig{
		EfsFilesystemArn:        efsFilesystemArn,
		AccessPointArn:          accessPointArn,
		FileSystemAccessRoleArn: fileSystemAccessRoleArn,
		InTransitEncryption:     inTransitEncryption,
	}
	if ec2Config != nil {
		cfg.Ec2Config = &storedEfsEc2Config{
			SubnetArn:         ec2Config.SubnetArn,
			SecurityGroupArns: ec2Config.SecurityGroupArns,
		}
	}

	l := &storedLocation{
		LocationArn:  locationArn,
		LocationURI:  locationURI,
		Subdirectory: subdirectory,
		LocationType: locationTypeEFS,
		CreationTime: now,
		Tags:         locationTags,
		Efs:          cfg,
	}
	b.locations.Put(l)

	if len(locationTags) > 0 {
		b.tags[locationArn] = make(map[string]string)
		maps.Copy(b.tags[locationArn], locationTags)
	}

	cp := l.toLocation()

	return &cp, nil
}

func (b *InMemoryBackend) DescribeLocationEfs(locationArn string) (*LocationEfs, error) {
	b.mu.RLock("DescribeLocationEfs")
	defer b.mu.RUnlock()

	l, ok := b.locations.Get(locationArn)
	if !ok || l.LocationType != locationTypeEFS {
		return nil, ErrNotFound
	}

	out := &LocationEfs{
		LocationArn:  l.LocationArn,
		LocationURI:  l.LocationURI,
		Subdirectory: l.Subdirectory,
		CreationTime: l.CreationTime,
	}

	if l.Efs != nil {
		out.EfsFilesystemArn = l.Efs.EfsFilesystemArn
		out.AccessPointArn = l.Efs.AccessPointArn
		out.FileSystemAccessRoleArn = l.Efs.FileSystemAccessRoleArn
		out.InTransitEncryption = l.Efs.InTransitEncryption

		if l.Efs.Ec2Config != nil {
			out.Ec2Config = &Ec2Config{
				SubnetArn:         l.Efs.Ec2Config.SubnetArn,
				SecurityGroupArns: l.Efs.Ec2Config.SecurityGroupArns,
			}
		}
	}

	return out, nil
}

func (b *InMemoryBackend) UpdateLocationEfs(
	locationArn, subdirectory, accessPointArn, fileSystemAccessRoleArn, inTransitEncryption string,
	ec2Config *Ec2Config,
) error {
	b.mu.Lock("UpdateLocationEfs")
	defer b.mu.Unlock()

	l, ok := b.locations.Get(locationArn)
	if !ok || l.LocationType != locationTypeEFS {
		return ErrNotFound
	}

	if subdirectory != "" {
		l.Subdirectory = subdirectory
		fsID := l.Efs.EfsFilesystemArn
		if idx := strings.LastIndex(fsID, "/"); idx >= 0 {
			fsID = fsID[idx+1:]
		}

		sub := strings.TrimPrefix(subdirectory, "/")
		l.LocationURI = fmt.Sprintf("efs://%s/%s", fsID, sub)
	}

	if l.Efs == nil {
		l.Efs = &storedEfsConfig{}
	}

	if accessPointArn != "" {
		l.Efs.AccessPointArn = accessPointArn
	}

	if fileSystemAccessRoleArn != "" {
		l.Efs.FileSystemAccessRoleArn = fileSystemAccessRoleArn
	}

	if inTransitEncryption != "" {
		l.Efs.InTransitEncryption = inTransitEncryption
	}

	if ec2Config != nil {
		l.Efs.Ec2Config = &storedEfsEc2Config{
			SubnetArn:         ec2Config.SubnetArn,
			SecurityGroupArns: ec2Config.SecurityGroupArns,
		}
	}

	return nil
}

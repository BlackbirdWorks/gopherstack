package datasync

import (
	"fmt"
	"maps"
	"strings"
	"time"
)

// --- FsxOntap ---

func toStoredFsxProtocol(p *FsxProtocol) *storedFsxProtocol {
	if p == nil {
		return nil
	}

	sp := &storedFsxProtocol{}

	if p.NFS != nil {
		sp.NFS = &storedFsxNfsProtocol{}
		if p.NFS.MountOptions != nil {
			sp.NFS.MountOptions = &storedFsxMountOptions{Version: p.NFS.MountOptions.Version}
		}
	}

	if p.SMB != nil {
		sp.SMB = &storedFsxSmbProtocol{
			Domain:   p.SMB.Domain,
			Password: p.SMB.Password,
			User:     p.SMB.User,
		}
		if p.SMB.MountOptions != nil {
			sp.SMB.MountOptions = &storedFsxMountOptions{Version: p.SMB.MountOptions.Version}
		}
	}

	return sp
}

func fromStoredFsxProtocol(sp *storedFsxProtocol) *FsxProtocol {
	if sp == nil {
		return nil
	}

	p := &FsxProtocol{}

	if sp.NFS != nil {
		p.NFS = &FsxNfsProtocol{}
		if sp.NFS.MountOptions != nil {
			p.NFS.MountOptions = &MountOptions{Version: sp.NFS.MountOptions.Version}
		}
	}

	if sp.SMB != nil {
		p.SMB = &FsxSmbProtocol{
			Domain:   sp.SMB.Domain,
			Password: sp.SMB.Password,
			User:     sp.SMB.User,
		}
		if sp.SMB.MountOptions != nil {
			p.SMB.MountOptions = &MountOptions{Version: sp.SMB.MountOptions.Version}
		}
	}

	return p
}

func (b *InMemoryBackend) CreateLocationFsxOntap(
	storageVirtualMachineArn, subdirectory string,
	protocol *FsxProtocol,
	securityGroupArns []string,
	tags map[string]string,
) (*Location, error) {
	b.mu.Lock("CreateLocationFsxOntap")
	defer b.mu.Unlock()

	id := newID()
	locationArn := b.locationARN(id)
	now := time.Now().UTC()

	sub := strings.TrimPrefix(subdirectory, "/")
	locationURI := fmt.Sprintf("ontap://%s/%s", storageVirtualMachineArn, sub)

	locationTags := make(map[string]string)
	maps.Copy(locationTags, tags)

	l := &storedLocation{
		LocationArn:  locationArn,
		LocationURI:  locationURI,
		Subdirectory: subdirectory,
		LocationType: locationTypeFsxOntap,
		CreationTime: now,
		Tags:         locationTags,
		FsxOntap: &storedFsxOntapConfig{
			StorageVirtualMachineArn: storageVirtualMachineArn,
			SecurityGroupArns:        securityGroupArns,
			Protocol:                 toStoredFsxProtocol(protocol),
		},
	}

	cp := b.storeLocation(l)

	return &cp, nil
}

func (b *InMemoryBackend) DescribeLocationFsxOntap(locationArn string) (*LocationFsxOntap, error) {
	b.mu.RLock("DescribeLocationFsxOntap")
	defer b.mu.RUnlock()

	l, ok := b.locations.Get(locationArn)
	if !ok || l.LocationType != locationTypeFsxOntap {
		return nil, ErrNotFound
	}

	out := &LocationFsxOntap{
		LocationArn:  l.LocationArn,
		LocationURI:  l.LocationURI,
		Subdirectory: l.Subdirectory,
		CreationTime: l.CreationTime,
	}

	if l.FsxOntap != nil {
		out.StorageVirtualMachineArn = l.FsxOntap.StorageVirtualMachineArn
		out.SecurityGroupArns = l.FsxOntap.SecurityGroupArns
		out.Protocol = fromStoredFsxProtocol(l.FsxOntap.Protocol)
	}

	return out, nil
}

func (b *InMemoryBackend) UpdateLocationFsxOntap(locationArn, subdirectory string, protocol *FsxProtocol) error {
	b.mu.Lock("UpdateLocationFsxOntap")
	defer b.mu.Unlock()

	l, ok := b.locations.Get(locationArn)
	if !ok || l.LocationType != locationTypeFsxOntap {
		return ErrNotFound
	}

	if subdirectory != "" {
		l.Subdirectory = subdirectory
		svm := ""
		if l.FsxOntap != nil {
			svm = l.FsxOntap.StorageVirtualMachineArn
		}

		sub := strings.TrimPrefix(subdirectory, "/")
		l.LocationURI = fmt.Sprintf("ontap://%s/%s", svm, sub)
	}

	if protocol != nil && l.FsxOntap != nil {
		l.FsxOntap.Protocol = toStoredFsxProtocol(protocol)
	}

	return nil
}

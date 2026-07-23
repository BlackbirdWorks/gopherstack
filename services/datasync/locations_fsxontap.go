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

// fsxOntapFilesystemArnFromSVM derives the parent FSx file system ARN from a
// storage virtual machine ARN, matching the real
// DescribeLocationFsxOntapOutput.FsxFilesystemArn field (which AWS returns
// even though CreateLocationFsxOntapInput never accepts a FsxFilesystemArn --
// only StorageVirtualMachineArn). SVM ARN format (confirmed via AWS's
// published pattern):
// arn:aws:fsx:region:account:storage-virtual-machine/fs-xxx/svm-xxx.
func fsxOntapFilesystemArnFromSVM(svmArn string) string {
	const svmResourcePrefix = ":storage-virtual-machine/"

	prefix, rest, found := strings.Cut(svmArn, svmResourcePrefix)
	if !found {
		return ""
	}

	fsID, _, ok := strings.Cut(rest, "/")
	if !ok {
		return ""
	}

	return prefix + ":file-system/" + fsID
}

// fsxOntapURIScheme picks the LocationUri scheme for an FSx ONTAP location
// based on its configured access protocol. AWS's published LocationUri regex
// (`^(efs|nfs|s3|smb|hdfs|fsx[a-z0-9-]+)://...$`) definitively rules out the
// bare "ontap://" this backend previously emitted. Unlike Lustre/OpenZFS,
// ONTAP volumes are mounted over an actual NFS-or-SMB protocol choice (see
// FsxProtocol), matching how FSx Windows reuses "smb://" rather than minting
// a distinct fsx-prefixed scheme -- so this picks the underlying protocol's
// own scheme instead of guessing an "fsxn://"-style prefix.
func fsxOntapURIScheme(p *storedFsxProtocol) string {
	if p != nil && p.SMB != nil {
		return "smb://"
	}

	return "nfs://"
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

	storedProtocol := toStoredFsxProtocol(protocol)

	sub := strings.TrimPrefix(subdirectory, "/")
	locationURI := fmt.Sprintf("%s%s/%s", fsxOntapURIScheme(storedProtocol), storageVirtualMachineArn, sub)

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
			FsxFilesystemArn:         fsxOntapFilesystemArnFromSVM(storageVirtualMachineArn),
			SecurityGroupArns:        securityGroupArns,
			Protocol:                 storedProtocol,
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
		out.FsxFilesystemArn = l.FsxOntap.FsxFilesystemArn
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
	}

	if protocol != nil && l.FsxOntap != nil {
		l.FsxOntap.Protocol = toStoredFsxProtocol(protocol)
	}

	// Recompute LocationURI whenever either the subdirectory or the protocol
	// (which determines the URI scheme -- see fsxOntapURIScheme) changed.
	if subdirectory != "" || protocol != nil {
		svm := ""
		if l.FsxOntap != nil {
			svm = l.FsxOntap.StorageVirtualMachineArn
		}

		sub := strings.TrimPrefix(l.Subdirectory, "/")
		l.LocationURI = fmt.Sprintf("%s%s/%s", fsxOntapURIScheme(l.FsxOntap.Protocol), svm, sub)
	}

	return nil
}

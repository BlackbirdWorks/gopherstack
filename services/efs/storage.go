package efs

import (
	"errors"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
)

const (
	storageDirPerm  os.FileMode = 0o755
	rootDirPerm     os.FileMode = 0o777
	accessPointKind             = "access-point/"
)

// ErrInvalidAccessPoint reports an ARN that does not name an EFS access point.
var ErrInvalidAccessPoint = errors.New("not an EFS access point ARN")

// ErrNoMountTarget reports a file system with no mount target in the requested VPC.
var ErrNoMountTarget = errors.New("no mount target in the function's VPC")

// AccessPointMount is the host-side view of an access point's root directory.
type AccessPointMount struct {
	HostDir      string
	FileSystemID string
	// SecondaryGIDs and HasPosixUser describe the access point's PosixUser; UID/GID are zero without one.
	SecondaryGIDs []int64
	UID           int64
	GID           int64
	HasPosixUser  bool
}

// SetStorageRoot sets the host directory that backs every file system's data.
// Left empty, a temp directory is created on first use.
func (b *InMemoryBackend) SetStorageRoot(dir string) {
	b.mu.Lock("SetStorageRoot")
	defer b.mu.Unlock()

	b.storageRoot = dir
}

func (b *InMemoryBackend) storageRootLocked() (string, error) {
	if b.storageRoot != "" {
		return b.storageRoot, nil
	}

	dir, err := os.MkdirTemp("", "gopherstack-efs-")
	if err != nil {
		return "", fmt.Errorf("create efs storage root: %w", err)
	}

	b.storageRoot = dir

	return dir, nil
}

func (b *InMemoryBackend) fileSystemDirLocked(region, fsID string) (string, error) {
	root, err := b.storageRootLocked()
	if err != nil {
		return "", err
	}

	return filepath.Clean(filepath.Join(root, region, fsID)), nil
}

func (b *InMemoryBackend) removeFileSystemStorageLocked(region, fsID string) {
	if b.storageRoot == "" {
		return
	}

	root := filepath.Clean(b.storageRoot)
	target := filepath.Clean(filepath.Join(root, region, fsID))

	if strings.HasPrefix(target, root+string(os.PathSeparator)) {
		_ = os.RemoveAll(target)
	}
}

func (b *InMemoryBackend) lookupAccessPointByARNLocked(accessPointARN string) (*AccessPoint, string, error) {
	if !strings.Contains(accessPointARN, ":elasticfilesystem:") ||
		!strings.Contains(accessPointARN, ":"+accessPointKind) {
		return nil, "", fmt.Errorf("%w: %s", ErrInvalidAccessPoint, accessPointARN)
	}

	region := regionFromARN(accessPointARN, b.region)

	ap, ok := b.accessPointsByARN.Get(regionKey(region, accessPointARN))
	if !ok {
		return nil, "", fmt.Errorf("%w: %s", ErrAccessPointNotFound, accessPointARN)
	}

	return ap, region, nil
}

// ResolveAccessPointMount returns the host directory backing an access point's
// RootDirectory, creating it with CreationInfo when it is missing.
func (b *InMemoryBackend) ResolveAccessPointMount(accessPointARN string) (AccessPointMount, error) {
	b.mu.Lock("ResolveAccessPointMount")
	defer b.mu.Unlock()

	ap, region, err := b.lookupAccessPointByARNLocked(accessPointARN)
	if err != nil {
		return AccessPointMount{}, err
	}

	fsDir, err := b.fileSystemDirLocked(region, ap.FileSystemID)
	if err != nil {
		return AccessPointMount{}, err
	}

	if !strings.HasPrefix(fsDir, filepath.Clean(b.storageRoot)+string(os.PathSeparator)) {
		return AccessPointMount{}, fmt.Errorf("%w: %s", ErrInvalidAccessPoint, accessPointARN)
	}

	if mkErr := os.MkdirAll(fsDir, storageDirPerm); mkErr != nil {
		return AccessPointMount{}, fmt.Errorf("create file system dir: %w", mkErr)
	}

	if chErr := os.Chmod(fsDir, rootDirPerm); chErr != nil {
		return AccessPointMount{}, fmt.Errorf("chmod file system dir: %w", chErr)
	}

	hostDir, err := ensureRootDirectory(fsDir, ap.RootDirectory)
	if err != nil {
		return AccessPointMount{}, err
	}

	m := AccessPointMount{HostDir: hostDir, FileSystemID: ap.FileSystemID}
	if ap.PosixUser != nil {
		m.UID, m.GID = ap.PosixUser.UID, ap.PosixUser.GID
		m.SecondaryGIDs = append([]int64(nil), ap.PosixUser.SecondaryGids...)
		m.HasPosixUser = true
	}

	return m, nil
}

// ensureRootDirectory maps rd.Path under fsDir; the rooted Clean cannot escape fsDir.
func ensureRootDirectory(fsDir string, rd *RootDirectory) (string, error) {
	if rd == nil || rd.Path == "" || rd.Path == "/" {
		return fsDir, nil
	}

	dir := filepath.Join(fsDir, filepath.FromSlash(path.Clean("/"+rd.Path)))
	if !strings.HasPrefix(dir, filepath.Clean(fsDir)+string(os.PathSeparator)) {
		return "", fmt.Errorf("%w: %s", ErrInvalidAccessPoint, rd.Path)
	}

	if _, err := os.Stat(dir); err == nil {
		return dir, nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return "", fmt.Errorf("stat access point root: %w", err)
	}

	if err := os.MkdirAll(dir, storageDirPerm); err != nil {
		return "", fmt.Errorf("create access point root: %w", err)
	}

	if ci := rd.CreationInfo; ci != nil {
		applyCreationInfo(dir, ci)
	}

	return dir, nil
}

// applyCreationInfo chmods always and chowns best-effort: an unprivileged
// server cannot hand files to arbitrary uids.
func applyCreationInfo(dir string, ci *CreationInfo) {
	if perm, err := strconv.ParseUint(ci.Permissions, 8, 32); err == nil {
		_ = os.Chmod(dir, os.FileMode(perm).Perm())
	}

	_ = os.Chown(dir, int(ci.OwnerUID), int(ci.OwnerGID))
}

// CheckMountTarget verifies the access point exists and its file system has a
// mount target in a VPC of one of subnetIDs.
func (b *InMemoryBackend) CheckMountTarget(accessPointARN string, subnetIDs []string) error {
	b.mu.RLock("CheckMountTarget")
	defer b.mu.RUnlock()

	ap, region, err := b.lookupAccessPointByARNLocked(accessPointARN)
	if err != nil {
		return err
	}

	vpcs := make(map[string]struct{}, len(subnetIDs))
	subnets := make(map[string]struct{}, len(subnetIDs))

	for _, s := range subnetIDs {
		subnets[s] = struct{}{}

		if b.ec2Resolver != nil {
			if vpc := b.ec2Resolver.SubnetVPC(s); vpc != "" {
				vpcs[vpc] = struct{}{}
			}
		}
	}

	for _, mtID := range b.mtSubnetIdx[region][ap.FileSystemID] {
		mt, ok := b.mountTargets.Get(regionKey(region, mtID))
		if !ok {
			continue
		}

		if _, in := subnets[mt.SubnetID]; in {
			return nil
		}

		if _, in := vpcs[mt.VpcID]; in && mt.VpcID != "" {
			return nil
		}
	}

	return fmt.Errorf("%w: file system %s", ErrNoMountTarget, ap.FileSystemID)
}

// FileSystemHostDir returns the host directory backing a file system's data, creating it when missing.
func (b *InMemoryBackend) FileSystemHostDir(region, fsID string) (string, error) {
	b.mu.Lock("FileSystemHostDir")
	defer b.mu.Unlock()

	if _, ok := b.fileSystems.Get(regionKey(region, fsID)); !ok {
		return "", fmt.Errorf("%w: file system %s", ErrNotFound, fsID)
	}

	dir, err := b.fileSystemDirLocked(region, fsID)
	if err != nil {
		return "", err
	}

	if mkErr := os.MkdirAll(dir, storageDirPerm); mkErr != nil {
		return "", fmt.Errorf("create file system dir: %w", mkErr)
	}

	return dir, nil
}

// ErrRootDirectoryWithAccessPoint reports a root directory other than "/" combined with an access point.
var ErrRootDirectoryWithAccessPoint = errors.New("rootDirectory must be omitted or / when an access point is used")

// ErrAccessPointFileSystemMismatch reports an access point that belongs to a different file system.
var ErrAccessPointFileSystemMismatch = errors.New("access point does not belong to the file system")

// ResolveVolumeMount returns the host directory for an ECS efsVolumeConfiguration: the access point's root
// directory when accessPointID is set (rootDirectory must then be omitted or "/"), else rootDirectory
// under the file system.
func (b *InMemoryBackend) ResolveVolumeMount(
	region, fsID, accessPointID, rootDirectory string,
) (AccessPointMount, error) {
	if accessPointID == "" {
		return b.resolveFileSystemRoot(region, fsID, rootDirectory)
	}

	b.mu.RLock("ResolveVolumeMount")
	ap, ok := b.accessPoints.Get(regionKey(region, accessPointID))
	b.mu.RUnlock()

	if !ok {
		return AccessPointMount{}, fmt.Errorf("%w: %s", ErrAccessPointNotFound, accessPointID)
	}

	if ap.FileSystemID != fsID {
		return AccessPointMount{}, fmt.Errorf("%w: %s", ErrAccessPointFileSystemMismatch, accessPointID)
	}

	if rootDirectory != "" && rootDirectory != "/" {
		return AccessPointMount{}, ErrRootDirectoryWithAccessPoint
	}

	return b.ResolveAccessPointMount(ap.AccessPointArn)
}

func (b *InMemoryBackend) resolveFileSystemRoot(region, fsID, rootDirectory string) (AccessPointMount, error) {
	fsDir, err := b.FileSystemHostDir(region, fsID)
	if err != nil {
		return AccessPointMount{}, err
	}

	if cherr := os.Chmod(fsDir, rootDirPerm); cherr != nil {
		return AccessPointMount{}, fmt.Errorf("chmod file system dir: %w", cherr)
	}

	dir, err := ensureRootDirectory(fsDir, &RootDirectory{Path: rootDirectory})
	if err != nil {
		return AccessPointMount{}, err
	}

	return AccessPointMount{HostDir: dir, FileSystemID: fsID}, nil
}

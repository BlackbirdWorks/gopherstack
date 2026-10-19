package s3files

import (
	"context"
	"maps"
	"slices"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awsid"
	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

const (
	maxSecondaryGIDs = 16
	maxUIDGID        = 4294967295
	maxPathLen       = 100
)

// CreateAccessPointInput is the backend form of CreateAccessPoint.
type CreateAccessPointInput struct {
	Tags          map[string]string
	PosixUser     *PosixUser
	RootDirectory *RootDirectory
	FileSystemID  string
	ClientToken   string
}

func validID(v *int64) bool { return v != nil && *v >= 0 && *v <= maxUIDGID }

func validateAccessPoint(in CreateAccessPointInput) error {
	if in.FileSystemID == "" {
		return validation("FileSystemId is required")
	}

	if len(in.ClientToken) > maxClientTokenLen {
		return validation("ClientToken must be at most 64 characters")
	}

	if err := validatePosixUser(in.PosixUser); err != nil {
		return err
	}

	if err := validateRootDirectory(in.RootDirectory); err != nil {
		return err
	}

	return validateTags(in.Tags)
}

func validatePosixUser(u *PosixUser) error {
	if u == nil {
		return nil
	}

	if !validID(u.UID) || !validID(u.GID) || len(u.SecondaryGIDs) > maxSecondaryGIDs {
		return validation("Invalid PosixUser")
	}

	for i := range u.SecondaryGIDs {
		if !validID(&u.SecondaryGIDs[i]) {
			return validation("Invalid PosixUser secondary GID")
		}
	}

	return nil
}

func validateRootDirectory(rd *RootDirectory) error {
	if rd == nil {
		return nil
	}

	if rd.Path != nil && (len(*rd.Path) > maxPathLen || !strings.HasPrefix(*rd.Path, "/")) {
		return validation("RootDirectory.Path must be an absolute path of at most 100 characters")
	}

	if cp := rd.CreationPermissions; cp != nil &&
		(!validID(cp.OwnerUID) || !validID(cp.OwnerGID) || cp.Permissions == nil || !validOctal(*cp.Permissions)) {
		return validation("Invalid RootDirectory.CreationPermissions")
	}

	return nil
}

func validOctal(s string) bool {
	const minLen, maxLen = 3, 4

	if len(s) < minLen || len(s) > maxLen {
		return false
	}

	for _, r := range s {
		if r < '0' || r > '7' {
			return false
		}
	}

	return true
}

// CreateAccessPoint creates an access point; created is false for an idempotent replay.
func (b *InMemoryBackend) CreateAccessPoint(
	ctx context.Context,
	in CreateAccessPointInput,
) (*AccessPoint, bool, error) {
	if err := validateAccessPoint(in); err != nil {
		return nil, false, err
	}

	region := getRegion(ctx, b.region)

	b.mu.Lock("CreateAccessPoint")
	defer b.mu.Unlock()

	b.sweepLocked()

	fs, err := b.liveFileSystem(region, in.FileSystemID)
	if err != nil {
		return nil, false, err
	}

	if in.ClientToken != "" {
		for _, existing := range b.accessPoints.All() {
			if existing.Region == region && existing.FileSystemID == fs.ID && existing.ClientToken == in.ClientToken {
				return b.apView(existing), false, nil
			}
		}
	}

	if state, _ := b.state(&fs.Lifecycle); state != statusAvailable {
		return nil, false, conflict("FileSystem", fs.ID, "File system "+fs.ID+" is "+state+", not available")
	}

	id := awsid.New("fsap-")
	rec := &AccessPoint{
		ID:            id,
		ARN:           arn.Build("s3files", region, b.accountID, "access-point/"+id),
		FileSystemID:  fs.ID,
		ClientToken:   in.ClientToken,
		OwnerID:       b.accountID,
		Region:        region,
		PosixUser:     in.PosixUser,
		RootDirectory: in.RootDirectory,
		Tags:          maps.Clone(in.Tags),
		Lifecycle:     b.newLifecycle(),
	}
	b.accessPoints.Put(rec)

	return b.apView(rec), true, nil
}

// GetAccessPoint returns an access point by ID or ARN.
func (b *InMemoryBackend) GetAccessPoint(ctx context.Context, ref string) (*AccessPoint, error) {
	region := getRegion(ctx, b.region)

	b.mu.RLock("GetAccessPoint")
	defer b.mu.RUnlock()

	ap, err := b.liveAccessPoint(region, ref)
	if err != nil {
		return nil, err
	}

	return b.apView(ap), nil
}

// ListAccessPoints lists the access points of one file system.
func (b *InMemoryBackend) ListAccessPoints(
	ctx context.Context,
	fileSystemRef string,
	maxResults int,
	token string,
) (page.Page[*AccessPoint], error) {
	if fileSystemRef == "" {
		return page.Page[*AccessPoint]{}, validation("FileSystemId is required")
	}

	if err := validatePaging(maxResults, token); err != nil {
		return page.Page[*AccessPoint]{}, err
	}

	region := getRegion(ctx, b.region)

	b.mu.RLock("ListAccessPoints")
	defer b.mu.RUnlock()

	fs, err := b.liveFileSystem(region, fileSystemRef)
	if err != nil {
		return page.Page[*AccessPoint]{}, err
	}

	var all []*AccessPoint

	for _, ap := range b.accessPoints.All() {
		if _, gone := b.state(&ap.Lifecycle); gone || ap.Region != region || ap.FileSystemID != fs.ID {
			continue
		}

		all = append(all, b.apView(ap))
	}

	slices.SortFunc(all, func(x, y *AccessPoint) int { return strings.Compare(x.ID, y.ID) })

	return page.New(all, token, maxResults, defaultPageSize), nil
}

// DeleteAccessPoint deletes an access point.
func (b *InMemoryBackend) DeleteAccessPoint(ctx context.Context, ref string) error {
	region := getRegion(ctx, b.region)

	b.mu.Lock("DeleteAccessPoint")
	defer b.mu.Unlock()

	b.sweepLocked()

	ap, err := b.liveAccessPoint(region, ref)
	if err != nil {
		return err
	}

	if b.beginDelete(&ap.Lifecycle) {
		b.accessPoints.Delete(ap.ID)
	}

	return nil
}

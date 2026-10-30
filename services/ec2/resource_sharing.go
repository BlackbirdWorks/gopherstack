package ec2

import (
	"errors"
	"fmt"
	"net/url"
	"slices"
)

const (
	sharingSelf = "self"
)

const callerAccountParam = "X-Gopherstack-Caller-Account"

// ErrUnauthorizedOperation is returned when a caller that can see a snapshot
// or AMI (shared with it) tries an owner-only operation on it.
var ErrUnauthorizedOperation = errors.New("UnauthorizedOperation")

var errNotAuthorized = fmt.Errorf("%w: you are not authorized to perform this operation", ErrUnauthorizedOperation)

// resourceSharingBackend is the caller-aware slice of the backend. Resources
// whose OwnerID is the backend account are treated as unowned and stay open to
// every caller; only resources claimed by an explicit caller account are
// private to it unless shared via createVolumePermission / launchPermission.
type resourceSharingBackend interface {
	ClaimSnapshotOwner(caller string, snapshotIDs ...string)
	ClaimImageOwner(caller, imageID string)
	SnapshotUsableBy(caller, snapshotID string) error
	SnapshotOwnedBy(caller, snapshotID string) error
	ImageUsableBy(caller, imageID string) error
	ImageOwnedBy(caller, imageID string) error
	VisibleSnapshots(caller string, snaps []*Snapshot) []*Snapshot
	NarrowSnapshots(caller string, snaps []*Snapshot, owners, restorableBy []string) []*Snapshot
	VisibleImages(caller string, imgs []*AMIStub) []*AMIStub
	NarrowImagesByExecutable(caller string, imgs []*AMIStub, executableBy []string) []*AMIStub
}

func (h *Handler) sharing() resourceSharingBackend {
	sb, _ := h.Backend.(resourceSharingBackend)

	return sb
}

func callerOf(vals url.Values) string { return vals.Get(callerAccountParam) }

// claimSnapshots makes caller the owner of freshly created snapshots and
// reflects it on the response copies.
func (h *Handler) claimSnapshots(caller string, snaps ...*Snapshot) {
	sb := h.sharing()
	if sb == nil || caller == "" {
		return
	}

	for _, s := range snaps {
		sb.ClaimSnapshotOwner(caller, s.SnapshotID)

		if caller != "" {
			s.OwnerID = caller
		}
	}
}

func (h *Handler) claimImage(caller string, img *AMIStub) {
	if sb := h.sharing(); sb != nil && caller != "" {
		sb.ClaimImageOwner(caller, img.ImageID)
		img.OwnerID = caller
	}
}

func (h *Handler) requireSnapshotUsable(vals url.Values, snapshotID string) error {
	if sb := h.sharing(); sb != nil && snapshotID != "" {
		return sb.SnapshotUsableBy(callerOf(vals), snapshotID)
	}

	return nil
}

func (h *Handler) requireImageUsable(vals url.Values, imageID string) error {
	if sb := h.sharing(); sb != nil && imageID != "" {
		return sb.ImageUsableBy(callerOf(vals), imageID)
	}

	return nil
}

func (h *Handler) requireSnapshotOwner(vals url.Values, snapshotID string) error {
	if sb := h.sharing(); sb != nil && snapshotID != "" {
		return sb.SnapshotOwnedBy(callerOf(vals), snapshotID)
	}

	return nil
}

func (h *Handler) requireImageOwner(vals url.Values, imageID string) error {
	if sb := h.sharing(); sb != nil && imageID != "" {
		return sb.ImageOwnedBy(callerOf(vals), imageID)
	}

	return nil
}

// ClaimSnapshotOwner records caller as the owner of the snapshots.
func (b *InMemoryBackend) ClaimSnapshotOwner(caller string, snapshotIDs ...string) {
	if caller == "" || caller == b.AccountID {
		return
	}

	b.mu.Lock("ClaimSnapshotOwner")
	defer b.mu.Unlock()

	for _, id := range snapshotIDs {
		if s, ok := b.snapshots.Get(id); ok {
			s.OwnerID = caller
		}
	}
}

// ClaimImageOwner records caller as the owner of the AMI.
func (b *InMemoryBackend) ClaimImageOwner(caller, imageID string) {
	if caller == "" || caller == b.AccountID {
		return
	}

	b.mu.Lock("ClaimImageOwner")
	defer b.mu.Unlock()

	if img, ok := b.images.Get(imageID); ok {
		img.OwnerID = caller
	}
}

func (b *InMemoryBackend) ownsLocked(caller, owner string) bool {
	return caller == "" || owner == "" || owner == b.AccountID || owner == caller
}

func (b *InMemoryBackend) snapshotUsableLocked(caller string, s *Snapshot) bool {
	return b.ownsLocked(caller, s.OwnerID) ||
		b.snapshotCreateVolumePerms[s.SnapshotID][caller] ||
		b.snapshotCreateVolumePubGroup[s.SnapshotID]
}

func (b *InMemoryBackend) imageUsableLocked(caller string, img *AMIStub) bool {
	return knownImageOwnerAliases[img.OwnerID] ||
		b.ownsLocked(caller, img.OwnerID) ||
		b.imageLaunchPermissions[img.ImageID][caller] ||
		b.imageLaunchPermissionPublic[img.ImageID]
}

// SnapshotUsableBy returns InvalidSnapshotID.NotFound for a snapshot the
// caller can neither own nor create volumes from; unknown ids pass through.
func (b *InMemoryBackend) SnapshotUsableBy(caller, snapshotID string) error {
	b.mu.RLock("SnapshotUsableBy")
	defer b.mu.RUnlock()

	if s, ok := b.snapshots.Get(snapshotID); ok && !b.snapshotUsableLocked(caller, s) {
		return fmt.Errorf("%w: %s", ErrSnapshotNotFound, snapshotID)
	}

	return nil
}

// SnapshotOwnedBy additionally requires ownership: a snapshot merely shared
// with the caller yields UnauthorizedOperation.
func (b *InMemoryBackend) SnapshotOwnedBy(caller, snapshotID string) error {
	b.mu.RLock("SnapshotOwnedBy")
	defer b.mu.RUnlock()

	s, ok := b.snapshots.Get(snapshotID)
	if !ok {
		return nil
	}

	switch {
	case !b.snapshotUsableLocked(caller, s):
		return fmt.Errorf("%w: %s", ErrSnapshotNotFound, snapshotID)
	case !b.ownsLocked(caller, s.OwnerID):
		return errNotAuthorized
	}

	return nil
}

// ImageUsableBy returns InvalidAMIID.NotFound for an AMI the caller can
// neither own nor launch.
func (b *InMemoryBackend) ImageUsableBy(caller, imageID string) error {
	b.mu.RLock("ImageUsableBy")
	defer b.mu.RUnlock()

	if img, ok := b.images.Get(imageID); ok && !b.imageUsableLocked(caller, img) {
		return fmt.Errorf("%w: The image id '[%s]' does not exist", ErrImageNotFound, imageID)
	}

	return nil
}

// ImageOwnedBy additionally requires ownership (UnauthorizedOperation otherwise).
func (b *InMemoryBackend) ImageOwnedBy(caller, imageID string) error {
	b.mu.RLock("ImageOwnedBy")
	defer b.mu.RUnlock()

	img, ok := b.images.Get(imageID)
	if !ok {
		return nil
	}

	switch {
	case !b.imageUsableLocked(caller, img):
		return fmt.Errorf("%w: The image id '[%s]' does not exist", ErrImageNotFound, imageID)
	case caller != "" && (knownImageOwnerAliases[img.OwnerID] || !b.ownsLocked(caller, img.OwnerID)):
		return errNotAuthorized
	}

	return nil
}

// VisibleSnapshots keeps the snapshots the caller owns, has create-volume
// permission on, or that are public.
func (b *InMemoryBackend) VisibleSnapshots(caller string, snaps []*Snapshot) []*Snapshot {
	if caller == "" {
		return snaps
	}

	b.mu.RLock("VisibleSnapshots")
	defer b.mu.RUnlock()

	return slices.DeleteFunc(slices.Clone(snaps), func(s *Snapshot) bool { return !b.snapshotUsableLocked(caller, s) })
}

// NarrowSnapshots applies DescribeSnapshots' OwnerIds (sharingSelf, account ids)
// and RestorableByUserIds (sharingSelf, account ids, "all") request members.
func (b *InMemoryBackend) NarrowSnapshots(
	caller string, snaps []*Snapshot, owners, restorableBy []string,
) []*Snapshot {
	if len(owners) == 0 && len(restorableBy) == 0 {
		return snaps
	}

	self := caller
	if self == "" {
		self = b.AccountID
	}

	b.mu.RLock("NarrowSnapshots")
	defer b.mu.RUnlock()

	return slices.DeleteFunc(slices.Clone(snaps), func(s *Snapshot) bool {
		if len(owners) > 0 && !slices.ContainsFunc(owners, func(o string) bool {
			return o == s.OwnerID ||
				(o == sharingSelf && (s.OwnerID == self || (caller == "" && s.OwnerID == b.AccountID)))
		}) {
			return true
		}

		return len(restorableBy) > 0 && !slices.ContainsFunc(restorableBy, func(u string) bool {
			if u == sharingSelf {
				u = self
			}

			return (u == permissionGroupAll && b.snapshotCreateVolumePubGroup[s.SnapshotID]) ||
				b.snapshotCreateVolumePerms[s.SnapshotID][u]
		})
	})
}

// VisibleImages keeps the AMIs the caller owns, may launch, or that are public.
func (b *InMemoryBackend) VisibleImages(caller string, imgs []*AMIStub) []*AMIStub {
	if caller == "" {
		return imgs
	}

	b.mu.RLock("VisibleImages")
	defer b.mu.RUnlock()

	return slices.DeleteFunc(slices.Clone(imgs), func(a *AMIStub) bool { return !b.imageUsableLocked(caller, a) })
}

// NarrowImagesByExecutable applies DescribeImages' ExecutableUsers (sharingSelf,
// account ids, "all") request member.
func (b *InMemoryBackend) NarrowImagesByExecutable(caller string, imgs []*AMIStub, executableBy []string) []*AMIStub {
	if len(executableBy) == 0 {
		return imgs
	}

	self := caller
	if self == "" {
		self = b.AccountID
	}

	b.mu.RLock("NarrowImagesByExecutable")
	defer b.mu.RUnlock()

	return slices.DeleteFunc(slices.Clone(imgs), func(a *AMIStub) bool {
		return !slices.ContainsFunc(executableBy, func(u string) bool {
			if u == sharingSelf {
				return a.OwnerID == self || b.imageLaunchPermissions[a.ImageID][self]
			}

			public := b.imageLaunchPermissionPublic[a.ImageID] || knownImageOwnerAliases[a.OwnerID]

			return (u == permissionGroupAll && public) || b.imageLaunchPermissions[a.ImageID][u]
		})
	})
}

// firstMissingSnapshot reports the first requested snapshot id absent from the visible set.
func firstMissingSnapshot(ids []string, visible []*Snapshot) error {
	for _, id := range ids {
		if !slices.ContainsFunc(visible, func(s *Snapshot) bool { return s.SnapshotID == id }) {
			return fmt.Errorf("%w: %s", ErrSnapshotNotFound, id)
		}
	}

	return nil
}

func (h *Handler) requireLaunchableImageFor(vals url.Values, imageID string) error {
	if err := h.requireImageUsable(vals, imageID); err != nil {
		return err
	}

	return h.requireLaunchableImage(h.svcCtx, imageID)
}

package azurefile

import (
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/strs"
)

const hoursPerDay = 24

// ShareError is a data-plane refusal surfaced to the management plane.
type ShareError struct {
	Code    string
	Message string
	Status  int
}

func (e *ShareError) Error() string { return e.Code + ": " + e.Message }

func shareErr(e *apiError) error {
	return &ShareError{Status: e.Status, Code: e.Code, Message: e.Message}
}

// ShareACL is one stored access policy.
type ShareACL struct {
	ID         string
	Start      string
	Expiry     string
	Permission string
}

// ShareInput is the settable part of a share as the management plane writes it.
type ShareInput struct {
	Metadata       map[string]string
	Quota          *int64
	Iops           *int64
	Bandwidth      *int64
	BurstIops      *int64
	BurstBandwidth *int64
	PaidBursting   *bool
	Tier           string
	Protocols      string
	RootSquash     string
	ACL            []ShareACL
	HasMetadata    bool
	HasACL         bool
}

// ShareView is a share, snapshot or soft-deleted share as the management plane reads it.
type ShareView struct {
	Created        time.Time
	LastModified   time.Time
	TierChanged    time.Time
	DeletedAt      time.Time
	Metadata       map[string]string
	Iops           *int64
	Bandwidth      *int64
	BurstIops      *int64
	BurstBandwidth *int64
	PaidBursting   *bool
	SnapshotVDir   *bool
	Name           string
	ETag           string
	Tier           string
	Protocols      string
	RootSquash     string
	Version        string
	Snapshot       string
	LeaseStatus    string
	LeaseState     string
	LeaseDuration  string
	ACL            []ShareACL
	Usage          int64
	RemainingDays  int32
	Quota          int32
	Deleted        bool
}

// ShareDeleteMode is the $include value of the management-plane delete.
type ShareDeleteMode string

// Delete modes of the Microsoft.Storage fileServices/shares DELETE.
const (
	ShareDeleteSnapshots       ShareDeleteMode = "snapshots"
	ShareDeleteLeasedSnapshots ShareDeleteMode = "leased-snapshots"
	ShareDeleteNone            ShareDeleteMode = "none"
)

func (in ShareInput) headers() http.Header {
	h := http.Header{}

	set := func(name string, v *int64) {
		if v != nil {
			h.Set(name, strconv.FormatInt(*v, 10))
		}
	}

	set("x-ms-share-quota", in.Quota)
	set("x-ms-share-provisioned-iops", in.Iops)
	set("x-ms-share-provisioned-bandwidth-mibps", in.Bandwidth)
	set("x-ms-share-paid-bursting-max-iops", in.BurstIops)
	set("x-ms-share-paid-bursting-max-bandwidth-mibps", in.BurstBandwidth)

	if in.PaidBursting != nil {
		h.Set("X-Ms-Share-Paid-Bursting-Enabled", strconv.FormatBool(*in.PaidBursting))
	}

	if in.Tier != "" {
		h.Set("X-Ms-Access-Tier", in.Tier)
	}

	if in.Protocols != "" {
		h.Set("X-Ms-Enabled-Protocols", in.Protocols)
	}

	if in.RootSquash != "" {
		h.Set("X-Ms-Root-Squash", in.RootSquash)
	}

	return h
}

func (in ShareInput) aclPolicies() []signedIdentifier {
	out := make([]signedIdentifier, 0, len(in.ACL))

	for _, a := range in.ACL {
		out = append(
			out,
			signedIdentifier{
				ID:     a.ID,
				Policy: accessPolicy{Start: a.Start, Expiry: a.Expiry, Permission: a.Permission},
			},
		)
	}

	return out
}

func capitalize(s string) string {
	if s == "" {
		return s
	}

	return strings.ToUpper(s[:1]) + s[1:]
}

func viewOf(sh *share, now time.Time, days int32) ShareView {
	lease := http.Header{}
	sh.Lease.setHeaders(lease, now)

	v := ShareView{
		Name: sh.Name, ETag: sh.ETag, Created: sh.Created, LastModified: sh.LastModified, TierChanged: sh.TierChanged,
		Tier: sh.Tier, Protocols: sh.Protocols, RootSquash: sh.RootSquash, Quota: sh.Quota,
		Metadata: sh.Metadata, Iops: sh.Extra.Iops, Bandwidth: sh.Extra.Bandwidth, BurstIops: sh.Extra.BurstIops,
		BurstBandwidth: sh.Extra.BurstBandwidth, PaidBursting: sh.Extra.PaidBursting,
		SnapshotVDir: sh.Extra.SnapshotVDirAccess, Snapshot: sh.SnapshotID, Usage: sh.Root.usage(),
		LeaseStatus:   capitalize(lease.Get("X-Ms-Lease-Status")),
		LeaseState:    capitalize(sh.Lease.effective(now)),
		LeaseDuration: capitalize(lease.Get("X-Ms-Lease-Duration")),
	}

	for _, id := range sh.ACL {
		v.ACL = append(
			v.ACL,
			ShareACL{ID: id.ID, Start: id.Policy.Start, Expiry: id.Policy.Expiry, Permission: id.Policy.Permission},
		)
	}

	if sh.Deleted {
		v.Deleted, v.DeletedAt, v.Version = true, sh.DeletedAt, sh.Version
		left := sh.DeletedAt.Add(time.Duration(days) * hoursPerDay * time.Hour).Sub(now)
		v.RemainingDays = max(0, int32(left.Hours()/hoursPerDay)+1)
	}

	return v
}

// PutShare creates the share or updates it in place, reporting whether it was created. create=false refuses a
// missing share (PATCH).
func (b *InMemoryBackend) PutShare(account, name string, in ShareInput, create bool) (ShareView, bool, error) {
	if !validShareName(name) {
		return ShareView{}, false, shareErr(errInvalidName())
	}

	opts, aerr := parseShareOptions(in.headers())
	if aerr != nil {
		return ShareView{}, false, shareErr(aerr)
	}

	if in.HasMetadata {
		if aerr = validateMetadata(in.Metadata); aerr != nil {
			return ShareView{}, false, shareErr(aerr)
		}
	}

	now := time.Now().UTC()

	b.mu.Lock("PutShare")
	defer b.mu.Unlock()

	a := b.acct(account)
	sh, exists := a.Shares[strs.Fold(name)]

	if !exists && !create {
		return ShareView{}, false, shareErr(errShareNotFound())
	}

	if !exists {
		sh = b.newShare(name, now)

		if opts.Protocols != "" {
			sh.Protocols = strings.ToUpper(opts.Protocols)
		}

		if aerr = sh.initNFS(sh.Root, nfsIn{}, true); aerr != nil {
			return ShareView{}, false, shareErr(aerr)
		}
	}

	opts.Protocols = ""

	if aerr = opts.applyTo(sh, now); aerr != nil {
		return ShareView{}, false, shareErr(aerr)
	}

	if in.HasMetadata {
		sh.Metadata = in.Metadata
	}

	if in.HasACL {
		sh.ACL = in.aclPolicies()
	}

	if exists {
		sh.ETag, sh.LastModified = b.etag(), now
	} else {
		a.Shares[strs.Fold(name)] = sh
	}

	return viewOf(sh, now, retentionDays(a)), !exists, nil
}

// GetShare returns a live share, or one of its snapshots when snapshot is set.
func (b *InMemoryBackend) GetShare(account, name, snapshot string) (ShareView, error) {
	now := time.Now().UTC()

	b.mu.RLock("GetShare")
	defer b.mu.RUnlock()

	sh, aerr := b.peek(account).findShare(name, snapshot, false)
	if aerr != nil {
		return ShareView{}, shareErr(aerr)
	}

	return viewOf(sh, now, retentionDays(b.peek(account))), nil
}

// ListShares returns the account's shares sorted by name; snapshots and soft-deleted shares are included on request.
func (b *InMemoryBackend) ListShares(account string, snapshots, deleted bool) []ShareView {
	now := time.Now().UTC()

	b.mu.RLock("ListShares")
	defer b.mu.RUnlock()

	a := b.peek(account)
	days := retentionDays(a)

	include := map[string]bool{"snapshots": snapshots, "deleted": deleted}
	entries := collectShareEntries(a, "", include, now)
	out := make([]ShareView, 0, len(entries))

	for _, e := range entries {
		out = append(out, viewOf(e.sh, now, days))
	}

	sort.SliceStable(out, func(i, j int) bool { return strs.Fold(out[i].Name) < strs.Fold(out[j].Name) })

	return out
}

// DeleteShare removes a share (or the named snapshot); it reports false when nothing existed.
func (b *InMemoryBackend) DeleteShare(account, name, snapshot string, mode ShareDeleteMode) (bool, error) {
	now := time.Now().UTC()

	b.mu.Lock("DeleteShareARM")
	defer b.mu.Unlock()

	a := b.peek(account)

	sh, _ := a.findShare(name, "", true)
	if sh == nil {
		return false, nil
	}

	if snapshot != "" {
		if sh.Snapshots[snapshot] == nil {
			return false, nil
		}

		delete(sh.Snapshots, snapshot)

		return true, nil
	}

	if len(sh.Snapshots) > 0 && mode == ShareDeleteNone {
		return false, shareErr(newErr(http.StatusConflict, "ShareHasSnapshots",
			"The share has snapshots and the operation requires no snapshots."))
	}

	if mode != ShareDeleteLeasedSnapshots {
		for _, sn := range sh.Snapshots {
			if sn.Lease.active(now) {
				return false, shareErr(newErr(http.StatusConflict, "LeaseIdMissing",
					"There is currently a lease on a share snapshot and no lease ID was specified in the request."))
			}
		}
	}

	delete(a.Shares, strs.Fold(name))

	if days := retentionDays(a); days > 0 {
		sh.Deleted, sh.DeletedAt = true, now
		sh.Version = fmtISO(now)
		sh.Lease = leaseInfo{}
		a.Deleted = append(a.Deleted, sh)
	}

	return true, nil
}

// RestoreShare revives a soft-deleted share under the given name.
func (b *InMemoryBackend) RestoreShare(account, target, deletedName, deletedVersion string) (ShareView, error) {
	now := time.Now().UTC()

	b.mu.Lock("RestoreShareARM")
	defer b.mu.Unlock()

	d, aerr := b.restoreLocked(account, target, deletedName, deletedVersion, now)
	if aerr != nil {
		return ShareView{}, shareErr(aerr)
	}

	return viewOf(d, now, retentionDays(b.peek(account))), nil
}

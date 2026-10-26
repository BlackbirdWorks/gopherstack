package azurefile

import (
	"encoding/json"
	"encoding/xml"
	"math"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/strs"
)

const metaPrefix = "X-Ms-Meta-"

func metadataFromHeaders(h http.Header) (map[string]string, *apiError) {
	var md map[string]string

	for k, v := range h {
		if len(k) > len(metaPrefix) && strings.EqualFold(k[:len(metaPrefix)], metaPrefix) && len(v) > 0 {
			if md == nil {
				md = make(map[string]string)
			}

			md[k[len(metaPrefix):]] = v[0]
		}
	}

	if err := validateMetadata(md); err != nil {
		return nil, err
	}

	return md, nil
}

func setMetadataHeaders(h http.Header, md map[string]string) {
	for k, v := range md {
		h.Set("X-Ms-Meta-"+k, v)
	}
}

// shareOptions are the optional share properties of Create/Set Share Properties.
type shareOptions struct {
	Extra     shareExtra
	Quota     *int32
	Tier      string
	Protocols string
	Squash    string
}

func optInt(h http.Header, name string) (*int64, *apiError) {
	v := h.Get(name)
	if v == "" {
		return nil, nil
	}

	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil || n < 0 {
		return nil, errInvalidHeader(name)
	}

	return &n, nil
}

func optBool(h http.Header, name string) (*bool, *apiError) {
	v := h.Get(name)
	if v == "" {
		return nil, nil
	}

	b, err := strconv.ParseBool(v)
	if err != nil {
		return nil, errInvalidHeader(name)
	}

	return &b, nil
}

func parseShareOptions(h http.Header) (shareOptions, *apiError) {
	var (
		o    shareOptions
		aerr *apiError
		q    *int64
	)

	if q, aerr = optInt(h, "x-ms-share-quota"); aerr != nil {
		return o, aerr
	}

	if q != nil {
		n := *q
		if n < 1 || n > maxQuotaGiB || n > math.MaxInt32 {
			return o, errInvalidHeader("x-ms-share-quota")
		}

		v := int32(n)
		o.Quota = &v
	}

	o.Tier = h.Get("X-Ms-Access-Tier")
	switch o.Tier {
	case "", "TransactionOptimized", "Hot", "Cool", "Premium":
	default:
		return o, errInvalidHeader("x-ms-access-tier")
	}

	o.Protocols = h.Get("X-Ms-Enabled-Protocols")
	if o.Protocols != "" && !strs.Equal(o.Protocols, protoSMB) && !strs.Equal(o.Protocols, protoNFS) {
		return o, errInvalidHeader("x-ms-enabled-protocols")
	}

	o.Squash = h.Get("X-Ms-Root-Squash")
	switch o.Squash {
	case "", "NoRootSquash", "RootSquash", "AllSquash":
	default:
		return o, errInvalidHeader("x-ms-root-squash")
	}

	return parseShareExtras(h, o)
}

func parseShareExtras(h http.Header, o shareOptions) (shareOptions, *apiError) {
	var aerr *apiError

	if o.Extra.SnapshotVDirAccess, aerr = optBool(h, "x-ms-enable-snapshot-virtual-directory-access"); aerr != nil {
		return o, aerr
	}

	if o.Extra.SMBDirLease, aerr = optBool(h, "x-ms-enable-smb-directory-lease"); aerr != nil {
		return o, aerr
	}

	if o.Extra.PaidBursting, aerr = optBool(h, "x-ms-share-paid-bursting-enabled"); aerr != nil {
		return o, aerr
	}

	if o.Extra.BurstBandwidth, aerr = optInt(h, "x-ms-share-paid-bursting-max-bandwidth-mibps"); aerr != nil {
		return o, aerr
	}

	if o.Extra.BurstIops, aerr = optInt(h, "x-ms-share-paid-bursting-max-iops"); aerr != nil {
		return o, aerr
	}

	if o.Extra.Iops, aerr = optInt(h, "x-ms-share-provisioned-iops"); aerr != nil {
		return o, aerr
	}

	o.Extra.Bandwidth, aerr = optInt(h, "x-ms-share-provisioned-bandwidth-mibps")

	return o, aerr
}

func (o shareOptions) applyTo(sh *share, now time.Time) *apiError {
	if o.Squash != "" && !strs.Equal(sh.Protocols, protoNFS) {
		return errInvalidHeader("x-ms-root-squash")
	}

	if o.Quota != nil {
		sh.Quota = *o.Quota
	}

	if o.Tier != "" && o.Tier != sh.Tier {
		sh.Tier = o.Tier
		sh.TierChanged = now
	}

	if o.Squash != "" {
		sh.RootSquash = o.Squash
	}

	mergeExtra(&sh.Extra, o.Extra)

	return nil
}

func mergeExtra(dst *shareExtra, src shareExtra) {
	if src.SnapshotVDirAccess != nil {
		dst.SnapshotVDirAccess = src.SnapshotVDirAccess
	}

	if src.SMBDirLease != nil {
		dst.SMBDirLease = src.SMBDirLease
	}

	if src.PaidBursting != nil {
		dst.PaidBursting = src.PaidBursting
	}

	if src.BurstBandwidth != nil {
		dst.BurstBandwidth = src.BurstBandwidth
	}

	if src.BurstIops != nil {
		dst.BurstIops = src.BurstIops
	}

	if src.Iops != nil {
		dst.Iops = src.Iops
	}

	if src.Bandwidth != nil {
		dst.Bandwidth = src.Bandwidth
	}
}

func (h *Handler) createShare(rc *reqCtx) (*resp, *apiError) {
	if !validShareName(rc.share) {
		return nil, errInvalidName()
	}

	opts, aerr := parseShareOptions(rc.r.Header)
	if aerr != nil {
		return nil, aerr
	}

	md, aerr := metadataFromHeaders(rc.r.Header)
	if aerr != nil {
		return nil, aerr
	}

	b := h.Backend
	b.mu.Lock("CreateShare")
	defer b.mu.Unlock()

	a := b.acct(rc.acct)
	if _, ok := a.Shares[strs.Fold(rc.share)]; ok {
		return nil, newErr(http.StatusConflict, "ShareAlreadyExists", "The specified share already exists.")
	}

	sh := b.newShare(rc.share, rc.now)
	sh.Metadata = md

	if opts.Protocols != "" {
		sh.Protocols = strings.ToUpper(opts.Protocols)
	}

	if err := opts.applyTo(sh, rc.now); err != nil {
		return nil, err
	}

	a.Shares[strs.Fold(rc.share)] = sh

	rs := newResp(http.StatusCreated)
	rs.hdr.Set("ETag", sh.ETag)
	rs.hdr.Set("Last-Modified", fmtHTTP(sh.LastModified))

	return rs, nil
}

func setShareHeaders(hdr http.Header, sh *share, now time.Time) {
	hdr.Set("ETag", sh.ETag)
	hdr.Set("Last-Modified", fmtHTTP(sh.LastModified))
	hdr.Set("X-Ms-Share-Quota", strconv.Itoa(int(sh.Quota)))
	hdr.Set("X-Ms-Access-Tier", sh.Tier)
	hdr.Set("X-Ms-Access-Tier-Change-Time", fmtHTTP(sh.TierChanged))
	hdr.Set("X-Ms-Enabled-Protocols", sh.Protocols)

	if sh.RootSquash != "" {
		hdr.Set("X-Ms-Root-Squash", sh.RootSquash)
	}

	setExtraHeaders(hdr, sh.Extra)
	setMetadataHeaders(hdr, sh.Metadata)
	sh.Lease.setHeaders(hdr, now)
}

func setExtraHeaders(hdr http.Header, e shareExtra) {
	if e.SnapshotVDirAccess != nil {
		hdr.Set("X-Ms-Enable-Snapshot-Virtual-Directory-Access", strconv.FormatBool(*e.SnapshotVDirAccess))
	}

	if e.SMBDirLease != nil {
		hdr.Set("X-Ms-Enable-Smb-Directory-Lease", strconv.FormatBool(*e.SMBDirLease))
	}

	if e.PaidBursting != nil {
		hdr.Set("X-Ms-Share-Paid-Bursting-Enabled", strconv.FormatBool(*e.PaidBursting))
	}

	for name, v := range map[string]*int64{
		"x-ms-share-paid-bursting-max-bandwidth-mibps": e.BurstBandwidth,
		"x-ms-share-paid-bursting-max-iops":            e.BurstIops,
		"x-ms-share-provisioned-iops":                  e.Iops,
		"x-ms-share-provisioned-bandwidth-mibps":       e.Bandwidth,
	} {
		if v != nil {
			hdr.Set(name, strconv.FormatInt(*v, 10))
		}
	}
}

// readLease validates an optional x-ms-lease-id on a read of a share.
func readLease(sh *share, rc *reqCtx) *apiError {
	if id := rc.hdr("x-ms-lease-id"); id != "" {
		return sh.Lease.check(id, rc.now, false)
	}

	return nil
}

func (h *Handler) getShareProps(rc *reqCtx) (*resp, *apiError) {
	h.Backend.mu.RLock("GetShareProperties")
	defer h.Backend.mu.RUnlock()

	sh, aerr := h.Backend.peek(rc.acct).findShare(rc.share, rc.q.Get("sharesnapshot"), false)
	if aerr != nil {
		return nil, aerr
	}

	if aerr = readLease(sh, rc); aerr != nil {
		return nil, aerr
	}

	rs := newResp(http.StatusOK)
	setShareHeaders(rs.hdr, sh, rc.now)

	return rs, nil
}

func (h *Handler) setShareProps(rc *reqCtx) (*resp, *apiError) {
	opts, aerr := parseShareOptions(rc.r.Header)
	if aerr != nil {
		return nil, aerr
	}

	b := h.Backend
	b.mu.Lock("SetShareProperties")
	defer b.mu.Unlock()

	sh, aerr := b.peek(rc.acct).findShare(rc.share, rc.q.Get("sharesnapshot"), true)
	if aerr != nil {
		return nil, aerr
	}

	if aerr = sh.Lease.check(rc.hdr("x-ms-lease-id"), rc.now, false); aerr != nil {
		return nil, aerr
	}

	if aerr = opts.applyTo(sh, rc.now); aerr != nil {
		return nil, aerr
	}

	sh.ETag, sh.LastModified = b.etag(), rc.now

	rs := newResp(http.StatusOK)
	rs.hdr.Set("ETag", sh.ETag)
	rs.hdr.Set("Last-Modified", fmtHTTP(sh.LastModified))

	return rs, nil
}

func (h *Handler) setShareMetadata(rc *reqCtx) (*resp, *apiError) {
	md, aerr := metadataFromHeaders(rc.r.Header)
	if aerr != nil {
		return nil, aerr
	}

	b := h.Backend
	b.mu.Lock("SetShareMetadata")
	defer b.mu.Unlock()

	sh, aerr := b.peek(rc.acct).findShare(rc.share, rc.q.Get("sharesnapshot"), true)
	if aerr != nil {
		return nil, aerr
	}

	if aerr = sh.Lease.check(rc.hdr("x-ms-lease-id"), rc.now, false); aerr != nil {
		return nil, aerr
	}

	sh.Metadata = md
	sh.ETag, sh.LastModified = b.etag(), rc.now

	rs := newResp(http.StatusOK)
	rs.hdr.Set("ETag", sh.ETag)
	rs.hdr.Set("Last-Modified", fmtHTTP(sh.LastModified))

	return rs, nil
}

func (h *Handler) snapshotShare(rc *reqCtx) (*resp, *apiError) {
	md, aerr := metadataFromHeaders(rc.r.Header)
	if aerr != nil {
		return nil, aerr
	}

	b := h.Backend
	b.mu.Lock("SnapshotShare")
	defer b.mu.Unlock()

	sh, aerr := b.peek(rc.acct).findShare(rc.share, "", true)
	if aerr != nil {
		return nil, aerr
	}

	if len(sh.Snapshots) >= maxSnapshots {
		return nil, newErr(http.StatusConflict, "ShareSnapshotCountExceeded",
			"The total number of snapshots for the share is over the limit.")
	}

	id := fmtISO(rc.now)
	for sh.Snapshots[id] != nil {
		rc.now = rc.now.Add(snapshotTick)
		id = fmtISO(rc.now)
	}

	sn := sh.snapshotCopy()
	sn.SnapshotID = id

	if md != nil {
		sn.Metadata = md
	}

	if sh.Snapshots == nil {
		sh.Snapshots = make(map[string]*share)
	}

	sh.Snapshots[id] = sn

	rs := newResp(http.StatusCreated)
	rs.hdr.Set("X-Ms-Snapshot", id)
	rs.hdr.Set("ETag", sh.ETag)
	rs.hdr.Set("Last-Modified", fmtHTTP(sh.LastModified))

	return rs, nil
}

func (h *Handler) deleteShare(rc *reqCtx) (*resp, *apiError) {
	b := h.Backend
	b.mu.Lock("DeleteShare")
	defer b.mu.Unlock()

	a := b.peek(rc.acct)

	sh, aerr := a.findShare(rc.share, "", true)
	if aerr != nil {
		return nil, aerr
	}

	if snap := rc.q.Get("sharesnapshot"); snap != "" {
		if sh.Snapshots[snap] == nil {
			return nil, errShareNotFound()
		}

		delete(sh.Snapshots, snap)

		return newResp(http.StatusAccepted), nil
	}

	mode := rc.hdr("x-ms-delete-snapshots")

	leaseErr := sh.Lease.check(rc.hdr("x-ms-lease-id"), rc.now, false)
	if leaseErr != nil && (mode != "include-leased" || !sh.Lease.active(rc.now)) {
		return nil, leaseErr
	}

	if len(sh.Snapshots) > 0 && mode != "include" && mode != "include-leased" {
		return nil, newErr(http.StatusConflict, "ShareHasSnapshots",
			"The share has snapshots and the operation requires no snapshots.")
	}

	delete(a.Shares, strs.Fold(rc.share))

	if days := retentionDays(a); days > 0 {
		sh.Deleted, sh.DeletedAt = true, rc.now
		sh.Version = fmtISO(rc.now)
		sh.Lease = leaseInfo{}
		a.Deleted = append(a.Deleted, sh)
	}

	return newResp(http.StatusAccepted), nil
}

func (s *share) expired(days int32, now time.Time) bool {
	return days <= 0 || !now.Before(s.DeletedAt.Add(time.Duration(days)*24*time.Hour))
}

// purgeExpired drops soft-deleted shares past the retention window.
func (a *account) purgeExpired(now time.Time) {
	days := retentionDays(a)
	kept := a.Deleted[:0]

	for _, d := range a.Deleted {
		if !d.expired(days, now) {
			kept = append(kept, d)
		}
	}

	a.Deleted = kept
}

func (h *Handler) restoreShare(rc *reqCtx) (*resp, *apiError) {
	name, version := rc.hdr("x-ms-deleted-share-name"), rc.hdr("x-ms-deleted-share-version")
	if name == "" || version == "" {
		return nil, errMissingHeader("x-ms-deleted-share-name")
	}

	b := h.Backend
	b.mu.Lock("RestoreShare")
	defer b.mu.Unlock()

	a := b.acct(rc.acct)
	a.purgeExpired(rc.now)

	if _, ok := a.Shares[strs.Fold(rc.share)]; ok {
		return nil, newErr(http.StatusConflict, "ShareAlreadyExists", "The specified share already exists.")
	}

	for i, d := range a.Deleted {
		if strs.Equal(d.Name, name) && d.Version == version {
			a.Deleted = append(a.Deleted[:i], a.Deleted[i+1:]...)
			d.Deleted, d.Version, d.DeletedAt, d.Name = false, "", time.Time{}, rc.share
			d.ETag, d.LastModified = b.etag(), rc.now
			a.Shares[strs.Fold(rc.share)] = d

			rs := newResp(http.StatusCreated)
			rs.hdr.Set("ETag", d.ETag)
			rs.hdr.Set("Last-Modified", fmtHTTP(d.LastModified))

			return rs, nil
		}
	}

	return nil, newErr(http.StatusNotFound, "ShareNotFound", "The specified deleted share does not exist.")
}

type accessPolicyXML struct {
	Start      string `xml:"Start,omitempty"`
	Expiry     string `xml:"Expiry,omitempty"`
	Permission string `xml:"Permission,omitempty"`
}

type signedIdentifierXML struct {
	ID     string          `xml:"Id"`
	Policy accessPolicyXML `xml:"AccessPolicy"`
}

type signedIdentifiersXML struct {
	XMLName xml.Name              `xml:"SignedIdentifiers"`
	Items   []signedIdentifierXML `xml:"SignedIdentifier"`
}

const maxStoredPolicies = 5

func (h *Handler) getShareACL(rc *reqCtx) (*resp, *apiError) {
	h.Backend.mu.RLock("GetShareACL")
	defer h.Backend.mu.RUnlock()

	sh, aerr := h.Backend.peek(rc.acct).findShare(rc.share, "", false)
	if aerr != nil {
		return nil, aerr
	}

	if aerr = readLease(sh, rc); aerr != nil {
		return nil, aerr
	}

	out := signedIdentifiersXML{}
	for _, id := range sh.ACL {
		out.Items = append(out.Items, signedIdentifierXML{
			ID:     id.ID,
			Policy: accessPolicyXML{Start: id.Policy.Start, Expiry: id.Policy.Expiry, Permission: id.Policy.Permission},
		})
	}

	rs := newResp(http.StatusOK).xml(out)
	rs.hdr.Set("ETag", sh.ETag)
	rs.hdr.Set("Last-Modified", fmtHTTP(sh.LastModified))

	return rs, nil
}

func (h *Handler) setShareACL(rc *reqCtx) (*resp, *apiError) {
	data, aerr := rc.body()
	if aerr != nil {
		return nil, aerr
	}

	var in signedIdentifiersXML

	if len(data) > 0 {
		if err := xml.Unmarshal(data, &in); err != nil {
			return nil, errInvalidXML()
		}
	}

	if len(in.Items) > maxStoredPolicies {
		return nil, errInvalidXML()
	}

	acl := make([]signedIdentifier, 0, len(in.Items))
	seen := map[string]bool{}

	for _, it := range in.Items {
		if it.ID == "" || len(it.ID) > 64 || seen[it.ID] {
			return nil, errInvalidXML()
		}

		seen[it.ID] = true
		acl = append(acl, signedIdentifier{
			ID:     it.ID,
			Policy: accessPolicy{Start: it.Policy.Start, Expiry: it.Policy.Expiry, Permission: it.Policy.Permission},
		})
	}

	b := h.Backend
	b.mu.Lock("SetShareACL")
	defer b.mu.Unlock()

	sh, aerr := b.peek(rc.acct).findShare(rc.share, "", true)
	if aerr != nil {
		return nil, aerr
	}

	if aerr = sh.Lease.check(rc.hdr("x-ms-lease-id"), rc.now, false); aerr != nil {
		return nil, aerr
	}

	sh.ACL = acl
	sh.ETag, sh.LastModified = b.etag(), rc.now

	rs := newResp(http.StatusOK)
	rs.hdr.Set("ETag", sh.ETag)
	rs.hdr.Set("Last-Modified", fmtHTTP(sh.LastModified))

	return rs, nil
}

type shareStatsXML struct {
	XMLName xml.Name `xml:"ShareStats"`
	Usage   int64    `xml:"ShareUsageBytes"`
}

func (n *node) usage() int64 {
	if !n.Dir {
		return n.Size
	}

	var total int64
	for _, c := range n.Children {
		total += c.usage()
	}

	return total
}

func (h *Handler) getShareStats(rc *reqCtx) (*resp, *apiError) {
	h.Backend.mu.RLock("GetShareStats")
	defer h.Backend.mu.RUnlock()

	sh, aerr := h.Backend.peek(rc.acct).findShare(rc.share, "", false)
	if aerr != nil {
		return nil, aerr
	}

	if aerr = readLease(sh, rc); aerr != nil {
		return nil, aerr
	}

	rs := newResp(http.StatusOK).xml(shareStatsXML{Usage: sh.Root.usage()})
	rs.hdr.Set("ETag", sh.ETag)
	rs.hdr.Set("Last-Modified", fmtHTTP(sh.LastModified))

	return rs, nil
}

func parseLeaseRequest(rc *reqCtx) (leaseRequest, *apiError) {
	req := leaseRequest{
		Action: strings.ToLower(rc.hdr(headerLeaseAction)), LeaseID: rc.hdr("x-ms-lease-id"),
		Proposed: rc.hdr("x-ms-proposed-lease-id"),
	}

	if v := rc.hdr("x-ms-lease-duration"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			return req, errInvalidHeader("x-ms-lease-duration")
		}

		req.Duration, req.HasDur = n, true
	}

	if v := rc.hdr("x-ms-lease-break-period"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 0 {
			return req, errInvalidHeader("x-ms-lease-break-period")
		}

		req.Break = &n
	}

	if req.Action == "" {
		return req, errMissingHeader(headerLeaseAction)
	}

	if (req.Action == "release" || req.Action == "renew" || req.Action == "change") && req.LeaseID == "" {
		return req, errMissingHeader("x-ms-lease-id")
	}

	return req, nil
}

func leaseResp(res leaseResult, etag string, lm time.Time) *resp {
	rs := newResp(res.Status)
	if res.ID != "" {
		rs.hdr.Set("X-Ms-Lease-Id", res.ID)
	}

	if res.Status == http.StatusAccepted {
		rs.hdr.Set("X-Ms-Lease-Time", strconv.Itoa(res.BreakTime))
	}

	rs.hdr.Set("ETag", etag)
	rs.hdr.Set("Last-Modified", fmtHTTP(lm))

	return rs
}

func (h *Handler) leaseShare(rc *reqCtx) (*resp, *apiError) {
	req, aerr := parseLeaseRequest(rc)
	if aerr != nil {
		return nil, aerr
	}

	h.Backend.mu.Lock("LeaseShare")
	defer h.Backend.mu.Unlock()

	sh, aerr := h.Backend.peek(rc.acct).findShare(rc.share, rc.q.Get("sharesnapshot"), true)
	if aerr != nil {
		return nil, aerr
	}

	res, aerr := sh.Lease.apply(req, rc.now, false)
	if aerr != nil {
		return nil, aerr
	}

	return leaseResp(res, sh.ETag, sh.LastModified), nil
}

type permissionJSON struct {
	Permission string `json:"permission"`
	Format     string `json:"format,omitempty"`
}

func (h *Handler) createPermission(rc *reqCtx) (*resp, *apiError) {
	data, aerr := rc.body()
	if aerr != nil {
		return nil, aerr
	}

	var in permissionJSON
	if err := json.Unmarshal(data, &in); err != nil || in.Permission == "" || len(in.Permission) > maxSDDLLen {
		return nil, errInvalidHeader("x-ms-file-permission")
	}

	if in.Format != "" && !strs.Equal(in.Format, "sddl") {
		return nil, errInvalidHeader("x-ms-file-permission-format")
	}

	h.Backend.mu.Lock("CreatePermission")
	defer h.Backend.mu.Unlock()

	sh, aerr := h.Backend.peek(rc.acct).findShare(rc.share, "", true)
	if aerr != nil {
		return nil, aerr
	}

	rs := newResp(http.StatusCreated)
	rs.hdr.Set("X-Ms-File-Permission-Key", sh.addPermission(in.Permission))

	return rs, nil
}

func (h *Handler) getPermission(rc *reqCtx) (*resp, *apiError) {
	key := rc.hdr("x-ms-file-permission-key")
	if key == "" {
		return nil, errMissingHeader("x-ms-file-permission-key")
	}

	h.Backend.mu.RLock("GetPermission")
	defer h.Backend.mu.RUnlock()

	sh, aerr := h.Backend.peek(rc.acct).findShare(rc.share, rc.q.Get("sharesnapshot"), false)
	if aerr != nil {
		return nil, aerr
	}

	sddl, ok := sh.Permissions[key]
	if !ok {
		return nil, errInvalidHeader("x-ms-file-permission-key")
	}

	body, err := json.Marshal(permissionJSON{Permission: sddl, Format: "Sddl"})
	if err != nil {
		return nil, newErr(http.StatusInternalServerError, "InternalError", "Failed to marshal response.")
	}

	return newResp(http.StatusOK).json(body), nil
}

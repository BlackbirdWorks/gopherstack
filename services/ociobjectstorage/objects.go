package ociobjectstorage

import (
	"maps"
	"net/http"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

const (
	maxObjectName = 1024
	restoreDelay  = time.Hour
	defaultHours  = 24
	maxHours      = 240
	hoursPerDay   = 24
	daysPerYear   = 365
)

// ObjectInfo is an object version plus its archival state evaluated at read time.
type ObjectInfo struct {
	Archival string
	Object
}

func newVersionID() string { return uuid.NewString() }

func (b *InMemoryBackend) info(o *Object) ObjectInfo {
	return ObjectInfo{Object: *o, Archival: archivalAt(o, b.nowUTC())}
}

func archivalAt(o *Object, now time.Time) string {
	if o.Meta.StorageTier != tierArchive {
		return ""
	}

	switch {
	case o.RestoreReadyAt.IsZero():
		return archivalArchived
	case now.Before(o.RestoreReadyAt):
		return archivalRestoring
	case now.Before(o.RestoreExpiry):
		return archivalRestored
	default:
		return archivalArchived
	}
}

func latest(bk *Bucket, name string) *Object {
	vs := bk.Objects[name]
	if len(vs) == 0 {
		return nil
	}

	return vs[len(vs)-1]
}

func live(bk *Bucket, name string) *Object {
	if o := latest(bk, name); o != nil && !o.IsDeleteMarker {
		return o
	}

	return nil
}

func ruleDuration(r *Retention) time.Duration {
	d := time.Duration(r.Amount) * hoursPerDay * time.Hour
	if r.Unit == "YEARS" {
		d *= daysPerYear
	}

	return d
}

func (b *InMemoryBackend) underRetention(bk *Bucket, o *Object) bool {
	if o.IsDeleteMarker {
		return false
	}

	now := b.nowUTC()

	for _, r := range bk.Retention {
		if !r.HasDuration || now.Before(o.TimeCreated.Add(ruleDuration(r))) {
			return true
		}
	}

	return false
}

func errRetention(name string) *APIError {
	return errConflict("ObjectUnderRetention", "Object '"+name+"' is protected by a retention rule")
}

// PutObject stores an object; contentMD5 is the optional base64 Content-MD5 request header.
func (b *InMemoryBackend) PutObject(
	ns, bucket, name string, data []byte, meta ObjectMeta, pre Preconds, contentMD5 string,
) (ObjectInfo, error) {
	if name == "" || len(name) > maxObjectName {
		return ObjectInfo{}, errParam("Object name must be 1-1024 characters")
	}

	sum := md5b64(data)
	if contentMD5 != "" && contentMD5 != sum {
		return ObjectInfo{}, newErr(http.StatusBadRequest, "InvalidDigest",
			"The Content-MD5 you specified did not match what we received")
	}

	defer b.flushEvents()

	b.mu.Lock("PutObject")
	defer b.mu.Unlock()

	bk, err := b.bucket(ns, bucket)
	if err != nil {
		return ObjectInfo{}, err
	}

	cur := live(bk, name)
	etag := ""

	if cur != nil {
		etag = cur.Etag
	}

	if err = pre.check(etag, cur != nil, false); err != nil {
		return ObjectInfo{}, err
	}

	o, err := b.storeLocked(bk, name, data, meta, sum, "")
	if err != nil {
		return ObjectInfo{}, err
	}

	b.recordEvent(bk, writeEventType(cur != nil), name, o.Etag)

	return b.info(o), nil
}

func (b *InMemoryBackend) storeLocked(
	bk *Bucket, name string, data []byte, meta ObjectMeta, sum, multipartMD5 string,
) (*Object, error) {
	switch meta.StorageTier {
	case "":
		meta.StorageTier = bk.StorageTier
	case tierStandard, tierIA, tierArchive:
	default:
		return nil, errParam("Invalid storage-tier")
	}

	if meta.ContentType == "" {
		meta.ContentType = "application/octet-stream"
	}

	now := b.nowUTC()
	o := &Object{
		Name: name, Data: data, Meta: meta, MD5: sum, MultipartMD5: multipartMD5,
		Etag: newEtag(), TimeCreated: now, TimeModified: now,
	}

	vs := bk.Objects[name]

	switch bk.Versioning {
	case versioningEnabled:
		o.VersionID = newVersionID()
		bk.Objects[name] = append(vs, o)
	case versioningSuspended:
		o.VersionID = nullVersion

		kept := make([]*Object, 0, len(vs)+1)

		for _, v := range vs {
			if v.VersionID != nullVersion {
				kept = append(kept, v)
			} else if b.underRetention(bk, v) {
				return nil, errRetention(name)
			}
		}

		bk.Objects[name] = append(kept, o)
	default:
		for _, v := range vs {
			if b.underRetention(bk, v) {
				return nil, errRetention(name)
			}
		}

		o.VersionID = newVersionID()
		bk.Objects[name] = []*Object{o}
	}

	return o, nil
}

func pick(bk *Bucket, name, versionID string) *Object {
	if versionID == "" {
		return live(bk, name)
	}

	for _, v := range bk.Objects[name] {
		if v.VersionID == versionID && !v.IsDeleteMarker {
			return v
		}
	}

	return nil
}

// GetObject reads an object (or only its headers when body is false).
func (b *InMemoryBackend) GetObject(
	ns, bucket, name, versionID string, pre Preconds, body bool,
) (ObjectInfo, error) {
	b.mu.RLock("GetObject")
	defer b.mu.RUnlock()

	bk, err := b.bucket(ns, bucket)
	if err != nil {
		return ObjectInfo{}, err
	}

	o := pick(bk, name, versionID)
	if o == nil {
		return ObjectInfo{}, errObject(name)
	}

	if err = pre.check(o.Etag, true, true); err != nil {
		return ObjectInfo{}, err
	}

	oi := b.info(o)
	if body && (oi.Archival == archivalArchived || oi.Archival == archivalRestoring) {
		return ObjectInfo{}, errConflict("NotRestored",
			"The object '"+name+"' is archived and must be restored before it can be read")
	}

	return oi, nil
}

// DeleteResult describes the outcome of DeleteObject.
type DeleteResult struct {
	LastModified   time.Time
	VersionID      string
	IsDeleteMarker bool
}

// DeleteObject deletes an object or one version of it.
func (b *InMemoryBackend) DeleteObject(ns, bucket, name, versionID, ifMatch string) (DeleteResult, error) {
	defer b.flushEvents()

	b.mu.Lock("DeleteObject")
	defer b.mu.Unlock()

	bk, err := b.bucket(ns, bucket)
	if err != nil {
		return DeleteResult{}, err
	}

	res, err := b.deleteLocked(bk, name, versionID, ifMatch)
	if err == nil {
		b.recordEvent(bk, EventDeleteObject, name, "")
	}

	return res, err
}

func (b *InMemoryBackend) deleteLocked(bk *Bucket, name, versionID, ifMatch string) (DeleteResult, error) {
	vs := bk.Objects[name]
	if len(vs) == 0 {
		return DeleteResult{}, errObject(name)
	}

	cur := vs[len(vs)-1]
	if ifMatch != "" && !etagMatches(ifMatch, cur.Etag) {
		return DeleteResult{}, errNoEtag()
	}

	switch {
	case versionID != "":
		return b.deleteVersionLocked(bk, name, versionID)
	case bk.Versioning == versioningDisabled:
		return b.deleteAllLocked(bk, name)
	default:
		return b.addMarkerLocked(bk, name), nil
	}
}

func (b *InMemoryBackend) deleteVersionLocked(bk *Bucket, name, versionID string) (DeleteResult, error) {
	vs := bk.Objects[name]

	i := slices.IndexFunc(vs, func(v *Object) bool { return v.VersionID == versionID })
	if i < 0 {
		return DeleteResult{}, errObject(name)
	}

	v := vs[i]
	if b.underRetention(bk, v) {
		return DeleteResult{}, errRetention(name)
	}

	if rest := slices.Delete(slices.Clone(vs), i, i+1); len(rest) > 0 {
		bk.Objects[name] = rest
	} else {
		delete(bk.Objects, name)
	}

	return DeleteResult{VersionID: versionID, IsDeleteMarker: v.IsDeleteMarker, LastModified: b.nowUTC()}, nil
}

func (b *InMemoryBackend) deleteAllLocked(bk *Bucket, name string) (DeleteResult, error) {
	vs := bk.Objects[name]
	cur := vs[len(vs)-1]

	if cur.IsDeleteMarker {
		return DeleteResult{}, errObject(name)
	}

	if slices.ContainsFunc(vs, func(v *Object) bool { return b.underRetention(bk, v) }) {
		return DeleteResult{}, errRetention(name)
	}

	delete(bk.Objects, name)

	return DeleteResult{VersionID: cur.VersionID, LastModified: b.nowUTC()}, nil
}

func (b *InMemoryBackend) addMarkerLocked(bk *Bucket, name string) DeleteResult {
	now := b.nowUTC()
	m := &Object{
		Name: name, IsDeleteMarker: true, TimeCreated: now, TimeModified: now,
		Etag: newEtag(), VersionID: newVersionID(),
	}
	vs := bk.Objects[name]

	if bk.Versioning == versioningSuspended {
		m.VersionID = nullVersion
		vs = slices.DeleteFunc(slices.Clone(vs), func(v *Object) bool { return v.VersionID == nullVersion })
	}

	bk.Objects[name] = append(vs, m)

	return DeleteResult{VersionID: m.VersionID, IsDeleteMarker: true, LastModified: now}
}

// ListOptions are the ListObjects / ListObjectVersions query parameters.
type ListOptions struct {
	Prefix, Start, End, StartAfter, Delimiter, Page string
	Limit                                           int
}

func (o ListOptions) inRange(name string) bool {
	return strings.HasPrefix(name, o.Prefix) &&
		(o.Start == "" || name >= o.Start) && (o.End == "" || name < o.End) &&
		(o.StartAfter == "" || name > o.StartAfter)
}

// collapse returns the common prefix a name rolls up into, or "".
func (o ListOptions) collapse(name string) string {
	if o.Delimiter == "" {
		return ""
	}

	rest := name[len(o.Prefix):]
	if i := strings.Index(rest, o.Delimiter); i >= 0 {
		return o.Prefix + rest[:i+len(o.Delimiter)]
	}

	return ""
}

// ListObjects lists live objects (names only are guaranteed meaningful for fields=name).
func (b *InMemoryBackend) ListObjects(ns, bucket string, opt ListOptions) ([]ObjectInfo, []string, string, error) {
	b.mu.RLock("ListObjects")
	defer b.mu.RUnlock()

	bk, err := b.bucket(ns, bucket)
	if err != nil {
		return nil, nil, "", err
	}

	names := make([]string, 0, len(bk.Objects))

	for n := range bk.Objects {
		if opt.inRange(n) && live(bk, n) != nil {
			names = append(names, n)
		}
	}

	sort.Strings(names)

	limit := opt.Limit
	if limit <= 0 {
		limit = defaultListLimit
	}

	var (
		objs     []ObjectInfo
		prefixes []string
		next     string
		seen     = map[string]bool{}
	)

	for _, n := range names {
		if p := opt.collapse(n); p != "" {
			if seen[p] {
				continue
			}

			if len(objs)+len(prefixes) >= limit {
				next = n

				break
			}

			seen[p] = true
			prefixes = append(prefixes, p)

			continue
		}

		if len(objs)+len(prefixes) >= limit {
			next = n

			break
		}

		objs = append(objs, b.info(live(bk, n)))
	}

	return objs, prefixes, next, nil
}

// ListObjectVersions lists every version and delete marker, newest first per name.
func (b *InMemoryBackend) ListObjectVersions(
	ns, bucket string, opt ListOptions,
) (page.Page[ObjectInfo], []string, error) {
	b.mu.RLock("ListObjectVersions")
	defer b.mu.RUnlock()

	bk, err := b.bucket(ns, bucket)
	if err != nil {
		return page.Page[ObjectInfo]{}, nil, err
	}

	names := make([]string, 0, len(bk.Objects))

	for n := range bk.Objects {
		if opt.inRange(n) {
			names = append(names, n)
		}
	}

	sort.Strings(names)

	var (
		items    []ObjectInfo
		prefixes []string
		seen     = map[string]bool{}
	)

	for _, n := range names {
		if p := opt.collapse(n); p != "" {
			if !seen[p] {
				seen[p] = true
				prefixes = append(prefixes, p)
			}

			continue
		}

		vs := bk.Objects[n]
		for _, v := range slices.Backward(vs) {
			items = append(items, b.info(v))
		}
	}

	return page.New(items, opt.Page, opt.Limit, defaultListLimit), prefixes, nil
}

// RestoreObjects begins restoring an archived object.
func (b *InMemoryBackend) RestoreObjects(ns, bucket, name, versionID string, hours int) error {
	if name == "" {
		return errMissing("objectName is required")
	}

	if hours == 0 {
		hours = defaultHours
	}

	if hours < 1 || hours > maxHours {
		return errParam("hours must be between 1 and 240")
	}

	b.mu.Lock("RestoreObjects")
	defer b.mu.Unlock()

	bk, err := b.bucket(ns, bucket)
	if err != nil {
		return err
	}

	o := pick(bk, name, versionID)
	if o == nil {
		return errObject(name)
	}

	switch archivalAt(o, b.nowUTC()) {
	case "":
		return errConflict("Conflict", "Only objects in the Archive storage tier can be restored")
	case archivalRestoring, archivalRestored:
		return errConflict("Conflict", "The object is already being restored or has been restored")
	default:
	}

	now := b.nowUTC()
	o.RestoreReadyAt = now.Add(restoreDelay)
	o.RestoreExpiry = o.RestoreReadyAt.Add(time.Duration(hours) * time.Hour)

	return nil
}

// UpdateStorageTier changes an object's tier.
func (b *InMemoryBackend) UpdateStorageTier(ns, bucket, name, versionID, tier string) error {
	if tier != tierStandard && tier != tierIA && tier != tierArchive {
		return errParam("storageTier must be Standard, InfrequentAccess or Archive")
	}

	b.mu.Lock("UpdateStorageTier")
	defer b.mu.Unlock()

	bk, err := b.bucket(ns, bucket)
	if err != nil {
		return err
	}

	o := pick(bk, name, versionID)
	if o == nil {
		return errObject(name)
	}

	st := archivalAt(o, b.nowUTC())
	if st == archivalArchived || st == archivalRestoring {
		return errConflict("Conflict", "An archived object must be restored before its tier can change")
	}

	o.Meta.StorageTier = tier
	o.RestoreReadyAt, o.RestoreExpiry = time.Time{}, time.Time{}
	o.TimeModified = b.nowUTC()

	return nil
}

// CopyInput is the CopyObjectDetails payload.
type CopyInput struct {
	DestinationMetadata map[string]string `json:"destinationObjectMetadata"`
	SourceObjectName    string            `json:"sourceObjectName"`
	DestinationRegion   string            `json:"destinationRegion"`
	DestinationNS       string            `json:"destinationNamespace"`
	DestinationBucket   string            `json:"destinationBucket"`
	DestinationObject   string            `json:"destinationObjectName"`
	SourceIfMatch       string            `json:"sourceObjectIfMatchETag"`
	SourceVersionID     string            `json:"sourceVersionId"`
	DestIfMatch         string            `json:"destinationObjectIfMatchETag"`
	DestIfNoneMatch     string            `json:"destinationObjectIfNoneMatchETag"`
	DestinationTier     string            `json:"destinationObjectStorageTier"`
}

func (in CopyInput) validate() error {
	if in.SourceObjectName == "" || in.DestinationObject == "" || in.DestinationBucket == "" ||
		in.DestinationNS == "" || in.DestinationRegion == "" {
		return errMissing("sourceObjectName, destinationRegion, destinationNamespace, " +
			"destinationBucket and destinationObjectName are required")
	}

	if in.DestinationRegion != Region {
		return errParam("Cross-region copy is not supported by this emulator; use region " + Region)
	}

	return nil
}

// copyMeta derives the destination metadata of a copy.
func copyMeta(o *Object, dst *Bucket, in CopyInput) ObjectMeta {
	meta := o.Meta
	meta.Metadata = copyMap(o.Meta.Metadata)

	if in.DestinationMetadata != nil {
		meta.Metadata = in.DestinationMetadata
	}

	if in.DestinationTier != "" {
		meta.StorageTier = in.DestinationTier
	} else if meta.StorageTier == tierArchive {
		meta.StorageTier = dst.StorageTier
	}

	return meta
}

// CopyObject copies an object and returns a (completed) work request id.
func (b *InMemoryBackend) CopyObject(ns, bucket string, in CopyInput) (string, error) {
	if err := in.validate(); err != nil {
		return "", err
	}

	defer b.flushEvents()

	b.mu.Lock("CopyObject")
	defer b.mu.Unlock()

	src, err := b.bucket(ns, bucket)
	if err != nil {
		return "", err
	}

	dst, err := b.bucket(in.DestinationNS, in.DestinationBucket)
	if err != nil {
		return "", err
	}

	o := pick(src, in.SourceObjectName, in.SourceVersionID)
	if o == nil {
		return "", errObject(in.SourceObjectName)
	}

	if err = (Preconds{IfMatch: in.SourceIfMatch}).check(o.Etag, true, false); err != nil {
		return "", err
	}

	if st := archivalAt(o, b.nowUTC()); st == archivalArchived || st == archivalRestoring {
		return "", errConflict(
			"NotRestored",
			"The source object is archived and must be restored before it can be copied",
		)
	}

	cur := live(dst, in.DestinationObject)
	etag := ""

	if cur != nil {
		etag = cur.Etag
	}

	if err = (Preconds{IfMatch: in.DestIfMatch, IfNoneMatch: in.DestIfNoneMatch}).check(
		etag,
		cur != nil,
		false,
	); err != nil {
		return "", err
	}

	stored, err := b.storeLocked(
		dst,
		in.DestinationObject,
		append([]byte(nil), o.Data...),
		copyMeta(o, dst, in),
		o.MD5,
		o.MultipartMD5,
	)
	if err != nil {
		return "", err
	}

	b.recordEvent(dst, writeEventType(cur != nil), in.DestinationObject, stored.Etag)

	now := b.nowUTC()
	wr := &WorkRequest{
		ID: "ocid1.coreservicesworkrequest.oc1." + Region + "." + newVersionID(), OperationType: "COPY_OBJECT",
		Status: "COMPLETED", CompartmentID: dst.CompartmentID, TimeAccepted: now, TimeFinished: now,
		EntityURI: "/n/" + in.DestinationNS + "/b/" + in.DestinationBucket + "/o/" + in.DestinationObject,
	}
	b.workRequests[wr.ID] = wr

	return wr.ID, nil
}

func copyMap(m map[string]string) map[string]string {
	if m == nil {
		return nil
	}

	out := make(map[string]string, len(m))
	maps.Copy(out, m)

	return out
}

// RenameInput is the RenameObjectDetails payload.
type RenameInput struct {
	SourceName   string `json:"sourceName"`
	NewName      string `json:"newName"`
	SrcIfMatch   string `json:"srcObjIfMatchETag"`
	NewIfMatch   string `json:"newObjIfMatchETag"`
	NewIfNoMatch string `json:"newObjIfNoneMatchETag"`
}

// RenameObject moves an object to a new name within its bucket.
func (b *InMemoryBackend) RenameObject(ns, bucket string, in RenameInput) (ObjectInfo, error) {
	if in.SourceName == "" || in.NewName == "" {
		return ObjectInfo{}, errMissing("sourceName and newName are required")
	}

	if in.SourceName == in.NewName {
		return ObjectInfo{}, errParam("sourceName and newName must differ")
	}

	b.mu.Lock("RenameObject")
	defer b.mu.Unlock()

	bk, err := b.bucket(ns, bucket)
	if err != nil {
		return ObjectInfo{}, err
	}

	src := live(bk, in.SourceName)
	if src == nil {
		return ObjectInfo{}, errObject(in.SourceName)
	}

	if err = (Preconds{IfMatch: in.SrcIfMatch}).check(src.Etag, true, false); err != nil {
		return ObjectInfo{}, err
	}

	cur := live(bk, in.NewName)
	etag := ""

	if cur != nil {
		etag = cur.Etag
	}

	if err = (Preconds{IfMatch: in.NewIfMatch, IfNoneMatch: in.NewIfNoMatch}).check(
		etag,
		cur != nil,
		false,
	); err != nil {
		return ObjectInfo{}, err
	}

	if b.underRetention(bk, src) {
		return ObjectInfo{}, errRetention(in.SourceName)
	}

	meta := src.Meta
	meta.Metadata = copyMap(src.Meta.Metadata)

	o, err := b.storeLocked(bk, in.NewName, src.Data, meta, src.MD5, src.MultipartMD5)
	if err != nil {
		return ObjectInfo{}, err
	}

	if _, err = b.deleteLocked(bk, in.SourceName, "", ""); err != nil {
		return ObjectInfo{}, err
	}

	return b.info(o), nil
}

// BatchItem is one entry of a BatchDeleteObjects request.
type BatchItem struct {
	ObjectName string `json:"objectName"`
	IfMatch    string `json:"ifMatch"`
}

// BatchFailure is a failed BatchDeleteObjects entry.
type BatchFailure struct {
	ObjectName   string `json:"objectName"`
	ErrorMessage string `json:"errorMessage"`
	StatusCode   int    `json:"statusCode"`
}

// BatchDeleted is a deleted BatchDeleteObjects entry.
type BatchDeleted struct {
	TimeLastModified time.Time `json:"timeLastModified"`
	ObjectName       string    `json:"objectName"`
}

// BatchDelete deletes several objects, reporting per-object outcomes.
func (b *InMemoryBackend) BatchDelete(ns, bucket string, items []BatchItem) ([]BatchDeleted, []BatchFailure, error) {
	defer b.flushEvents()

	b.mu.Lock("BatchDelete")
	defer b.mu.Unlock()

	bk, err := b.bucket(ns, bucket)
	if err != nil {
		return nil, nil, err
	}

	deleted, failed := []BatchDeleted{}, []BatchFailure{}

	for _, it := range items {
		res, derr := b.deleteLocked(bk, it.ObjectName, "", it.IfMatch)
		if derr != nil {
			status, msg := http.StatusInternalServerError, derr.Error()
			if ae, ok := derr.(*APIError); ok { //nolint:errorlint // only *APIError is produced here
				status, msg = ae.Status, ae.Message
			}

			failed = append(failed, BatchFailure{ObjectName: it.ObjectName, StatusCode: status, ErrorMessage: msg})

			continue
		}

		b.recordEvent(bk, EventDeleteObject, it.ObjectName, "")

		deleted = append(deleted, BatchDeleted{ObjectName: it.ObjectName, TimeLastModified: res.LastModified})
	}

	return deleted, failed, nil
}

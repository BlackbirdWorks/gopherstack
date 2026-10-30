package ociobjectstorage

import (
	"crypto/md5" //nolint:gosec // OCI object md5 is an integrity checksum defined by the API
	"encoding/base64"
	"net/http"
	"regexp"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/awsid"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/ociauth"
	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

const (
	// Region is the fixed region baked into emulated OCIDs.
	Region           = "us-ashburn-1"
	defaultListLimit = 1000
	maxBucketName    = 256
	anonymousUser    = "ocid1.user.oc1..anonymous"
	ocidHexLen       = 40
)

var bucketNameRe = regexp.MustCompile(`^[A-Za-z0-9._-]+$`)

// InMemoryBackend holds every bucket under one lock.
type InMemoryBackend struct {
	mu           *lockmetrics.RWMutex
	now          func() time.Time
	buckets      map[string]*Bucket
	workRequests map[string]*WorkRequest
	Auth         *ociauth.Registry
	namespace    string
	nsMeta       nsMetaWire
	sink         func(ObjectEvent)
	pending      []ObjectEvent
}

// NewInMemoryBackend creates an empty backend serving the default namespace.
func NewInMemoryBackend() *InMemoryBackend {
	return &InMemoryBackend{
		mu:           lockmetrics.New("ociobjectstorage"),
		now:          time.Now,
		buckets:      map[string]*Bucket{},
		workRequests: map[string]*WorkRequest{},
		Auth:         ociauth.NewRegistry(),
		namespace:    DefaultNamespace,
	}
}

// SetClock injects the time source used for expiry, restore and lifecycle decisions.
func (b *InMemoryBackend) SetClock(now func() time.Time) {
	b.mu.Lock("SetClock")
	b.now = now
	b.mu.Unlock()
}

// SetNamespace overrides the single tenancy namespace this backend serves.
func (b *InMemoryBackend) SetNamespace(ns string) {
	b.mu.Lock("SetNamespace")
	b.namespace = ns
	b.mu.Unlock()
}

// Namespace returns the served namespace.
func (b *InMemoryBackend) Namespace() string {
	b.mu.RLock("Namespace")
	defer b.mu.RUnlock()

	return b.namespace
}

// Reset clears all state.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	b.buckets = map[string]*Bucket{}
	b.workRequests = map[string]*WorkRequest{}
	b.mu.Unlock()
	b.Auth.Reset()
}

func (b *InMemoryBackend) nowUTC() time.Time { return b.now().UTC() }

func newEtag() string { return uuid.NewString() }

func md5Raw(data []byte) []byte {
	sum := md5.Sum(data) //nolint:gosec // see import

	return sum[:]
}

func md5b64(data []byte) string { return base64.StdEncoding.EncodeToString(md5Raw(data)) }

func (b *InMemoryBackend) checkNS(ns string) error {
	if ns != b.namespace {
		return errNamespace(ns)
	}

	return nil
}

func (b *InMemoryBackend) bucket(ns, name string) (*Bucket, error) {
	if err := b.checkNS(ns); err != nil {
		return nil, err
	}

	bk, ok := b.buckets[name]
	if !ok {
		return nil, errBucket(ns, name)
	}

	return bk, nil
}

func etagMatches(header, etag string) bool {
	for c := range strings.SplitSeq(header, ",") {
		c = strings.TrimPrefix(strings.TrimSpace(c), "W/")
		c = strings.Trim(c, `"`)

		if c == "*" || c == etag {
			return true
		}
	}

	return false
}

// Preconds are the If-Match / If-None-Match request headers.
type Preconds struct {
	IfMatch     string
	IfNoneMatch string
}

// check evaluates preconditions against a current etag ("" = resource absent).
// notModified is the 304 outcome for safe reads.
func (p Preconds) check(etag string, exists, safeRead bool) error {
	if p.IfMatch != "" && (!exists || !etagMatches(p.IfMatch, etag)) {
		return errNoEtag()
	}

	if p.IfNoneMatch != "" && exists && etagMatches(p.IfNoneMatch, etag) {
		if safeRead {
			return &APIError{Status: http.StatusNotModified, Code: "NotModified"}
		}

		return newErr(http.StatusPreconditionFailed, "IfNoneMatchFailed",
			"The If-None-Match header does not allow this operation")
	}

	return nil
}

// CreateBucketInput is the CreateBucketDetails payload.
type CreateBucketInput struct {
	Metadata            map[string]string         `json:"metadata"`
	FreeformTags        map[string]string         `json:"freeformTags"`
	DefinedTags         map[string]map[string]any `json:"definedTags"`
	ObjectEventsEnabled *bool                     `json:"objectEventsEnabled"`
	IsBucketKeyEnabled  *bool                     `json:"isBucketKeyEnabled"`
	Name                string                    `json:"name"`
	CompartmentID       string                    `json:"compartmentId"`
	PublicAccessType    string                    `json:"publicAccessType"`
	StorageTier         string                    `json:"storageTier"`
	KmsKeyID            string                    `json:"kmsKeyId"`
	Versioning          string                    `json:"versioning"`
	AutoTiering         string                    `json:"autoTiering"`
}

func validAccess(s string) bool { return s == accessNone || s == accessRead || s == accessReadNoLs }

func (in *CreateBucketInput) normalize() error {
	if in.Name == "" {
		return errMissing("name is required")
	}

	if in.CompartmentID == "" {
		return errMissing("compartmentId is required")
	}

	if len(in.Name) > maxBucketName || !bucketNameRe.MatchString(in.Name) {
		return errParam("Bucket name must be 1-256 characters of letters, digits, '-', '_' and '.'")
	}

	defaultTo(&in.PublicAccessType, accessNone)
	defaultTo(&in.StorageTier, tierStandard)
	defaultTo(&in.Versioning, versioningDisabled)

	switch {
	case !validAccess(in.PublicAccessType):
		return errParam("Invalid publicAccessType")
	case in.StorageTier != tierStandard && in.StorageTier != tierArchive:
		return errParam("storageTier must be Standard or Archive")
	case in.Versioning != versioningDisabled && in.Versioning != versioningEnabled:
		return errParam("versioning must be Enabled or Disabled on create")
	}

	return nil
}

func defaultTo(dst *string, v string) {
	if *dst == "" {
		*dst = v
	}
}

// CreateBucket creates a bucket.
func (b *InMemoryBackend) CreateBucket(ns string, in CreateBucketInput, user string) (*BucketWire, error) {
	if err := in.normalize(); err != nil {
		return nil, err
	}

	b.mu.Lock("CreateBucket")
	defer b.mu.Unlock()

	if err := b.checkNS(ns); err != nil {
		return nil, err
	}

	if _, dup := b.buckets[in.Name]; dup {
		return nil, errConflict("BucketAlreadyExists",
			"The bucket '"+in.Name+"' already exists in the namespace '"+ns+"'")
	}

	if user == "" {
		user = anonymousUser
	}

	bk := &Bucket{
		Name: in.Name, CompartmentID: in.CompartmentID, Metadata: in.Metadata, CreatedBy: user,
		TimeCreated: b.nowUTC(), Etag: newEtag(), PublicAccessType: in.PublicAccessType,
		StorageTier: in.StorageTier, Versioning: in.Versioning, AutoTiering: in.AutoTiering,
		FreeformTags: in.FreeformTags, DefinedTags: in.DefinedTags, KmsKeyID: in.KmsKeyID,
		ObjectEventsEnabled: in.ObjectEventsEnabled != nil && *in.ObjectEventsEnabled,
		IsBucketKeyEnabled:  in.IsBucketKeyEnabled != nil && *in.IsBucketKeyEnabled,
		ID:                  "ocid1.bucket.oc1." + Region + "." + awsid.NewN("", ocidHexLen),
		Objects:             map[string][]*Object{}, Uploads: map[string]*Upload{},
		PARs: map[string]*PAR{}, Retention: map[string]*Retention{},
	}
	b.buckets[bk.Name] = bk

	return b.bucketWire(ns, bk, nil), nil
}

// UpdateBucketInput is the UpdateBucketDetails payload; nil/empty fields are left unchanged.
type UpdateBucketInput struct {
	Metadata            map[string]string         `json:"metadata"`
	FreeformTags        map[string]string         `json:"freeformTags"`
	DefinedTags         map[string]map[string]any `json:"definedTags"`
	ObjectEventsEnabled *bool                     `json:"objectEventsEnabled"`
	IsBucketKeyEnabled  *bool                     `json:"isBucketKeyEnabled"`
	Name                string                    `json:"name"`
	PublicAccessType    string                    `json:"publicAccessType"`
	KmsKeyID            string                    `json:"kmsKeyId"`
	Versioning          string                    `json:"versioning"`
	AutoTiering         string                    `json:"autoTiering"`
	CompartmentID       string                    `json:"compartmentId"`
}

// UpdateBucket applies a partial update.
func (b *InMemoryBackend) UpdateBucket(ns, name string, in UpdateBucketInput, pre Preconds) (*BucketWire, error) {
	b.mu.Lock("UpdateBucket")
	defer b.mu.Unlock()

	bk, err := b.bucket(ns, name)
	if err != nil {
		return nil, err
	}

	if err = pre.check(bk.Etag, true, false); err != nil {
		return nil, err
	}

	if err = b.applyBucketUpdate(bk, in); err != nil {
		return nil, err
	}

	bk.Etag = newEtag()

	return b.bucketWire(ns, bk, nil), nil
}

func (in UpdateBucketInput) validate(bk *Bucket) error {
	if in.PublicAccessType != "" && !validAccess(in.PublicAccessType) {
		return errParam("Invalid publicAccessType")
	}

	switch in.Versioning {
	case "":
	case versioningEnabled, versioningSuspended:
		if bk.Versioning == versioningDisabled && in.Versioning == versioningSuspended {
			return errParam("Versioning cannot be suspended before it has been enabled")
		}
	default:
		return errParam("versioning may only be changed to Enabled or Suspended")
	}

	return nil
}

func (b *InMemoryBackend) applyBucketUpdate(bk *Bucket, in UpdateBucketInput) error {
	if err := in.validate(bk); err != nil {
		return err
	}

	if in.Name != "" && in.Name != bk.Name {
		if err := b.renameBucket(bk, in.Name); err != nil {
			return err
		}
	}

	setIf(&bk.PublicAccessType, in.PublicAccessType)
	setIf(&bk.Versioning, in.Versioning)
	setIf(&bk.AutoTiering, in.AutoTiering)
	setIf(&bk.KmsKeyID, in.KmsKeyID)
	setIf(&bk.CompartmentID, in.CompartmentID)

	if in.Metadata != nil {
		bk.Metadata = in.Metadata
	}

	if in.FreeformTags != nil {
		bk.FreeformTags = in.FreeformTags
	}

	if in.DefinedTags != nil {
		bk.DefinedTags = in.DefinedTags
	}

	if in.ObjectEventsEnabled != nil {
		bk.ObjectEventsEnabled = *in.ObjectEventsEnabled
	}

	if in.IsBucketKeyEnabled != nil {
		bk.IsBucketKeyEnabled = *in.IsBucketKeyEnabled
	}

	return nil
}

func (b *InMemoryBackend) renameBucket(bk *Bucket, name string) error {
	if !bucketNameRe.MatchString(name) || len(name) > maxBucketName {
		return errParam("Invalid bucket name")
	}

	if _, dup := b.buckets[name]; dup {
		return errConflict("BucketAlreadyExists", "The bucket '"+name+"' already exists")
	}

	delete(b.buckets, bk.Name)
	bk.Name = name
	b.buckets[name] = bk

	return nil
}

func setIf(dst *string, v string) {
	if v != "" {
		*dst = v
	}
}

// GetBucket returns a bucket; fields may include approximateCount, approximateSize, autoTiering.
func (b *InMemoryBackend) GetBucket(ns, name string, fields []string, pre Preconds) (*BucketWire, error) {
	b.mu.RLock("GetBucket")
	defer b.mu.RUnlock()

	bk, err := b.bucket(ns, name)
	if err != nil {
		return nil, err
	}

	if err = pre.check(bk.Etag, true, true); err != nil {
		return nil, err
	}

	return b.bucketWire(ns, bk, fields), nil
}

// DeleteBucket removes an empty bucket.
func (b *InMemoryBackend) DeleteBucket(ns, name string, pre Preconds) error {
	b.mu.Lock("DeleteBucket")
	defer b.mu.Unlock()

	bk, err := b.bucket(ns, name)
	if err != nil {
		return err
	}

	if err = pre.check(bk.Etag, true, false); err != nil {
		return err
	}

	if len(bk.Objects) > 0 || len(bk.Uploads) > 0 {
		return errConflict(
			"BucketNotEmpty",
			"Bucket named '"+name+"' is not empty. Delete all objects, versions and uploads first",
		)
	}

	delete(b.buckets, name)

	return nil
}

// ListBuckets lists buckets in a compartment.
func (b *InMemoryBackend) ListBuckets(ns, compartment, token string, limit int) (page.Page[*BucketWire], error) {
	if compartment == "" {
		return page.Page[*BucketWire]{}, errMissing("compartmentId is required")
	}

	b.mu.RLock("ListBuckets")
	defer b.mu.RUnlock()

	if err := b.checkNS(ns); err != nil {
		return page.Page[*BucketWire]{}, err
	}

	names := make([]string, 0, len(b.buckets))

	for n, bk := range b.buckets {
		if bk.CompartmentID == compartment {
			names = append(names, n)
		}
	}

	sort.Strings(names)

	all := make([]*BucketWire, len(names))
	for i, n := range names {
		all[i] = b.bucketWire(ns, b.buckets[n], nil)
	}

	return page.New(all, token, limit, defaultListLimit), nil
}

// BucketWire is the Bucket / BucketSummary JSON body.
type BucketWire struct {
	TimeCreated               time.Time                 `json:"timeCreated"`
	Metadata                  map[string]string         `json:"metadata,omitempty"`
	FreeformTags              map[string]string         `json:"freeformTags"`
	DefinedTags               map[string]map[string]any `json:"definedTags"`
	ApproximateCount          *int64                    `json:"approximateCount,omitempty"`
	ApproximateSize           *int64                    `json:"approximateSize,omitempty"`
	Namespace                 string                    `json:"namespace"`
	Name                      string                    `json:"name"`
	CompartmentID             string                    `json:"compartmentId"`
	CreatedBy                 string                    `json:"createdBy"`
	Etag                      string                    `json:"etag"`
	PublicAccessType          string                    `json:"publicAccessType"`
	StorageTier               string                    `json:"storageTier"`
	KmsKeyID                  *string                   `json:"kmsKeyId"`
	ObjectLifecyclePolicyEtag *string                   `json:"objectLifecyclePolicyEtag"`
	ID                        string                    `json:"id"`
	Versioning                string                    `json:"versioning"`
	AutoTiering               string                    `json:"autoTiering,omitempty"`
	ObjectEventsEnabled       bool                      `json:"objectEventsEnabled"`
	IsBucketKeyEnabled        bool                      `json:"isBucketKeyEnabled"`
	ReplicationEnabled        bool                      `json:"replicationEnabled"`
	IsReadOnly                bool                      `json:"isReadOnly"`
}

func (b *InMemoryBackend) bucketWire(ns string, bk *Bucket, fields []string) *BucketWire {
	w := &BucketWire{
		Namespace: ns, Name: bk.Name, CompartmentID: bk.CompartmentID, Metadata: bk.Metadata,
		CreatedBy: bk.CreatedBy, TimeCreated: bk.TimeCreated, Etag: bk.Etag,
		PublicAccessType: bk.PublicAccessType, StorageTier: bk.StorageTier, Versioning: bk.Versioning,
		ObjectEventsEnabled: bk.ObjectEventsEnabled, IsBucketKeyEnabled: bk.IsBucketKeyEnabled,
		FreeformTags: bk.FreeformTags, DefinedTags: bk.DefinedTags, ID: bk.ID, AutoTiering: bk.AutoTiering,
	}

	if w.FreeformTags == nil {
		w.FreeformTags = map[string]string{}
	}

	if w.DefinedTags == nil {
		w.DefinedTags = map[string]map[string]any{}
	}

	if bk.KmsKeyID != "" {
		w.KmsKeyID = &bk.KmsKeyID
	}

	if bk.LifecycleEtag != "" {
		w.ObjectLifecyclePolicyEtag = &bk.LifecycleEtag
	}

	if slices.Contains(fields, "approximateCount") || slices.Contains(fields, "approximateSize") {
		count, size := bucketTotals(bk)

		if slices.Contains(fields, "approximateCount") {
			w.ApproximateCount = &count
		}

		if slices.Contains(fields, "approximateSize") {
			w.ApproximateSize = &size
		}
	}

	return w
}

// NamespaceMetadata returns the namespace's default compartments.
func (b *InMemoryBackend) NamespaceMetadata(ns string) (nsMetaWire, error) {
	b.mu.RLock("NamespaceMetadata")
	defer b.mu.RUnlock()

	if err := b.checkNS(ns); err != nil {
		return nsMetaWire{}, err
	}

	m := b.nsMeta
	m.Namespace = ns

	return m, nil
}

// UpdateNamespaceMetadata sets the default S3 / Swift compartments.
func (b *InMemoryBackend) UpdateNamespaceMetadata(ns string, in nsMetaWire) (nsMetaWire, error) {
	b.mu.Lock("UpdateNamespaceMetadata")
	defer b.mu.Unlock()

	if err := b.checkNS(ns); err != nil {
		return nsMetaWire{}, err
	}

	setIf(&b.nsMeta.DefaultS3CompartmentID, in.DefaultS3CompartmentID)
	setIf(&b.nsMeta.DefaultSwiftCompartmentID, in.DefaultSwiftCompartmentID)

	m := b.nsMeta
	m.Namespace = ns

	return m, nil
}

func bucketTotals(bk *Bucket) (int64, int64) {
	var count, size int64

	for _, vs := range bk.Objects {
		for _, o := range vs {
			if !o.IsDeleteMarker {
				count++
				size += int64(len(o.Data))
			}
		}
	}

	return count, size
}

package gcpstorage

import (
	"crypto/md5" //nolint:gosec // GCS md5Hash is an integrity checksum defined by the API
	"encoding/base64"
	"encoding/binary"
	"hash/crc32"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

const (
	initialIAMEtag     = "CAE="
	maxObjectNameBytes = 1024
	defaultMaxResults  = 1000
	minBucketNameLen   = 3
	maxBucketNameLen   = 63
	maxDottedNameLen   = 222
	maxLabelCount      = 64
)

// InMemoryBackend holds all buckets, objects and resumable upload sessions
// under one lock.
type InMemoryBackend struct {
	mu        *lockmetrics.RWMutex
	buckets   map[string]*Bucket
	uploads   map[string]*uploadSession
	observers []ObjectObserver
	pending   []ObjectEvent
	lastGen   int64
	iamSeq    uint64
}

// NewInMemoryBackend creates an empty backend.
func NewInMemoryBackend() *InMemoryBackend {
	return &InMemoryBackend{
		mu:      lockmetrics.New("gcpstorage"),
		buckets: make(map[string]*Bucket),
		uploads: make(map[string]*uploadSession),
	}
}

// Reset clears all state.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	b.buckets = make(map[string]*Bucket)
	b.uploads = make(map[string]*uploadSession)
	b.lastGen = 0
}

func (b *InMemoryBackend) nextGen() int64 {
	g := time.Now().UnixMicro()
	if g <= b.lastGen {
		g = b.lastGen + 1
	}

	b.lastGen = g

	return g
}

func validBucketName(name string) bool {
	if len(name) < minBucketNameLen || len(name) > maxDottedNameLen {
		return false
	}

	if len(name) > maxBucketNameLen && !strings.Contains(name, ".") {
		return false
	}

	if strings.HasPrefix(name, "goog") || strings.Contains(name, "google") {
		return false
	}

	for i, r := range name {
		if !validBucketRune(r, i == 0 || i == len(name)-1) {
			return false
		}
	}

	return !looksLikeIP(name)
}

func validBucketRune(r rune, edge bool) bool {
	if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') {
		return true
	}

	return !edge && (r == '-' || r == '_' || r == '.')
}

func looksLikeIP(name string) bool {
	parts := strings.Split(name, ".")
	if len(parts) != 4 { //nolint:mnd // dotted quad
		return false
	}

	for _, p := range parts {
		if _, err := strconv.ParseUint(p, 10, 8); err != nil {
			return false
		}
	}

	return true
}

func validObjectName(name string) bool {
	if name == "" || len(name) > maxObjectNameBytes || name == "." || name == ".." {
		return false
	}

	return !strings.ContainsAny(name, "\r\n")
}

func isMultiRegion(loc string) bool {
	switch loc {
	case "US", "EU", "ASIA":
		return true
	default:
		return false
	}
}

// LocationType returns the GCS locationType of a bucket location.
func LocationType(loc string) string {
	switch {
	case isMultiRegion(loc):
		return "multi-region"
	case loc == "EUR4" || loc == "NAM4" || loc == "ASIA1":
		return "dual-region"
	default:
		return "region"
	}
}

// CreateBucket creates a bucket in project from the request body p.
func (b *InMemoryBackend) CreateBucket(project string, p Patch) (*Bucket, error) {
	name := p.str("name")
	if !validBucketName(name) {
		return nil, errInvalid("Invalid bucket name: '" + name + "'")
	}

	loc := strings.ToUpper(p.str("location"))
	if loc == "" {
		loc = defaultLocation
	}

	now := time.Now().UTC()
	bk := &Bucket{
		Name: name, Project: project, Location: loc, StorageClass: defaultStorageClass,
		Created: now, Updated: now, Metageneration: 1,
		Objects: map[string][]*Object{},
		IAM:     IAMPolicy{Etag: initialIAMEtag, Version: 1, Bindings: []IAMBinding{}},
	}

	if apiErr := applyBucket(bk, p); apiErr != nil {
		return nil, apiErr
	}

	if len(bk.Labels) > maxLabelCount {
		return nil, errInvalid("Too many labels.")
	}

	b.mu.Lock("CreateBucket")
	defer b.mu.Unlock()

	if _, exists := b.buckets[name]; exists {
		return nil, errConflict("Your previous request to create the named bucket succeeded and you already own it.")
	}

	b.buckets[name] = bk

	return bk.clone(), nil
}

// GetBucket returns the bucket metadata.
func (b *InMemoryBackend) GetBucket(name string, pre Preconds) (*Bucket, error) {
	b.mu.RLock("GetBucket")
	defer b.mu.RUnlock()

	bk, err := b.bucketLocked(name)
	if err != nil {
		return nil, err
	}

	if apiErr := pre.checkBucket(bk); apiErr != nil {
		return nil, apiErr
	}

	return bk.clone(), nil
}

func (b *InMemoryBackend) bucketLocked(name string) (*Bucket, error) {
	bk, ok := b.buckets[name]
	if !ok {
		return nil, errNotFound("The specified bucket does not exist.")
	}

	return bk, nil
}

// ListBuckets lists the project's buckets whose names start with prefix.
func (b *InMemoryBackend) ListBuckets(project, prefix, token string, limit int) page.Page[*Bucket] {
	b.mu.RLock("ListBuckets")
	defer b.mu.RUnlock()

	var all []*Bucket

	for name, bk := range b.buckets {
		if bk.Project == project && strings.HasPrefix(name, prefix) {
			all = append(all, bk.clone())
		}
	}

	slices.SortFunc(all, func(x, y *Bucket) int { return strings.Compare(x.Name, y.Name) })

	return page.New(all, token, limit, defaultMaxResults)
}

// UpdateBucket applies a PATCH (replace=false) or PUT (replace=true) body.
func (b *InMemoryBackend) UpdateBucket(name string, p Patch, replace bool, pre Preconds) (*Bucket, error) {
	b.mu.Lock("UpdateBucket")
	defer b.mu.Unlock()

	bk, err := b.bucketLocked(name)
	if err != nil {
		return nil, err
	}

	if apiErr := pre.checkBucket(bk); apiErr != nil {
		return nil, apiErr
	}

	next := bk.clone()
	next.Objects = bk.Objects

	if replace {
		next.resetMutable()
	}

	if apiErr := applyBucket(next, p); apiErr != nil {
		return nil, apiErr
	}

	next.Metageneration++
	next.Updated = time.Now().UTC()
	b.buckets[name] = next

	return next.clone(), nil
}

// DeleteBucket deletes an empty bucket.
func (b *InMemoryBackend) DeleteBucket(name string, pre Preconds) error {
	b.mu.Lock("DeleteBucket")
	defer b.mu.Unlock()

	bk, err := b.bucketLocked(name)
	if err != nil {
		return err
	}

	if apiErr := pre.checkBucket(bk); apiErr != nil {
		return apiErr
	}

	if !bk.empty() {
		return errConflict("The bucket you tried to delete was not empty.")
	}

	delete(b.buckets, name)

	return nil
}

// GetIAMPolicy returns the bucket IAM policy.
func (b *InMemoryBackend) GetIAMPolicy(name string) (IAMPolicy, error) {
	b.mu.RLock("GetIAMPolicy")
	defer b.mu.RUnlock()

	bk, err := b.bucketLocked(name)
	if err != nil {
		return IAMPolicy{}, err
	}

	pol := bk.IAM
	pol.Bindings = slices.Clone(pol.Bindings)

	return pol, nil
}

// SetIAMPolicy replaces the bucket IAM policy; a non-empty etag must match.
func (b *InMemoryBackend) SetIAMPolicy(name string, pol IAMPolicy) (IAMPolicy, error) {
	b.mu.Lock("SetIAMPolicy")
	defer b.mu.Unlock()

	bk, err := b.bucketLocked(name)
	if err != nil {
		return IAMPolicy{}, err
	}

	if pol.Etag != "" && pol.Etag != bk.IAM.Etag {
		return IAMPolicy{}, errPrecondition()
	}

	for _, bind := range pol.Bindings {
		if bind.Role == "" || len(bind.Members) == 0 {
			return IAMPolicy{}, errInvalid("Invalid IAM binding: role and members are required.")
		}
	}

	b.iamSeq++

	var seq [8]byte

	binary.BigEndian.PutUint64(seq[:], b.iamSeq)

	if pol.Bindings == nil {
		pol.Bindings = []IAMBinding{}
	}

	if pol.Version == 0 {
		pol.Version = 1
	}

	pol.Etag = base64.StdEncoding.EncodeToString(seq[:])
	bk.IAM = pol
	bk.Updated = time.Now().UTC()

	out := pol
	out.Bindings = slices.Clone(pol.Bindings)

	return out, nil
}

func crc32cOf(data []byte) string {
	var buf [4]byte

	binary.BigEndian.PutUint32(buf[:], crc32.Checksum(data, crc32.MakeTable(crc32.Castagnoli)))

	return base64.StdEncoding.EncodeToString(buf[:])
}

func md5Of(data []byte) string {
	sum := md5.Sum(data) //nolint:gosec // integrity checksum required by the GCS API

	return base64.StdEncoding.EncodeToString(sum[:])
}

// newObject builds an object version from data and the request metadata p.
func newObject(bk *Bucket, name string, data []byte, p Patch, gen int64) (*Object, *APIError) {
	if !validObjectName(name) {
		return nil, errInvalid("Invalid object name: '" + name + "'")
	}

	now := time.Now().UTC()
	o := &Object{
		Bucket: bk.Name, Name: name, Data: data, Generation: gen, Metageneration: 1,
		Created: now, Updated: now, StorageClass: bk.StorageClass,
		EventBasedHold: bk.DefaultEventBasedHold,
	}

	if apiErr := applyObject(o, p); apiErr != nil {
		return nil, apiErr
	}

	if o.ContentType == "" {
		o.ContentType = defaultContentType
	}

	o.CRC32C, o.MD5 = crc32cOf(data), md5Of(data)

	if want := p.str("crc32c"); want != "" && want != o.CRC32C {
		return nil, errInvalid("Provided CRC32C \"" + want + "\" doesn't match calculated CRC32C \"" + o.CRC32C + "\".")
	}

	if want := p.str("md5Hash"); want != "" && want != o.MD5 {
		return nil, errInvalid(
			"Provided MD5 hash \"" + want + "\" doesn't match calculated MD5 hash \"" + o.MD5 + "\".",
		)
	}

	return o, nil
}

// store makes o the live version of its name. A versioned bucket archives the
// previous live version; otherwise it is dropped.
func (bk *Bucket) store(o *Object) {
	if prev := bk.live(o.Name); prev != nil {
		if bk.Versioning {
			prev.Deleted = o.Created
		} else {
			bk.Objects[o.Name] = bk.Objects[o.Name][:len(bk.Objects[o.Name])-1]
		}
	}

	bk.Objects[o.Name] = append(bk.Objects[o.Name], o)
}

// PutObject creates or overwrites an object.
func (b *InMemoryBackend) PutObject(bucket, name string, data []byte, p Patch, pre Preconds) (*Object, error) {
	defer b.flushEvents()

	b.mu.Lock("PutObject")
	defer b.mu.Unlock()

	return b.putLocked(bucket, name, data, p, pre)
}

func (b *InMemoryBackend) putLocked(bucket, name string, data []byte, p Patch, pre Preconds) (*Object, error) {
	bk, err := b.bucketLocked(bucket)
	if err != nil {
		return nil, err
	}

	if apiErr := pre.checkObject(bk.live(name), false); apiErr != nil {
		return nil, apiErr
	}

	o, apiErr := newObject(bk, name, data, p, b.nextGen())
	if apiErr != nil {
		return nil, apiErr
	}

	b.replacedLocked(bk, bk.live(name))
	bk.store(o)
	b.queueLocked(ObjectFinalized, o)

	return o.clone(), nil
}

func objectNotFound(bucket, name string) *APIError {
	return errNotFound("No such object: " + bucket + "/" + name)
}

// GetObject returns generation gen of an object, or the live one when gen is 0.
func (b *InMemoryBackend) GetObject(bucket, name string, gen int64, pre Preconds) (*Object, error) {
	b.mu.RLock("GetObject")
	defer b.mu.RUnlock()

	bk, err := b.bucketLocked(bucket)
	if err != nil {
		return nil, err
	}

	o := bk.version(name, gen)
	if o == nil {
		return nil, objectNotFound(bucket, name)
	}

	if apiErr := pre.checkObject(o, true); apiErr != nil {
		return nil, apiErr
	}

	return o.clone(), nil
}

// UpdateObject applies a PATCH (replace=false) or PUT (replace=true) body.
func (b *InMemoryBackend) UpdateObject(
	bucket, name string, gen int64, p Patch, replace bool, pre Preconds,
) (*Object, error) {
	defer b.flushEvents()

	b.mu.Lock("UpdateObject")
	defer b.mu.Unlock()

	bk, err := b.bucketLocked(bucket)
	if err != nil {
		return nil, err
	}

	cur := bk.version(name, gen)
	if cur == nil {
		return nil, objectNotFound(bucket, name)
	}

	if apiErr := pre.checkObject(cur, false); apiErr != nil {
		return nil, apiErr
	}

	next := cur.clone()
	if replace {
		next.resetMutable()
	}

	if apiErr := applyObject(next, p); apiErr != nil {
		return nil, apiErr
	}

	if next.ContentType == "" {
		next.ContentType = defaultContentType
	}

	next.Metageneration++
	next.Updated = time.Now().UTC()
	*cur = *next
	b.queueLocked(ObjectMetadataUpdated, cur)

	return next.clone(), nil
}

// DeleteObject deletes the live version (archiving it in a versioned bucket)
// or, with gen set, permanently removes that generation.
func (b *InMemoryBackend) DeleteObject(bucket, name string, gen int64, pre Preconds) error {
	defer b.flushEvents()

	b.mu.Lock("DeleteObject")
	defer b.mu.Unlock()

	bk, err := b.bucketLocked(bucket)
	if err != nil {
		return err
	}

	cur := bk.version(name, gen)
	if cur == nil {
		return objectNotFound(bucket, name)
	}

	if apiErr := pre.checkObject(cur, false); apiErr != nil {
		return apiErr
	}

	if gen == 0 && bk.Versioning {
		cur.Deleted = time.Now().UTC()
		b.queueLocked(ObjectArchived, cur)

		return nil
	}

	b.queueLocked(ObjectDeleted, cur)
	bk.Objects[name] = slices.DeleteFunc(bk.Objects[name], func(o *Object) bool { return o == cur })
	if len(bk.Objects[name]) == 0 {
		delete(bk.Objects, name)
	}

	return nil
}

// ListOptions are the object list parameters.
type ListOptions struct {
	Prefix                   string
	Delimiter                string
	StartOffset              string
	EndOffset                string
	PageToken                string
	MaxResults               int
	Versions                 bool
	IncludeTrailingDelimiter bool
}

// ListResult is one page of objects and collapsed prefixes.
type ListResult struct {
	Next     string
	Items    []*Object
	Prefixes []string
}

type listEntry struct {
	obj    *Object
	prefix string
}

// ListObjects lists objects in name order, collapsing delimited prefixes.
func (b *InMemoryBackend) ListObjects(bucket string, opts ListOptions) (ListResult, error) {
	b.mu.RLock("ListObjects")
	defer b.mu.RUnlock()

	bk, err := b.bucketLocked(bucket)
	if err != nil {
		return ListResult{}, err
	}

	pg := page.New(listEntries(bk, opts), opts.PageToken, opts.MaxResults, defaultMaxResults)
	res := ListResult{Next: pg.Next}

	for _, e := range pg.Data {
		if e.obj != nil {
			res.Items = append(res.Items, e.obj)
		} else {
			res.Prefixes = append(res.Prefixes, e.prefix)
		}
	}

	return res, nil
}

func matchingNames(bk *Bucket, opts ListOptions) []string {
	names := make([]string, 0, len(bk.Objects))

	for n := range bk.Objects {
		if strings.HasPrefix(n, opts.Prefix) && n >= opts.StartOffset && (opts.EndOffset == "" || n < opts.EndOffset) {
			names = append(names, n)
		}
	}

	sort.Strings(names)

	return names
}

func listEntries(bk *Bucket, opts ListOptions) []listEntry {
	names := matchingNames(bk, opts)

	var entries []listEntry

	seen := map[string]bool{}

	for _, n := range names {
		if cp := collapsedPrefix(n, opts); cp != "" {
			if !seen[cp] {
				seen[cp] = true

				entries = append(entries, listEntry{prefix: cp})
			}

			if cp != n || !opts.IncludeTrailingDelimiter {
				continue
			}
		}

		for _, o := range bk.Objects[n] {
			if opts.Versions || o.Deleted.IsZero() {
				entries = append(entries, listEntry{obj: o.clone()})
			}
		}
	}

	return entries
}

func collapsedPrefix(name string, opts ListOptions) string {
	if opts.Delimiter == "" {
		return ""
	}

	rest := name[len(opts.Prefix):]

	i := strings.Index(rest, opts.Delimiter)
	if i < 0 {
		return ""
	}

	return opts.Prefix + rest[:i+len(opts.Delimiter)]
}

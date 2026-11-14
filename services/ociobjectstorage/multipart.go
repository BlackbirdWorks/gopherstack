package ociobjectstorage

import (
	"encoding/base64"
	"net/http"
	"slices"
	"sort"
	"strconv"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

const maxPartNumber = 10000

// CreateUploadInput is the CreateMultipartUploadDetails payload.
type CreateUploadInput struct {
	Metadata           map[string]string `json:"metadata"`
	Object             string            `json:"object"`
	ContentType        string            `json:"contentType"`
	ContentLanguage    string            `json:"contentLanguage"`
	ContentEncoding    string            `json:"contentEncoding"`
	ContentDisposition string            `json:"contentDisposition"`
	CacheControl       string            `json:"cacheControl"`
	StorageTier        string            `json:"storageTier"`
}

// UploadOptions are the opc-checksum-algorithm and encryption headers of CreateMultipartUpload.
type UploadOptions struct {
	Algorithm string
	SSE       SSEReq
}

// CreateUpload starts a multipart upload.
func (b *InMemoryBackend) CreateUpload(ns, bucket string, in CreateUploadInput, pre Preconds) (*Upload, error) {
	return b.CreateUploadWith(ns, bucket, in, pre, UploadOptions{})
}

// CreateUploadWith starts a multipart upload with a checksum algorithm and encryption selection.
func (b *InMemoryBackend) CreateUploadWith(
	ns, bucket string, in CreateUploadInput, pre Preconds, opt UploadOptions,
) (*Upload, error) {
	if in.Object == "" || len(in.Object) > maxObjectName {
		return nil, errMissing("object is required")
	}

	alg, err := normalizeAlg(opt.Algorithm)
	if err != nil {
		return nil, err
	}

	b.mu.Lock("CreateUpload")
	defer b.mu.Unlock()

	bk, err := b.bucket(ns, bucket)
	if err != nil {
		return nil, err
	}

	if err = writable(bk); err != nil {
		return nil, err
	}

	hash, err := opt.SSE.resolve(b.checkKMSKey)
	if err != nil {
		return nil, err
	}

	cur := live(bk, in.Object)
	etag := ""

	if cur != nil {
		etag = cur.Etag
	}

	if err = pre.check(etag, cur != nil, false); err != nil {
		return nil, err
	}

	u := &Upload{
		ID: uuid.NewString(), Object: in.Object, TimeCreated: b.nowUTC(), Parts: map[int]Part{},
		ChecksumAlg: alg, SseKeySha: hash, KmsKeyID: opt.SSE.KmsKeyID,
		Meta: ObjectMeta{
			Metadata: in.Metadata, ContentType: in.ContentType, ContentLanguage: in.ContentLanguage,
			ContentEncoding: in.ContentEncoding, ContentDisposition: in.ContentDisposition,
			CacheControl: in.CacheControl, StorageTier: in.StorageTier,
		},
	}
	bk.Uploads[u.ID] = u
	cp := *u

	return &cp, nil
}

func (b *InMemoryBackend) upload(bk *Bucket, object, id string) (*Upload, error) {
	u, ok := bk.Uploads[id]
	if !ok || u.Object != object {
		return nil, newErr(
			http.StatusNotFound,
			"NoSuchUpload",
			"The upload '"+id+"' does not exist for object '"+object+"'",
		)
	}

	return u, nil
}

// PartOptions are the optional integrity and encryption inputs of UploadPart.
type PartOptions struct {
	ContentMD5 string
	Checksum   ChecksumReq
	Key        SSEKey
}

// PartResult is what UploadPart reports back.
type PartResult struct {
	Etag     string
	MD5      string
	Checksum Checksum
}

// UploadPart stores one part and returns its etag and md5.
func (b *InMemoryBackend) UploadPart(
	ns, bucket, object, id string, num int, data []byte, contentMD5 string,
) (string, string, error) {
	r, err := b.UploadPartWith(ns, bucket, object, id, num, data, PartOptions{ContentMD5: contentMD5})
	if err != nil {
		return "", "", err
	}

	return r.Etag, r.MD5, nil
}

// UploadPartWith stores one part, replacing any earlier part with the same number.
func (b *InMemoryBackend) UploadPartWith(
	ns, bucket, object, id string, num int, data []byte, opt PartOptions,
) (PartResult, error) {
	if num < 1 || num > maxPartNumber {
		return PartResult{}, errParam("uploadPartNum must be between 1 and 10000")
	}

	sum := md5b64(data)
	if opt.ContentMD5 != "" && opt.ContentMD5 != sum {
		return PartResult{}, errParam("The Content-MD5 you specified did not match what we received")
	}

	alg, err := normalizeAlg(opt.Checksum.Algorithm)
	if err != nil {
		return PartResult{}, err
	}

	opt.Checksum.Algorithm = alg

	b.mu.Lock("UploadPart")
	defer b.mu.Unlock()

	bk, err := b.bucket(ns, bucket)
	if err != nil {
		return PartResult{}, err
	}

	u, err := b.upload(bk, object, id)
	if err != nil {
		return PartResult{}, err
	}

	if err = requireKey(u.SseKeySha, opt.Key); err != nil {
		return PartResult{}, err
	}

	if alg == "" {
		opt.Checksum.Algorithm = u.ChecksumAlg
	}

	ck, err := opt.Checksum.forObject(data)
	if err != nil {
		return PartResult{}, err
	}

	etag := newEtag()
	u.Parts[num] = Part{Etag: etag, MD5: sum, Data: data, ChecksumAlg: ck.Algorithm, Checksum: ck.Content}

	return PartResult{Etag: etag, MD5: sum, Checksum: ck}, nil
}

// CommitPart names a part to include in the final object.
type CommitPart struct {
	Etag    string `json:"etag"`
	PartNum int    `json:"partNum"`
}

// CommitInput is the CommitMultipartUploadDetails payload.
type CommitInput struct {
	PartsToCommit  []CommitPart `json:"partsToCommit"`
	PartsToExclude []int        `json:"partsToExclude"`
}

// CommitUpload assembles the parts into an object.
func (b *InMemoryBackend) CommitUpload(
	ns, bucket, object, id string, in CommitInput, pre Preconds,
) (ObjectInfo, error) {
	if len(in.PartsToCommit) == 0 {
		return ObjectInfo{}, errMissing("partsToCommit must list at least one part")
	}

	defer b.flushEvents()

	b.mu.Lock("CommitUpload")
	defer b.mu.Unlock()

	bk, err := b.bucket(ns, bucket)
	if err != nil {
		return ObjectInfo{}, err
	}

	if err = writable(bk); err != nil {
		return ObjectInfo{}, err
	}

	u, err := b.upload(bk, object, id)
	if err != nil {
		return ObjectInfo{}, err
	}

	if err = checkPartCoverage(u, in); err != nil {
		return ObjectInfo{}, err
	}

	used, data, digest, err := b.collectParts(u, in.PartsToCommit)
	if err != nil {
		return ObjectInfo{}, err
	}

	cur := live(bk, object)
	etag := ""

	if cur != nil {
		etag = cur.Etag
	}

	if err = pre.check(etag, cur != nil, false); err != nil {
		return ObjectInfo{}, err
	}

	mp := base64.StdEncoding.EncodeToString(md5Raw(digest)) + "-" + strconv.Itoa(len(used))

	spec := writeSpec{
		MD5: md5b64(data), MultipartMD5: mp, Checksum: assembleChecksum(used[0].ChecksumAlg, used, data),
		SseHash: u.SseKeySha, Kms: u.KmsKeyID,
	}

	o, err := b.storeLocked(bk, object, data, u.Meta, spec)
	if err != nil {
		return ObjectInfo{}, err
	}

	delete(bk.Uploads, id)
	b.recordEvent(bk, writeEventType(cur != nil), object, o.Etag)
	b.replicate(bk, object, false)

	return b.info(o), nil
}

// AbortUpload discards an upload.
func (b *InMemoryBackend) AbortUpload(ns, bucket, object, id string) error {
	b.mu.Lock("AbortUpload")
	defer b.mu.Unlock()

	bk, err := b.bucket(ns, bucket)
	if err != nil {
		return err
	}

	if _, err = b.upload(bk, object, id); err != nil {
		return err
	}

	delete(bk.Uploads, id)

	return nil
}

// PartSummary is a ListMultipartUploadParts item.
type PartSummary struct {
	Etag       string `json:"etag"`
	MD5        string `json:"md5"`
	Size       int64  `json:"size"`
	PartNumber int    `json:"partNumber"`
}

// ListParts lists uploaded parts in part-number order.
func (b *InMemoryBackend) ListParts(ns, bucket, object, id, token string, limit int) (page.Page[PartSummary], error) {
	b.mu.RLock("ListParts")
	defer b.mu.RUnlock()

	bk, err := b.bucket(ns, bucket)
	if err != nil {
		return page.Page[PartSummary]{}, err
	}

	u, err := b.upload(bk, object, id)
	if err != nil {
		return page.Page[PartSummary]{}, err
	}

	nums := make([]int, 0, len(u.Parts))
	for n := range u.Parts {
		nums = append(nums, n)
	}

	sort.Ints(nums)

	all := make([]PartSummary, len(nums))
	for i, n := range nums {
		p := u.Parts[n]
		all[i] = PartSummary{Etag: p.Etag, MD5: p.MD5, Size: int64(len(p.Data)), PartNumber: n}
	}

	return page.New(all, token, limit, defaultListLimit), nil
}

// UploadWire is the MultipartUpload JSON body.
type UploadWire struct {
	TimeCreated time.Time `json:"timeCreated"`
	Namespace   string    `json:"namespace"`
	Bucket      string    `json:"bucket"`
	Object      string    `json:"object"`
	UploadID    string    `json:"uploadId"`
	StorageTier string    `json:"storageTier,omitempty"`
}

func uploadWire(ns, bucket string, u *Upload) UploadWire {
	return UploadWire{
		Namespace: ns, Bucket: bucket, Object: u.Object, UploadID: u.ID,
		TimeCreated: u.TimeCreated, StorageTier: u.Meta.StorageTier,
	}
}

// ListUploads lists in-flight uploads ordered by object name then id.
func (b *InMemoryBackend) ListUploads(ns, bucket, token string, limit int) (page.Page[UploadWire], error) {
	b.mu.RLock("ListUploads")
	defer b.mu.RUnlock()

	bk, err := b.bucket(ns, bucket)
	if err != nil {
		return page.Page[UploadWire]{}, err
	}

	us := make([]*Upload, 0, len(bk.Uploads))
	for _, u := range bk.Uploads {
		us = append(us, u)
	}

	sort.Slice(us, func(i, j int) bool {
		if us[i].Object != us[j].Object {
			return us[i].Object < us[j].Object
		}

		return us[i].ID < us[j].ID
	})

	all := make([]UploadWire, len(us))
	for i, u := range us {
		all[i] = uploadWire(ns, bucket, u)
	}

	return page.New(all, token, limit, defaultListLimit), nil
}

// UploadInfo returns the wire form of a freshly created upload.
func UploadInfo(ns, bucket string, u *Upload) UploadWire { return uploadWire(ns, bucket, u) }

// collectParts validates the committed parts in part-number order and returns them with the assembled data
// and the concatenated part md5 digests.
func (b *InMemoryBackend) collectParts(u *Upload, commit []CommitPart) ([]Part, []byte, []byte, error) {
	parts := slices.Clone(commit)
	sort.Slice(parts, func(i, j int) bool { return parts[i].PartNum < parts[j].PartNum })

	var (
		data, digest []byte
		used         = make([]Part, 0, len(parts))
	)

	for i, p := range parts {
		stored, ok := u.Parts[p.PartNum]
		if !ok || (i > 0 && parts[i-1].PartNum == p.PartNum) {
			return nil, nil, nil, partErr("Part " + strconv.Itoa(p.PartNum) + " is missing or listed twice")
		}

		if !etagMatches(p.Etag, stored.Etag) {
			return nil, nil, nil, partErr("ETag for part " + strconv.Itoa(p.PartNum) + " does not match")
		}

		if i < len(parts)-1 && int64(len(stored.Data)) < b.minPartSize {
			return nil, nil, nil, partErr(
				"Part " + strconv.Itoa(p.PartNum) + " is smaller than the minimum part size of " +
					strconv.FormatInt(b.minPartSize, 10) + " bytes; only the last part may be smaller",
			)
		}

		if i > 0 && stored.ChecksumAlg != used[0].ChecksumAlg {
			return nil, nil, nil, errParam("All parts must be uploaded with the same opc-checksum-algorithm")
		}

		raw, _ := base64.StdEncoding.DecodeString(stored.MD5)
		digest = append(digest, raw...)
		data = append(data, stored.Data...)
		used = append(used, stored)
	}

	return used, data, digest, nil
}

// checkPartCoverage enforces that every uploaded part is in exactly one of partsToCommit and partsToExclude
// (CommitMultipartUploadDetails.PartsToExclude, oci-go-sdk objectstorage).
func checkPartCoverage(u *Upload, in CommitInput) error {
	seen := make(map[int]bool, len(u.Parts))

	for _, p := range in.PartsToCommit {
		seen[p.PartNum] = true
	}

	for _, n := range in.PartsToExclude {
		if _, ok := u.Parts[n]; !ok {
			return partErr("Excluded part " + strconv.Itoa(n) + " was not uploaded")
		}

		if seen[n] {
			return partErr("Part " + strconv.Itoa(n) + " is in both partsToCommit and partsToExclude")
		}

		seen[n] = true
	}

	for n := range u.Parts {
		if !seen[n] {
			return partErr("Part " + strconv.Itoa(n) + " must be in partsToCommit or partsToExclude")
		}
	}

	return nil
}

func partErr(msg string) *APIError { return newErr(http.StatusBadRequest, "InvalidUploadPart", msg) }

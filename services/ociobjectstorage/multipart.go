package ociobjectstorage

import (
	"encoding/base64"
	"net/http"
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

// CreateUpload starts a multipart upload.
func (b *InMemoryBackend) CreateUpload(ns, bucket string, in CreateUploadInput, pre Preconds) (*Upload, error) {
	if in.Object == "" || len(in.Object) > maxObjectName {
		return nil, errMissing("object is required")
	}

	b.mu.Lock("CreateUpload")
	defer b.mu.Unlock()

	bk, err := b.bucket(ns, bucket)
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

// UploadPart stores one part and returns its etag and md5.
func (b *InMemoryBackend) UploadPart(
	ns, bucket, object, id string, num int, data []byte, contentMD5 string,
) (string, string, error) {
	if num < 1 || num > maxPartNumber {
		return "", "", errParam("uploadPartNum must be between 1 and 10000")
	}

	sum := md5b64(data)
	if contentMD5 != "" && contentMD5 != sum {
		return "", "", errParam("The Content-MD5 you specified did not match what we received")
	}

	b.mu.Lock("UploadPart")
	defer b.mu.Unlock()

	bk, err := b.bucket(ns, bucket)
	if err != nil {
		return "", "", err
	}

	u, err := b.upload(bk, object, id)
	if err != nil {
		return "", "", err
	}

	etag := newEtag()
	u.Parts[num] = Part{Etag: etag, MD5: sum, Data: data}

	return etag, sum, nil
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

	u, err := b.upload(bk, object, id)
	if err != nil {
		return ObjectInfo{}, err
	}

	parts := append([]CommitPart(nil), in.PartsToCommit...)
	sort.Slice(parts, func(i, j int) bool { return parts[i].PartNum < parts[j].PartNum })

	var (
		data   []byte
		digest []byte
	)

	for i, p := range parts {
		stored, ok := u.Parts[p.PartNum]
		if !ok || (i > 0 && parts[i-1].PartNum == p.PartNum) {
			return ObjectInfo{}, newErr(
				http.StatusBadRequest,
				"InvalidUploadPart",
				"Part "+strconv.Itoa(p.PartNum)+" is missing or listed twice",
			)
		}

		if !etagMatches(p.Etag, stored.Etag) {
			return ObjectInfo{}, newErr(
				http.StatusBadRequest,
				"InvalidUploadPart",
				"ETag for part "+strconv.Itoa(p.PartNum)+" does not match",
			)
		}

		raw, _ := base64.StdEncoding.DecodeString(stored.MD5)
		digest = append(digest, raw...)
		data = append(data, stored.Data...)
	}

	cur := live(bk, object)
	etag := ""

	if cur != nil {
		etag = cur.Etag
	}

	if err = pre.check(etag, cur != nil, false); err != nil {
		return ObjectInfo{}, err
	}

	mp := base64.StdEncoding.EncodeToString(md5Raw(digest)) + "-" + strconv.Itoa(len(parts))

	o, err := b.storeLocked(bk, object, data, u.Meta, md5b64(data), mp)
	if err != nil {
		return ObjectInfo{}, err
	}

	delete(bk.Uploads, id)
	b.recordEvent(bk, writeEventType(cur != nil), object, o.Etag)

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

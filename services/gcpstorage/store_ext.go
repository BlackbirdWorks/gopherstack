package gcpstorage

import (
	"maps"
	"slices"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/awsid"
)

const (
	maxComposeSources = 32
	uploadSessionTTL  = 7 * 24 * time.Hour
	uploadIDLen       = 32
)

// CopyRequest describes a copy or rewrite.
type CopyRequest struct {
	DstPre        Preconds
	SrcPre        Preconds
	Meta          Patch
	SrcBucket     string
	SrcName       string
	DstBucket     string
	DstName       string
	SrcGeneration int64
}

// CopyObject copies an object. A nil Meta inherits the source metadata;
// otherwise Meta supplies the destination metadata.
func (b *InMemoryBackend) CopyObject(req CopyRequest) (*Object, error) {
	b.mu.Lock("CopyObject")
	defer b.mu.Unlock()

	srcBk, err := b.bucketLocked(req.SrcBucket)
	if err != nil {
		return nil, err
	}

	src := srcBk.version(req.SrcName, req.SrcGeneration)
	if src == nil {
		return nil, objectNotFound(req.SrcBucket, req.SrcName)
	}

	if apiErr := req.SrcPre.checkObject(src, false); apiErr != nil {
		return nil, apiErr
	}

	dstBk, err := b.bucketLocked(req.DstBucket)
	if err != nil {
		return nil, err
	}

	if apiErr := req.DstPre.checkObject(dstBk.live(req.DstName), false); apiErr != nil {
		return nil, apiErr
	}

	meta := req.Meta
	if meta == nil {
		meta = inheritMeta(src)
	} else if _, ok := meta[keyContentType]; !ok {
		meta = maps.Clone(meta)
		meta[keyContentType] = marshalRaw(src.ContentType)
	}

	o, apiErr := newObject(dstBk, req.DstName, src.Data, meta, b.nextGen())
	if apiErr != nil {
		return nil, apiErr
	}

	if src.ComponentCount > 0 {
		o.ComponentCount, o.MD5 = src.ComponentCount, ""
	}

	dstBk.store(o)

	return o.clone(), nil
}

func inheritMeta(src *Object) Patch {
	p := Patch{}
	set := func(k, v string) {
		if v != "" {
			p[k] = marshalRaw(v)
		}
	}

	set(keyContentType, src.ContentType)
	set(keyContentEncoding, src.ContentEncoding)
	set(keyContentDisposition, src.ContentDisposition)
	set(keyContentLanguage, src.ContentLanguage)
	set(keyCacheControl, src.CacheControl)

	if len(src.Metadata) > 0 {
		p[keyMetadata] = marshalRaw(src.Metadata)
	}

	return p
}

// ComposeSource is one source object of a compose request.
type ComposeSource struct {
	Pre        Preconds
	Name       string
	Generation int64
}

// ComposeObject concatenates sources (same bucket) into dst.
func (b *InMemoryBackend) ComposeObject(
	bucket, dst string, sources []ComposeSource, meta Patch, pre Preconds,
) (*Object, error) {
	if len(sources) == 0 {
		return nil, errInvalid("You must provide at least one source component.")
	}

	if len(sources) > maxComposeSources {
		return nil, errInvalid("The number of source components provided (" + itoa(len(sources)) +
			") exceeds the maximum (32)")
	}

	b.mu.Lock("ComposeObject")
	defer b.mu.Unlock()

	bk, err := b.bucketLocked(bucket)
	if err != nil {
		return nil, err
	}

	var (
		data  []byte
		count int64
	)

	for _, s := range sources {
		src := bk.version(s.Name, s.Generation)
		if src == nil {
			return nil, objectNotFound(bucket, s.Name)
		}

		if apiErr := s.Pre.checkObject(src, false); apiErr != nil {
			return nil, apiErr
		}

		data = append(data, src.Data...)
		count += max(src.ComponentCount, 1)
	}

	if apiErr := pre.checkObject(bk.live(dst), false); apiErr != nil {
		return nil, apiErr
	}

	o, apiErr := newObject(bk, dst, data, meta, b.nextGen())
	if apiErr != nil {
		return nil, apiErr
	}

	o.ComponentCount, o.MD5 = count, ""
	bk.store(o)

	return o.clone(), nil
}

type uploadSession struct {
	Created time.Time
	Meta    Patch
	Pre     Preconds
	Bucket  string
	Name    string
	Data    []byte
}

// StartUpload opens a resumable upload session and returns its id.
func (b *InMemoryBackend) StartUpload(bucket, name string, meta Patch, pre Preconds) (string, error) {
	b.mu.Lock("StartUpload")
	defer b.mu.Unlock()

	if _, err := b.bucketLocked(bucket); err != nil {
		return "", err
	}

	if !validObjectName(name) {
		return "", errInvalid("Invalid object name: '" + name + "'")
	}

	now := time.Now().UTC()
	for id, s := range b.uploads {
		if now.Sub(s.Created) > uploadSessionTTL {
			delete(b.uploads, id)
		}
	}

	id := awsid.NewN("", uploadIDLen)
	b.uploads[id] = &uploadSession{Created: now, Bucket: bucket, Name: name, Meta: meta, Pre: pre}

	return id, nil
}

// UploadProgress reports a session's state after an append.
type UploadProgress struct {
	Object   *Object
	Received int64
}

// AppendUpload appends chunk at offset. total is the declared object size or
// -1 when unknown; once the received bytes reach it the object is committed.
func (b *InMemoryBackend) AppendUpload(id string, offset int64, chunk []byte, total int64) (UploadProgress, error) {
	b.mu.Lock("AppendUpload")
	defer b.mu.Unlock()

	s, ok := b.uploads[id]
	if !ok {
		return UploadProgress{}, errNotFound("Upload session not found.")
	}

	have := int64(len(s.Data))
	if offset <= have {
		skip := min(have-offset, int64(len(chunk)))
		s.Data = append(s.Data, chunk[skip:]...)
	}

	got := int64(len(s.Data))

	switch {
	case total < 0 || got < total:
		return UploadProgress{Received: got}, nil
	case got > total:
		return UploadProgress{}, errInvalid("Upload exceeds the declared total size.")
	}

	o, err := b.putLocked(s.Bucket, s.Name, slices.Clone(s.Data), s.Meta, s.Pre)
	if err != nil {
		return UploadProgress{}, err
	}

	delete(b.uploads, id)

	return UploadProgress{Received: got, Object: o}, nil
}

// CancelUpload discards a resumable session.
func (b *InMemoryBackend) CancelUpload(id string) error {
	b.mu.Lock("CancelUpload")
	defer b.mu.Unlock()

	if _, ok := b.uploads[id]; !ok {
		return errNotFound("Upload session not found.")
	}

	delete(b.uploads, id)

	return nil
}

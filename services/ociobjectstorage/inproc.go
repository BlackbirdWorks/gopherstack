package ociobjectstorage

import (
	"net/http"
	"net/url"
	"sort"
	"strings"
)

// AllBuckets lists every bucket of the namespace across compartments, sorted by name.
func (b *InMemoryBackend) AllBuckets() []*BucketWire {
	b.mu.RLock("AllBuckets")
	defer b.mu.RUnlock()

	names := make([]string, 0, len(b.buckets))
	for n := range b.buckets {
		names = append(names, n)
	}

	sort.Strings(names)

	out := make([]*BucketWire, 0, len(names))
	for _, n := range names {
		out = append(out, b.bucketWire(b.namespace, b.buckets[n], []string{"approximateCount", "approximateSize"}))
	}

	return out
}

// PutBlob stores an object for an in-process caller, bypassing HTTP and authentication.
func (b *InMemoryBackend) PutBlob(ns, bucket, object string, data []byte) error {
	_, err := b.PutObject(ns, bucket, object, data, ObjectMeta{ContentType: "application/octet-stream"},
		Preconds{}, "")

	return err
}

// GetBlob reads an object body for an in-process caller.
func (b *InMemoryBackend) GetBlob(ns, bucket, object string) ([]byte, error) {
	oi, err := b.GetObject(ns, bucket, object, "", Preconds{}, true)
	if err != nil {
		return nil, err
	}

	return oi.Data, nil
}

// PutBlobPAR writes through a pre-authenticated request URI (…/p/<token>/n/<ns>/b/<bucket>/o/<object>).
func (b *InMemoryBackend) PutBlobPAR(uri string, data []byte) error {
	token, ns, bucket, object, err := parsePARURI(uri)
	if err != nil {
		return err
	}

	if err = b.AuthorizePAR(token, ns, bucket, object, PARWrite); err != nil {
		return err
	}

	return b.PutBlob(ns, bucket, object, data)
}

// GetBlobPAR reads through a pre-authenticated request URI.
func (b *InMemoryBackend) GetBlobPAR(uri string) ([]byte, error) {
	token, ns, bucket, object, err := parsePARURI(uri)
	if err != nil {
		return nil, err
	}

	if err = b.AuthorizePAR(token, ns, bucket, object, PARRead); err != nil {
		return nil, err
	}

	return b.GetBlob(ns, bucket, object)
}

func parsePARURI(raw string) (string, string, string, string, error) {
	u, perr := url.Parse(raw)
	if perr != nil {
		return "", "", "", "", errParam("Invalid pre-authenticated request URI")
	}

	rt := parsePAR(&http.Request{Method: http.MethodGet, URL: u},
		strings.Split(strings.Trim(u.EscapedPath(), "/"), "/"))
	if rt.parToken == "" || rt.object == "" {
		return "", "", "", "", errParam("Invalid pre-authenticated request URI")
	}

	return rt.parToken, rt.ns, rt.bucket, rt.object, nil
}

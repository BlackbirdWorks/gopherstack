package gcpstorage

import (
	"bytes"
	"encoding/json"
	"time"
)

// Patch is a decoded JSON request body, keyed by field name. A null value
// clears the field, as in a JSON merge patch.
type Patch map[string]json.RawMessage

const (
	keyContentType        = "contentType"
	keyContentEncoding    = "contentEncoding"
	keyContentDisposition = "contentDisposition"
	keyContentLanguage    = "contentLanguage"
	keyCacheControl       = "cacheControl"
	keyStorageClass       = "storageClass"
	keyMetadata           = "metadata"
	keyCustomTime         = "customTime"
	keyKind               = "kind"
	keyEnabled            = "enabled"
	defaultLocation       = "US"
	defaultStorageClass   = "STANDARD"
	defaultContentType    = "application/octet-stream"
)

func isNull(raw json.RawMessage) bool { return string(bytes.TrimSpace(raw)) == "null" }

func decodeField[T any](raw json.RawMessage, field string) (T, *APIError) {
	var v T

	if isNull(raw) {
		return v, nil
	}

	if err := json.Unmarshal(raw, &v); err != nil {
		return v, errInvalid("Invalid value for field '" + field + "'.")
	}

	return v, nil
}

func (p Patch) str(key string) string {
	v, _ := decodeField[string](p[key], key)

	return v
}

func validStorageClass(sc string) bool {
	switch sc {
	case "STANDARD", "NEARLINE", "COLDLINE", "ARCHIVE", "MULTI_REGIONAL", "REGIONAL",
		"DURABLE_REDUCED_AVAILABILITY":
		return true
	default:
		return false
	}
}

func mergeStringMap(raw json.RawMessage, dst map[string]string, field string) (map[string]string, *APIError) {
	if isNull(raw) {
		return nil, nil
	}

	in, apiErr := decodeField[map[string]*string](raw, field)
	if apiErr != nil {
		return nil, apiErr
	}

	if dst == nil {
		dst = map[string]string{}
	}

	for k, v := range in {
		if v == nil {
			delete(dst, k)
		} else {
			dst[k] = *v
		}
	}

	if len(dst) == 0 {
		return nil, nil
	}

	return dst, nil
}

type stringTarget struct {
	dst *string
	key string
}

// applyObject applies the writable object fields of p to o.
func applyObject(o *Object, p Patch) *APIError {
	targets := []stringTarget{
		{&o.ContentType, keyContentType}, {&o.ContentEncoding, keyContentEncoding},
		{&o.ContentDisposition, keyContentDisposition}, {&o.ContentLanguage, keyContentLanguage},
		{&o.CacheControl, keyCacheControl},
	}

	for _, t := range targets {
		if raw, ok := p[t.key]; ok {
			v, apiErr := decodeField[string](raw, t.key)
			if apiErr != nil {
				return apiErr
			}

			*t.dst = v
		}
	}

	return applyObjectRest(o, p)
}

func applyObjectRest(o *Object, p Patch) *APIError {
	var apiErr *APIError

	if raw, ok := p[keyMetadata]; ok {
		if o.Metadata, apiErr = mergeStringMap(raw, o.Metadata, keyMetadata); apiErr != nil {
			return apiErr
		}
	}

	if raw, ok := p[keyCustomTime]; ok {
		s, e := decodeField[string](raw, keyCustomTime)
		if e != nil {
			return e
		}

		o.CustomTime = time.Time{}

		if s != "" {
			t, err := time.Parse(time.RFC3339, s)
			if err != nil {
				return errInvalid("Invalid value for field 'customTime'.")
			}

			o.CustomTime = t.UTC()
		}
	}

	if raw, ok := p["temporaryHold"]; ok {
		if o.TemporaryHold, apiErr = decodeField[bool](raw, "temporaryHold"); apiErr != nil {
			return apiErr
		}
	}

	if raw, ok := p["eventBasedHold"]; ok {
		if o.EventBasedHold, apiErr = decodeField[bool](raw, "eventBasedHold"); apiErr != nil {
			return apiErr
		}
	}

	if sc := p.str(keyStorageClass); sc != "" {
		if !validStorageClass(sc) {
			return errInvalid("Invalid storage class '" + sc + "'.")
		}

		o.StorageClass = sc
	}

	return nil
}

func (o *Object) resetMutable() {
	o.ContentType, o.ContentEncoding, o.ContentDisposition = "", "", ""
	o.ContentLanguage, o.CacheControl = "", ""
	o.Metadata, o.CustomTime = nil, time.Time{}
	o.TemporaryHold, o.EventBasedHold = false, false
}

func isConfigKey(k string) bool {
	switch k {
	case "cors", "lifecycle", "website", "logging", "encryption", "retentionPolicy", "iamConfiguration",
		"billing", "autoclass", "softDeletePolicy", "customPlacementConfig", "hierarchicalNamespace",
		"ipFilter", "objectRetention", "rpo":
		return true
	default:
		return false
	}
}

// applyBucket applies the writable bucket fields of p to b.
func applyBucket(b *Bucket, p Patch, create bool) *APIError {
	var apiErr *APIError

	if raw, ok := p["labels"]; ok {
		if b.Labels, apiErr = mergeStringMap(raw, b.Labels, "labels"); apiErr != nil {
			return apiErr
		}
	}

	if raw, ok := p["versioning"]; ok {
		v, e := decodeField[struct {
			Enabled bool `json:"enabled"`
		}](raw, "versioning")
		if e != nil {
			return e
		}

		b.Versioning = v.Enabled
	}

	if raw, ok := p["defaultEventBasedHold"]; ok {
		if b.DefaultEventBasedHold, apiErr = decodeField[bool](raw, "defaultEventBasedHold"); apiErr != nil {
			return apiErr
		}
	}

	if sc := p.str(keyStorageClass); sc != "" {
		if !validStorageClass(sc) {
			return errInvalid("Invalid storage class '" + sc + "'.")
		}

		b.StorageClass = sc
	}

	if apiErr = applyBucketACL(b, p, create); apiErr != nil {
		return apiErr
	}

	for k, raw := range p {
		if !isConfigKey(k) {
			continue
		}

		if isNull(raw) {
			delete(b.Config, k)

			continue
		}

		if b.Config == nil {
			b.Config = map[string]json.RawMessage{}
		}

		b.Config[k] = raw
	}

	return nil
}

func (b *Bucket) resetMutable() {
	b.Labels, b.Config = nil, nil
	b.Versioning, b.DefaultEventBasedHold = false, false
}

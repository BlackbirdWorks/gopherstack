package gcpstorage

import (
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/url"
	"strconv"
)

type objectJSON struct {
	Metadata           map[string]string `json:"metadata,omitempty"`
	Kind               string            `json:"kind"`
	ID                 string            `json:"id"`
	SelfLink           string            `json:"selfLink"`
	MediaLink          string            `json:"mediaLink"`
	Name               string            `json:"name"`
	Bucket             string            `json:"bucket"`
	Generation         string            `json:"generation"`
	Metageneration     string            `json:"metageneration"`
	ContentType        string            `json:"contentType,omitempty"`
	ContentEncoding    string            `json:"contentEncoding,omitempty"`
	ContentDisposition string            `json:"contentDisposition,omitempty"`
	ContentLanguage    string            `json:"contentLanguage,omitempty"`
	CacheControl       string            `json:"cacheControl,omitempty"`
	TimeCreated        string            `json:"timeCreated"`
	Updated            string            `json:"updated"`
	TimeDeleted        string            `json:"timeDeleted,omitempty"`
	TimeStorageClass   string            `json:"timeStorageClassUpdated"`
	CustomTime         string            `json:"customTime,omitempty"`
	StorageClass       string            `json:"storageClass"`
	Size               string            `json:"size"`
	MD5Hash            string            `json:"md5Hash,omitempty"`
	CRC32C             string            `json:"crc32c"`
	ETag               string            `json:"etag"`
	ACL                []aclResource     `json:"acl,omitempty"`
	ComponentCount     int64             `json:"componentCount,omitempty"`
	TemporaryHold      bool              `json:"temporaryHold,omitempty"`
	EventBasedHold     bool              `json:"eventBasedHold,omitempty"`
}

func baseURL(r *http.Request) string {
	scheme := "http"
	if r.TLS != nil || r.Header.Get("X-Forwarded-Proto") == "https" {
		scheme = "https"
	}

	return scheme + "://" + r.Host
}

func objectETag(o *Object) string {
	return base64.StdEncoding.EncodeToString([]byte(strconv.FormatInt(o.Generation, 10) + "/" +
		strconv.FormatInt(o.Metageneration, 10)))
}

func objectResource(base string, o *Object) objectJSON {
	gen := strconv.FormatInt(o.Generation, 10)
	esc := url.PathEscape(o.Name)

	res := objectResourceBase(base, o, gen, esc)
	if !o.hideACL && len(o.ACL) > 0 {
		res.ACL = renderACLList(base, aclTarget{bucket: o.Bucket, object: o.Name, gen: o.Generation, kind: aclObject}, o.ACL)
	}

	return res
}

func objectResourceBase(base string, o *Object, gen, esc string) objectJSON {
	return objectJSON{
		Kind:               "storage#object",
		ID:                 o.Bucket + "/" + o.Name + "/" + gen,
		SelfLink:           base + "/storage/v1/b/" + o.Bucket + "/o/" + esc,
		MediaLink:          base + "/download/storage/v1/b/" + o.Bucket + "/o/" + esc + "?generation=" + gen + "&alt=media",
		Name:               o.Name,
		Bucket:             o.Bucket,
		Generation:         gen,
		Metageneration:     strconv.FormatInt(o.Metageneration, 10),
		ContentType:        o.ContentType,
		ContentEncoding:    o.ContentEncoding,
		ContentDisposition: o.ContentDisposition,
		ContentLanguage:    o.ContentLanguage,
		CacheControl:       o.CacheControl,
		Metadata:           o.Metadata,
		TimeCreated:        fmtTime(o.Created),
		Updated:            fmtTime(o.Updated),
		TimeDeleted:        fmtTime(o.Deleted),
		TimeStorageClass:   fmtTime(o.Created),
		CustomTime:         fmtTime(o.CustomTime),
		StorageClass:       o.StorageClass,
		Size:               strconv.FormatInt(o.Size(), 10),
		MD5Hash:            o.MD5,
		CRC32C:             o.CRC32C,
		ETag:               objectETag(o),
		ComponentCount:     o.ComponentCount,
		TemporaryHold:      o.TemporaryHold,
		EventBasedHold:     o.EventBasedHold,
	}
}

func bucketResource(base string, b *Bucket) map[string]any {
	res := map[string]any{
		keyKind:          "storage#bucket",
		"id":             b.Name,
		"selfLink":       base + "/storage/v1/b/" + b.Name,
		"name":           b.Name,
		"timeCreated":    fmtTime(b.Created),
		"updated":        fmtTime(b.Updated),
		"metageneration": strconv.FormatInt(b.Metageneration, 10),
		"location":       b.Location,
		"locationType":   LocationType(b.Location),
		keyStorageClass:  b.StorageClass,
		"etag":           base64.StdEncoding.EncodeToString([]byte("CAE=" + strconv.FormatInt(b.Metageneration, 10))),
		"iamConfiguration": map[string]any{
			"bucketPolicyOnly":         map[string]bool{keyEnabled: false},
			"uniformBucketLevelAccess": map[string]bool{keyEnabled: false},
			"publicAccessPrevention":   "inherited",
		},
	}

	if len(b.Labels) > 0 {
		res["labels"] = b.Labels
	}

	if b.Versioning {
		res["versioning"] = map[string]bool{keyEnabled: true}
	}

	if b.DefaultEventBasedHold {
		res["defaultEventBasedHold"] = true
	}

	if !b.ubla() {
		res[keyACL] = renderACLList(base, aclTarget{bucket: b.Name, kind: aclBucket}, b.ACL)
		res[keyDefaultACL] = renderACLList(base, aclTarget{bucket: b.Name, kind: aclDefaultObject}, b.DefaultObjectACL)
	}

	for k, v := range b.Config {
		res[k] = v
	}

	return res
}

type policyJSON struct {
	Kind       string       `json:"kind"`
	ResourceID string       `json:"resourceId"`
	Etag       string       `json:"etag"`
	Bindings   []IAMBinding `json:"bindings"`
	Version    int          `json:"version"`
}

func policyResource(bucket string, p IAMPolicy) policyJSON {
	return policyJSON{
		Kind: "storage#policy", ResourceID: "projects/_/buckets/" + bucket,
		Etag: p.Etag, Bindings: p.Bindings, Version: p.Version,
	}
}

// parsePreconds reads <prefix>GenerationMatch etc. from q; prefix is "if" or "ifSource".
func parsePreconds(q url.Values, prefix string) (Preconds, *APIError) {
	var (
		pre    Preconds
		apiErr *APIError
	)

	fields := []struct {
		dst  **int64
		name string
	}{
		{&pre.GenMatch, prefix + "GenerationMatch"},
		{&pre.GenNotMatch, prefix + "GenerationNotMatch"},
		{&pre.MetaMatch, prefix + "MetagenerationMatch"},
		{&pre.MetaNotMatch, prefix + "MetagenerationNotMatch"},
	}

	for _, f := range fields {
		if apiErr = parseOptInt(q, f.name, f.dst); apiErr != nil {
			return pre, apiErr
		}
	}

	return pre, nil
}

func parseOptInt(q url.Values, key string, dst **int64) *APIError {
	s := q.Get(key)
	if s == "" {
		return nil
	}

	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return errInvalid("Invalid value for parameter '" + key + "'.")
	}

	*dst = &n

	return nil
}

func parseGeneration(q url.Values, key string) (int64, *APIError) {
	var g *int64

	if apiErr := parseOptInt(q, key, &g); apiErr != nil {
		return 0, apiErr
	}

	if g == nil {
		return 0, nil
	}

	return *g, nil
}

func decodePatch(body []byte) (Patch, *APIError) {
	p := Patch{}
	if len(body) == 0 {
		return p, nil
	}

	if err := json.Unmarshal(body, &p); err != nil || p == nil {
		return nil, errInvalid("Invalid JSON payload received.")
	}

	return p, nil
}

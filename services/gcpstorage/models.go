package gcpstorage

import (
	"encoding/json"
	"maps"
	"slices"
	"time"
)

// IAMBinding is one role binding of a bucket IAM policy.
type IAMBinding struct {
	Condition json.RawMessage `json:"condition,omitempty"`
	Role      string          `json:"role"`
	Members   []string        `json:"members"`
}

// IAMPolicy is a bucket IAM policy.
type IAMPolicy struct {
	Etag     string       `json:"etag"`
	Bindings []IAMBinding `json:"bindings"`
	Version  int          `json:"version"`
}

// Object is one stored object version. A version whose Deleted time is set is
// noncurrent; the live version, if any, is always last in its slice.
type Object struct {
	Created            time.Time         `json:"created"`
	Updated            time.Time         `json:"updated"`
	Deleted            time.Time         `json:"deleted"`
	CustomTime         time.Time         `json:"customTime"`
	Metadata           map[string]string `json:"metadata,omitempty"`
	Bucket             string            `json:"bucket"`
	Name               string            `json:"name"`
	ContentType        string            `json:"contentType"`
	ContentEncoding    string            `json:"contentEncoding,omitempty"`
	ContentDisposition string            `json:"contentDisposition,omitempty"`
	ContentLanguage    string            `json:"contentLanguage,omitempty"`
	CacheControl       string            `json:"cacheControl,omitempty"`
	StorageClass       string            `json:"storageClass"`
	MD5                string            `json:"md5,omitempty"`
	CRC32C             string            `json:"crc32c"`
	Data               []byte            `json:"data"`
	Generation         int64             `json:"generation"`
	Metageneration     int64             `json:"metageneration"`
	ComponentCount     int64             `json:"componentCount,omitempty"`
	TemporaryHold      bool              `json:"temporaryHold,omitempty"`
	EventBasedHold     bool              `json:"eventBasedHold,omitempty"`
}

// Size returns the object size in bytes.
func (o *Object) Size() int64 { return int64(len(o.Data)) }

func (o *Object) clone() *Object {
	c := *o
	c.Metadata = maps.Clone(o.Metadata)

	return &c
}

// Bucket is a bucket with all of its object versions.
type Bucket struct {
	Created               time.Time                  `json:"created"`
	Updated               time.Time                  `json:"updated"`
	Objects               map[string][]*Object       `json:"objects"`
	Labels                map[string]string          `json:"labels,omitempty"`
	Config                map[string]json.RawMessage `json:"config,omitempty"`
	Name                  string                     `json:"name"`
	Project               string                     `json:"project"`
	Location              string                     `json:"location"`
	StorageClass          string                     `json:"storageClass"`
	IAM                   IAMPolicy                  `json:"iam"`
	Metageneration        int64                      `json:"metageneration"`
	Versioning            bool                       `json:"versioning,omitempty"`
	DefaultEventBasedHold bool                       `json:"defaultEventBasedHold,omitempty"`
}

// clone copies the bucket metadata without its objects.
func (b *Bucket) clone() *Bucket {
	c := *b
	c.Objects = nil
	c.Labels = maps.Clone(b.Labels)
	c.Config = maps.Clone(b.Config)
	c.IAM.Bindings = slices.Clone(b.IAM.Bindings)

	return &c
}

// live returns the current version of name, or nil.
func (b *Bucket) live(name string) *Object {
	v := b.Objects[name]
	if n := len(v); n > 0 && v[n-1].Deleted.IsZero() {
		return v[n-1]
	}

	return nil
}

// version returns generation gen of name, or the live version when gen is 0.
func (b *Bucket) version(name string, gen int64) *Object {
	if gen == 0 {
		return b.live(name)
	}

	for _, o := range b.Objects[name] {
		if o.Generation == gen {
			return o
		}
	}

	return nil
}

func (b *Bucket) empty() bool { return len(b.Objects) == 0 }

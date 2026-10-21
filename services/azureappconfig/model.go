package azureappconfig

import (
	"maps"
	"sort"
	"time"
)

// KeyValue is one configuration setting. An empty Label is the null label.
type KeyValue struct {
	LastModified time.Time         `json:"lastModified"`
	Value        *string           `json:"value,omitempty"`
	ContentType  *string           `json:"contentType,omitempty"`
	Tags         map[string]string `json:"tags,omitempty"`
	Key          string            `json:"key"`
	Label        string            `json:"label,omitempty"`
	ETag         string            `json:"etag"`
	Locked       bool              `json:"locked,omitempty"`
}

// AccessKey is one of a store's four HMAC credentials.
type AccessKey struct {
	LastModified time.Time `json:"lastModified"`
	ID           string    `json:"id"`
	Name         string    `json:"name"`
	Secret       string    `json:"secret"`
	ReadOnly     bool      `json:"readOnly,omitempty"`
}

// histEntry is one point in a key-value's history; a tombstone keeps the last state.
type histEntry struct {
	At      time.Time `json:"at"`
	KV      KeyValue  `json:"kv"`
	Deleted bool      `json:"deleted,omitempty"`
}

// Filter selects key-values for a snapshot.
type Filter struct {
	Key   string `json:"key,omitempty"`
	Label string `json:"label,omitempty"`
}

// Snapshot is an immutable point-in-time set of key-values.
type Snapshot struct {
	Created     time.Time         `json:"created"`
	Expires     *time.Time        `json:"expires,omitempty"`
	Tags        map[string]string `json:"tags,omitempty"`
	Name        string            `json:"name"`
	Status      string            `json:"status"`
	Composition string            `json:"composition"`
	ETag        string            `json:"etag"`
	Filters     []Filter          `json:"filters"`
	Items       []KeyValue        `json:"items"`
	Retention   int64             `json:"retention"`
	Size        int64             `json:"size"`
}

type store struct {
	Created   time.Time            `json:"created"`
	KVs       map[string]*KeyValue `json:"kvs"`
	Snapshots map[string]*Snapshot `json:"snapshots"`
	Name      string               `json:"name"`
	Keys      []AccessKey          `json:"keys"`
	History   []histEntry          `json:"history"`
	Seq       int64                `json:"seq"`
}

// StoreInfo is the ARM-visible view of a store.
type StoreInfo struct {
	Created time.Time
	Name    string
}

func kvID(key, label string) string { return key + "\x00" + label }

func sortKVs(kvs []KeyValue) {
	sort.Slice(kvs, func(i, j int) bool {
		if kvs[i].Key != kvs[j].Key {
			return kvs[i].Key < kvs[j].Key
		}

		return kvs[i].Label < kvs[j].Label
	})
}

func cloneKV(kv KeyValue) KeyValue {
	out := kv
	out.Value, out.ContentType = cloneStr(kv.Value), cloneStr(kv.ContentType)

	if kv.Tags != nil {
		out.Tags = make(map[string]string, len(kv.Tags))
		maps.Copy(out.Tags, kv.Tags)
	}

	return out
}

func cloneStr(s *string) *string {
	if s == nil {
		return nil
	}

	c := *s

	return &c
}

func kvBytes(kv KeyValue) int64 {
	n := len(kv.Key) + len(kv.Label)
	if kv.Value != nil {
		n += len(*kv.Value)
	}

	if kv.ContentType != nil {
		n += len(*kv.ContentType)
	}

	for k, v := range kv.Tags {
		n += len(k) + len(v)
	}

	return int64(n)
}

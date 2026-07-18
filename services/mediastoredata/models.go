package mediastoredata

import "time"

// Object represents a stored media object.
//
// Path is the object's normalized path (see normalizePath) and is also the
// primary key of the per-region [store.Table] it is stored in (see
// objectKeyFn in store_setup.go) -- it must always be set to the same value
// as the map key that used to hold the object pre-Phase-3.3, and must stay
// exported/serialized (no json:"-") since Object is registered directly
// rather than through a DTO (see .claude/memories/parity-principles.md's
// json:"-" hidden-ID gotcha).
type Object struct {
	LastModified       time.Time
	Path               string
	ETag               string
	SHA256             string // cached hex-encoded SHA-256 of Body
	ContentType        string
	CacheControl       string
	StorageClass       string
	UploadAvailability string
	Body               []byte
	ContentLength      int64
}

// Item is a metadata entry for a folder or object returned by ListItems.
type Item struct {
	LastModified  time.Time
	Name          string
	Type          string
	ETag          string
	SHA256        string
	ContentType   string
	CacheControl  string
	StorageClass  string
	ContentLength int64
}

// ListItemsInput parameterises a ListItems call.
type ListItemsInput struct {
	FolderPath string
	NextToken  string // opaque pagination cursor (item Name)
	MaxResults int    // 0 → defaultMaxResults
}

// ListItemsOutput is returned by ListItems.
type ListItemsOutput struct {
	NextToken string // empty when no further pages remain
	Items     []*Item
}

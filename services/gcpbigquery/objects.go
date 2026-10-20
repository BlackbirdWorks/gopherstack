package gcpbigquery

import (
	"encoding/base64"
	"errors"
	"strings"
)

// ErrObjectNotFound is returned by an ObjectStore when a bucket or object does not exist.
var ErrObjectNotFound = errors.New("object not found")

// ObjectStore is the slice of Cloud Storage that load and extract jobs need.
type ObjectStore interface {
	// ListObjects returns the names of every object in bucket that starts with prefix.
	ListObjects(bucket, prefix string) ([]string, error)
	// ReadObject returns the content of one object.
	ReadObject(bucket, name string) ([]byte, error)
	// WriteObject creates or replaces one object.
	WriteObject(bucket, name string, data []byte, contentType string) error
}

type gcsURI struct {
	bucket string
	object string
}

func parseGCSURI(uri string) (gcsURI, bool) {
	rest, ok := strings.CutPrefix(uri, "gs://")
	if !ok {
		return gcsURI{}, false
	}

	bucket, object, _ := strings.Cut(rest, "/")
	if bucket == "" {
		return gcsURI{}, false
	}

	return gcsURI{bucket: bucket, object: object}, true
}

// wildcardMatch reports whether name matches pattern where each * matches any run of characters.
func wildcardMatch(pattern, name string) bool {
	parts := strings.Split(pattern, "*")
	if len(parts) == 1 {
		return pattern == name
	}

	if !strings.HasPrefix(name, parts[0]) {
		return false
	}

	name = name[len(parts[0]):]

	for _, mid := range parts[1 : len(parts)-1] {
		i := strings.Index(name, mid)
		if i < 0 {
			return false
		}

		name = name[i+len(mid):]
	}

	return strings.HasSuffix(name, parts[len(parts)-1])
}

func bqsqlDecodeBase64(s string) ([]byte, error) { return base64.StdEncoding.DecodeString(s) }

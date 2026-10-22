package azureredis

import (
	"fmt"
	"net/url"
	"strings"
)

const (
	rdbFormat   = "RDB"
	blobHostTag = ".blob."
	rdbSuffix   = ".rdb"
)

// splitBlobURL returns the container and blob of a Blob Storage URL, virtual-hosted
// (https://{account}.blob.core.windows.net/{container}/{blob}) or path style
// (http://host:port/{account}/{container}/{blob}).
func splitBlobURL(raw string) (string, string, error) {
	u, perr := url.Parse(raw)
	if perr != nil || u.Host == "" {
		return "", "", fmt.Errorf("%w: %q is not a blob URL", ErrInvalid, redactURL(raw))
	}

	segs := strings.Split(strings.Trim(u.Path, "/"), "/")
	if !strings.Contains(u.Hostname(), blobHostTag) && len(segs) > 0 {
		segs = segs[1:]
	}

	if len(segs) == 0 || segs[0] == "" {
		return "", "", fmt.Errorf("%w: %q does not name a container", ErrInvalid, redactURL(raw))
	}

	return segs[0], strings.Join(segs[1:], "/"), nil
}

// redactURL drops the query (SAS token) before a URL is put in an error.
func redactURL(raw string) string {
	before, _, _ := strings.Cut(raw, "?")

	return before
}

func (b *InMemoryBackend) blobStore() (BlobStore, error) {
	if b.blobs == nil {
		return nil, ErrNoBlobStore
	}

	return b.blobs, nil
}

// Import replaces the cache's data with the RDB file at the blob URL.
func (b *InMemoryBackend) Import(key, format string, files []string) error {
	if format != "" && !strings.EqualFold(format, rdbFormat) {
		return fmt.Errorf("%w: only the RDB format is supported", ErrInvalid)
	}

	if len(files) != 1 {
		return fmt.Errorf("%w: exactly one file is required for a non-clustered cache", ErrInvalid)
	}

	container, blob, err := splitBlobURL(files[0])
	if err != nil {
		return err
	}

	b.mu.Lock("Import")
	defer b.mu.Unlock()

	c, err := b.lookup(key)
	if err != nil {
		return err
	}

	store, err := b.blobStore()
	if err != nil {
		return err
	}

	data, err := store.ReadBlob(container, blob)
	if err != nil {
		return fmt.Errorf("%w: cannot read %s/%s: %w", ErrInvalid, container, blob, err)
	}

	if err = c.engine.ImportRDB(data); err != nil {
		return fmt.Errorf("%w: cannot import %s/%s: %w", ErrInvalid, container, blob, err)
	}

	return nil
}

// Export writes the cache's data to <prefix>.rdb in the container named by containerURL.
func (b *InMemoryBackend) Export(key, format, prefix, containerURL string) error {
	if format != "" && !strings.EqualFold(format, rdbFormat) {
		return fmt.Errorf("%w: only the RDB format is supported", ErrInvalid)
	}

	if prefix == "" {
		return fmt.Errorf("%w: prefix is required", ErrInvalid)
	}

	container, sub, err := splitBlobURL(containerURL)
	if err != nil {
		return err
	}

	if sub != "" {
		return fmt.Errorf("%w: container must be a container URL", ErrInvalid)
	}

	b.mu.RLock("Export")
	defer b.mu.RUnlock()

	c, err := b.lookup(key)
	if err != nil {
		return err
	}

	store, err := b.blobStore()
	if err != nil {
		return err
	}

	data, err := c.engine.ExportRDB()
	if err != nil {
		return fmt.Errorf("%w: cannot export data: %w", ErrInvalid, err)
	}

	if err = store.WriteBlob(container, prefix+rdbSuffix, data); err != nil {
		return fmt.Errorf("%w: cannot write %s/%s%s: %w", ErrInvalid, container, prefix, rdbSuffix, err)
	}

	return nil
}

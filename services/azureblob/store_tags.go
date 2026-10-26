package azureblob

import (
	"encoding/base64"
	"maps"
	"sort"
)

// FindBlobsOptions are the Find Blobs by Tags parameters. Container is empty
// for account scope; the filter's own @container clause narrows it further.
type FindBlobsOptions struct {
	Filter     *tagFilter
	Container  string
	Marker     string
	MaxResults int
}

// FoundBlob is one Find Blobs by Tags match.
type FoundBlob struct {
	Tags      map[string]string
	Container string
	Name      string
}

// FindBlobsPage is one page of a Find Blobs by Tags response.
type FindBlobsPage struct {
	NextMarker string
	Blobs      []FoundBlob
}

// tagTargetLocked resolves a blob or snapshot for a tag operation: the lease
// gate (base blob only) and x-ms-if-tags apply; other conditional headers do
// not.
func (b *InMemoryBackend) tagTargetLocked(
	container, blob, snapshot string, p Precond, require bool,
) (*storedBlob, error) {
	stored, err := b.lookupBlobLocked(container, blob, snapshot)
	if err != nil {
		return nil, err
	}

	if snapshot == "" {
		if err = b.containers[container].Blobs[blob].Lease.gate(b.now(), p.LeaseID, require, "Blob"); err != nil {
			return nil, err
		}
	}

	if err = p.checkTags(true, stored.Tags); err != nil {
		return nil, err
	}

	return stored, nil
}

// SetBlobTags replaces all tags of a blob or snapshot. ETag and Last-Modified
// are unchanged.
func (b *InMemoryBackend) SetBlobTags(
	container, blob, snapshot string, tags map[string]string, pre ...Precond,
) error {
	b.mu.Lock("SetBlobTags")
	defer b.mu.Unlock()

	stored, err := b.tagTargetLocked(container, blob, snapshot, firstPre(pre), true)
	if err != nil {
		return err
	}

	stored.Tags = maps.Clone(tags)

	return nil
}

// GetBlobTags returns a copy of a blob's or snapshot's tags.
func (b *InMemoryBackend) GetBlobTags(
	container, blob, snapshot string, pre ...Precond,
) (map[string]string, error) {
	b.mu.RLock("GetBlobTags")
	defer b.mu.RUnlock()

	stored, err := b.tagTargetLocked(container, blob, snapshot, firstPre(pre), false)
	if err != nil {
		return nil, err
	}

	return maps.Clone(stored.Tags), nil
}

// FindBlobsByTags lists base blobs whose tags satisfy the filter, ordered by
// container then blob name, with marker paging.
func (b *InMemoryBackend) FindBlobsByTags(container string, opts FindBlobsOptions) (FindBlobsPage, error) {
	b.mu.RLock("FindBlobsByTags")
	defer b.mu.RUnlock()

	scope := container
	if scope == "" {
		scope = opts.Filter.container
	}

	if container != "" {
		if _, err := b.containerLocked(container); err != nil {
			return FindBlobsPage{}, err
		}
	}

	type hit struct {
		key  string
		blob FoundBlob
	}

	var hits []hit

	for cname, c := range b.containers {
		if scope != "" && cname != scope {
			continue
		}

		for bname, s := range c.Blobs {
			if opts.Filter.match(s.Tags) {
				hits = append(hits, hit{
					key:  cname + "\x00" + bname,
					blob: FoundBlob{Container: cname, Name: bname, Tags: maps.Clone(s.Tags)},
				})
			}
		}
	}

	sort.Slice(hits, func(i, j int) bool { return hits[i].key < hits[j].key })

	if opts.Marker != "" {
		raw, err := base64.RawURLEncoding.DecodeString(opts.Marker)
		if err != nil {
			return FindBlobsPage{}, ErrInvalidMarker
		}

		from := string(raw)
		hits = hits[sort.Search(len(hits), func(i int) bool { return hits[i].key >= from }):]
	}

	var page FindBlobsPage

	if opts.MaxResults > 0 && len(hits) > opts.MaxResults {
		page.NextMarker = base64.RawURLEncoding.EncodeToString([]byte(hits[opts.MaxResults].key))
		hits = hits[:opts.MaxResults]
	}

	page.Blobs = make([]FoundBlob, len(hits))
	for i := range hits {
		page.Blobs[i] = hits[i].blob
	}

	return page, nil
}

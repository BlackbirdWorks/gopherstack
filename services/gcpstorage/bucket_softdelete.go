package gcpstorage

import (
	"net/http"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

// softDeleteBucketLocked parks a removed bucket for its soft-delete window.
func (b *InMemoryBackend) softDeleteBucketLocked(bk *Bucket) {
	d := bk.softDeleteDuration()
	if d <= 0 {
		return
	}

	now := b.now().UTC()
	if bk.Generation == 0 {
		bk.Generation = b.nextGen()
	}

	bk.SoftDeleteTime, bk.HardDeleteTime = now, now.Add(d)
	b.softBkts = append(b.softBkts, bk)
}

func bucketGeneration(raw string) (int64, *APIError) {
	if raw == "" {
		return 0, errRequired("generation")
	}

	gen, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || gen <= 0 {
		return 0, errInvalid("Invalid generation: '" + raw + "'")
	}

	return gen, nil
}

func (b *InMemoryBackend) softBucketIndexLocked(name string, gen int64) int {
	now := b.now().UTC()

	return slices.IndexFunc(b.softBkts, func(bk *Bucket) bool {
		return bk.Name == name && bk.Generation == gen && now.Before(bk.HardDeleteTime)
	})
}

func errSoftBucketNotFound() *APIError {
	return errNotFound("The specified soft-deleted bucket does not exist.")
}

// GetSoftDeletedBucket returns generation gen of a soft-deleted bucket.
func (b *InMemoryBackend) GetSoftDeletedBucket(name string, gen int64) (*Bucket, error) {
	b.mu.RLock("GetSoftDeletedBucket")
	defer b.mu.RUnlock()

	i := b.softBucketIndexLocked(name, gen)
	if i < 0 {
		return nil, errSoftBucketNotFound()
	}

	return b.softBkts[i].clone(), nil
}

// ListSoftDeletedBuckets lists the project's soft-deleted bucket generations.
func (b *InMemoryBackend) ListSoftDeletedBuckets(project, prefix, token string, limit int) page.Page[*Bucket] {
	b.mu.RLock("ListSoftDeletedBuckets")
	defer b.mu.RUnlock()

	now := b.now().UTC()

	var all []*Bucket

	for _, bk := range b.softBkts {
		if bk.Project == project && strings.HasPrefix(bk.Name, prefix) && now.Before(bk.HardDeleteTime) {
			all = append(all, bk.clone())
		}
	}

	slices.SortFunc(all, func(x, y *Bucket) int {
		if c := strings.Compare(x.Name, y.Name); c != 0 {
			return c
		}

		return int(x.Generation - y.Generation)
	})

	return page.New(all, token, limit, defaultMaxResults)
}

// RestoreBucket brings a soft-deleted bucket back under its original name.
func (b *InMemoryBackend) RestoreBucket(name string, gen int64) (*Bucket, error) {
	bk, err := b.restoreBucket(name, gen)
	if err == nil {
		b.auditBucket("storage.buckets.restore", bk.Project, bk)
	}

	return bk, err
}

func (b *InMemoryBackend) restoreBucket(name string, gen int64) (*Bucket, error) {
	b.mu.Lock("RestoreBucket")
	defer b.mu.Unlock()

	i := b.softBucketIndexLocked(name, gen)
	if i < 0 {
		return nil, errSoftBucketNotFound()
	}

	if _, live := b.buckets[name]; live {
		return nil, errConflict("Your previous request to create the named bucket succeeded and you already own it.")
	}

	bk := b.softBkts[i]
	b.softBkts = slices.Delete(b.softBkts, i, i+1)

	bk.SoftDeleteTime, bk.HardDeleteTime = time.Time{}, time.Time{}
	bk.Generation = b.nextGen()
	bk.Updated = b.now().UTC()
	b.buckets[name] = bk

	return bk.clone(), nil
}

func (h *Handler) restoreBucket(c *echo.Context, rt route) error {
	gen, apiErr := bucketGeneration(c.QueryParam("generation"))
	if apiErr != nil {
		return apiErr
	}

	bk, err := h.Backend.RestoreBucket(rt.bucket, gen)
	if err != nil {
		return err
	}

	return c.JSON(http.StatusOK, bucketResource(baseURL(c.Request()), bk))
}

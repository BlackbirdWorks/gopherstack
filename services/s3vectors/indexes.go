package s3vectors

import (
	"cmp"
	"slices"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

func (b *InMemoryBackend) resolveIndex(sc scope, ref indexRef) (*Bucket, *Index, error) {
	if ref.IndexArn != "" {
		parsed, ok := parseARN(ref.IndexArn)
		rest, found := strings.CutPrefix(parsed.resource, "bucket/")
		bname, iname, hasIdx := strings.Cut(rest, "/index/")

		if !ok || !found || !hasIdx || bname == "" || iname == "" {
			return nil, nil, invalid("indexArn %q is not a valid vector index ARN", ref.IndexArn)
		}

		if (ref.VectorBucketName != "" && ref.VectorBucketName != bname) ||
			(ref.IndexName != "" && ref.IndexName != iname) {
			return nil, nil, invalid("indexArn conflicts with vectorBucketName or indexName")
		}

		sc, ref.VectorBucketName, ref.IndexName = scope{account: parsed.account, region: parsed.region}, bname, iname
	}

	if ref.VectorBucketName == "" || ref.IndexName == "" {
		return nil, nil, invalid("either indexArn or both vectorBucketName and indexName are required")
	}

	bk, ok := b.buckets.Get(bucketID(sc.account, sc.region, ref.VectorBucketName))
	if !ok {
		return nil, nil, notFound("The specified vector bucket could not be found")
	}

	ix, ok := b.indexes.Get(indexID(sc.account, sc.region, bk.Name, ref.IndexName))
	if !ok {
		return nil, nil, notFound("The specified vector index could not be found")
	}

	return bk, ix, nil
}

type createIndexIn struct {
	Encryption *encryptionIn     `json:"encryptionConfiguration"`
	Metadata   *metadataConfig   `json:"metadataConfiguration"`
	Dimension  *int              `json:"dimension"`
	Tags       map[string]string `json:"tags"`
	bucketRef
	IndexName      string `json:"indexName"`
	DataType       string `json:"dataType"`
	DistanceMetric string `json:"distanceMetric"`
}

func validateIndexShape(in *createIndexIn) error {
	if err := validateIndexName(in.IndexName); err != nil {
		return err
	}

	if in.DataType != "float32" {
		return invalid("dataType must be float32")
	}

	if in.DistanceMetric != "cosine" && in.DistanceMetric != "euclidean" {
		return invalid("distanceMetric must be cosine or euclidean")
	}

	if in.Dimension == nil || *in.Dimension < 1 || *in.Dimension > maxDimension {
		return invalid("dimension must be between 1 and %d", maxDimension)
	}

	return validateNonFilterable(in.Metadata)
}

func validateNonFilterable(m *metadataConfig) error {
	if m == nil {
		return nil
	}

	if len(m.NonFilterableMetadataKeys) > maxNonFilterable {
		return invalid("nonFilterableMetadataKeys can list at most %d keys", maxNonFilterable)
	}

	seen := map[string]bool{}

	for _, k := range m.NonFilterableMetadataKeys {
		if k == "" || len(k) > maxNameLen {
			return invalid("non-filterable metadata keys must be 1-%d characters", maxNameLen)
		}

		if seen[k] {
			return invalid("non-filterable metadata key %q is listed twice", k)
		}

		seen[k] = true
	}

	return nil
}

func (b *InMemoryBackend) createIndex(sc scope, in *createIndexIn) (any, error) {
	if err := validateIndexShape(in); err != nil {
		return nil, err
	}

	if err := validateTags(in.Tags); err != nil {
		return nil, err
	}

	b.mu.Lock("createIndex")
	defer b.mu.Unlock()

	bk, err := b.resolveBucket(sc, in.bucketRef)
	if err != nil {
		return nil, err
	}

	enc := bk.Encryption
	if in.Encryption != nil {
		if enc, err = validateEncryption(in.Encryption); err != nil {
			return nil, err
		}
	}

	if b.indexes.Has(indexID(bk.Account, bk.Region, bk.Name, in.IndexName)) {
		return nil, conflict("A vector index with the name %q already exists in the vector bucket", in.IndexName)
	}

	if len(b.bucketIndexes(bk)) >= maxIndexes {
		return nil, quotaExceeded("the vector bucket already has %d vector indexes", maxIndexes)
	}

	ix := &Index{
		Account: bk.Account, Region: bk.Region, Bucket: bk.Name, Name: in.IndexName,
		ARN: indexARN(scope{account: bk.Account, region: bk.Region}, bk.Name, in.IndexName), Created: b.now(),
		DataType: in.DataType, DistanceMetric: in.DistanceMetric, Dimension: *in.Dimension, Mode: bk.DefaultIndexMode,
		Encryption: enc, Tags: in.Tags,
	}
	if in.Metadata != nil {
		ix.NonFilterable = slices.Clone(in.Metadata.NonFilterableMetadataKeys)
	}

	b.indexes.Put(ix)
	b.vectors.Put(&VectorSet{Key: indexKey(ix), Vectors: map[string]*Vector{}})

	return map[string]any{"indexArn": ix.ARN}, nil
}

func (b *InMemoryBackend) getIndex(sc scope, ref indexRef) (any, error) {
	b.mu.RLock("getIndex")
	defer b.mu.RUnlock()

	_, ix, err := b.resolveIndex(sc, ref)
	if err != nil {
		return nil, err
	}

	return map[string]any{"index": toIndexOut(ix)}, nil
}

func (b *InMemoryBackend) deleteIndex(sc scope, ref indexRef) (any, error) {
	b.mu.Lock("deleteIndex")
	defer b.mu.Unlock()

	_, ix, err := b.resolveIndex(sc, ref)
	if err != nil {
		return nil, err
	}

	b.indexes.Delete(indexKey(ix))
	b.vectors.Delete(indexKey(ix))

	return struct{}{}, nil
}

type listIndexesIn struct {
	bucketRef
	MaxResults *int   `json:"maxResults"`
	NextToken  string `json:"nextToken"`
	Prefix     string `json:"prefix"`
}

func (b *InMemoryBackend) listIndexes(sc scope, in *listIndexesIn) (any, error) {
	size, err := pageSize(in.MaxResults, maxListBuckets)
	if err != nil {
		return nil, err
	}

	if err = page.ValidateToken(in.NextToken); err != nil {
		return nil, invalid("nextToken is not valid")
	}

	b.mu.RLock("listIndexes")
	defer b.mu.RUnlock()

	bk, err := b.resolveBucket(sc, in.bucketRef)
	if err != nil {
		return nil, err
	}

	var all []indexSummary

	for _, ix := range b.bucketIndexes(bk) {
		if strings.HasPrefix(ix.Name, in.Prefix) {
			all = append(all, indexSummary{
				CreationTime: toIndexOut(
					ix,
				).CreationTime,
				IndexArn:         ix.ARN,
				IndexName:        ix.Name,
				VectorBucketName: ix.Bucket,
			})
		}
	}

	slices.SortFunc(all, func(x, y indexSummary) int { return cmp.Compare(x.IndexName, y.IndexName) })

	pg := page.New(all, in.NextToken, size, maxListBuckets)
	out := map[string]any{"indexes": pg.Data}

	if pg.Next != "" {
		out["nextToken"] = pg.Next
	}

	return out, nil
}

type updateIndexModeIn struct {
	indexRef
	IndexMode string `json:"indexMode"`
}

func (b *InMemoryBackend) updateIndexMode(sc scope, in *updateIndexModeIn) (any, error) {
	if in.IndexMode != modeClassic && in.IndexMode != modeEnhanced {
		return nil, invalid("indexMode must be CLASSIC or ENHANCED")
	}

	b.mu.Lock("updateIndexMode")
	defer b.mu.Unlock()

	bk, ix, err := b.resolveIndex(sc, in.indexRef)
	if err != nil {
		return nil, err
	}

	if in.IndexMode == modeClassic && !bk.Created.Before(classicCutoff()) {
		return nil, invalid("CLASSIC is only available for indexes in vector buckets created before September 30, 2026")
	}

	ix.Mode = in.IndexMode

	return struct{}{}, nil
}

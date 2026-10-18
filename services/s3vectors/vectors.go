package s3vectors

import (
	"encoding/json"
	"hash/fnv"
	"maps"
	"slices"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

func (b *InMemoryBackend) setOf(ix *Index) *VectorSet {
	if s, ok := b.vectors.Get(indexKey(ix)); ok {
		return s
	}

	s := &VectorSet{Key: indexKey(ix), Vectors: map[string]*Vector{}}
	b.vectors.Put(s)

	return s
}

func (ix *Index) filterable(key string) bool { return !slices.Contains(ix.NonFilterable, key) }

func validateKey(key string) error {
	if key == "" || len(key) > maxKeyLen {
		return invalid("vector keys must be between 1 and %d characters", maxKeyLen)
	}

	return nil
}

func validateMetadata(ix *Index, meta any) (map[string]any, error) {
	if meta == nil {
		return nil, nil //nolint:nilnil // absent metadata is valid
	}

	m, ok := meta.(map[string]any)
	if !ok {
		return nil, invalid("metadata must be a JSON object")
	}

	if len(m) > maxMetadataKeys {
		return nil, invalid("metadata can have at most %d keys", maxMetadataKeys)
	}

	total, filterable := 0, 0

	for k, v := range m {
		if err := validateMetadataValue(k, v); err != nil {
			return nil, err
		}

		raw, _ := json.Marshal(v)
		total += len(k) + len(raw)

		if ix.filterable(k) {
			filterable += len(k) + len(raw)
		}
	}

	if total > maxMetadataBytes {
		return nil, invalid("metadata exceeds %d bytes", maxMetadataBytes)
	}

	if filterable > maxFilterBytes {
		return nil, invalid(
			"filterable metadata exceeds %d bytes; list large keys in nonFilterableMetadataKeys",
			maxFilterBytes,
		)
	}

	return m, nil
}

func validateMetadataValue(key string, v any) error {
	switch t := v.(type) {
	case string, float64, bool:
		return nil
	case []any:
		for _, e := range t {
			if _, isStr := e.(string); !isStr {
				return invalid("metadata key %q: list values must contain only strings", key)
			}
		}

		return nil
	}

	return invalid("metadata key %q must be a string, number, boolean or list of strings", key)
}

type putVectorsIn struct {
	indexRef
	Vectors []putVector `json:"vectors"`
}

func (b *InMemoryBackend) putVectors(sc scope, in *putVectorsIn) (any, error) {
	if n := len(in.Vectors); n < 1 || n > maxPutBatch {
		return nil, invalid("vectors must contain between 1 and %d items", maxPutBatch)
	}

	b.mu.Lock("putVectors")
	defer b.mu.Unlock()

	_, ix, err := b.resolveIndex(sc, in.indexRef)
	if err != nil {
		return nil, err
	}

	staged := make([]*Vector, 0, len(in.Vectors))

	for i, v := range in.Vectors {
		if err = validateKey(v.Key); err != nil {
			return nil, err
		}

		if err = validateVectorData(ix, v.Data.Float32, "vectors["+itoa(i)+"].data"); err != nil {
			return nil, err
		}

		meta, merr := validateMetadata(ix, v.Metadata)
		if merr != nil {
			return nil, merr
		}

		staged = append(staged, &Vector{Key: v.Key, Data: slices.Clone(v.Data.Float32), Meta: meta})
	}

	set := b.setOf(ix)
	for _, v := range staged {
		set.Vectors[v.Key] = v
	}

	return struct{}{}, nil
}

func itoa(i int) string {
	b, _ := json.Marshal(i)

	return string(b)
}

func validateVectorData(ix *Index, data []float32, field string) error {
	if len(data) != ix.Dimension {
		return invalid("%s has %d dimensions but the vector index has %d", field, len(data), ix.Dimension)
	}

	if ix.DistanceMetric == "cosine" && isZero(data) {
		return invalid("%s is a zero vector, which is not allowed with the cosine distance metric", field)
	}

	return nil
}

func isZero(data []float32) bool {
	for _, f := range data {
		if f != 0 {
			return false
		}
	}

	return true
}

type keysIn struct {
	indexRef
	Keys           []string `json:"keys"`
	ReturnData     bool     `json:"returnData"`
	ReturnMetadata bool     `json:"returnMetadata"`
}

func validateKeys(keys []string) error {
	if n := len(keys); n < 1 || n > maxKeyBatch {
		return invalid("keys must contain between 1 and %d items", maxKeyBatch)
	}

	for _, k := range keys {
		if err := validateKey(k); err != nil {
			return err
		}
	}

	return nil
}

func outputOf(v *Vector, withData, withMeta bool) outVector {
	out := outVector{Key: v.Key}
	if withData {
		out.Data = &vectorData{Float32: slices.Clone(v.Data)}
	}

	if withMeta && v.Meta != nil {
		out.Metadata = maps.Clone(v.Meta)
	}

	return out
}

func (b *InMemoryBackend) getVectors(sc scope, in *keysIn) (any, error) {
	if err := validateKeys(in.Keys); err != nil {
		return nil, err
	}

	b.mu.RLock("getVectors")
	defer b.mu.RUnlock()

	_, ix, err := b.resolveIndex(sc, in.indexRef)
	if err != nil {
		return nil, err
	}

	set := b.setOf(ix)
	out := make([]outVector, 0, len(in.Keys))
	seen := map[string]bool{}

	for _, k := range in.Keys {
		if v, ok := set.Vectors[k]; ok && !seen[k] {
			seen[k] = true

			out = append(out, outputOf(v, in.ReturnData, in.ReturnMetadata))
		}
	}

	return map[string]any{keyVectors: out}, nil
}

func (b *InMemoryBackend) deleteVectors(sc scope, in *keysIn) (any, error) {
	if err := validateKeys(in.Keys); err != nil {
		return nil, err
	}

	b.mu.Lock("deleteVectors")
	defer b.mu.Unlock()

	_, ix, err := b.resolveIndex(sc, in.indexRef)
	if err != nil {
		return nil, err
	}

	set := b.setOf(ix)
	for _, k := range in.Keys {
		delete(set.Vectors, k)
	}

	return struct{}{}, nil
}

type listVectorsIn struct {
	MaxResults   *int `json:"maxResults"`
	SegmentCount *int `json:"segmentCount"`
	indexRef
	NextToken      string `json:"nextToken"`
	SegmentIndex   int    `json:"segmentIndex"`
	ReturnData     bool   `json:"returnData"`
	ReturnMetadata bool   `json:"returnMetadata"`
}

func segmentOf(key string, count int) int {
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))

	return int(h.Sum32() % uint32(count)) //nolint:gosec // count is validated to 1..maxSegments
}

func (b *InMemoryBackend) listVectors(sc scope, in *listVectorsIn) (any, error) {
	size, err := pageSize(in.MaxResults, maxListVectors)
	if err != nil {
		return nil, err
	}

	if in.MaxResults == nil {
		size = defaultListVecs
	}

	if err = validateSegments(in); err != nil {
		return nil, err
	}

	if err = page.ValidateToken(in.NextToken); err != nil {
		return nil, invalid("nextToken is not valid")
	}

	b.mu.RLock("listVectors")
	defer b.mu.RUnlock()

	_, ix, err := b.resolveIndex(sc, in.indexRef)
	if err != nil {
		return nil, err
	}

	set := b.setOf(ix)
	keys := make([]string, 0, len(set.Vectors))

	for k := range set.Vectors {
		if in.SegmentCount == nil || segmentOf(k, *in.SegmentCount) == in.SegmentIndex {
			keys = append(keys, k)
		}
	}

	slices.Sort(keys)

	pg := page.New(keys, in.NextToken, size, defaultListVecs)
	out := make([]outVector, 0, len(pg.Data))

	for _, k := range pg.Data {
		out = append(out, outputOf(set.Vectors[k], in.ReturnData, in.ReturnMetadata))
	}

	resp := map[string]any{keyVectors: out}
	if pg.Next != "" {
		resp["nextToken"] = pg.Next
	}

	return resp, nil
}

func validateSegments(in *listVectorsIn) error {
	if in.SegmentCount == nil {
		if in.SegmentIndex != 0 {
			return invalid("segmentIndex requires segmentCount")
		}

		return nil
	}

	if *in.SegmentCount < 1 || *in.SegmentCount > maxSegments {
		return invalid("segmentCount must be between 1 and %d", maxSegments)
	}

	if in.SegmentIndex < 0 || in.SegmentIndex >= *in.SegmentCount {
		return invalid("segmentIndex must be at least 0 and less than segmentCount")
	}

	return nil
}

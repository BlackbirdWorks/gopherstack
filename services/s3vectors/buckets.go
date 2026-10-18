package s3vectors

import (
	"cmp"
	"encoding/json"
	"slices"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

const (
	arnService = "s3vectors"
	arnFields  = 6
)

func bucketARN(sc scope, name string) string {
	return arn.Build(arnService, sc.region, sc.account, "bucket/"+name)
}

func indexARN(sc scope, bucket, name string) string {
	return arn.Build(arnService, sc.region, sc.account, "bucket/"+bucket+"/index/"+name)
}

type arnParts struct{ account, region, resource string }

// parseARN splits an S3 Vectors ARN into account, Region and the resource path after the service.
func parseARN(a string) (arnParts, bool) {
	parts := strings.SplitN(a, ":", arnFields)
	if len(parts) != arnFields || parts[0] != "arn" || parts[2] != arnService {
		return arnParts{}, false
	}

	return arnParts{account: parts[4], region: parts[3], resource: parts[5]}, true
}

func (b *InMemoryBackend) resolveBucket(sc scope, ref bucketRef) (*Bucket, error) {
	account, region, name := sc.account, sc.region, ref.VectorBucketName

	if ref.VectorBucketArn != "" {
		parsed, ok := parseARN(ref.VectorBucketArn)
		bname, found := strings.CutPrefix(parsed.resource, "bucket/")

		if !ok || !found || strings.Contains(bname, "/") {
			return nil, invalid("vectorBucketArn %q is not a valid vector bucket ARN", ref.VectorBucketArn)
		}

		if name != "" && name != bname {
			return nil, invalid("vectorBucketName and vectorBucketArn refer to different buckets")
		}

		account, region, name = parsed.account, parsed.region, bname
	}

	if name == "" {
		return nil, invalid("either vectorBucketName or vectorBucketArn is required")
	}

	if bk, ok := b.buckets.Get(bucketID(account, region, name)); ok {
		return bk, nil
	}

	return nil, notFound("The specified vector bucket could not be found")
}

type createBucketIn struct {
	Encryption *encryptionIn     `json:"encryptionConfiguration"`
	Tags       map[string]string `json:"tags"`
	Name       string            `json:"vectorBucketName"`
}

func (b *InMemoryBackend) createBucket(sc scope, in *createBucketIn) (any, error) {
	if in.Name == "" {
		return nil, invalid("vectorBucketName is required")
	}

	if err := validateBucketName(in.Name); err != nil {
		return nil, err
	}

	enc, err := validateEncryption(in.Encryption)
	if err != nil {
		return nil, err
	}

	if err = validateTags(in.Tags); err != nil {
		return nil, err
	}

	b.mu.Lock("createBucket")
	defer b.mu.Unlock()

	if b.buckets.Has(bucketID(sc.account, sc.region, in.Name)) {
		return nil, conflict("A vector bucket with the name %q already exists", in.Name)
	}

	if b.countBuckets(sc) >= maxBuckets {
		return nil, quotaExceeded("the account already has %d vector buckets in this Region", maxBuckets)
	}

	now := b.now()
	mode := modeEnhanced

	if now.Before(classicCutoff()) {
		mode = modeClassic
	}

	bk := &Bucket{
		Account: sc.account, Region: sc.region, Name: in.Name, ARN: bucketARN(sc, in.Name), Created: now,
		Encryption: enc, Tags: in.Tags, DefaultIndexMode: mode,
	}
	b.buckets.Put(bk)

	return map[string]any{"vectorBucketArn": bk.ARN}, nil
}

func (b *InMemoryBackend) countBuckets(sc scope) int {
	n := 0

	for _, bk := range b.buckets.All() {
		if bk.Account == sc.account && bk.Region == sc.region {
			n++
		}
	}

	return n
}

func (b *InMemoryBackend) getBucket(sc scope, ref bucketRef) (any, error) {
	b.mu.RLock("getBucket")
	defer b.mu.RUnlock()

	bk, err := b.resolveBucket(sc, ref)
	if err != nil {
		return nil, err
	}

	return map[string]any{"vectorBucket": toBucketOut(bk)}, nil
}

func (b *InMemoryBackend) deleteBucket(sc scope, ref bucketRef) (any, error) {
	b.mu.Lock("deleteBucket")
	defer b.mu.Unlock()

	bk, err := b.resolveBucket(sc, ref)
	if err != nil {
		return nil, err
	}

	if len(b.bucketIndexes(bk)) > 0 {
		return nil, conflict("The vector bucket %q still contains vector indexes", bk.Name)
	}

	b.buckets.Delete(bucketKey(bk))

	return struct{}{}, nil
}

func (b *InMemoryBackend) bucketIndexes(bk *Bucket) []*Index {
	var out []*Index

	for _, i := range b.indexes.All() {
		if i.Account == bk.Account && i.Region == bk.Region && i.Bucket == bk.Name {
			out = append(out, i)
		}
	}

	slices.SortFunc(out, func(x, y *Index) int { return cmp.Compare(x.Name, y.Name) })

	return out
}

type listBucketsIn struct {
	MaxResults *int   `json:"maxResults"`
	NextToken  string `json:"nextToken"`
	Prefix     string `json:"prefix"`
}

func pageSize(requested *int, limit int) (int, error) {
	if requested == nil {
		return limit, nil
	}

	if *requested < 1 || *requested > limit {
		return 0, invalid("maxResults must be between 1 and %d", limit)
	}

	return *requested, nil
}

func (b *InMemoryBackend) listBuckets(sc scope, in *listBucketsIn) (any, error) {
	size, err := pageSize(in.MaxResults, maxListBuckets)
	if err != nil {
		return nil, err
	}

	if err = page.ValidateToken(in.NextToken); err != nil {
		return nil, invalid("nextToken is not valid")
	}

	b.mu.RLock("listBuckets")
	defer b.mu.RUnlock()

	var all []bucketSummary

	for _, bk := range b.buckets.All() {
		if bk.Account == sc.account && bk.Region == sc.region && strings.HasPrefix(bk.Name, in.Prefix) {
			all = append(all, bucketSummary{
				CreationTime: toBucketOut(bk).CreationTime, VectorBucketArn: bk.ARN, VectorBucketName: bk.Name,
			})
		}
	}

	slices.SortFunc(all, func(x, y bucketSummary) int { return cmp.Compare(x.VectorBucketName, y.VectorBucketName) })

	pg := page.New(all, in.NextToken, size, maxListBuckets)
	out := map[string]any{"vectorBuckets": pg.Data}

	if pg.Next != "" {
		out["nextToken"] = pg.Next
	}

	return out, nil
}

type putPolicyIn struct {
	bucketRef
	Policy string `json:"policy"`
}

func (b *InMemoryBackend) putBucketPolicy(sc scope, in *putPolicyIn) (any, error) {
	var doc struct {
		Statement any `json:"Statement"`
	}

	if in.Policy == "" {
		return nil, invalid("policy is required")
	}

	if err := json.Unmarshal([]byte(in.Policy), &doc); err != nil || doc.Statement == nil {
		return nil, invalid("policy must be a JSON policy document with a Statement")
	}

	b.mu.Lock("putBucketPolicy")
	defer b.mu.Unlock()

	bk, err := b.resolveBucket(sc, in.bucketRef)
	if err != nil {
		return nil, err
	}

	bk.Policy = in.Policy

	return struct{}{}, nil
}

func (b *InMemoryBackend) getBucketPolicy(sc scope, ref bucketRef) (any, error) {
	b.mu.RLock("getBucketPolicy")
	defer b.mu.RUnlock()

	bk, err := b.resolveBucket(sc, ref)
	if err != nil {
		return nil, err
	}

	if bk.Policy == "" {
		return nil, notFound("The vector bucket %q has no policy", bk.Name)
	}

	return map[string]any{"policy": bk.Policy}, nil
}

func (b *InMemoryBackend) deleteBucketPolicy(sc scope, ref bucketRef) (any, error) {
	b.mu.Lock("deleteBucketPolicy")
	defer b.mu.Unlock()

	bk, err := b.resolveBucket(sc, ref)
	if err != nil {
		return nil, err
	}

	bk.Policy = ""

	return struct{}{}, nil
}

type defaultModeIn struct {
	bucketRef
	DefaultIndexMode string `json:"defaultIndexMode"`
}

func (b *InMemoryBackend) putDefaultIndexMode(sc scope, in *defaultModeIn) (any, error) {
	if in.DefaultIndexMode != modeClassic && in.DefaultIndexMode != modeEnhanced {
		return nil, invalid("defaultIndexMode must be CLASSIC or ENHANCED")
	}

	b.mu.Lock("putDefaultIndexMode")
	defer b.mu.Unlock()

	bk, err := b.resolveBucket(sc, in.bucketRef)
	if err != nil {
		return nil, err
	}

	if in.DefaultIndexMode == modeClassic && !bk.Created.Before(classicCutoff()) {
		return nil, invalid("CLASSIC is only available for vector buckets created before September 30, 2026")
	}

	bk.DefaultIndexMode = in.DefaultIndexMode

	return struct{}{}, nil
}

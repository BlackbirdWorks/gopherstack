package s3_test

import (
	"fmt"
	"math/rand/v2"
	"sort"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk_s3 "github.com/aws/aws-sdk-go-v2/service/s3"
	sdk_s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/stretchr/testify/require"
)

// randomListingKeys generates a set of unique, randomly nested keys built
// from a small alphabet of ASCII and non-ASCII segments, so the resulting
// bucket has plenty of shared prefixes at multiple delimiter boundaries.
func randomListingKeys(rng *rand.Rand, n int) []string {
	segments := []string{"a", "b", "c", "dir", "sub", "文件", "日本語", "x", "y", "z", "e😀moji", "1", "2"}
	delims := []string{"/", "-"}

	seen := make(map[string]bool, n)
	keys := make([]string, 0, n)

	for len(keys) < n {
		depth := 1 + rng.IntN(3)

		var sb strings.Builder
		for d := range depth {
			if d > 0 {
				sb.WriteString(delims[rng.IntN(len(delims))])
			}
			sb.WriteString(segments[rng.IntN(len(segments))])
		}

		fmt.Fprintf(&sb, "-%d", rng.IntN(1000))

		k := sb.String()
		if !seen[k] {
			seen[k] = true
			keys = append(keys, k)
		}
	}

	return keys
}

// listingCandidatePrefixes derives a handful of prefixes worth testing from
// an already-sorted key set: the empty prefix, a few real prefix cuts taken
// from actual keys (so at least some calls are selective), and one prefix
// that matches nothing.
func listingCandidatePrefixes(rng *rand.Rand, sorted []string) []string {
	prefixes := make([]string, 0, 7)
	prefixes = append(prefixes, "", "zzz-does-not-exist")

	for range 5 {
		k := sorted[rng.IntN(len(sorted))]
		cut := rng.IntN(len(k) + 1)
		prefixes = append(prefixes, k[:cut])
	}

	return prefixes
}

// listingCandidateMarkers derives a few markers worth resuming from: none,
// a few real keys, and a synthetic common-prefix-shaped marker.
func listingCandidateMarkers(rng *rand.Rand, sorted []string) []string {
	markers := make([]string, 0, 4)
	markers = append(markers, "")

	for range 3 {
		markers = append(markers, sorted[rng.IntN(len(sorted))])
	}

	return markers
}

// bruteForceEntry is one lexicographically-ordered slot the reference
// listing produces: either a plain key or a common-prefix group.
type bruteForceEntry struct {
	key  string
	isCP bool
}

// bruteForceList is a from-scratch, unoptimized reference implementation of
// S3's ListObjects semantics (prefix filter, marker/common-prefix-boundary
// skip, delimiter grouping, MaxKeys truncation), independent of the
// production code path under test. It exists purely to catch regressions in
// the sorted-key-index optimization (missed/duplicated/misordered keys,
// off-by-one seeks) -- not to re-validate delimiter-grouping edge cases
// already covered elsewhere.
// filterByPrefix returns the keys in sorted that start with prefix, in order.
func filterByPrefix(sorted []string, prefix string) []string {
	var filtered []string
	for _, k := range sorted {
		if strings.HasPrefix(k, prefix) {
			filtered = append(filtered, k)
		}
	}

	return filtered
}

// filterAfterMarker returns the prefix-filtered keys that lexicographically
// follow marker, honoring the S3 rule that a marker ending in delimiter
// skips the whole common-prefix group it names, not just that exact key.
func filterAfterMarker(filtered []string, delimiter, marker string) []string {
	skipWholePrefix := delimiter != "" && marker != "" && strings.HasSuffix(marker, delimiter)

	var after []string
	for _, k := range filtered {
		switch {
		case marker == "":
			after = append(after, k)
		case skipWholePrefix:
			if k > marker && !strings.HasPrefix(k, marker) {
				after = append(after, k)
			}
		case k > marker:
			after = append(after, k)
		}
	}

	return after
}

// groupByDelimiter turns the post-marker keys into the lexicographically
// ordered mix of plain keys and collapsed common-prefix groups that
// delimiter grouping produces.
func groupByDelimiter(after []string, prefix, delimiter string) []bruteForceEntry {
	var entries []bruteForceEntry

	if delimiter == "" {
		for _, k := range after {
			entries = append(entries, bruteForceEntry{key: k})
		}

		return entries
	}

	var lastCP string

	haveCP := false

	for _, k := range after {
		rest := strings.TrimPrefix(k, prefix)
		if idx := strings.Index(rest, delimiter); idx != -1 {
			cp := prefix + rest[:idx+len(delimiter)]
			if !haveCP || cp != lastCP {
				lastCP = cp
				haveCP = true
				entries = append(entries, bruteForceEntry{key: cp, isCP: true})
			}

			continue
		}

		entries = append(entries, bruteForceEntry{key: k})
	}

	return entries
}

// paginateEntries applies MaxKeys truncation to entries and splits the
// resulting page back into S3's separate Contents/CommonPrefixes lists.
func paginateEntries(entries []bruteForceEntry, maxKeys int32) ([]string, []string, bool, string) {
	if maxKeys <= 0 {
		return nil, nil, len(entries) > 0, ""
	}

	isTruncated := int64(len(entries)) > int64(maxKeys)
	page := entries

	var nextMarker string

	if isTruncated {
		page = entries[:maxKeys]
		nextMarker = page[len(page)-1].key
	}

	var contents, commonPrefixes []string

	for _, e := range page {
		if e.isCP {
			commonPrefixes = append(commonPrefixes, e.key)
		} else {
			contents = append(contents, e.key)
		}
	}

	return contents, commonPrefixes, isTruncated, nextMarker
}

func bruteForceList(keys []string, prefix, delimiter, marker string, maxKeys int32) (
	[]string, []string, bool, string,
) {
	sorted := append([]string(nil), keys...)
	sort.Strings(sorted)

	filtered := filterByPrefix(sorted, prefix)
	after := filterAfterMarker(filtered, delimiter, marker)
	entries := groupByDelimiter(after, prefix, delimiter)

	return paginateEntries(entries, maxKeys)
}

func contentKeys(objs []sdk_s3types.Object) []string {
	if len(objs) == 0 {
		return nil
	}

	keys := make([]string, 0, len(objs))
	for _, o := range objs {
		keys = append(keys, aws.ToString(o.Key))
	}

	return keys
}

func commonPrefixStrings(cps []sdk_s3types.CommonPrefix) []string {
	if len(cps) == 0 {
		return nil
	}

	out := make([]string, 0, len(cps))
	for _, cp := range cps {
		out = append(out, aws.ToString(cp.Prefix))
	}

	return out
}

// TestListObjects_PrefixIndex_Differential compares the production
// ListObjects/ListObjectsV2 implementations (backed by the sorted
// per-bucket key index) against bruteForceList across many randomized
// bucket shapes and list parameters, including full pagination sequences.
func TestListObjects_PrefixIndex_Differential(t *testing.T) {
	t.Parallel()

	seeds := []uint64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 100, 12345}

	for _, seed := range seeds {
		t.Run(fmt.Sprintf("seed_%d", seed), func(t *testing.T) {
			t.Parallel()

			rng := rand.New(rand.NewPCG(seed, seed^0xabcdef))
			keys := randomListingKeys(rng, 30+rng.IntN(120))

			_, backend := newTestHandler(t)
			bucket := "diff-bucket"
			mustCreateBucket(t, backend, bucket)

			for _, k := range keys {
				mustPutObject(t, backend, bucket, k, []byte("x"))
			}

			sorted := append([]string(nil), keys...)
			sort.Strings(sorted)

			delimiters := []string{"", "/", "-"}
			prefixes := listingCandidatePrefixes(rng, sorted)
			markers := listingCandidateMarkers(rng, sorted)
			maxKeysOpts := []int32{0, 1, 2, 3, 5, 1000}

			for _, delim := range delimiters {
				for _, prefix := range prefixes {
					for _, marker := range markers {
						for _, mk := range maxKeysOpts {
							name := fmt.Sprintf("delim=%q/prefix=%q/marker=%q/max=%d", delim, prefix, marker, mk)
							t.Run(name, func(t *testing.T) {
								t.Parallel()

								wantContents, wantCPs, wantTruncated, wantNext := bruteForceList(
									keys, prefix, delim, marker, mk,
								)

								out, err := backend.ListObjects(t.Context(), &sdk_s3.ListObjectsInput{
									Bucket:    aws.String(bucket),
									Prefix:    aws.String(prefix),
									Delimiter: aws.String(delim),
									Marker:    aws.String(marker),
									MaxKeys:   aws.Int32(mk),
								})
								require.NoError(t, err)

								require.Equal(t, wantContents, contentKeys(out.Contents), "Contents")
								require.Equal(t, wantCPs, commonPrefixStrings(out.CommonPrefixes), "CommonPrefixes")
								require.Equal(t, wantTruncated, aws.ToBool(out.IsTruncated), "IsTruncated")
								require.Equal(t, wantNext, aws.ToString(out.NextMarker), "NextMarker")
							})
						}
					}
				}
			}
		})
	}
}

// TestListObjectsV2_PrefixIndex_Differential mirrors the V1 differential
// test but drives StartAfter and pages via ContinuationToken, checking that
// a full pagination walk reassembles exactly the un-paginated reference
// listing with no dropped or duplicated keys/common-prefixes.
func TestListObjectsV2_PrefixIndex_Differential(t *testing.T) {
	t.Parallel()

	seeds := []uint64{11, 22, 33, 44, 55}

	for _, seed := range seeds {
		t.Run(fmt.Sprintf("seed_%d", seed), func(t *testing.T) {
			t.Parallel()

			rng := rand.New(rand.NewPCG(seed, seed^0x123456))
			keys := randomListingKeys(rng, 40+rng.IntN(100))

			_, backend := newTestHandler(t)
			bucket := "diff-bucket-v2"
			mustCreateBucket(t, backend, bucket)

			for _, k := range keys {
				mustPutObject(t, backend, bucket, k, []byte("x"))
			}

			sorted := append([]string(nil), keys...)
			sort.Strings(sorted)

			delimiters := []string{"", "/"}
			prefixes := listingCandidatePrefixes(rng, sorted)

			for _, delim := range delimiters {
				for _, prefix := range prefixes {
					t.Run(fmt.Sprintf("delim=%q/prefix=%q", delim, prefix), func(t *testing.T) {
						t.Parallel()

						wantContents, wantCPs, _, _ := bruteForceList(keys, prefix, delim, "", int32(len(keys)+1))

						var gotContents, gotCPs []string
						token := ""

						for range len(keys) + 2 {
							out, err := backend.ListObjectsV2(t.Context(), &sdk_s3.ListObjectsV2Input{
								Bucket:            aws.String(bucket),
								Prefix:            aws.String(prefix),
								Delimiter:         aws.String(delim),
								MaxKeys:           aws.Int32(3),
								ContinuationToken: aws.String(token),
							})
							require.NoError(t, err)

							gotContents = append(gotContents, contentKeys(out.Contents)...)
							gotCPs = append(gotCPs, commonPrefixStrings(out.CommonPrefixes)...)

							wantCount := int32(len(out.Contents) + len(out.CommonPrefixes))
							require.Equal(t, wantCount, aws.ToInt32(out.KeyCount), "KeyCount")

							if !aws.ToBool(out.IsTruncated) {
								break
							}

							token = aws.ToString(out.NextContinuationToken)
							require.NotEmpty(t, token, "truncated page must carry a continuation token")
						}

						require.Equal(t, wantContents, gotContents, "paginated Contents")
						require.Equal(t, wantCPs, gotCPs, "paginated CommonPrefixes")
					})
				}
			}
		})
	}
}

// TestListObjectVersions_PrefixIndex_Differential exercises the
// ListObjectVersions prefix walk (its own, separately-maintained scan over
// bucket.keyIndex) against the same reference used for ListObjects, using
// one version per key so version-ordering semantics (untouched by this
// change) stay out of scope.
func TestListObjectVersions_PrefixIndex_Differential(t *testing.T) {
	t.Parallel()

	seeds := []uint64{201, 202, 203}

	for _, seed := range seeds {
		t.Run(fmt.Sprintf("seed_%d", seed), func(t *testing.T) {
			t.Parallel()

			rng := rand.New(rand.NewPCG(seed, seed^0x777))
			keys := randomListingKeys(rng, 30+rng.IntN(60))

			_, backend := newTestHandler(t)
			bucket := "diff-bucket-versions"
			mustCreateBucket(t, backend, bucket)

			for _, k := range keys {
				mustPutObject(t, backend, bucket, k, []byte("x"))
			}

			sorted := append([]string(nil), keys...)
			sort.Strings(sorted)

			prefixes := listingCandidatePrefixes(rng, sorted)
			delimiters := []string{"", "/"}

			for _, delim := range delimiters {
				for _, prefix := range prefixes {
					t.Run(fmt.Sprintf("delim=%q/prefix=%q", delim, prefix), func(t *testing.T) {
						t.Parallel()

						wantContents, wantCPs, _, _ := bruteForceList(keys, prefix, delim, "", int32(len(keys)+1))

						out, err := backend.ListObjectVersions(t.Context(), &sdk_s3.ListObjectVersionsInput{
							Bucket:    aws.String(bucket),
							Prefix:    aws.String(prefix),
							Delimiter: aws.String(delim),
							MaxKeys:   aws.Int32(int32(len(keys) + 1)),
						})
						require.NoError(t, err)

						var gotKeys []string
						for _, v := range out.Versions {
							gotKeys = append(gotKeys, aws.ToString(v.Key))
						}

						require.Equal(t, wantContents, gotKeys, "Versions")
						require.Equal(t, wantCPs, commonPrefixStrings(out.CommonPrefixes), "CommonPrefixes")
						require.Empty(t, out.DeleteMarkers)
					})
				}
			}
		})
	}
}

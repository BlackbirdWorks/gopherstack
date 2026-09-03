package macie2_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/macie2"
)

// TestDescribeBuckets_TiedObjectCount_NoDropOrDupAcrossPages proves DescribeBuckets loses
// (or repeats) buckets at a page boundary when several buckets tie on the requested sort
// attribute. sortBuckets' custom-attribute branches (buckets.go) compare only
// ClassifiableObjectCount/ClassifiableSizeInBytes/ObjectCount/SizeInBytes/AccountID/
// BucketName with no secondary key, over a *store.Table map walk whose iteration order
// varies between calls; handleDescribeBuckets pages the resort with an HMAC offset
// cursor (page.NewHMAC). Looped since this depends on map iteration reshuffling the tie
// group between the calls backing page 1 and page 2, which does not reproduce every run.
func TestDescribeBuckets_TiedObjectCount_NoDropOrDupAcrossPages(t *testing.T) {
	t.Parallel()

	for range 30 {
		b := macie2.NewInMemoryBackend("000000000000", "us-east-1")
		h := macie2.NewHandler(b)

		const dupCount = 5
		created := make(map[string]bool, dupCount)

		for i := range dupCount {
			name := fmt.Sprintf("tie-bucket-%02d", i)
			arn := "arn:aws:s3:::" + name
			macie2.SeedS3Bucket(b, macie2.S3BucketMetadata{
				AccountID:   "000000000000",
				BucketArn:   arn,
				BucketName:  name,
				Region:      "us-east-1",
				ObjectCount: 0,
			})
			created[arn] = true
		}

		seen := make(map[string]bool, dupCount)
		body := map[string]any{
			"sortCriteria": map[string]any{"attributeName": "objectCount"},
			"maxResults":   2,
		}

		for range dupCount + 1 {
			rec := doRequest(t, h, http.MethodPost, "/datasources/s3", body)
			require.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

			buckets, _ := resp["buckets"].([]any)
			for _, item := range buckets {
				m, isMap := item.(map[string]any)
				require.True(t, isMap)
				seen[m["bucketArn"].(string)] = true
			}

			nextToken, hasToken := resp["nextToken"].(string)
			if !hasToken {
				break
			}

			body = map[string]any{
				"sortCriteria": map[string]any{"attributeName": "objectCount"},
				"maxResults":   2,
				"nextToken":    nextToken,
			}
		}

		assert.Equal(t, created, seen, "paged DescribeBuckets dropped or duplicated tied buckets across pages")
	}
}

// TestListClassificationJobs_TiedName_NoDropOrDupAcrossPages proves ListClassificationJobs
// loses (or repeats) jobs at a page boundary when several jobs share a Name. Job names
// have no uniqueness constraint, yet sortJobSummaries' "name" branch (classification_jobs.go)
// compares only Name with no secondary key, over a *store.Table map walk whose iteration
// order varies between calls; handleListClassificationJobs pages the resort with an HMAC
// offset cursor. Looped for the same map-iteration-reshuffling reason as the bucket test.
func TestListClassificationJobs_TiedName_NoDropOrDupAcrossPages(t *testing.T) {
	t.Parallel()

	for range 30 {
		h := newTestHandler(t)

		const dupCount = 5
		created := make(map[string]bool, dupCount)

		for range dupCount {
			rec := doRequest(t, h, http.MethodPost, "/jobs", map[string]any{
				"name":    "dup-job-name",
				"jobType": "ONE_TIME",
				"s3JobDefinition": map[string]any{
					"bucketDefinitions": []any{},
				},
			})
			require.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]string
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			created[resp["jobId"]] = true
		}

		seen := make(map[string]bool, dupCount)
		body := map[string]any{
			"sortCriteria": map[string]any{"attributeName": "name"},
			"maxResults":   2,
		}

		for range dupCount + 1 {
			rec := doRequest(t, h, http.MethodPost, "/jobs/list", body)
			require.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

			items, _ := resp["items"].([]any)
			for _, item := range items {
				m, isMap := item.(map[string]any)
				require.True(t, isMap)
				seen[m["jobId"].(string)] = true
			}

			nextToken, hasToken := resp["nextToken"].(string)
			if !hasToken {
				break
			}

			body = map[string]any{
				"sortCriteria": map[string]any{"attributeName": "name"},
				"maxResults":   2,
				"nextToken":    nextToken,
			}
		}

		assert.Equal(t, created, seen, "paged ListClassificationJobs dropped or duplicated tied jobs across pages")
	}
}

// TestListCustomDataIdentifiers_TiedName_NoDropOrDupAcrossPages proves
// ListCustomDataIdentifiers loses (or repeats) identifiers at a page boundary when several
// share a Name. CreateCustomDataIdentifier never checks for an existing Name, yet
// ListCustomDataIdentifiers sorts solely by Name with no secondary key, over a
// *store.Table map walk whose iteration order varies between calls; the handler pages the
// resort with an HMAC offset cursor. Looped for the same map-iteration reason as above.
func TestListCustomDataIdentifiers_TiedName_NoDropOrDupAcrossPages(t *testing.T) {
	t.Parallel()

	for range 30 {
		h := newTestHandler(t)

		const dupCount = 5
		created := make(map[string]bool, dupCount)

		for i := range dupCount {
			rec := doRequest(t, h, http.MethodPost, "/custom-data-identifiers", map[string]any{
				"name":  "dup-cdi-name",
				"regex": fmt.Sprintf(`\d{%d}`, i+1),
			})
			require.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]string
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			created[resp["customDataIdentifierId"]] = true
		}

		seen := make(map[string]bool, dupCount)
		body := map[string]any{"maxResults": 2}

		for range dupCount + 1 {
			rec := doRequest(t, h, http.MethodPost, "/custom-data-identifiers/list", body)
			require.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

			items, _ := resp["items"].([]any)
			for _, item := range items {
				m, isMap := item.(map[string]any)
				require.True(t, isMap)
				seen[m["id"].(string)] = true
			}

			nextToken, hasToken := resp["nextToken"].(string)
			if !hasToken {
				break
			}

			body = map[string]any{"maxResults": 2, "nextToken": nextToken}
		}

		assert.Equal(
			t, created, seen,
			"paged ListCustomDataIdentifiers dropped or duplicated tied identifiers across pages",
		)
	}
}

// TestListAllowLists_TiedName_NoDropOrDupAcrossPages proves ListAllowLists loses (or
// repeats) allow lists at a page boundary when several share a Name. CreateAllowList
// never checks for an existing Name, yet ListAllowLists sorts solely by Name with no
// secondary key, over a *store.Table map walk whose iteration order varies between
// calls; handleListAllowLists pages the resort with an HMAC offset cursor. Looped for the
// same map-iteration reason as above.
func TestListAllowLists_TiedName_NoDropOrDupAcrossPages(t *testing.T) {
	t.Parallel()

	for range 30 {
		h := newTestHandler(t)

		const dupCount = 5
		created := make(map[string]bool, dupCount)

		for i := range dupCount {
			rec := doRequest(t, h, http.MethodPost, "/allow-lists", map[string]any{
				"clientToken": fmt.Sprintf("tok-%d", i),
				"name":        "dup-allow-list-name",
				"criteria":    map[string]any{"regex": "test-\\w+"},
			})
			require.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]string
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			created[resp["id"]] = true
		}

		seen := make(map[string]bool, dupCount)
		path := "/allow-lists?maxResults=2"

		for range dupCount + 1 {
			rec := doRequest(t, h, http.MethodGet, path, nil)
			require.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

			items, _ := resp["allowLists"].([]any)
			for _, item := range items {
				m, isMap := item.(map[string]any)
				require.True(t, isMap)
				seen[m["id"].(string)] = true
			}

			nextToken, hasToken := resp["nextToken"].(string)
			if !hasToken {
				break
			}

			path = "/allow-lists?maxResults=2&nextToken=" + nextToken
		}

		assert.Equal(t, created, seen, "paged ListAllowLists dropped or duplicated tied allow lists across pages")
	}
}

// TestListFindingsFilters_TiedPosition_NoDropOrDupAcrossPages proves ListFindingsFilters
// loses (or repeats) filters at a page boundary when several share a Position (the default
// when a caller does not supply one, per CreateFindingsFilter). ListFindingsFilters sorts
// solely by Position with no secondary key, over a *store.Table map walk whose iteration
// order varies between calls; handleListFindingsFilters pages the resort with an HMAC
// offset cursor. Looped for the same map-iteration reason as above.
func TestListFindingsFilters_TiedPosition_NoDropOrDupAcrossPages(t *testing.T) {
	t.Parallel()

	for i := range 30 {
		h := newTestHandler(t)

		const dupCount = 5
		created := make(map[string]bool, dupCount)

		for j := range dupCount {
			rec := doRequest(t, h, http.MethodPost, "/findingsfilters", map[string]any{
				"name":   fmt.Sprintf("tie-filter-%d-%d", i, j),
				"action": "NOOP",
				// Position omitted: CreateFindingsFilter defaults every filter to
				// position 1, so all five filters tie on the sort key.
			})
			require.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]string
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			created[resp["id"]] = true
		}

		seen := make(map[string]bool, dupCount)
		path := "/findingsfilters?maxResults=2"

		for range dupCount + 1 {
			rec := doRequest(t, h, http.MethodGet, path, nil)
			require.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

			items, _ := resp["findingsFilterListItems"].([]any)
			for _, item := range items {
				m, isMap := item.(map[string]any)
				require.True(t, isMap)
				seen[m["id"].(string)] = true
			}

			nextToken, hasToken := resp["nextToken"].(string)
			if !hasToken {
				break
			}

			path = "/findingsfilters?maxResults=2&nextToken=" + nextToken
		}

		assert.Equal(t, created, seen, "paged ListFindingsFilters dropped or duplicated tied filters across pages")
	}
}

// TestListFindings_TiedType_NoDropOrDupAcrossPages proves ListFindings loses (or repeats)
// findings at a page boundary when several share a Type. CreateSampleFindings computes
// one shared "now" for the whole call and takes Type verbatim from the caller-supplied
// list, so passing the same type five times ties count/createdAt/updatedAt/type/
// severity.score all at once. sortFindings' custom-attribute branches (findings.go)
// compare only that one field with no secondary key, over a *store.Table map walk whose
// iteration order varies between calls; handleListFindings pages the resort with an HMAC
// offset cursor. Looped for the same map-iteration reason as above.
func TestListFindings_TiedType_NoDropOrDupAcrossPages(t *testing.T) {
	t.Parallel()

	for range 30 {
		h := newTestHandler(t)

		const dupCount = 5
		dupTypes := make([]string, dupCount)
		for i := range dupTypes {
			dupTypes[i] = "SensitiveData:S3Object/Personal"
		}

		rec := doRequest(t, h, http.MethodPost, "/findings/sample", map[string]any{"findingTypes": dupTypes})
		require.Equal(t, http.StatusOK, rec.Code)

		// Ground truth: an unpaginated, default-sorted (by unique ID) listing, proven
		// safe above and elsewhere in this file, establishes which IDs actually exist.
		truthRec := doRequest(t, h, http.MethodPost, "/findings", map[string]any{"maxResults": dupCount + 5})
		require.Equal(t, http.StatusOK, truthRec.Code)

		var truthResp map[string]any
		require.NoError(t, json.Unmarshal(truthRec.Body.Bytes(), &truthResp))

		truthIDs, _ := truthResp["findingIds"].([]any)
		require.Len(t, truthIDs, dupCount)

		created := make(map[string]bool, dupCount)
		for _, id := range truthIDs {
			created[id.(string)] = true
		}

		seen := make(map[string]bool, dupCount)
		body := map[string]any{
			"sortCriteria": map[string]any{"attributeName": "type"},
			"maxResults":   2,
		}

		for range dupCount + 1 {
			listRec := doRequest(t, h, http.MethodPost, "/findings", body)
			require.Equal(t, http.StatusOK, listRec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &resp))

			ids, _ := resp["findingIds"].([]any)
			for _, id := range ids {
				seen[id.(string)] = true
			}

			nextToken, hasToken := resp["nextToken"].(string)
			if !hasToken {
				break
			}

			body = map[string]any{
				"sortCriteria": map[string]any{"attributeName": "type"},
				"maxResults":   2,
				"nextToken":    nextToken,
			}
		}

		assert.Equal(t, created, seen, "paged ListFindings dropped or duplicated tied findings across pages")
	}
}

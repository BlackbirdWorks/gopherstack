package mediapackage_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/mediapackage"
)

func createTestOriginEndpointForHarvest(t *testing.T, h *mediapackage.Handler, channelID, epID string) string {
	t.Helper()

	rec := doRequest(t, h, http.MethodPost, "/origin_endpoints", map[string]any{
		"channelId": channelID,
		"id":        epID,
	})
	require.Equal(t, http.StatusCreated, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	return resp["id"].(string)
}

func TestHarvestJob_Create(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(h *mediapackage.Handler) (channelID, epID string)
		body     map[string]any
		check    func(t *testing.T, body []byte)
		name     string
		wantCode int
	}{
		{
			name: "create returns harvest job with arn and status",
			setup: func(h *mediapackage.Handler) (string, string) {
				chID := createTestChannel(t, h)
				epID := createTestOriginEndpointForHarvest(t, h, chID, "ep-harvest")

				return chID, epID
			},
			body: map[string]any{
				"id":               "job-1",
				"originEndpointId": "ep-harvest",
				"startTime":        "2024-01-01T00:00:00Z",
				"endTime":          "2024-01-01T01:00:00Z",
				"s3Destination": map[string]any{
					"bucketName":  "my-bucket",
					"manifestKey": "out/manifest.m3u8",
					"roleArn":     "arn:aws:iam::000000000000:role/harvest-role",
				},
			},
			wantCode: http.StatusCreated,
			check: func(t *testing.T, body []byte) {
				t.Helper()

				var resp map[string]any
				require.NoError(t, json.Unmarshal(body, &resp))
				assert.Equal(t, "job-1", resp["id"])
				assert.Equal(t, "IN_PROGRESS", resp["status"])
				assert.NotEmpty(t, resp["arn"])
				assert.NotEmpty(t, resp["channelId"])
				assert.NotEmpty(t, resp["createdAt"])

				s3 := resp["s3Destination"].(map[string]any)
				assert.Equal(t, "my-bucket", s3["bucketName"])
				assert.Equal(t, "out/manifest.m3u8", s3["manifestKey"])
			},
		},
		{
			name: "duplicate id returns conflict",
			setup: func(h *mediapackage.Handler) (string, string) {
				chID := createTestChannel(t, h)
				epID := createTestOriginEndpointForHarvest(t, h, chID, "ep-dup")

				return chID, epID
			},
			body: map[string]any{
				"id":               "dup-job",
				"originEndpointId": "ep-dup",
				"startTime":        "2024-01-01T00:00:00Z",
				"endTime":          "2024-01-01T01:00:00Z",
				"s3Destination": map[string]any{
					"bucketName":  "b",
					"manifestKey": "m",
					"roleArn":     "r",
				},
			},
			wantCode: http.StatusUnprocessableEntity,
			check: func(t *testing.T, body []byte) {
				t.Helper()
				// first call passed; second call was the duplicate — just verify non-empty body
				assert.NotEmpty(t, body)
			},
		},
		{
			name: "missing origin endpoint returns not found",
			setup: func(h *mediapackage.Handler) (string, string) { //nolint:revive // existing issue.
				return "", ""
			},
			body: map[string]any{
				"id":               "job-missing-ep",
				"originEndpointId": "no-such-ep",
				"startTime":        "2024-01-01T00:00:00Z",
				"endTime":          "2024-01-01T01:00:00Z",
				"s3Destination": map[string]any{
					"bucketName":  "b",
					"manifestKey": "m",
					"roleArn":     "r",
				},
			},
			wantCode: http.StatusNotFound,
		},
		{
			name: "missing id returns unprocessable entity",
			setup: func(h *mediapackage.Handler) (string, string) { //nolint:revive // existing issue.
				return "", ""
			},
			body: map[string]any{
				"originEndpointId": "ep",
				"startTime":        "2024-01-01T00:00:00Z",
				"endTime":          "2024-01-01T01:00:00Z",
				"s3Destination":    map[string]any{"bucketName": "b", "manifestKey": "m", "roleArn": "r"},
			},
			wantCode: http.StatusUnprocessableEntity,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			backend := mediapackage.NewInMemoryBackend("000000000000", "us-east-1")
			h := mediapackage.NewHandler(backend)

			if tc.setup != nil {
				tc.setup(h)
			}

			// For duplicate test: create once first
			if tc.name == "duplicate id returns conflict" {
				doRequest(t, h, http.MethodPost, "/harvest_jobs", tc.body)
			}

			rec := doRequest(t, h, http.MethodPost, "/harvest_jobs", tc.body)
			assert.Equal(t, tc.wantCode, rec.Code)

			if tc.check != nil {
				tc.check(t, rec.Body.Bytes())
			}
		})
	}
}

func TestHarvestJob_Describe(t *testing.T) {
	t.Parallel()

	tests := []struct {
		check    func(t *testing.T, body []byte)
		name     string
		jobID    string
		wantCode int
	}{
		{
			name:     "describe existing job returns full detail",
			jobID:    "job-desc",
			wantCode: http.StatusOK,
			check: func(t *testing.T, body []byte) {
				t.Helper()

				var resp map[string]any
				require.NoError(t, json.Unmarshal(body, &resp))
				assert.Equal(t, "job-desc", resp["id"])
				assert.Equal(t, "IN_PROGRESS", resp["status"])
				assert.NotEmpty(t, resp["arn"])
			},
		},
		{
			name:     "describe missing job returns not found",
			jobID:    "no-such-job",
			wantCode: http.StatusNotFound,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			backend := mediapackage.NewInMemoryBackend("000000000000", "us-east-1")
			h := mediapackage.NewHandler(backend)

			if tc.jobID == "job-desc" {
				chID := createTestChannel(t, h)
				createTestOriginEndpointForHarvest(t, h, chID, "ep-for-desc")
				rec := doRequest(t, h, http.MethodPost, "/harvest_jobs", map[string]any{
					"id":               "job-desc",
					"originEndpointId": "ep-for-desc",
					"startTime":        "2024-01-01T00:00:00Z",
					"endTime":          "2024-01-01T01:00:00Z",
					"s3Destination": map[string]any{
						"bucketName":  "b",
						"manifestKey": "m",
						"roleArn":     "r",
					},
				})
				require.Equal(t, http.StatusCreated, rec.Code)
			}

			rec := doRequest(t, h, http.MethodGet, "/harvest_jobs/"+tc.jobID, nil)
			assert.Equal(t, tc.wantCode, rec.Code)

			if tc.check != nil {
				tc.check(t, rec.Body.Bytes())
			}
		})
	}
}

func TestHarvestJob_List(t *testing.T) {
	t.Parallel()

	tests := []struct {
		check      func(t *testing.T, body []byte)
		name       string
		queryParam string
		wantCode   int
	}{
		{
			name:     "list all jobs returns all",
			wantCode: http.StatusOK,
			check: func(t *testing.T, body []byte) {
				t.Helper()

				var resp map[string]any
				require.NoError(t, json.Unmarshal(body, &resp))
				jobs := resp["harvestJobs"].([]any)
				assert.Len(t, jobs, 4)
			},
		},
		{
			name:       "filter by channel returns subset, excludes other channel",
			wantCode:   http.StatusOK,
			queryParam: "includeChannelId=test-channel",
			check: func(t *testing.T, body []byte) {
				t.Helper()

				var resp map[string]any
				require.NoError(t, json.Unmarshal(body, &resp))
				jobs := resp["harvestJobs"].([]any)
				assert.Len(t, jobs, 3)
				for _, j := range jobs {
					jm := j.(map[string]any)
					assert.Equal(t, "test-channel", jm["channelId"])
					assert.NotEqual(t, "job-other-channel", jm["id"])
				}
			},
		},
		{
			name:       "filter by status returns matching",
			wantCode:   http.StatusOK,
			queryParam: "includeStatus=IN_PROGRESS",
			check: func(t *testing.T, body []byte) {
				t.Helper()

				var resp map[string]any
				require.NoError(t, json.Unmarshal(body, &resp))
				jobs := resp["harvestJobs"].([]any)
				assert.Len(t, jobs, 4)
				for _, j := range jobs {
					jm := j.(map[string]any)
					assert.Equal(t, "IN_PROGRESS", jm["status"])
				}
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			backend := mediapackage.NewInMemoryBackend("000000000000", "us-east-1")
			h := mediapackage.NewHandler(backend)

			chID := createTestChannel(t, h)

			for i := range 3 {
				epID := fmt.Sprintf("ep-list-%d", i)
				createTestOriginEndpointForHarvest(t, h, chID, epID)
				rec := doRequest(t, h, http.MethodPost, "/harvest_jobs", map[string]any{
					"id":               fmt.Sprintf("job-list-%d", i),
					"originEndpointId": epID,
					"startTime":        "2024-01-01T00:00:00Z",
					"endTime":          "2024-01-01T01:00:00Z",
					"s3Destination":    map[string]any{"bucketName": "b", "manifestKey": "m", "roleArn": "r"},
				})
				require.Equal(t, http.StatusCreated, rec.Code)
			}

			otherChRec := doRequest(t, h, http.MethodPost, "/channels", map[string]any{
				"id": "other-channel",
			})
			require.Equal(t, http.StatusCreated, otherChRec.Code)

			otherEPID := createTestOriginEndpointForHarvest(t, h, "other-channel", "ep-other-channel")
			otherJobRec := doRequest(t, h, http.MethodPost, "/harvest_jobs", map[string]any{
				"id":               "job-other-channel",
				"originEndpointId": otherEPID,
				"startTime":        "2024-01-01T00:00:00Z",
				"endTime":          "2024-01-01T01:00:00Z",
				"s3Destination":    map[string]any{"bucketName": "b", "manifestKey": "m", "roleArn": "r"},
			})
			require.Equal(t, http.StatusCreated, otherJobRec.Code)

			path := "/harvest_jobs"
			if tc.queryParam != "" {
				path += "?" + tc.queryParam
			}

			rec := doRequest(t, h, http.MethodGet, path, nil)
			assert.Equal(t, tc.wantCode, rec.Code)

			if tc.check != nil {
				tc.check(t, rec.Body.Bytes())
			}
		})
	}
}

func TestHarvestJob_CycleCreateDescribeList(t *testing.T) {
	t.Parallel()

	backend := mediapackage.NewInMemoryBackend("000000000000", "us-east-1")
	h := mediapackage.NewHandler(backend)

	// Setup: channel + endpoint
	chID := createTestChannel(t, h)
	createTestOriginEndpointForHarvest(t, h, chID, "ep-cycle")

	s3Body := map[string]any{
		"bucketName":  "cycle-bucket",
		"manifestKey": "cycle/manifest.m3u8",
		"roleArn":     "arn:aws:iam::000000000000:role/r",
	}

	// Create
	rec := doRequest(t, h, http.MethodPost, "/harvest_jobs", map[string]any{
		"id":               "cycle-job",
		"originEndpointId": "ep-cycle",
		"startTime":        "2024-06-01T00:00:00Z",
		"endTime":          "2024-06-01T02:00:00Z",
		"s3Destination":    s3Body,
	})
	require.Equal(t, http.StatusCreated, rec.Code)
	assert.Equal(t, 1, mediapackage.HarvestJobCount(backend))

	var created map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &created))
	assert.Equal(t, "cycle-job", created["id"])
	assert.Contains(t, created["arn"].(string), "harvest_jobs/cycle-job")

	// Describe
	rec2 := doRequest(t, h, http.MethodGet, "/harvest_jobs/cycle-job", nil)
	require.Equal(t, http.StatusOK, rec2.Code)

	var described map[string]any
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &described))
	assert.Equal(t, "cycle-job", described["id"])
	assert.Equal(t, created["arn"], described["arn"])
	assert.Equal(t, "2024-06-01T00:00:00Z", described["startTime"])
	assert.Equal(t, "2024-06-01T02:00:00Z", described["endTime"])

	// List
	rec3 := doRequest(t, h, http.MethodGet, "/harvest_jobs", nil)
	require.Equal(t, http.StatusOK, rec3.Code)

	var listed map[string]any
	require.NoError(t, json.Unmarshal(rec3.Body.Bytes(), &listed))
	jobs := listed["harvestJobs"].([]any)
	assert.Len(t, jobs, 1)

	job := jobs[0].(map[string]any)
	s3 := job["s3Destination"].(map[string]any)
	assert.Equal(t, "cycle-bucket", s3["bucketName"])
	assert.Equal(t, "cycle/manifest.m3u8", s3["manifestKey"])
}

// TestHarvestJob_Create_NeverClaimsSuccessBeforeWorkHappens verifies
// CreateHarvestJob does not synchronously claim SUCCEEDED. Real MediaPackage
// harvest jobs start IN_PROGRESS and transition to SUCCEEDED/FAILED
// asynchronously as content is actually copied to S3 (types.HarvestJob.Status
// doc comment, aws-sdk-go-v2/service/mediapackage@v1.42.4,
// types/types.go:291-294); this backend does not perform that copy, so
// SUCCEEDED on create would claim S3 work that never happened.
func TestHarvestJob_Create_NeverClaimsSuccessBeforeWorkHappens(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	chID := createTestChannel(t, h)
	createTestOriginEndpointForHarvest(t, h, chID, "ep-async")

	rec := doRequest(t, h, http.MethodPost, "/harvest_jobs", map[string]any{
		"id":               "job-async",
		"originEndpointId": "ep-async",
		"startTime":        "2024-01-01T00:00:00Z",
		"endTime":          "2024-01-01T01:00:00Z",
		"s3Destination": map[string]any{
			"bucketName":  "b",
			"manifestKey": "m",
			"roleArn":     "r",
		},
	})
	require.Equal(t, http.StatusCreated, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "IN_PROGRESS", resp["status"])
}

// TestHarvestJob_RequiredFields verifies that missing required fields
// return 422 rather than 500.
func TestHarvestJob_RequiredFields(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     map[string]any
		name     string
		wantCode int
	}{
		{
			name: "missing id returns 422",
			body: map[string]any{
				"originEndpointId": "ep",
				"startTime":        "2024-01-01T00:00:00Z",
				"endTime":          "2024-01-02T00:00:00Z",
				"s3Destination":    map[string]any{"bucketName": "b", "manifestKey": "m", "roleArn": "r"},
			},
			wantCode: http.StatusUnprocessableEntity,
		},
		{
			name: "missing originEndpointId returns 422",
			body: map[string]any{
				"id":            "job1",
				"startTime":     "2024-01-01T00:00:00Z",
				"endTime":       "2024-01-02T00:00:00Z",
				"s3Destination": map[string]any{"bucketName": "b", "manifestKey": "m", "roleArn": "r"},
			},
			wantCode: http.StatusUnprocessableEntity,
		},
		{
			name: "missing startTime returns 422",
			body: map[string]any{
				"id":               "job2",
				"originEndpointId": "ep",
				"endTime":          "2024-01-02T00:00:00Z",
				"s3Destination":    map[string]any{"bucketName": "b", "manifestKey": "m", "roleArn": "r"},
			},
			wantCode: http.StatusUnprocessableEntity,
		},
		{
			name: "missing endTime returns 422",
			body: map[string]any{
				"id":               "job3",
				"originEndpointId": "ep",
				"startTime":        "2024-01-01T00:00:00Z",
				"s3Destination":    map[string]any{"bucketName": "b", "manifestKey": "m", "roleArn": "r"},
			},
			wantCode: http.StatusUnprocessableEntity,
		},
		{
			name: "missing s3Destination returns 422",
			body: map[string]any{
				"id":               "job4",
				"originEndpointId": "ep",
				"startTime":        "2024-01-01T00:00:00Z",
				"endTime":          "2024-01-02T00:00:00Z",
			},
			wantCode: http.StatusUnprocessableEntity,
		},
		{
			name: "s3Destination missing bucketName returns 422",
			body: map[string]any{
				"id":               "job5",
				"originEndpointId": "ep",
				"startTime":        "2024-01-01T00:00:00Z",
				"endTime":          "2024-01-02T00:00:00Z",
				"s3Destination":    map[string]any{"manifestKey": "m", "roleArn": "r"},
			},
			wantCode: http.StatusUnprocessableEntity,
		},
		{
			name: "s3Destination missing manifestKey returns 422",
			body: map[string]any{
				"id":               "job6",
				"originEndpointId": "ep",
				"startTime":        "2024-01-01T00:00:00Z",
				"endTime":          "2024-01-02T00:00:00Z",
				"s3Destination":    map[string]any{"bucketName": "b", "roleArn": "r"},
			},
			wantCode: http.StatusUnprocessableEntity,
		},
		{
			name: "s3Destination missing roleArn returns 422",
			body: map[string]any{
				"id":               "job7",
				"originEndpointId": "ep",
				"startTime":        "2024-01-01T00:00:00Z",
				"endTime":          "2024-01-02T00:00:00Z",
				"s3Destination":    map[string]any{"bucketName": "b", "manifestKey": "m"},
			},
			wantCode: http.StatusUnprocessableEntity,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			code, _ := doRequestJSON(t, h, http.MethodPost, "/harvest_jobs", tc.body)
			assert.Equal(t, tc.wantCode, code)
		})
	}
}

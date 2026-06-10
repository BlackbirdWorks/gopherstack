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
		"ChannelId": channelID,
		"Id":        epID,
	})
	require.Equal(t, http.StatusCreated, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	return resp["Id"].(string)
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
				"Id":               "job-1",
				"OriginEndpointId": "ep-harvest",
				"StartTime":        "2024-01-01T00:00:00Z",
				"EndTime":          "2024-01-01T01:00:00Z",
				"S3Destination": map[string]any{
					"BucketName":  "my-bucket",
					"ManifestKey": "out/manifest.m3u8",
					"RoleArn":     "arn:aws:iam::000000000000:role/harvest-role",
				},
			},
			wantCode: http.StatusCreated,
			check: func(t *testing.T, body []byte) {
				t.Helper()

				var resp map[string]any
				require.NoError(t, json.Unmarshal(body, &resp))
				assert.Equal(t, "job-1", resp["Id"])
				assert.Equal(t, "SUCCEEDED", resp["Status"])
				assert.NotEmpty(t, resp["Arn"])
				assert.NotEmpty(t, resp["ChannelId"])
				assert.NotEmpty(t, resp["CreatedAt"])

				s3 := resp["S3Destination"].(map[string]any)
				assert.Equal(t, "my-bucket", s3["BucketName"])
				assert.Equal(t, "out/manifest.m3u8", s3["ManifestKey"])
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
				"Id":               "dup-job",
				"OriginEndpointId": "ep-dup",
				"StartTime":        "2024-01-01T00:00:00Z",
				"EndTime":          "2024-01-01T01:00:00Z",
				"S3Destination": map[string]any{
					"BucketName":  "b",
					"ManifestKey": "m",
					"RoleArn":     "r",
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
				"Id":               "job-missing-ep",
				"OriginEndpointId": "no-such-ep",
				"StartTime":        "2024-01-01T00:00:00Z",
				"EndTime":          "2024-01-01T01:00:00Z",
				"S3Destination": map[string]any{
					"BucketName":  "b",
					"ManifestKey": "m",
					"RoleArn":     "r",
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
				"OriginEndpointId": "ep",
				"StartTime":        "2024-01-01T00:00:00Z",
				"EndTime":          "2024-01-01T01:00:00Z",
				"S3Destination":    map[string]any{"BucketName": "b", "ManifestKey": "m", "RoleArn": "r"},
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
				assert.Equal(t, "job-desc", resp["Id"])
				assert.Equal(t, "SUCCEEDED", resp["Status"])
				assert.NotEmpty(t, resp["Arn"])
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
					"Id":               "job-desc",
					"OriginEndpointId": "ep-for-desc",
					"StartTime":        "2024-01-01T00:00:00Z",
					"EndTime":          "2024-01-01T01:00:00Z",
					"S3Destination": map[string]any{
						"BucketName":  "b",
						"ManifestKey": "m",
						"RoleArn":     "r",
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
				jobs := resp["HarvestJobs"].([]any)
				assert.Len(t, jobs, 3)
			},
		},
		{
			name:       "filter by channel returns subset",
			wantCode:   http.StatusOK,
			queryParam: "includeChannelId=test-channel",
			check: func(t *testing.T, body []byte) {
				t.Helper()

				var resp map[string]any
				require.NoError(t, json.Unmarshal(body, &resp))
				jobs := resp["HarvestJobs"].([]any)
				assert.NotEmpty(t, jobs)
				for _, j := range jobs {
					jm := j.(map[string]any)
					assert.Equal(t, "test-channel", jm["ChannelId"])
				}
			},
		},
		{
			name:       "filter by status returns matching",
			wantCode:   http.StatusOK,
			queryParam: "includeStatus=SUCCEEDED",
			check: func(t *testing.T, body []byte) {
				t.Helper()

				var resp map[string]any
				require.NoError(t, json.Unmarshal(body, &resp))
				jobs := resp["HarvestJobs"].([]any)
				assert.Len(t, jobs, 3)
				for _, j := range jobs {
					jm := j.(map[string]any)
					assert.Equal(t, "SUCCEEDED", jm["Status"])
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
					"Id":               fmt.Sprintf("job-list-%d", i),
					"OriginEndpointId": epID,
					"StartTime":        "2024-01-01T00:00:00Z",
					"EndTime":          "2024-01-01T01:00:00Z",
					"S3Destination":    map[string]any{"BucketName": "b", "ManifestKey": "m", "RoleArn": "r"},
				})
				require.Equal(t, http.StatusCreated, rec.Code)
			}

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

func TestRotateIngestEndpointCredentials(t *testing.T) {
	t.Parallel()

	tests := []struct {
		check    func(t *testing.T, body []byte, oldPassword string)
		name     string
		wantCode int
		badEpID  bool
		badChID  bool
	}{
		{
			name:     "rotate credentials changes password",
			wantCode: http.StatusOK,
			check: func(t *testing.T, body []byte, oldPassword string) {
				t.Helper()

				var resp map[string]any
				require.NoError(t, json.Unmarshal(body, &resp))
				assert.NotEmpty(t, resp["Id"])

				hls := resp["HlsIngest"].(map[string]any)
				eps := hls["IngestEndpoints"].([]any)
				require.NotEmpty(t, eps)

				// Find the rotated endpoint — at least one should have changed password
				rotated := false
				for _, ep := range eps {
					epm := ep.(map[string]any)
					if epm["Password"].(string) != oldPassword {
						rotated = true

						break
					}
				}

				assert.True(t, rotated, "expected at least one endpoint to have new password")
			},
		},
		{
			name:     "bad channel returns not found",
			badChID:  true,
			wantCode: http.StatusNotFound,
		},
		{
			name:     "bad ingest endpoint returns not found",
			badEpID:  true,
			wantCode: http.StatusNotFound,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			backend := mediapackage.NewInMemoryBackend("000000000000", "us-east-1")
			h := mediapackage.NewHandler(backend)

			// Create channel and capture original ingest endpoint info
			rec := doRequest(t, h, http.MethodPost, "/channels", map[string]any{
				"Id": "ch-rotate",
			})
			require.Equal(t, http.StatusCreated, rec.Code)

			var chResp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &chResp))
			hls := chResp["HlsIngest"].(map[string]any)
			eps := hls["IngestEndpoints"].([]any)
			require.NotEmpty(t, eps)

			firstEP := eps[0].(map[string]any)
			epID := firstEP["Id"].(string)
			oldPassword := firstEP["Password"].(string)

			channelID := "ch-rotate"
			ingestEPID := epID

			if tc.badChID {
				channelID = "no-such-channel"
			}

			if tc.badEpID {
				ingestEPID = "no-such-ep"
			}

			path := fmt.Sprintf("/channels/%s/ingest_endpoints/%s/credentials", channelID, ingestEPID)
			rec2 := doRequest(t, h, http.MethodPut, path, nil)
			assert.Equal(t, tc.wantCode, rec2.Code)

			if tc.check != nil {
				tc.check(t, rec2.Body.Bytes(), oldPassword)
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
		"BucketName":  "cycle-bucket",
		"ManifestKey": "cycle/manifest.m3u8",
		"RoleArn":     "arn:aws:iam::000000000000:role/r",
	}

	// Create
	rec := doRequest(t, h, http.MethodPost, "/harvest_jobs", map[string]any{
		"Id":               "cycle-job",
		"OriginEndpointId": "ep-cycle",
		"StartTime":        "2024-06-01T00:00:00Z",
		"EndTime":          "2024-06-01T02:00:00Z",
		"S3Destination":    s3Body,
	})
	require.Equal(t, http.StatusCreated, rec.Code)
	assert.Equal(t, 1, mediapackage.HarvestJobCount(backend))

	var created map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &created))
	assert.Equal(t, "cycle-job", created["Id"])
	assert.Contains(t, created["Arn"].(string), "harvest_jobs/cycle-job")

	// Describe
	rec2 := doRequest(t, h, http.MethodGet, "/harvest_jobs/cycle-job", nil)
	require.Equal(t, http.StatusOK, rec2.Code)

	var described map[string]any
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &described))
	assert.Equal(t, "cycle-job", described["Id"])
	assert.Equal(t, created["Arn"], described["Arn"])
	assert.Equal(t, "2024-06-01T00:00:00Z", described["StartTime"])
	assert.Equal(t, "2024-06-01T02:00:00Z", described["EndTime"])

	// List
	rec3 := doRequest(t, h, http.MethodGet, "/harvest_jobs", nil)
	require.Equal(t, http.StatusOK, rec3.Code)

	var listed map[string]any
	require.NoError(t, json.Unmarshal(rec3.Body.Bytes(), &listed))
	jobs := listed["HarvestJobs"].([]any)
	assert.Len(t, jobs, 1)

	job := jobs[0].(map[string]any)
	s3 := job["S3Destination"].(map[string]any)
	assert.Equal(t, "cycle-bucket", s3["BucketName"])
	assert.Equal(t, "cycle/manifest.m3u8", s3["ManifestKey"])
}

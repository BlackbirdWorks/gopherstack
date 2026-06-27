package s3control_test

import (
	"encoding/xml"
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/s3control"
)

func TestParity_ListAccessGrantsInstances(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		createAGI bool
		wantCode  int
		wantCount int
	}{
		{
			name:      "no_instance_returns_empty",
			createAGI: false,
			wantCode:  http.StatusOK,
			wantCount: 0,
		},
		{
			name:      "created_instance_returned",
			createAGI: true,
			wantCode:  http.StatusOK,
			wantCount: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestS3ControlHandler(t)
			if tt.createAGI {
				h.Backend.AddAccessGrantsInstanceInternal("acct1", "")
			}

			rec := doS3Request(t, h, http.MethodGet, "/v20180820/accessgrantsinstances", "")
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantCode == http.StatusOK {
				var out struct {
					XMLName               xml.Name `xml:"ListAccessGrantsInstancesResult"`
					AccessGrantsInstances []struct {
						AccessGrantsInstanceID string `xml:"AccessGrantsInstanceId"`
					} `xml:"AccessGrantsInstancesList>AccessGrantsInstance"`
				}
				require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &out))
				assert.Len(t, out.AccessGrantsInstances, tt.wantCount)
			}
		})
	}
}

func TestParity_ListAccessPoints_Pagination(t *testing.T) {
	t.Parallel()

	b := s3control.NewInMemoryBackend()
	for i := range 4 {
		b.AddAccessPointInternal("acct1", fmt.Sprintf("ap-%d", i), fmt.Sprintf("bucket-%d", i))
	}
	h := s3control.NewHandler(b)

	tests := []struct {
		path          string
		name          string
		wantLen       int
		wantNextToken bool
	}{
		{
			name:          "no_limit_returns_all",
			path:          "/v20180820/accesspoint",
			wantLen:       4,
			wantNextToken: false,
		},
		{
			name:          "page1_two_items",
			path:          "/v20180820/accesspoint?maxResults=2",
			wantLen:       2,
			wantNextToken: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rec := doS3Request(t, h, http.MethodGet, tt.path, "")
			require.Equal(t, http.StatusOK, rec.Code)

			var out struct {
				XMLName      xml.Name `xml:"ListAccessPointsResult"`
				NextToken    string   `xml:"NextToken"`
				AccessPoints []struct {
					Name string `xml:"Name"`
				} `xml:"AccessPointList>AccessPoint"`
			}
			require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &out))
			assert.Len(t, out.AccessPoints, tt.wantLen)
			if tt.wantNextToken {
				assert.NotEmpty(t, out.NextToken)
			} else {
				assert.Empty(t, out.NextToken)
			}
		})
	}
}

func TestParity_ListAccessPoints_BucketFilter(t *testing.T) {
	t.Parallel()

	b := s3control.NewInMemoryBackend()
	b.AddAccessPointInternal("acct1", "ap-a1", "bucket-a")
	b.AddAccessPointInternal("acct1", "ap-a2", "bucket-a")
	b.AddAccessPointInternal("acct1", "ap-b1", "bucket-b")
	h := s3control.NewHandler(b)

	tests := []struct {
		path    string
		name    string
		wantLen int
	}{
		{name: "filter_bucket_a", path: "/v20180820/accesspoint?bucket=bucket-a", wantLen: 2},
		{name: "filter_bucket_b", path: "/v20180820/accesspoint?bucket=bucket-b", wantLen: 1},
		{name: "no_filter_all", path: "/v20180820/accesspoint", wantLen: 3},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rec := doS3Request(t, h, http.MethodGet, tt.path, "")
			require.Equal(t, http.StatusOK, rec.Code)

			var out struct {
				XMLName      xml.Name `xml:"ListAccessPointsResult"`
				AccessPoints []struct {
					Name string `xml:"Name"`
				} `xml:"AccessPointList>AccessPoint"`
			}
			require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &out))
			assert.Len(t, out.AccessPoints, tt.wantLen)
		})
	}
}

func TestParity_ListAccessGrants_Pagination(t *testing.T) {
	t.Parallel()

	b := s3control.NewInMemoryBackend()
	b.AddAccessGrantsInstanceInternal("acct1", "")
	loc := b.CreateAccessGrantsLocation("acct1", "s3://", "arn:aws:iam::123456789012:role/role")
	for range 4 {
		b.AddAccessGrantInternal("acct1", loc.AccessGrantsLocationID, "IAM", "arn:aws:iam::123456789012:user/u", "READ")
	}
	h := s3control.NewHandler(b)

	tests := []struct {
		path          string
		name          string
		wantLen       int
		wantNextToken bool
	}{
		{
			name:          "no_limit_returns_all",
			path:          "/v20180820/accessgrantsinstance/grant",
			wantLen:       4,
			wantNextToken: false,
		},
		{
			name:          "page1_two_items",
			path:          "/v20180820/accessgrantsinstance/grant?maxResults=2",
			wantLen:       2,
			wantNextToken: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rec := doS3Request(t, h, http.MethodGet, tt.path, "")
			require.Equal(t, http.StatusOK, rec.Code)

			var out struct {
				XMLName      xml.Name `xml:"ListAccessGrantsResult"`
				NextToken    string   `xml:"NextToken"`
				AccessGrants []struct {
					AccessGrantID string `xml:"AccessGrantId"`
				} `xml:"AccessGrantsList>AccessGrant"`
			}
			require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &out))
			assert.Len(t, out.AccessGrants, tt.wantLen)
			if tt.wantNextToken {
				assert.NotEmpty(t, out.NextToken)
			} else {
				assert.Empty(t, out.NextToken)
			}
		})
	}
}

func TestParity_ListAccessGrantsLocations_Pagination(t *testing.T) {
	t.Parallel()

	b := s3control.NewInMemoryBackend()
	b.AddAccessGrantsInstanceInternal("acct1", "")
	for i := range 4 {
		b.CreateAccessGrantsLocation(
			"acct1",
			fmt.Sprintf("s3://bucket-%d/", i),
			"arn:aws:iam::123456789012:role/role",
		)
	}
	h := s3control.NewHandler(b)

	tests := []struct {
		path          string
		name          string
		wantLen       int
		wantNextToken bool
	}{
		{
			name:          "no_limit_returns_all",
			path:          "/v20180820/accessgrantsinstance/location",
			wantLen:       4,
			wantNextToken: false,
		},
		{
			name:          "page1_two_items",
			path:          "/v20180820/accessgrantsinstance/location?maxResults=2",
			wantLen:       2,
			wantNextToken: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rec := doS3Request(t, h, http.MethodGet, tt.path, "")
			require.Equal(t, http.StatusOK, rec.Code)

			var out struct {
				XMLName   xml.Name `xml:"ListAccessGrantsLocationsResult"`
				NextToken string   `xml:"NextToken"`
				Locations []struct {
					AccessGrantsLocationID string `xml:"AccessGrantsLocationId"`
				} `xml:"AccessGrantsLocationsList>AccessGrantsLocation"`
			}
			require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &out))
			assert.Len(t, out.Locations, tt.wantLen)
			if tt.wantNextToken {
				assert.NotEmpty(t, out.NextToken)
			} else {
				assert.Empty(t, out.NextToken)
			}
		})
	}
}

func TestParity_ListJobs_Pagination(t *testing.T) {
	t.Parallel()

	b := s3control.NewInMemoryBackend()
	for range 4 {
		b.AddBatchJobInternal("acct1", "arn:aws:iam::000000000000:role/r", 1)
	}
	h := s3control.NewHandler(b)

	tests := []struct {
		path          string
		name          string
		wantLen       int
		wantNextToken bool
	}{
		{
			name:          "no_limit_returns_all",
			path:          "/v20180820/jobs",
			wantLen:       4,
			wantNextToken: false,
		},
		{
			name:          "page1_two_items",
			path:          "/v20180820/jobs?maxResults=2",
			wantLen:       2,
			wantNextToken: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rec := doS3Request(t, h, http.MethodGet, tt.path, "")
			require.Equal(t, http.StatusOK, rec.Code)

			var out struct {
				XMLName   xml.Name `xml:"ListJobsResult"`
				NextToken string   `xml:"NextToken"`
				Jobs      []struct {
					JobID string `xml:"JobId"`
				} `xml:"Jobs>member"`
			}
			require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &out))
			assert.Len(t, out.Jobs, tt.wantLen)
			if tt.wantNextToken {
				assert.NotEmpty(t, out.NextToken)
			} else {
				assert.Empty(t, out.NextToken)
			}
		})
	}
}

func TestParity_ListJobs_JobStatusesFilter(t *testing.T) {
	t.Parallel()

	b := s3control.NewInMemoryBackend()
	j1 := b.AddBatchJobInternal("acct1", "arn:aws:iam::000000000000:role/r", 1)
	j2 := b.AddBatchJobInternal("acct1", "arn:aws:iam::000000000000:role/r", 1)
	_ = b.AddBatchJobInternal("acct1", "arn:aws:iam::000000000000:role/r", 1)
	_, _ = b.UpdateJobStatus("acct1", j1.JobID, "Active")
	_, _ = b.UpdateJobStatus("acct1", j2.JobID, "Cancelled")
	h := s3control.NewHandler(b)

	tests := []struct {
		path    string
		name    string
		wantLen int
	}{
		{
			name:    "no_filter_returns_all",
			path:    "/v20180820/jobs",
			wantLen: 3,
		},
		{
			name:    "filter_active_only",
			path:    "/v20180820/jobs?jobStatuses=Active",
			wantLen: 1,
		},
		{
			name:    "filter_active_and_cancelled",
			path:    "/v20180820/jobs?jobStatuses=Active&jobStatuses=Cancelled",
			wantLen: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rec := doS3Request(t, h, http.MethodGet, tt.path, "")
			require.Equal(t, http.StatusOK, rec.Code)

			var out struct {
				XMLName xml.Name `xml:"ListJobsResult"`
				Jobs    []struct {
					JobID string `xml:"JobId"`
				} `xml:"Jobs>member"`
			}
			require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &out))
			assert.Len(t, out.Jobs, tt.wantLen, "path: %s", tt.path)
		})
	}
}

func TestParity_ListMultiRegionAccessPoints_Pagination(t *testing.T) {
	t.Parallel()

	b := s3control.NewInMemoryBackend()
	for i := range 4 {
		b.CreateMultiRegionAccessPoint("acct1", fmt.Sprintf("mrap-%d", i), "")
	}
	h := s3control.NewHandler(b)

	tests := []struct {
		path          string
		name          string
		wantLen       int
		wantNextToken bool
	}{
		{
			name:          "no_limit_returns_all",
			path:          "/v20180820/mrap/instances",
			wantLen:       4,
			wantNextToken: false,
		},
		{
			name:          "page1_two_items",
			path:          "/v20180820/mrap/instances?maxResults=2",
			wantLen:       2,
			wantNextToken: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rec := doS3Request(t, h, http.MethodGet, tt.path, "")
			require.Equal(t, http.StatusOK, rec.Code)

			var out struct {
				XMLName      xml.Name `xml:"ListMultiRegionAccessPointsResult"`
				NextToken    string   `xml:"NextToken"`
				AccessPoints []struct {
					Name string `xml:"Name"`
				} `xml:"AccessPoints>item"`
			}
			require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &out))
			assert.Len(t, out.AccessPoints, tt.wantLen)
			if tt.wantNextToken {
				assert.NotEmpty(t, out.NextToken)
			} else {
				assert.Empty(t, out.NextToken)
			}
		})
	}
}

func TestParity_ListRegionalBuckets_Pagination(t *testing.T) {
	t.Parallel()

	b := s3control.NewInMemoryBackend()
	for i := range 4 {
		b.CreateBucket("acct1", fmt.Sprintf("bucket-%d", i))
	}
	h := s3control.NewHandler(b)

	tests := []struct {
		path          string
		name          string
		wantLen       int
		wantNextToken bool
	}{
		{
			name:          "no_limit_returns_all",
			path:          "/v20180820/bucket",
			wantLen:       4,
			wantNextToken: false,
		},
		{
			name:          "page1_two_items",
			path:          "/v20180820/bucket?maxResults=2",
			wantLen:       2,
			wantNextToken: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rec := doS3Request(t, h, http.MethodGet, tt.path, "")
			require.Equal(t, http.StatusOK, rec.Code)

			var out struct {
				XMLName   xml.Name `xml:"ListRegionalBucketsResult"`
				NextToken string   `xml:"NextToken"`
				Buckets   []struct {
					Bucket string `xml:"Bucket"`
				} `xml:"RegionalBucketList>RegionalBucket"`
			}
			require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &out))
			assert.Len(t, out.Buckets, tt.wantLen)
			if tt.wantNextToken {
				assert.NotEmpty(t, out.NextToken)
			} else {
				assert.Empty(t, out.NextToken)
			}
		})
	}
}

func TestParity_ListAccessPointsForObjectLambda_Pagination(t *testing.T) {
	t.Parallel()

	b := s3control.NewInMemoryBackend()
	for i := range 4 {
		b.CreateAccessPointForObjectLambda("acct1", fmt.Sprintf("olap-%d", i))
	}
	h := s3control.NewHandler(b)

	tests := []struct {
		path          string
		name          string
		wantLen       int
		wantNextToken bool
	}{
		{
			name:          "no_limit_returns_all",
			path:          "/v20180820/accesspointforobjectlambda",
			wantLen:       4,
			wantNextToken: false,
		},
		{
			name:          "page1_two_items",
			path:          "/v20180820/accesspointforobjectlambda?maxResults=2",
			wantLen:       2,
			wantNextToken: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rec := doS3Request(t, h, http.MethodGet, tt.path, "")
			require.Equal(t, http.StatusOK, rec.Code)

			var out struct {
				XMLName                     xml.Name `xml:"ListAccessPointsForObjectLambdaResult"`
				NextToken                   string   `xml:"NextToken"`
				ObjectLambdaAccessPointList []struct {
					Name string `xml:"Name"`
				} `xml:"ObjectLambdaAccessPointList>ObjectLambdaAccessPoint"`
			}
			require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &out))
			assert.Len(t, out.ObjectLambdaAccessPointList, tt.wantLen)
			if tt.wantNextToken {
				assert.NotEmpty(t, out.NextToken)
			} else {
				assert.Empty(t, out.NextToken)
			}
		})
	}
}

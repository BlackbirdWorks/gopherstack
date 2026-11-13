package redshift_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
)

// ---- DescribeAccountAttributes ----

func TestHandler_DescribeAccountAttributes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		body         string
		wantContains []string
		wantAbsent   []string
		wantCode     int
	}{
		{
			name:     "success",
			body:     "Action=DescribeAccountAttributes&Version=2012-12-01",
			wantCode: http.StatusOK,
			wantContains: []string{
				"DescribeAccountAttributesResponse",
				"<AttributeName>max-defer-maintenance-duration</AttributeName>",
				"<AttributeValue>45</AttributeValue>",
			},
		},
		{
			name: "filter match",
			body: "Action=DescribeAccountAttributes&Version=2012-12-01" +
				"&AttributeNames.AttributeName.1=max-defer-maintenance-duration",
			wantCode:     http.StatusOK,
			wantContains: []string{"max-defer-maintenance-duration"},
		},
		{
			name:         "filter miss",
			body:         "Action=DescribeAccountAttributes&Version=2012-12-01&AttributeNames.AttributeName.1=nope",
			wantCode:     http.StatusOK,
			wantContains: []string{"DescribeAccountAttributesResult"},
			wantAbsent:   []string{"max-defer-maintenance-duration"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newRedshiftHandler()
			rec := postRedshiftForm(t, h, tt.body)

			assert.Equal(t, tt.wantCode, rec.Code)

			for _, s := range tt.wantContains {
				assert.Contains(t, rec.Body.String(), s)
			}

			for _, s := range tt.wantAbsent {
				assert.NotContains(t, rec.Body.String(), s)
			}
		})
	}
}

// ---- DescribeClusterTracks ----

func TestHandler_DescribeClusterTracks(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		body         string
		wantContains []string
		wantCode     int
	}{
		{
			name:         "success",
			body:         "Action=DescribeClusterTracks&Version=2012-12-01",
			wantCode:     http.StatusOK,
			wantContains: []string{"DescribeClusterTracksResponse", "current", "trailing"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newRedshiftHandler()
			rec := postRedshiftForm(t, h, tt.body)

			assert.Equal(t, tt.wantCode, rec.Code)

			for _, s := range tt.wantContains {
				assert.Contains(t, rec.Body.String(), s)
			}
		})
	}
}

// ---- DescribeClusterVersions ----

func TestHandler_DescribeClusterVersions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		body         string
		wantContains []string
		wantCode     int
	}{
		{
			name:         "success",
			body:         "Action=DescribeClusterVersions&Version=2012-12-01",
			wantCode:     http.StatusOK,
			wantContains: []string{"DescribeClusterVersionsResponse", "1.0"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newRedshiftHandler()
			rec := postRedshiftForm(t, h, tt.body)

			assert.Equal(t, tt.wantCode, rec.Code)

			for _, s := range tt.wantContains {
				assert.Contains(t, rec.Body.String(), s)
			}
		})
	}
}

// ---- DescribeOrderableClusterOptions ----

func TestHandler_DescribeOrderableClusterOptions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		body         string
		wantContains []string
		wantCode     int
	}{
		{
			name:         "success",
			body:         "Action=DescribeOrderableClusterOptions&Version=2012-12-01",
			wantCode:     http.StatusOK,
			wantContains: []string{"DescribeOrderableClusterOptionsResponse", "dc2.large"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newRedshiftHandler()
			rec := postRedshiftForm(t, h, tt.body)

			assert.Equal(t, tt.wantCode, rec.Code)

			for _, s := range tt.wantContains {
				assert.Contains(t, rec.Body.String(), s)
			}
		})
	}
}

// ---- DescribeStorage ----

func TestHandler_DescribeStorage(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		body         string
		wantContains []string
		wantCode     int
	}{
		{
			name:         "success",
			body:         "Action=DescribeStorage&Version=2012-12-01",
			wantCode:     http.StatusOK,
			wantContains: []string{"DescribeStorageResponse"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newRedshiftHandler()
			rec := postRedshiftForm(t, h, tt.body)

			assert.Equal(t, tt.wantCode, rec.Code)

			for _, s := range tt.wantContains {
				assert.Contains(t, rec.Body.String(), s)
			}
		})
	}
}

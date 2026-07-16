package redshift_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/redshift"
)

// ---- CreateUsageLimit ----

func TestHandler_CreateUsageLimit(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup        func(t *testing.T, h *redshift.Handler)
		name         string
		body         string
		wantContains []string
		wantCode     int
	}{
		{
			name: "success",
			setup: func(t *testing.T, h *redshift.Handler) {
				t.Helper()
				postRedshiftForm(t, h, "Action=CreateCluster&Version=2012-12-01&ClusterIdentifier=ul-cluster")
			},
			body: "Action=CreateUsageLimit&Version=2012-12-01&ClusterIdentifier=ul-cluster" +
				"&FeatureType=spectrum&LimitType=time&Amount=100&BreachAction=log",
			wantCode:     http.StatusOK,
			wantContains: []string{"CreateUsageLimitResponse", "ul-cluster", "spectrum"},
		},
		{
			name: "cluster_not_found",
			body: "Action=CreateUsageLimit&Version=2012-12-01&ClusterIdentifier=missing" +
				"&FeatureType=spectrum&LimitType=time&Amount=100",
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "missing_cluster",
			body:     "Action=CreateUsageLimit&Version=2012-12-01&ClusterIdentifier=",
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newRedshiftHandler()
			if tt.setup != nil {
				tt.setup(t, h)
			}

			rec := postRedshiftForm(t, h, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)

			for _, s := range tt.wantContains {
				assert.Contains(t, rec.Body.String(), s)
			}
		})
	}
}

// ---- DeleteUsageLimit / DescribeUsageLimits / ModifyUsageLimit ----

func TestHandler_DeleteUsageLimit(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		wantCode int
		notFound bool
	}{
		{
			name:     "success",
			wantCode: http.StatusOK,
		},
		{
			name:     "not_found",
			notFound: true,
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newRedshiftHandler()
			id := "ul-nonexistent"

			if !tt.notFound {
				postRedshiftForm(t, h, "Action=CreateCluster&Version=2012-12-01&ClusterIdentifier=del-ul-cluster")
				rec := postRedshiftForm(
					t, h,
					"Action=CreateUsageLimit&Version=2012-12-01&ClusterIdentifier=del-ul-cluster"+
						"&FeatureType=spectrum&LimitType=time&Amount=10",
				)
				require.Equal(t, http.StatusOK, rec.Code)
				body := rec.Body.String()
				startTag := "<UsageLimitId>"
				endTag := "</UsageLimitId>"
				if si := stringIndex(body, startTag); si >= 0 {
					si += len(startTag)
					if ei := stringIndex(body[si:], endTag); ei >= 0 {
						id = body[si : si+ei]
					}
				}
			}

			rec := postRedshiftForm(t, h, "Action=DeleteUsageLimit&Version=2012-12-01&UsageLimitId="+id)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

// stringIndex returns the index of the first occurrence of sub in s, or -1.
func stringIndex(s, sub string) int {
	for i := 0; i <= len(s)-len(sub); i++ {
		if s[i:i+len(sub)] == sub {
			return i
		}
	}

	return -1
}

func TestHandler_DescribeUsageLimits(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup        func(t *testing.T, h *redshift.Handler)
		name         string
		body         string
		wantContains []string
		wantCode     int
	}{
		{
			name:         "empty",
			body:         "Action=DescribeUsageLimits&Version=2012-12-01",
			wantCode:     http.StatusOK,
			wantContains: []string{"DescribeUsageLimitsResponse"},
		},
		{
			name: "filter_by_cluster",
			setup: func(t *testing.T, h *redshift.Handler) {
				t.Helper()
				postRedshiftForm(t, h, "Action=CreateCluster&Version=2012-12-01&ClusterIdentifier=dul-cluster")
				postRedshiftForm(
					t, h,
					"Action=CreateUsageLimit&Version=2012-12-01&ClusterIdentifier=dul-cluster"+
						"&FeatureType=spectrum&LimitType=time&Amount=10",
				)
			},
			body:         "Action=DescribeUsageLimits&Version=2012-12-01&ClusterIdentifier=dul-cluster",
			wantCode:     http.StatusOK,
			wantContains: []string{"dul-cluster"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newRedshiftHandler()
			if tt.setup != nil {
				tt.setup(t, h)
			}

			rec := postRedshiftForm(t, h, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)

			for _, s := range tt.wantContains {
				assert.Contains(t, rec.Body.String(), s)
			}
		})
	}
}

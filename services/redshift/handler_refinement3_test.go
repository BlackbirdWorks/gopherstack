package redshift_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/redshift"
)

// ---- CreateSnapshotCopyGrant ----

func TestHandler_CreateSnapshotCopyGrant(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		body         string
		wantContains []string
		wantCode     int
	}{
		{
			name:         "success",
			body:         "Action=CreateSnapshotCopyGrant&Version=2012-12-01&SnapshotCopyGrantName=my-grant&KmsKeyId=key123",
			wantCode:     http.StatusOK,
			wantContains: []string{"CreateSnapshotCopyGrantResponse", "my-grant"},
		},
		{
			name:     "duplicate",
			body:     "Action=CreateSnapshotCopyGrant&Version=2012-12-01&SnapshotCopyGrantName=my-grant",
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "missing_name",
			body:     "Action=CreateSnapshotCopyGrant&Version=2012-12-01&SnapshotCopyGrantName=",
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newRedshiftHandler()
			if tt.name == "duplicate" {
				postRedshiftForm(
					t,
					h,
					"Action=CreateSnapshotCopyGrant&Version=2012-12-01&SnapshotCopyGrantName=my-grant",
				)
			}

			rec := postRedshiftForm(t, h, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)

			for _, s := range tt.wantContains {
				assert.Contains(t, rec.Body.String(), s)
			}
		})
	}
}

// ---- DeleteSnapshotCopyGrant ----

func TestHandler_DeleteSnapshotCopyGrant(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(t *testing.T, h *redshift.Handler)
		name     string
		body     string
		wantCode int
	}{
		{
			name: "success",
			setup: func(t *testing.T, h *redshift.Handler) {
				t.Helper()
				postRedshiftForm(
					t,
					h,
					"Action=CreateSnapshotCopyGrant&Version=2012-12-01&SnapshotCopyGrantName=grant-to-delete",
				)
			},
			body:     "Action=DeleteSnapshotCopyGrant&Version=2012-12-01&SnapshotCopyGrantName=grant-to-delete",
			wantCode: http.StatusOK,
		},
		{
			name:     "not_found",
			body:     "Action=DeleteSnapshotCopyGrant&Version=2012-12-01&SnapshotCopyGrantName=missing",
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "missing_name",
			body:     "Action=DeleteSnapshotCopyGrant&Version=2012-12-01&SnapshotCopyGrantName=",
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
		})
	}
}

// ---- DescribeSnapshotCopyGrants ----

func TestHandler_DescribeSnapshotCopyGrants(t *testing.T) {
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
			body:         "Action=DescribeSnapshotCopyGrants&Version=2012-12-01",
			wantCode:     http.StatusOK,
			wantContains: []string{"DescribeSnapshotCopyGrantsResponse"},
		},
		{
			name: "with_grant",
			setup: func(t *testing.T, h *redshift.Handler) {
				t.Helper()
				postRedshiftForm(t, h, "Action=CreateSnapshotCopyGrant&Version=2012-12-01&SnapshotCopyGrantName=g1")
			},
			body:         "Action=DescribeSnapshotCopyGrants&Version=2012-12-01",
			wantCode:     http.StatusOK,
			wantContains: []string{"g1"},
		},
		{
			name:     "not_found_filter",
			body:     "Action=DescribeSnapshotCopyGrants&Version=2012-12-01&SnapshotCopyGrantName=missing",
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

// ---- EnableSnapshotCopy ----

func TestHandler_EnableSnapshotCopy(t *testing.T) {
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
				postRedshiftForm(t, h, "Action=CreateCluster&Version=2012-12-01&ClusterIdentifier=sc-cluster")
			},
			body: "Action=EnableSnapshotCopy&Version=2012-12-01" +
				"&ClusterIdentifier=sc-cluster&DestinationRegion=us-west-2",
			wantCode:     http.StatusOK,
			wantContains: []string{"EnableSnapshotCopyResponse", "sc-cluster"},
		},
		{
			name: "already_enabled",
			setup: func(t *testing.T, h *redshift.Handler) {
				t.Helper()
				postRedshiftForm(t, h, "Action=CreateCluster&Version=2012-12-01&ClusterIdentifier=sc-cluster2")
				postRedshiftForm(
					t,
					h,
					"Action=EnableSnapshotCopy&Version=2012-12-01&ClusterIdentifier=sc-cluster2&DestinationRegion=us-west-2",
				)
			},
			body:     "Action=EnableSnapshotCopy&Version=2012-12-01&ClusterIdentifier=sc-cluster2&DestinationRegion=us-west-2",
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "cluster_not_found",
			body:     "Action=EnableSnapshotCopy&Version=2012-12-01&ClusterIdentifier=missing&DestinationRegion=us-west-2",
			wantCode: http.StatusBadRequest,
		},
		{
			name: "missing_region",
			setup: func(t *testing.T, h *redshift.Handler) {
				t.Helper()
				postRedshiftForm(t, h, "Action=CreateCluster&Version=2012-12-01&ClusterIdentifier=sc-cluster3")
			},
			body:     "Action=EnableSnapshotCopy&Version=2012-12-01&ClusterIdentifier=sc-cluster3&DestinationRegion=",
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

// ---- DisableSnapshotCopy ----

func TestHandler_DisableSnapshotCopy(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(t *testing.T, h *redshift.Handler)
		name     string
		body     string
		wantCode int
	}{
		{
			name: "success",
			setup: func(t *testing.T, h *redshift.Handler) {
				t.Helper()
				postRedshiftForm(t, h, "Action=CreateCluster&Version=2012-12-01&ClusterIdentifier=ds-cluster")
				postRedshiftForm(
					t,
					h,
					"Action=EnableSnapshotCopy&Version=2012-12-01&ClusterIdentifier=ds-cluster&DestinationRegion=us-west-2",
				)
			},
			body:     "Action=DisableSnapshotCopy&Version=2012-12-01&ClusterIdentifier=ds-cluster",
			wantCode: http.StatusOK,
		},
		{
			name: "not_enabled",
			setup: func(t *testing.T, h *redshift.Handler) {
				t.Helper()
				postRedshiftForm(t, h, "Action=CreateCluster&Version=2012-12-01&ClusterIdentifier=ds-cluster2")
			},
			body:     "Action=DisableSnapshotCopy&Version=2012-12-01&ClusterIdentifier=ds-cluster2",
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "cluster_not_found",
			body:     "Action=DisableSnapshotCopy&Version=2012-12-01&ClusterIdentifier=missing",
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
		})
	}
}

// ---- ModifySnapshotCopyRetentionPeriod ----

func TestHandler_ModifySnapshotCopyRetentionPeriod(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(t *testing.T, h *redshift.Handler)
		name     string
		body     string
		wantCode int
	}{
		{
			name: "success",
			setup: func(t *testing.T, h *redshift.Handler) {
				t.Helper()
				postRedshiftForm(t, h, "Action=CreateCluster&Version=2012-12-01&ClusterIdentifier=mr-cluster")
				postRedshiftForm(
					t,
					h,
					"Action=EnableSnapshotCopy&Version=2012-12-01&ClusterIdentifier=mr-cluster&DestinationRegion=us-west-2",
				)
			},
			body: "Action=ModifySnapshotCopyRetentionPeriod&Version=2012-12-01" +
				"&ClusterIdentifier=mr-cluster&RetentionPeriod=14",
			wantCode: http.StatusOK,
		},
		{
			name: "not_enabled",
			body: "Action=ModifySnapshotCopyRetentionPeriod&Version=2012-12-01" +
				"&ClusterIdentifier=missing&RetentionPeriod=14",
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
		})
	}
}

// ---- CreateSnapshotSchedule ----

func TestHandler_CreateSnapshotSchedule(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		body         string
		wantContains []string
		wantCode     int
	}{
		{
			name: "success",
			body: "Action=CreateSnapshotSchedule&Version=2012-12-01" +
				"&ScheduleIdentifier=my-schedule&ScheduleDescription=test",
			wantCode:     http.StatusOK,
			wantContains: []string{"CreateSnapshotScheduleResponse", "my-schedule"},
		},
		{
			name:     "missing_id",
			body:     "Action=CreateSnapshotSchedule&Version=2012-12-01&ScheduleIdentifier=",
			wantCode: http.StatusBadRequest,
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

// ---- DeleteSnapshotSchedule ----

func TestHandler_DeleteSnapshotSchedule(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(t *testing.T, h *redshift.Handler)
		name     string
		body     string
		wantCode int
	}{
		{
			name: "success",
			setup: func(t *testing.T, h *redshift.Handler) {
				t.Helper()
				postRedshiftForm(t, h, "Action=CreateSnapshotSchedule&Version=2012-12-01&ScheduleIdentifier=sched-del")
			},
			body:     "Action=DeleteSnapshotSchedule&Version=2012-12-01&ScheduleIdentifier=sched-del",
			wantCode: http.StatusOK,
		},
		{
			name:     "not_found",
			body:     "Action=DeleteSnapshotSchedule&Version=2012-12-01&ScheduleIdentifier=missing",
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
		})
	}
}

// ---- DescribeSnapshotSchedules ----

func TestHandler_DescribeSnapshotSchedules(t *testing.T) {
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
			body:         "Action=DescribeSnapshotSchedules&Version=2012-12-01",
			wantCode:     http.StatusOK,
			wantContains: []string{"DescribeSnapshotSchedulesResponse"},
		},
		{
			name: "with_schedule",
			setup: func(t *testing.T, h *redshift.Handler) {
				t.Helper()
				postRedshiftForm(t, h, "Action=CreateSnapshotSchedule&Version=2012-12-01&ScheduleIdentifier=sched1")
			},
			body:         "Action=DescribeSnapshotSchedules&Version=2012-12-01",
			wantCode:     http.StatusOK,
			wantContains: []string{"sched1"},
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

// ---- ModifySnapshotSchedule ----

func TestHandler_ModifySnapshotSchedule(t *testing.T) {
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
				postRedshiftForm(t, h, "Action=CreateSnapshotSchedule&Version=2012-12-01&ScheduleIdentifier=mod-sched")
			},
			body:         "Action=ModifySnapshotSchedule&Version=2012-12-01&ScheduleIdentifier=mod-sched",
			wantCode:     http.StatusOK,
			wantContains: []string{"ModifySnapshotScheduleResponse"},
		},
		{
			name:     "not_found",
			body:     "Action=ModifySnapshotSchedule&Version=2012-12-01&ScheduleIdentifier=missing",
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

// ---- ModifyClusterSnapshotSchedule ----

func TestHandler_ModifyClusterSnapshotSchedule(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(t *testing.T, h *redshift.Handler)
		name     string
		body     string
		wantCode int
	}{
		{
			name: "associate",
			setup: func(t *testing.T, h *redshift.Handler) {
				t.Helper()
				postRedshiftForm(t, h, "Action=CreateCluster&Version=2012-12-01&ClusterIdentifier=mcs-cluster")
				postRedshiftForm(t, h, "Action=CreateSnapshotSchedule&Version=2012-12-01&ScheduleIdentifier=s1")
			},
			body: "Action=ModifyClusterSnapshotSchedule&Version=2012-12-01" +
				"&ClusterIdentifier=mcs-cluster&ScheduleIdentifier=s1",
			wantCode: http.StatusOK,
		},
		{
			name: "disassociate",
			setup: func(t *testing.T, h *redshift.Handler) {
				t.Helper()
				postRedshiftForm(t, h, "Action=CreateCluster&Version=2012-12-01&ClusterIdentifier=mcs-cluster2")
			},
			body: "Action=ModifyClusterSnapshotSchedule&Version=2012-12-01" +
				"&ClusterIdentifier=mcs-cluster2&DisassociateSchedule=true",
			wantCode: http.StatusOK,
		},
		{
			name:     "cluster_not_found",
			body:     "Action=ModifyClusterSnapshotSchedule&Version=2012-12-01&ClusterIdentifier=missing",
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
		})
	}
}

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

// ---- CreateAuthenticationProfile ----

func TestHandler_CreateAuthenticationProfile(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		body         string
		wantContains []string
		wantCode     int
	}{
		{
			name: "success",
			body: "Action=CreateAuthenticationProfile&Version=2012-12-01" +
				"&AuthenticationProfileName=myprofile&AuthenticationProfileContent={\"x\":1}",
			wantCode:     http.StatusOK,
			wantContains: []string{"CreateAuthenticationProfileResponse", "myprofile"},
		},
		{
			name:     "missing_name",
			body:     "Action=CreateAuthenticationProfile&Version=2012-12-01&AuthenticationProfileName=",
			wantCode: http.StatusBadRequest,
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

// ---- DeleteAuthenticationProfile ----

func TestHandler_DeleteAuthenticationProfile(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(t *testing.T, h *redshift.Handler)
		name     string
		body     string
		wantCode int
	}{
		{
			name: "success",
			setup: func(t *testing.T, h *redshift.Handler) {
				t.Helper()
				postRedshiftForm(
					t,
					h,
					"Action=CreateAuthenticationProfile&Version=2012-12-01&AuthenticationProfileName=prof-del",
				)
			},
			body:     "Action=DeleteAuthenticationProfile&Version=2012-12-01&AuthenticationProfileName=prof-del",
			wantCode: http.StatusOK,
		},
		{
			name:     "not_found",
			body:     "Action=DeleteAuthenticationProfile&Version=2012-12-01&AuthenticationProfileName=missing",
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
		})
	}
}

// ---- DescribeAuthenticationProfiles ----

func TestHandler_DescribeAuthenticationProfiles(t *testing.T) {
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
			body:         "Action=DescribeAuthenticationProfiles&Version=2012-12-01",
			wantCode:     http.StatusOK,
			wantContains: []string{"DescribeAuthenticationProfilesResponse"},
		},
		{
			name: "with_profile",
			setup: func(t *testing.T, h *redshift.Handler) {
				t.Helper()
				postRedshiftForm(
					t,
					h,
					"Action=CreateAuthenticationProfile&Version=2012-12-01&AuthenticationProfileName=p1",
				)
			},
			body:         "Action=DescribeAuthenticationProfiles&Version=2012-12-01",
			wantCode:     http.StatusOK,
			wantContains: []string{"p1"},
		},
		{
			name:     "not_found_filter",
			body:     "Action=DescribeAuthenticationProfiles&Version=2012-12-01&AuthenticationProfileName=missing",
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

// ---- ModifyAuthenticationProfile ----

func TestHandler_ModifyAuthenticationProfile(t *testing.T) {
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
				postRedshiftForm(
					t,
					h,
					"Action=CreateAuthenticationProfile&Version=2012-12-01&AuthenticationProfileName=mod-prof",
				)
			},
			body: "Action=ModifyAuthenticationProfile&Version=2012-12-01" +
				"&AuthenticationProfileName=mod-prof&AuthenticationProfileContent={}",
			wantCode:     http.StatusOK,
			wantContains: []string{"ModifyAuthenticationProfileResponse", "mod-prof"},
		},
		{
			name:     "not_found",
			body:     "Action=ModifyAuthenticationProfile&Version=2012-12-01&AuthenticationProfileName=missing",
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

// ---- PutResourcePolicy / GetResourcePolicy / DeleteResourcePolicy ----

func TestHandler_ResourcePolicy(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup        func(t *testing.T, h *redshift.Handler)
		name         string
		body         string
		wantContains []string
		wantCode     int
	}{
		{
			name: "put_success",
			body: "Action=PutResourcePolicy&Version=2012-12-01" +
				"&ResourceArn=arn:aws:redshift:us-east-1:000000000000:cluster:test&Policy={}",
			wantCode:     http.StatusOK,
			wantContains: []string{"PutResourcePolicyResponse"},
		},
		{
			name: "get_success",
			setup: func(t *testing.T, h *redshift.Handler) {
				t.Helper()
				postRedshiftForm(
					t, h,
					"Action=PutResourcePolicy&Version=2012-12-01"+
						"&ResourceArn=arn:aws:redshift:us-east-1:000000000000:cluster:c1&Policy={}",
				)
			},
			body: "Action=GetResourcePolicy&Version=2012-12-01" +
				"&ResourceArn=arn:aws:redshift:us-east-1:000000000000:cluster:c1",
			wantCode:     http.StatusOK,
			wantContains: []string{"GetResourcePolicyResponse"},
		},
		{
			name: "get_not_found",
			body: "Action=GetResourcePolicy&Version=2012-12-01" +
				"&ResourceArn=arn:aws:redshift:us-east-1:000000000000:cluster:missing",
			wantCode: http.StatusBadRequest,
		},
		{
			name: "delete_success",
			setup: func(t *testing.T, h *redshift.Handler) {
				t.Helper()
				postRedshiftForm(t, h, "Action=PutResourcePolicy&Version=2012-12-01&ResourceArn=arn:rp:del&Policy={}")
			},
			body:     "Action=DeleteResourcePolicy&Version=2012-12-01&ResourceArn=arn:rp:del",
			wantCode: http.StatusOK,
		},
		{
			name:     "delete_not_found",
			body:     "Action=DeleteResourcePolicy&Version=2012-12-01&ResourceArn=arn:rp:missing",
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

// ---- GetClusterCredentialsWithIAM ----

func TestHandler_GetClusterCredentialsWithIAM(t *testing.T) {
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
				postRedshiftForm(t, h, "Action=CreateCluster&Version=2012-12-01&ClusterIdentifier=iam-cluster")
			},
			body:         "Action=GetClusterCredentialsWithIAM&Version=2012-12-01&ClusterIdentifier=iam-cluster&DbName=dev",
			wantCode:     http.StatusOK,
			wantContains: []string{"GetClusterCredentialsWithIAMResponse", "DbUser", "DbPassword"},
		},
		{
			name:     "cluster_not_found",
			body:     "Action=GetClusterCredentialsWithIAM&Version=2012-12-01&ClusterIdentifier=missing",
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

// ---- FailoverPrimaryCompute ----

func TestHandler_FailoverPrimaryCompute(t *testing.T) {
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
				postRedshiftForm(t, h, "Action=CreateCluster&Version=2012-12-01&ClusterIdentifier=fo-cluster")
			},
			body:         "Action=FailoverPrimaryCompute&Version=2012-12-01&ClusterIdentifier=fo-cluster",
			wantCode:     http.StatusOK,
			wantContains: []string{"FailoverPrimaryComputeResponse", "fo-cluster"},
		},
		{
			name:     "cluster_not_found",
			body:     "Action=FailoverPrimaryCompute&Version=2012-12-01&ClusterIdentifier=missing",
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

// ---- DescribeTableRestoreStatus ----

func TestHandler_DescribeTableRestoreStatus(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		body         string
		wantContains []string
		wantCode     int
	}{
		{
			name:         "empty",
			body:         "Action=DescribeTableRestoreStatus&Version=2012-12-01",
			wantCode:     http.StatusOK,
			wantContains: []string{"DescribeTableRestoreStatusResponse"},
		},
		{
			name:         "with_cluster_filter",
			body:         "Action=DescribeTableRestoreStatus&Version=2012-12-01&ClusterIdentifier=c1",
			wantCode:     http.StatusOK,
			wantContains: []string{"DescribeTableRestoreStatusResponse"},
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

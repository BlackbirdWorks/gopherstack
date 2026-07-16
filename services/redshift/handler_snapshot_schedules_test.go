package redshift_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/blackbirdworks/gopherstack/services/redshift"
)

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

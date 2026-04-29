package redshift_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/blackbirdworks/gopherstack/services/redshift"
)

// ---- ModifyCluster ----

func TestRedshiftHandler_ModifyCluster(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup        func(h *redshift.Handler)
		name         string
		body         string
		wantContains []string
		wantCode     int
	}{
		{
			name: "success",
			setup: func(h *redshift.Handler) {
				postRedshiftForm(t, h, "Action=CreateCluster&Version=2012-12-01&ClusterIdentifier=mod-cluster")
			},
			body: "Action=ModifyCluster&Version=2012-12-01" +
				"&ClusterIdentifier=mod-cluster&NodeType=ra3.xlplus&NumberOfNodes=3",
			wantCode:     http.StatusOK,
			wantContains: []string{"ModifyClusterResponse", "mod-cluster"},
		},
		{
			name:         "not_found",
			body:         "Action=ModifyCluster&Version=2012-12-01&ClusterIdentifier=nonexistent",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"ClusterNotFound"},
		},
		{
			name:         "missing_id",
			body:         "Action=ModifyCluster&Version=2012-12-01",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"InvalidParameterValue"},
		},
		{
			name: "invalid_number_of_nodes",
			setup: func(h *redshift.Handler) {
				postRedshiftForm(t, h, "Action=CreateCluster&Version=2012-12-01&ClusterIdentifier=invalid-cluster")
			},
			body: "Action=ModifyCluster&Version=2012-12-01" +
				"&ClusterIdentifier=invalid-cluster&NumberOfNodes=notanumber",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"InvalidParameterValue"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := redshift.NewInMemoryBackend("000000000000", "us-east-1")
			h := redshift.NewHandler(b)

			if tt.setup != nil {
				tt.setup(h)
			}

			rec := postRedshiftForm(t, h, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)

			for _, s := range tt.wantContains {
				assert.Contains(t, rec.Body.String(), s)
			}
		})
	}
}

// ---- RebootCluster ----

func TestRedshiftHandler_RebootCluster(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup        func(h *redshift.Handler)
		name         string
		body         string
		wantContains []string
		wantCode     int
	}{
		{
			name: "success",
			setup: func(h *redshift.Handler) {
				postRedshiftForm(t, h, "Action=CreateCluster&Version=2012-12-01&ClusterIdentifier=reboot-cluster")
			},
			body:         "Action=RebootCluster&Version=2012-12-01&ClusterIdentifier=reboot-cluster",
			wantCode:     http.StatusOK,
			wantContains: []string{"RebootClusterResponse", "reboot-cluster"},
		},
		{
			name:         "not_found",
			body:         "Action=RebootCluster&Version=2012-12-01&ClusterIdentifier=nonexistent",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"ClusterNotFound"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := redshift.NewInMemoryBackend("000000000000", "us-east-1")
			h := redshift.NewHandler(b)

			if tt.setup != nil {
				tt.setup(h)
			}

			rec := postRedshiftForm(t, h, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)

			for _, s := range tt.wantContains {
				assert.Contains(t, rec.Body.String(), s)
			}
		})
	}
}

// ---- PauseCluster ----

func TestRedshiftHandler_PauseCluster(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup        func(h *redshift.Handler)
		name         string
		body         string
		wantContains []string
		wantCode     int
	}{
		{
			name: "success",
			setup: func(h *redshift.Handler) {
				postRedshiftForm(t, h, "Action=CreateCluster&Version=2012-12-01&ClusterIdentifier=pause-cluster")
			},
			body:         "Action=PauseCluster&Version=2012-12-01&ClusterIdentifier=pause-cluster",
			wantCode:     http.StatusOK,
			wantContains: []string{"PauseClusterResponse", "pause-cluster"},
		},
		{
			name:         "not_found",
			body:         "Action=PauseCluster&Version=2012-12-01&ClusterIdentifier=nonexistent",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"ClusterNotFound"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := redshift.NewInMemoryBackend("000000000000", "us-east-1")
			h := redshift.NewHandler(b)

			if tt.setup != nil {
				tt.setup(h)
			}

			rec := postRedshiftForm(t, h, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)

			for _, s := range tt.wantContains {
				assert.Contains(t, rec.Body.String(), s)
			}
		})
	}
}

// ---- ResumeCluster ----

func TestRedshiftHandler_ResumeCluster(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup        func(h *redshift.Handler)
		name         string
		body         string
		wantContains []string
		wantCode     int
	}{
		{
			name: "success",
			setup: func(h *redshift.Handler) {
				postRedshiftForm(t, h, "Action=CreateCluster&Version=2012-12-01&ClusterIdentifier=resume-cluster")
				postRedshiftForm(t, h, "Action=PauseCluster&Version=2012-12-01&ClusterIdentifier=resume-cluster")
			},
			body:         "Action=ResumeCluster&Version=2012-12-01&ClusterIdentifier=resume-cluster",
			wantCode:     http.StatusOK,
			wantContains: []string{"ResumeClusterResponse", "resume-cluster"},
		},
		{
			name:         "not_found",
			body:         "Action=ResumeCluster&Version=2012-12-01&ClusterIdentifier=nonexistent",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"ClusterNotFound"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := redshift.NewInMemoryBackend("000000000000", "us-east-1")
			h := redshift.NewHandler(b)

			if tt.setup != nil {
				tt.setup(h)
			}

			rec := postRedshiftForm(t, h, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)

			for _, s := range tt.wantContains {
				assert.Contains(t, rec.Body.String(), s)
			}
		})
	}
}

// ---- ResizeCluster ----

func TestRedshiftHandler_ResizeCluster(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup        func(h *redshift.Handler)
		name         string
		body         string
		wantContains []string
		wantCode     int
	}{
		{
			name: "success",
			setup: func(h *redshift.Handler) {
				postRedshiftForm(t, h, "Action=CreateCluster&Version=2012-12-01&ClusterIdentifier=resize-cluster")
			},
			body: "Action=ResizeCluster&Version=2012-12-01" +
				"&ClusterIdentifier=resize-cluster&NodeType=ra3.4xlarge&NumberOfNodes=4",
			wantCode:     http.StatusOK,
			wantContains: []string{"ResizeClusterResponse", "resize-cluster"},
		},
		{
			name:         "not_found",
			body:         "Action=ResizeCluster&Version=2012-12-01&ClusterIdentifier=nonexistent",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"ClusterNotFound"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := redshift.NewInMemoryBackend("000000000000", "us-east-1")
			h := redshift.NewHandler(b)

			if tt.setup != nil {
				tt.setup(h)
			}

			rec := postRedshiftForm(t, h, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)

			for _, s := range tt.wantContains {
				assert.Contains(t, rec.Body.String(), s)
			}
		})
	}
}

// ---- RotateEncryptionKey ----

func TestRedshiftHandler_RotateEncryptionKey(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup        func(h *redshift.Handler)
		name         string
		body         string
		wantContains []string
		wantCode     int
	}{
		{
			name: "success",
			setup: func(h *redshift.Handler) {
				postRedshiftForm(t, h, "Action=CreateCluster&Version=2012-12-01&ClusterIdentifier=encrypt-cluster")
			},
			body:         "Action=RotateEncryptionKey&Version=2012-12-01&ClusterIdentifier=encrypt-cluster",
			wantCode:     http.StatusOK,
			wantContains: []string{"RotateEncryptionKeyResponse", "encrypt-cluster"},
		},
		{
			name:         "not_found",
			body:         "Action=RotateEncryptionKey&Version=2012-12-01&ClusterIdentifier=nonexistent",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"ClusterNotFound"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := redshift.NewInMemoryBackend("000000000000", "us-east-1")
			h := redshift.NewHandler(b)

			if tt.setup != nil {
				tt.setup(h)
			}

			rec := postRedshiftForm(t, h, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)

			for _, s := range tt.wantContains {
				assert.Contains(t, rec.Body.String(), s)
			}
		})
	}
}

// ---- ModifyClusterIamRoles ----

func TestRedshiftHandler_ModifyClusterIamRoles(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup        func(h *redshift.Handler)
		name         string
		body         string
		wantContains []string
		wantCode     int
	}{
		{
			name: "success",
			setup: func(h *redshift.Handler) {
				postRedshiftForm(t, h, "Action=CreateCluster&Version=2012-12-01&ClusterIdentifier=iam-cluster")
			},
			body: "Action=ModifyClusterIamRoles&Version=2012-12-01" +
				"&ClusterIdentifier=iam-cluster" +
				"&AddIamRoles.IamRoleArn.1=arn:aws:iam::123:role/myrole",
			wantCode:     http.StatusOK,
			wantContains: []string{"ModifyClusterIamRolesResponse", "iam-cluster"},
		},
		{
			name:         "not_found",
			body:         "Action=ModifyClusterIamRoles&Version=2012-12-01&ClusterIdentifier=nonexistent",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"ClusterNotFound"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := redshift.NewInMemoryBackend("000000000000", "us-east-1")
			h := redshift.NewHandler(b)

			if tt.setup != nil {
				tt.setup(h)
			}

			rec := postRedshiftForm(t, h, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)

			for _, s := range tt.wantContains {
				assert.Contains(t, rec.Body.String(), s)
			}
		})
	}
}

// ---- ModifyClusterMaintenance ----

func TestRedshiftHandler_ModifyClusterMaintenance(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup        func(h *redshift.Handler)
		name         string
		body         string
		wantContains []string
		wantCode     int
	}{
		{
			name: "success",
			setup: func(h *redshift.Handler) {
				postRedshiftForm(t, h, "Action=CreateCluster&Version=2012-12-01&ClusterIdentifier=maint-cluster")
			},
			body: "Action=ModifyClusterMaintenance&Version=2012-12-01" +
				"&ClusterIdentifier=maint-cluster&MaintenanceTrackName=current",
			wantCode:     http.StatusOK,
			wantContains: []string{"ModifyClusterMaintenanceResponse", "maint-cluster"},
		},
		{
			name:         "not_found",
			body:         "Action=ModifyClusterMaintenance&Version=2012-12-01&ClusterIdentifier=nonexistent",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"ClusterNotFound"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := redshift.NewInMemoryBackend("000000000000", "us-east-1")
			h := redshift.NewHandler(b)

			if tt.setup != nil {
				tt.setup(h)
			}

			rec := postRedshiftForm(t, h, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)

			for _, s := range tt.wantContains {
				assert.Contains(t, rec.Body.String(), s)
			}
		})
	}
}

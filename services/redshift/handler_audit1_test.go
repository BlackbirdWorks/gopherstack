package redshift_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/redshift"
)

// ---- CreateHsmClientCertificate ----

func TestHandler_CreateHsmClientCertificate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		body         string
		wantContains []string
		wantCode     int
	}{
		{
			name:         "success",
			body:         "Action=CreateHsmClientCertificate&Version=2012-12-01&HsmClientCertificateIdentifier=my-cert",
			wantCode:     http.StatusOK,
			wantContains: []string{"CreateHsmClientCertificateResponse", "my-cert", "PUBLIC KEY"},
		},
		{
			name:         "missing_identifier",
			body:         "Action=CreateHsmClientCertificate&Version=2012-12-01",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"InvalidParameterValue"},
		},
		{
			name:         "duplicate",
			body:         "Action=CreateHsmClientCertificate&Version=2012-12-01&HsmClientCertificateIdentifier=dup-cert",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"HsmClientCertificateAlreadyExists"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newRedshiftHandler()
			if tt.name == "duplicate" {
				postRedshiftForm(t, h,
					"Action=CreateHsmClientCertificate&Version=2012-12-01&HsmClientCertificateIdentifier=dup-cert")
			}

			rec := postRedshiftForm(t, h, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)

			for _, s := range tt.wantContains {
				assert.Contains(t, rec.Body.String(), s)
			}
		})
	}
}

// ---- DeleteHsmClientCertificate ----

func TestHandler_DeleteHsmClientCertificate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		body         string
		wantContains []string
		wantCode     int
	}{
		{
			name:         "success",
			body:         "Action=DeleteHsmClientCertificate&Version=2012-12-01&HsmClientCertificateIdentifier=my-cert",
			wantCode:     http.StatusOK,
			wantContains: []string{"DeleteHsmClientCertificateResponse"},
		},
		{
			name:         "not_found",
			body:         "Action=DeleteHsmClientCertificate&Version=2012-12-01&HsmClientCertificateIdentifier=nonexistent",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"HsmClientCertificateNotFound"},
		},
		{
			name:         "missing_identifier",
			body:         "Action=DeleteHsmClientCertificate&Version=2012-12-01",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"InvalidParameterValue"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newRedshiftHandler()
			if tt.name == "success" {
				postRedshiftForm(t, h,
					"Action=CreateHsmClientCertificate&Version=2012-12-01&HsmClientCertificateIdentifier=my-cert")
			}

			rec := postRedshiftForm(t, h, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)

			for _, s := range tt.wantContains {
				assert.Contains(t, rec.Body.String(), s)
			}
		})
	}
}

// ---- DescribeHsmClientCertificates ----

func TestHandler_DescribeHsmClientCertificates(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		body         string
		wantContains []string
		wantCode     int
	}{
		{
			name:         "empty",
			body:         "Action=DescribeHsmClientCertificates&Version=2012-12-01",
			wantCode:     http.StatusOK,
			wantContains: []string{"DescribeHsmClientCertificatesResponse"},
		},
		{
			name:         "with_data",
			body:         "Action=DescribeHsmClientCertificates&Version=2012-12-01",
			wantCode:     http.StatusOK,
			wantContains: []string{"DescribeHsmClientCertificatesResponse", "test-cert-1"},
		},
		{
			name:         "filter_by_id",
			body:         "Action=DescribeHsmClientCertificates&Version=2012-12-01&HsmClientCertificateIdentifier=test-cert-1",
			wantCode:     http.StatusOK,
			wantContains: []string{"test-cert-1"},
		},
		{
			name:         "filter_not_found",
			body:         "Action=DescribeHsmClientCertificates&Version=2012-12-01&HsmClientCertificateIdentifier=nonexistent",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"HsmClientCertificateNotFound"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newRedshiftHandler()
			if tt.name == "with_data" || tt.name == "filter_by_id" {
				postRedshiftForm(t, h,
					"Action=CreateHsmClientCertificate&Version=2012-12-01&HsmClientCertificateIdentifier=test-cert-1")
			}

			rec := postRedshiftForm(t, h, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)

			for _, s := range tt.wantContains {
				assert.Contains(t, rec.Body.String(), s)
			}
		})
	}
}

// ---- CreateHsmConfiguration ----

func TestHandler_CreateHsmConfiguration(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		body         string
		wantContains []string
		wantCode     int
	}{
		{
			name: "success",
			body: "Action=CreateHsmConfiguration&Version=2012-12-01" +
				"&HsmConfigurationIdentifier=my-hsm-config" +
				"&Description=My+HSM+configuration" +
				"&HsmIPAddress=192.168.1.100" +
				"&HsmPartitionName=my-partition",
			wantCode:     http.StatusOK,
			wantContains: []string{"CreateHsmConfigurationResponse", "my-hsm-config", "192.168.1.100", "my-partition"},
		},
		{
			name:         "missing_identifier",
			body:         "Action=CreateHsmConfiguration&Version=2012-12-01",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"InvalidParameterValue"},
		},
		{
			name: "duplicate",
			body: "Action=CreateHsmConfiguration&Version=2012-12-01" +
				"&HsmConfigurationIdentifier=dup-config" +
				"&HsmIPAddress=10.0.0.1" +
				"&HsmPartitionName=p1",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"HsmConfigurationAlreadyExists"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newRedshiftHandler()
			if tt.name == "duplicate" {
				postRedshiftForm(t, h, "Action=CreateHsmConfiguration&Version=2012-12-01"+
					"&HsmConfigurationIdentifier=dup-config&HsmIPAddress=10.0.0.1&HsmPartitionName=p1")
			}

			rec := postRedshiftForm(t, h, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)

			for _, s := range tt.wantContains {
				assert.Contains(t, rec.Body.String(), s)
			}
		})
	}
}

// ---- DeleteHsmConfiguration ----

func TestHandler_DeleteHsmConfiguration(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		body         string
		wantContains []string
		wantCode     int
	}{
		{
			name:         "success",
			body:         "Action=DeleteHsmConfiguration&Version=2012-12-01&HsmConfigurationIdentifier=my-config",
			wantCode:     http.StatusOK,
			wantContains: []string{"DeleteHsmConfigurationResponse"},
		},
		{
			name:         "not_found",
			body:         "Action=DeleteHsmConfiguration&Version=2012-12-01&HsmConfigurationIdentifier=nonexistent",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"HsmConfigurationNotFound"},
		},
		{
			name:         "missing_identifier",
			body:         "Action=DeleteHsmConfiguration&Version=2012-12-01",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"InvalidParameterValue"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newRedshiftHandler()
			if tt.name == "success" {
				postRedshiftForm(t, h, "Action=CreateHsmConfiguration&Version=2012-12-01"+
					"&HsmConfigurationIdentifier=my-config&HsmIPAddress=10.0.0.1&HsmPartitionName=p1")
			}

			rec := postRedshiftForm(t, h, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)

			for _, s := range tt.wantContains {
				assert.Contains(t, rec.Body.String(), s)
			}
		})
	}
}

// ---- DescribeHsmConfigurations ----

func TestHandler_DescribeHsmConfigurations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		body         string
		wantContains []string
		wantCode     int
	}{
		{
			name:         "empty",
			body:         "Action=DescribeHsmConfigurations&Version=2012-12-01",
			wantCode:     http.StatusOK,
			wantContains: []string{"DescribeHsmConfigurationsResponse"},
		},
		{
			name:         "with_data",
			body:         "Action=DescribeHsmConfigurations&Version=2012-12-01",
			wantCode:     http.StatusOK,
			wantContains: []string{"DescribeHsmConfigurationsResponse", "hsm-config-1", "10.0.0.1"},
		},
		{
			name:         "filter_by_id",
			body:         "Action=DescribeHsmConfigurations&Version=2012-12-01&HsmConfigurationIdentifier=hsm-config-1",
			wantCode:     http.StatusOK,
			wantContains: []string{"hsm-config-1"},
		},
		{
			name:         "filter_not_found",
			body:         "Action=DescribeHsmConfigurations&Version=2012-12-01&HsmConfigurationIdentifier=nonexistent",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"HsmConfigurationNotFound"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newRedshiftHandler()
			if tt.name == "with_data" || tt.name == "filter_by_id" {
				postRedshiftForm(t, h, "Action=CreateHsmConfiguration&Version=2012-12-01"+
					"&HsmConfigurationIdentifier=hsm-config-1&HsmIPAddress=10.0.0.1&HsmPartitionName=p1")
			}

			rec := postRedshiftForm(t, h, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)

			for _, s := range tt.wantContains {
				assert.Contains(t, rec.Body.String(), s)
			}
		})
	}
}

// ---- CreateScheduledAction ----

func TestHandler_CreateScheduledAction(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		body         string
		wantContains []string
		wantCode     int
	}{
		{
			name: "success",
			body: "Action=CreateScheduledAction&Version=2012-12-01" +
				"&ScheduledActionName=my-action" +
				"&Schedule=cron(0+12+*+*+?+*)" +
				"&IamRole=arn:aws:iam::123456789012:role/MyRole" +
				"&ScheduledActionDescription=Daily+resize",
			wantCode:     http.StatusOK,
			wantContains: []string{"CreateScheduledActionResponse", "my-action", "ACTIVE"},
		},
		{
			name:         "missing_name",
			body:         "Action=CreateScheduledAction&Version=2012-12-01&Schedule=cron(0+12+*+*+?+*)",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"InvalidParameterValue"},
		},
		{
			name: "duplicate",
			body: "Action=CreateScheduledAction&Version=2012-12-01" +
				"&ScheduledActionName=dup-action" +
				"&Schedule=cron(0+12+*+*+?+*)",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"ScheduledActionAlreadyExists"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newRedshiftHandler()
			if tt.name == "duplicate" {
				postRedshiftForm(t, h, "Action=CreateScheduledAction&Version=2012-12-01"+
					"&ScheduledActionName=dup-action&Schedule=cron(0+12+*+*+?+*)")
			}

			rec := postRedshiftForm(t, h, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)

			for _, s := range tt.wantContains {
				assert.Contains(t, rec.Body.String(), s)
			}
		})
	}
}

// ---- DeleteScheduledAction ----

func TestHandler_DeleteScheduledAction(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		body         string
		wantContains []string
		wantCode     int
	}{
		{
			name:         "success",
			body:         "Action=DeleteScheduledAction&Version=2012-12-01&ScheduledActionName=my-action",
			wantCode:     http.StatusOK,
			wantContains: []string{"DeleteScheduledActionResponse"},
		},
		{
			name:         "not_found",
			body:         "Action=DeleteScheduledAction&Version=2012-12-01&ScheduledActionName=nonexistent",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"ScheduledActionNotFound"},
		},
		{
			name:         "missing_name",
			body:         "Action=DeleteScheduledAction&Version=2012-12-01",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"InvalidParameterValue"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newRedshiftHandler()
			if tt.name == "success" {
				postRedshiftForm(t, h, "Action=CreateScheduledAction&Version=2012-12-01"+
					"&ScheduledActionName=my-action&Schedule=cron(0+12+*+*+?+*)")
			}

			rec := postRedshiftForm(t, h, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)

			for _, s := range tt.wantContains {
				assert.Contains(t, rec.Body.String(), s)
			}
		})
	}
}

// ---- DescribeScheduledActions ----

func TestHandler_DescribeScheduledActions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		body         string
		wantContains []string
		wantCode     int
	}{
		{
			name:         "empty",
			body:         "Action=DescribeScheduledActions&Version=2012-12-01",
			wantCode:     http.StatusOK,
			wantContains: []string{"DescribeScheduledActionsResponse"},
		},
		{
			name:         "with_data",
			body:         "Action=DescribeScheduledActions&Version=2012-12-01",
			wantCode:     http.StatusOK,
			wantContains: []string{"DescribeScheduledActionsResponse", "test-action"},
		},
		{
			name:         "filter_by_name",
			body:         "Action=DescribeScheduledActions&Version=2012-12-01&ScheduledActionName=test-action",
			wantCode:     http.StatusOK,
			wantContains: []string{"test-action", "ACTIVE"},
		},
		{
			name:         "filter_not_found",
			body:         "Action=DescribeScheduledActions&Version=2012-12-01&ScheduledActionName=nonexistent",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"ScheduledActionNotFound"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newRedshiftHandler()
			if tt.name == "with_data" || tt.name == "filter_by_name" {
				postRedshiftForm(t, h, "Action=CreateScheduledAction&Version=2012-12-01"+
					"&ScheduledActionName=test-action&Schedule=cron(0+12+*+*+?+*)")
			}

			rec := postRedshiftForm(t, h, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)

			for _, s := range tt.wantContains {
				assert.Contains(t, rec.Body.String(), s)
			}
		})
	}
}

// ---- ModifyScheduledAction ----

func TestHandler_ModifyScheduledAction(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		body         string
		wantContains []string
		wantCode     int
	}{
		{
			name: "success_update_schedule",
			body: "Action=ModifyScheduledAction&Version=2012-12-01" +
				"&ScheduledActionName=my-action" +
				"&Schedule=cron(0+6+*+*+?+*)",
			wantCode:     http.StatusOK,
			wantContains: []string{"ModifyScheduledActionResponse", "my-action"},
		},
		{
			name: "success_update_description",
			body: "Action=ModifyScheduledAction&Version=2012-12-01" +
				"&ScheduledActionName=my-action" +
				"&ScheduledActionDescription=Updated+description",
			wantCode:     http.StatusOK,
			wantContains: []string{"ModifyScheduledActionResponse", "my-action"},
		},
		{
			name:         "not_found",
			body:         "Action=ModifyScheduledAction&Version=2012-12-01&ScheduledActionName=nonexistent",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"ScheduledActionNotFound"},
		},
		{
			name:         "missing_name",
			body:         "Action=ModifyScheduledAction&Version=2012-12-01&Schedule=cron(0+6+*+*+?+*)",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"InvalidParameterValue"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newRedshiftHandler()
			if tt.name == "success_update_schedule" || tt.name == "success_update_description" {
				postRedshiftForm(t, h, "Action=CreateScheduledAction&Version=2012-12-01"+
					"&ScheduledActionName=my-action&Schedule=cron(0+12+*+*+?+*)")
			}

			rec := postRedshiftForm(t, h, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)

			for _, s := range tt.wantContains {
				assert.Contains(t, rec.Body.String(), s)
			}
		})
	}
}

// ---- RestoreTableFromClusterSnapshot ----

func TestHandler_RestoreTableFromClusterSnapshot(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		body         string
		wantContains []string
		wantCode     int
	}{
		{
			name: "success",
			body: "Action=RestoreTableFromClusterSnapshot&Version=2012-12-01" +
				"&ClusterIdentifier=my-cluster" +
				"&SnapshotIdentifier=my-snapshot" +
				"&SourceDatabaseName=mydb" +
				"&SourceTableName=orders" +
				"&TargetDatabaseName=mydb" +
				"&NewTableName=orders_restored",
			wantCode:     http.StatusOK,
			wantContains: []string{"RestoreTableFromClusterSnapshotResponse", "IN_PROGRESS"},
		},
		{
			name:         "missing_cluster_id",
			body:         "Action=RestoreTableFromClusterSnapshot&Version=2012-12-01&SnapshotIdentifier=my-snap",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"InvalidParameterValue"},
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

// ---- Backend tests for HSM ----

func TestBackend_HsmClientCertificate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T, b *redshift.InMemoryBackend)
		name string
	}{
		{
			name: "create_increments_count",
			run: func(t *testing.T, b *redshift.InMemoryBackend) {
				t.Helper()
				_, err := b.CreateHsmClientCertificate("cert-1", nil)
				require.NoError(t, err)
				assert.Equal(t, 1, redshift.HsmClientCertCount(b))
			},
		},
		{
			name: "delete_decrements_count",
			run: func(t *testing.T, b *redshift.InMemoryBackend) {
				t.Helper()
				_, err := b.CreateHsmClientCertificate("cert-del", nil)
				require.NoError(t, err)
				err = b.DeleteHsmClientCertificate("cert-del")
				require.NoError(t, err)
				assert.Equal(t, 0, redshift.HsmClientCertCount(b))
			},
		},
		{
			name: "describe_returns_created",
			run: func(t *testing.T, b *redshift.InMemoryBackend) {
				t.Helper()
				_, err := b.CreateHsmClientCertificate("cert-desc", nil)
				require.NoError(t, err)
				certs, err := b.DescribeHsmClientCertificates("")
				require.NoError(t, err)
				assert.Len(t, certs, 1)
				assert.Equal(t, "cert-desc", certs[0].HsmClientCertificateIdentifier)
			},
		},
		{
			name: "duplicate_create_returns_error",
			run: func(t *testing.T, b *redshift.InMemoryBackend) {
				t.Helper()
				_, err := b.CreateHsmClientCertificate("cert-dup", nil)
				require.NoError(t, err)
				_, err = b.CreateHsmClientCertificate("cert-dup", nil)
				require.Error(t, err)
				assert.ErrorIs(t, err, redshift.ErrHsmClientCertAlreadyExists)
			},
		},
		{
			name: "delete_not_found_returns_error",
			run: func(t *testing.T, b *redshift.InMemoryBackend) {
				t.Helper()
				err := b.DeleteHsmClientCertificate("nonexistent")
				require.Error(t, err)
				assert.ErrorIs(t, err, redshift.ErrHsmClientCertNotFound)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := redshift.NewInMemoryBackend("123456789012", "us-east-1")
			tt.run(t, b)
		})
	}
}

// ---- Backend tests for HSM Configuration ----

func TestBackend_HsmConfiguration(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T, b *redshift.InMemoryBackend)
		name string
	}{
		{
			name: "create_increments_count",
			run: func(t *testing.T, b *redshift.InMemoryBackend) {
				t.Helper()
				_, err := b.CreateHsmConfiguration("cfg-1", "desc", "10.0.0.1", "p1", nil)
				require.NoError(t, err)
				assert.Equal(t, 1, redshift.HsmConfigCount(b))
			},
		},
		{
			name: "delete_decrements_count",
			run: func(t *testing.T, b *redshift.InMemoryBackend) {
				t.Helper()
				_, err := b.CreateHsmConfiguration("cfg-del", "desc", "10.0.0.1", "p1", nil)
				require.NoError(t, err)
				err = b.DeleteHsmConfiguration("cfg-del")
				require.NoError(t, err)
				assert.Equal(t, 0, redshift.HsmConfigCount(b))
			},
		},
		{
			name: "describe_returns_fields",
			run: func(t *testing.T, b *redshift.InMemoryBackend) {
				t.Helper()
				_, err := b.CreateHsmConfiguration("cfg-desc", "My HSM", "192.168.1.1", "partition-a", nil)
				require.NoError(t, err)
				cfgs, err := b.DescribeHsmConfigurations("cfg-desc")
				require.NoError(t, err)
				require.Len(t, cfgs, 1)
				assert.Equal(t, "cfg-desc", cfgs[0].HsmConfigurationIdentifier)
				assert.Equal(t, "My HSM", cfgs[0].Description)
				assert.Equal(t, "192.168.1.1", cfgs[0].HsmIPAddress)
				assert.Equal(t, "partition-a", cfgs[0].HsmPartitionName)
			},
		},
		{
			name: "duplicate_create_returns_error",
			run: func(t *testing.T, b *redshift.InMemoryBackend) {
				t.Helper()
				_, err := b.CreateHsmConfiguration("cfg-dup", "desc", "10.0.0.1", "p1", nil)
				require.NoError(t, err)
				_, err = b.CreateHsmConfiguration("cfg-dup", "desc", "10.0.0.1", "p1", nil)
				require.Error(t, err)
				assert.ErrorIs(t, err, redshift.ErrHsmConfigAlreadyExists)
			},
		},
		{
			name: "describe_not_found_returns_error",
			run: func(t *testing.T, b *redshift.InMemoryBackend) {
				t.Helper()
				_, err := b.DescribeHsmConfigurations("nonexistent")
				require.Error(t, err)
				assert.ErrorIs(t, err, redshift.ErrHsmConfigNotFound)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := redshift.NewInMemoryBackend("123456789012", "us-east-1")
			tt.run(t, b)
		})
	}
}

// ---- Backend tests for ScheduledAction ----

func TestBackend_ScheduledAction(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T, b *redshift.InMemoryBackend)
		name string
	}{
		{
			name: "create_increments_count",
			run: func(t *testing.T, b *redshift.InMemoryBackend) {
				t.Helper()
				_, err := b.CreateScheduledAction(
					"action-1", "cron(0 12 * * ? *)", "arn:aws:iam::123:role/R", "desc", "",
				)
				require.NoError(t, err)
				assert.Equal(t, 1, redshift.ScheduledActionCount(b))
			},
		},
		{
			name: "delete_decrements_count",
			run: func(t *testing.T, b *redshift.InMemoryBackend) {
				t.Helper()
				_, err := b.CreateScheduledAction("action-del", "cron(0 12 * * ? *)", "", "", "")
				require.NoError(t, err)
				err = b.DeleteScheduledAction("action-del")
				require.NoError(t, err)
				assert.Equal(t, 0, redshift.ScheduledActionCount(b))
			},
		},
		{
			name: "describe_all_returns_all",
			run: func(t *testing.T, b *redshift.InMemoryBackend) {
				t.Helper()
				_, err := b.CreateScheduledAction("a1", "cron(0 12 * * ? *)", "", "", "")
				require.NoError(t, err)
				_, err = b.CreateScheduledAction("a2", "rate(1 day)", "", "", "")
				require.NoError(t, err)
				actions, err := b.DescribeScheduledActions("")
				require.NoError(t, err)
				assert.Len(t, actions, 2)
			},
		},
		{
			name: "modify_updates_schedule",
			run: func(t *testing.T, b *redshift.InMemoryBackend) {
				t.Helper()
				_, err := b.CreateScheduledAction("action-mod", "cron(0 12 * * ? *)", "", "", "")
				require.NoError(t, err)
				updated, err := b.ModifyScheduledAction("action-mod", "rate(1 hour)", "", "")
				require.NoError(t, err)
				assert.Equal(t, "rate(1 hour)", updated.Schedule)
			},
		},
		{
			name: "modify_updates_description",
			run: func(t *testing.T, b *redshift.InMemoryBackend) {
				t.Helper()
				_, err := b.CreateScheduledAction("action-desc", "cron(0 12 * * ? *)", "", "old desc", "")
				require.NoError(t, err)
				updated, err := b.ModifyScheduledAction("action-desc", "", "", "new desc")
				require.NoError(t, err)
				assert.Equal(t, "new desc", updated.ScheduledActionDescription)
			},
		},
		{
			name: "modify_not_found_returns_error",
			run: func(t *testing.T, b *redshift.InMemoryBackend) {
				t.Helper()
				_, err := b.ModifyScheduledAction("nonexistent", "rate(1 day)", "", "")
				require.Error(t, err)
				assert.ErrorIs(t, err, redshift.ErrScheduledActionNotFound)
			},
		},
		{
			name: "duplicate_create_returns_error",
			run: func(t *testing.T, b *redshift.InMemoryBackend) {
				t.Helper()
				_, err := b.CreateScheduledAction("action-dup", "cron(0 12 * * ? *)", "", "", "")
				require.NoError(t, err)
				_, err = b.CreateScheduledAction("action-dup", "rate(1 day)", "", "", "")
				require.Error(t, err)
				assert.ErrorIs(t, err, redshift.ErrScheduledActionAlreadyExists)
			},
		},
		{
			name: "delete_not_found_returns_error",
			run: func(t *testing.T, b *redshift.InMemoryBackend) {
				t.Helper()
				err := b.DeleteScheduledAction("nonexistent")
				require.Error(t, err)
				assert.ErrorIs(t, err, redshift.ErrScheduledActionNotFound)
			},
		},
		{
			name: "state_is_active_on_create",
			run: func(t *testing.T, b *redshift.InMemoryBackend) {
				t.Helper()
				a, err := b.CreateScheduledAction("action-state", "cron(0 12 * * ? *)", "", "", "")
				require.NoError(t, err)
				assert.Equal(t, "ACTIVE", a.State)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := redshift.NewInMemoryBackend("123456789012", "us-east-1")
			tt.run(t, b)
		})
	}
}

// ---- Backend tests for TableRestoreStatus ----

func TestBackend_TableRestoreStatus(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T, b *redshift.InMemoryBackend)
		name string
	}{
		{
			name: "create_returns_in_progress",
			run: func(t *testing.T, b *redshift.InMemoryBackend) {
				t.Helper()
				tr, err := b.CreateTableRestoreStatus(
					"my-cluster", "my-snap", "db1", "table1", "db1", "table1_restored",
				)
				require.NoError(t, err)
				assert.Equal(t, "IN_PROGRESS", tr.Status)
				assert.Equal(t, "my-cluster", tr.ClusterIdentifier)
				assert.NotEmpty(t, tr.TableRestoreRequestID)
			},
		},
		{
			name: "describe_returns_created",
			run: func(t *testing.T, b *redshift.InMemoryBackend) {
				t.Helper()
				_, err := b.CreateTableRestoreStatus("my-cluster", "snap-1", "db1", "t1", "db1", "t1_new")
				require.NoError(t, err)
				statuses, err := b.DescribeTableRestoreStatus("my-cluster")
				require.NoError(t, err)
				assert.Len(t, statuses, 1)
			},
		},
		{
			name: "missing_cluster_id_returns_error",
			run: func(t *testing.T, b *redshift.InMemoryBackend) {
				t.Helper()
				_, err := b.CreateTableRestoreStatus("", "snap-1", "db1", "t1", "db1", "t1_new")
				require.Error(t, err)
				assert.ErrorIs(t, err, redshift.ErrInvalidParameter)
			},
		},
		{
			name: "multiple_restores_unique_ids",
			run: func(t *testing.T, b *redshift.InMemoryBackend) {
				t.Helper()
				tr1, err := b.CreateTableRestoreStatus("c1", "snap-1", "db1", "t1", "db1", "t1_new")
				require.NoError(t, err)
				tr2, err := b.CreateTableRestoreStatus("c1", "snap-1", "db1", "t2", "db1", "t2_new")
				require.NoError(t, err)
				assert.NotEqual(t, tr1.TableRestoreRequestID, tr2.TableRestoreRequestID)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := redshift.NewInMemoryBackend("123456789012", "us-east-1")
			tt.run(t, b)
		})
	}
}

// ---- Backend Reset clears HSM and ScheduledAction state ----

func TestBackend_Reset_ClearsNewState(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T, b *redshift.InMemoryBackend)
		name string
	}{
		{
			name: "reset_clears_hsm_certs",
			run: func(t *testing.T, b *redshift.InMemoryBackend) {
				t.Helper()
				_, err := b.CreateHsmClientCertificate("cert-1", nil)
				require.NoError(t, err)
				b.Reset()
				assert.Equal(t, 0, redshift.HsmClientCertCount(b))
			},
		},
		{
			name: "reset_clears_hsm_configs",
			run: func(t *testing.T, b *redshift.InMemoryBackend) {
				t.Helper()
				_, err := b.CreateHsmConfiguration("cfg-1", "desc", "10.0.0.1", "p1", nil)
				require.NoError(t, err)
				b.Reset()
				assert.Equal(t, 0, redshift.HsmConfigCount(b))
			},
		},
		{
			name: "reset_clears_scheduled_actions",
			run: func(t *testing.T, b *redshift.InMemoryBackend) {
				t.Helper()
				_, err := b.CreateScheduledAction("action-1", "cron(0 12 * * ? *)", "", "", "")
				require.NoError(t, err)
				b.Reset()
				assert.Equal(t, 0, redshift.ScheduledActionCount(b))
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := redshift.NewInMemoryBackend("123456789012", "us-east-1")
			tt.run(t, b)
		})
	}
}

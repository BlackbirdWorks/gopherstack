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
			name: "success",
			body: "Action=CreateHsmClientCertificate&Version=2012-12-01&HsmClientCertificateIdentifier=my-cert" +
				"&Tags.Tag.1.Key=env&Tags.Tag.1.Value=prod",
			wantCode: http.StatusOK,
			wantContains: []string{
				"CreateHsmClientCertificateResponse", "my-cert", "PUBLIC KEY",
				"<Tags><Tag><Key>env</Key><Value>prod</Value></Tag></Tags>",
			},
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
				"&HsmIpAddress=192.168.1.100" +
				"&HsmPartitionName=my-partition" +
				"&Tags.Tag.1.Key=env&Tags.Tag.1.Value=prod",
			wantCode: http.StatusOK,
			wantContains: []string{
				"CreateHsmConfigurationResponse", "my-hsm-config", "192.168.1.100", "my-partition",
				"<Tags><Tag><Key>env</Key><Value>prod</Value></Tag></Tags>",
			},
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
				"&HsmIpAddress=10.0.0.1" +
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
					"&HsmConfigurationIdentifier=dup-config&HsmIpAddress=10.0.0.1&HsmPartitionName=p1")
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
					"&HsmConfigurationIdentifier=my-config&HsmIpAddress=10.0.0.1&HsmPartitionName=p1")
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
					"&HsmConfigurationIdentifier=hsm-config-1&HsmIpAddress=10.0.0.1&HsmPartitionName=p1")
			}

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

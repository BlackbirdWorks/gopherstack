package rds_test

import (
	"net/http"
	"net/url"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/rds"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDescribeAccountAttributes(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		wantCount int
	}{
		{name: "returns attributes", wantCount: 8},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := newTestBackend(t)
			got := b.DescribeAccountAttributes()
			assert.Len(t, got, tt.wantCount)
			assert.Equal(t, "DBInstances", got[1].AttributeName)
		})
	}
}

func TestDescribeCertificates(t *testing.T) {
	t.Parallel()
	tests := []struct {
		wantErrIs error
		name      string
		certID    string
		wantCount int
		wantErr   bool
	}{
		{name: "all certs", certID: "", wantCount: 4},
		{name: "specific cert", certID: "rds-ca-2019", wantCount: 1},
		{name: "not found", certID: "missing-cert", wantErr: true, wantErrIs: rds.ErrInvalidParameter},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := newTestBackend(t)
			got, err := b.DescribeCertificates(tt.certID)
			if tt.wantErr {
				require.Error(t, err)
				require.ErrorIs(t, err, tt.wantErrIs)

				return
			}
			require.NoError(t, err)
			assert.Len(t, got, tt.wantCount)
		})
	}
}

func TestModifyCertificates(t *testing.T) {
	t.Parallel()
	tests := []struct {
		wantErrIs error
		name      string
		certID    string
		wantErr   bool
	}{
		{name: "success", certID: "rds-ca-rsa2048-g1"},
		{name: "not found", certID: "missing", wantErr: true, wantErrIs: rds.ErrInvalidParameter},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := newTestBackend(t)
			got, err := b.ModifyCertificates(tt.certID)
			if tt.wantErr {
				require.Error(t, err)
				require.ErrorIs(t, err, tt.wantErrIs)

				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.certID, got.CertificateIdentifier)
		})
	}
}

func TestDescribeSourceRegions(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		regionName string
		wantCount  int
	}{
		{name: "all regions", regionName: "", wantCount: 12},
		{name: "specific region", regionName: "us-east-1", wantCount: 1},
		{name: "no match", regionName: "ap-south-2", wantCount: 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := newTestBackend(t)
			got := b.DescribeSourceRegions(tt.regionName)
			assert.Len(t, got, tt.wantCount)
		})
	}
}

func TestDescribeDBMajorEngineVersions(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		engine    string
		wantEmpty bool
	}{
		{name: "all engines", engine: ""},
		{name: "mysql only", engine: "mysql"},
		{name: "postgres only", engine: "postgres"},
		{name: "no match", engine: "nonexistent", wantEmpty: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := newTestBackend(t)
			got := b.DescribeDBMajorEngineVersions(tt.engine)
			if tt.wantEmpty {
				assert.Empty(t, got)

				return
			}
			assert.NotEmpty(t, got)
			if tt.engine != "" {
				for _, v := range got {
					assert.Equal(t, tt.engine, v.Engine)
				}
			}
		})
	}
}

// TestDescribeServerlessV2PlatformVersions exercises the backend method directly:
// Engine validation (the only enumerable constraint the installed SDK documents for
// this action -- see reference_data.go's doc comment on why the returned catalog is
// always empty) and that every valid filter combination still returns cleanly.
func TestDescribeServerlessV2PlatformVersions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErrIs   error
		name        string
		engine      string
		version     string
		defaultOnly bool
		includeAll  bool
		wantErr     bool
	}{
		{name: "no_filters", engine: ""},
		{name: "aurora_mysql", engine: "aurora-mysql"},
		{name: "aurora_postgresql", engine: "aurora-postgresql"},
		{name: "default_only", engine: "aurora-mysql", defaultOnly: true},
		{name: "include_all", engine: "aurora-mysql", includeAll: true},
		{name: "specific_version", engine: "aurora-mysql", version: "3"},
		{
			name:      "invalid_engine_rejected",
			engine:    "aurora-mariadb",
			wantErr:   true,
			wantErrIs: rds.ErrInvalidParameter,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend(t)
			got, err := b.DescribeServerlessV2PlatformVersions(tt.engine, tt.version, tt.defaultOnly, tt.includeAll)

			if tt.wantErr {
				require.Error(t, err)
				require.ErrorIs(t, err, tt.wantErrIs)

				return
			}

			require.NoError(t, err)
			// This backend's catalog is always empty (see reference_data.go) -- no
			// real AWS platform version numbers to derive from in the installed SDK.
			assert.Empty(t, got)
		})
	}
}

// TestHandler_DescribeServerlessV2PlatformVersions asserts the wire shape through the
// real router path (form-encoded request -> Handler.Handler() -> XML response),
// matching this package's established harness (doAccuracyRDS/newAccuracyRDSHandler,
// see engine_versions_test.go for the same pattern applied to other Describe* ops).
func TestHandler_DescribeServerlessV2PlatformVersions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		vals       url.Values
		name       string
		wantBody   []string
		wantStatus int
	}{
		{
			name: "success_no_filters",
			vals: url.Values{
				"Action":  {"DescribeServerlessV2PlatformVersions"},
				"Version": {"2014-10-31"},
			},
			wantStatus: http.StatusOK,
			wantBody: []string{
				"DescribeServerlessV2PlatformVersionsResponse",
				"DescribeServerlessV2PlatformVersionsResult",
			},
		},
		{
			name: "success_valid_engine_filter",
			vals: url.Values{
				"Action":  {"DescribeServerlessV2PlatformVersions"},
				"Version": {"2014-10-31"},
				"Engine":  {"aurora-postgresql"},
			},
			wantStatus: http.StatusOK,
			wantBody:   []string{"DescribeServerlessV2PlatformVersionsResponse"},
		},
		{
			name: "invalid_engine_errors",
			vals: url.Values{
				"Action":  {"DescribeServerlessV2PlatformVersions"},
				"Version": {"2014-10-31"},
				"Engine":  {"not-a-real-engine"},
			},
			wantStatus: http.StatusBadRequest,
			wantBody:   []string{"InvalidParameterValue"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newAccuracyRDSHandler()
			rec := doAccuracyRDS(t, h, tt.vals)
			assert.Equal(t, tt.wantStatus, rec.Code, "body: %s", rec.Body.String())

			for _, want := range tt.wantBody {
				assert.Contains(t, rec.Body.String(), want)
			}
		})
	}
}

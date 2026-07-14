package rds_test

import (
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

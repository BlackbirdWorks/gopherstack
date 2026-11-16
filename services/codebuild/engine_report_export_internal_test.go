package codebuild

import (
	"archive/zip"
	"bytes"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	codebuildsdk "github.com/aws/aws-sdk-go-v2/service/codebuild"
	cbtypes "github.com/aws/aws-sdk-go-v2/service/codebuild/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEngineReportExport(t *testing.T) {
	t.Parallel()

	tests := []struct {
		cfg      *cbtypes.ReportExportConfig
		name     string
		wantKeys int
		zipped   bool
	}{
		{
			name: "raw files",
			cfg: &cbtypes.ReportExportConfig{
				ExportConfigType: cbtypes.ReportExportConfigTypeS3,
				S3Destination: &cbtypes.S3ReportExportConfig{
					Bucket: aws.String("exp"), Path: aws.String("raw/data"), Packaging: cbtypes.ReportPackagingTypeNone,
				},
			},
			wantKeys: 1,
		},
		{
			name: "zip",
			cfg: &cbtypes.ReportExportConfig{
				ExportConfigType: cbtypes.ReportExportConfigTypeS3,
				S3Destination: &cbtypes.S3ReportExportConfig{
					Bucket: aws.String("exp"), Path: aws.String("z"), Packaging: cbtypes.ReportPackagingTypeZip,
				},
			},
			wantKeys: 1, zipped: true,
		},
		{
			name:     "no export",
			cfg:      &cbtypes.ReportExportConfig{ExportConfigType: cbtypes.ReportExportConfigTypeNoExport},
			wantKeys: 0,
		},
		{
			name: "no export ignores a leftover destination",
			cfg: &cbtypes.ReportExportConfig{
				ExportConfigType: cbtypes.ReportExportConfigTypeNoExport,
				S3Destination:    &cbtypes.S3ReportExportConfig{Bucket: aws.String("exp")},
			},
			wantKeys: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rt := newFakeRuntime(writeFilesRule(map[string]string{"out/r.xml": junitPassing}))
			h := newHarnessWith(t, rt, nil)
			c := sdkClientFor(t, h.b)

			rg, err := c.CreateReportGroup(t.Context(), &codebuildsdk.CreateReportGroupInput{
				Name: aws.String("proj-unit"), Type: cbtypes.ReportTypeTest, ExportConfig: tt.cfg,
			})
			require.NoError(t, err)
			assert.Equal(t, tt.cfg.ExportConfigType, rg.ReportGroup.ExportConfig.ExportConfigType)
			assert.Equal(t, tt.cfg.S3Destination, rg.ReportGroup.ExportConfig.S3Destination)

			createSDKProject(t, c, sdkProject{source: &cbtypes.ProjectSource{
				Type:      cbtypes.SourceTypeNoSource,
				Buildspec: aws.String(reportSpec("unit", "JUNITXML", "r.xml")),
			}})

			b := startAndWait(t, h, c, &codebuildsdk.StartBuildInput{})
			require.Equal(t, cbtypes.StatusTypeSucceeded, b.BuildStatus, failedPhaseMessage(b))

			keys := h.store.keys()
			require.Len(t, keys, tt.wantKeys)

			if tt.wantKeys == 0 {
				return
			}

			data := h.store.objects[keys[0]]

			if !tt.zipped {
				assert.Contains(t, keys[0], "exp/raw/data/")
				assert.Contains(t, keys[0], "/r.xml")
				assert.Equal(t, junitPassing, string(data))

				return
			}

			assert.Contains(t, keys[0], ".zip")

			zr, zerr := zip.NewReader(bytes.NewReader(data), int64(len(data)))
			require.NoError(t, zerr)
			require.Len(t, zr.File, 1)

			f, ferr := zr.File[0].Open()
			require.NoError(t, ferr)

			body, _ := io.ReadAll(f)
			assert.Equal(t, junitPassing, string(body))
		})
	}
}

func TestReportExportConfigValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		cfg  ReportExportConfig
		name string
		bad  bool
	}{
		{name: "empty", cfg: ReportExportConfig{}},
		{name: "no export", cfg: ReportExportConfig{ExportConfigType: "NO_EXPORT"}},
		{name: "s3 without destination", cfg: ReportExportConfig{ExportConfigType: "S3"}},
		{
			name: "bad packaging",
			cfg: ReportExportConfig{
				ExportConfigType: "S3", S3Destination: &S3ReportExportConfig{Bucket: "b", Packaging: "TAR"},
			},
			bad: true,
		},
		{name: "unknown type", cfg: ReportExportConfig{ExportConfigType: "FTP"}, bad: true},
		{
			name: "valid", cfg: ReportExportConfig{
				ExportConfigType: "S3", S3Destination: &S3ReportExportConfig{Bucket: "b", Packaging: "ZIP"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := NewInMemoryBackend("000000000000", "us-east-1")
			_, err := b.CreateReportGroup("g", "TEST", tt.cfg, nil)

			if tt.bad {
				require.ErrorIs(t, err, ErrValidation)

				return
			}

			require.NoError(t, err)

			snap := b.Snapshot(t.Context())
			b2 := NewInMemoryBackend("000000000000", "us-east-1")
			require.NoError(t, b2.Restore(t.Context(), snap))

			got, nf := b2.BatchGetReportGroups([]string{"g"})
			require.Empty(t, nf)
			assert.Equal(t, tt.cfg, got[0].ExportConfig)
		})
	}
}

func TestReadReportFilesConfinement(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		link    bool
		wantErr bool
	}{
		{name: "regular file"},
		{name: "symlink out of base", link: true, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			outside := t.TempDir()
			base := t.TempDir()

			require.NoError(t, os.WriteFile(filepath.Join(outside, "secret"), []byte("s"), 0o600))

			if tt.link {
				require.NoError(t, os.Symlink(filepath.Join(outside, "secret"), filepath.Join(base, "r.xml")))
			} else {
				require.NoError(t, os.WriteFile(filepath.Join(base, "r.xml"), []byte("ok"), 0o600))
			}

			got, err := readReportFiles(base, []string{"r.xml"}, maxReportExportBytes)
			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, "ok", string(got["r.xml"]))
		})
	}
}

func TestReadReportFilesCap(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		limit   int
		wantErr bool
	}{
		{name: "exact fit", limit: 8},
		{name: "one byte over", limit: 7, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			dir := t.TempDir()
			require.NoError(t, os.WriteFile(filepath.Join(dir, "a"), []byte("1234"), 0o600))
			require.NoError(t, os.WriteFile(filepath.Join(dir, "b"), []byte("5678"), 0o600))

			got, err := readReportFiles(dir, []string{"a", "b"}, tt.limit)
			if tt.wantErr {
				require.ErrorIs(t, err, errUnsafePath)

				return
			}

			require.NoError(t, err)
			assert.Len(t, got, 2)
		})
	}
}

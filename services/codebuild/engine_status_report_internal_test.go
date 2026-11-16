package codebuild

import (
	"bytes"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

func TestLogSourceStatus(t *testing.T) {
	t.Parallel()

	tests := []struct {
		src  *ProjectSource
		name string
		want bool
	}{
		{name: "github reporting", src: &ProjectSource{Type: sourceTypeGitHub, ReportBuildStatus: true}, want: true},
		{
			name: "with context",
			src: &ProjectSource{
				Type: sourceTypeGitLab, ReportBuildStatus: true, BuildStatusConfig: &BuildStatusConfig{Context: "ci"},
			},
			want: true,
		},
		{name: "reporting off", src: &ProjectSource{Type: sourceTypeGitHub}},
		{name: "not a git host", src: &ProjectSource{Type: "S3", ReportBuildStatus: true}},
		{name: "no source"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var buf bytes.Buffer

			ctx := logger.Save(t.Context(), slog.New(slog.NewTextHandler(&buf, nil)))
			r := &buildRunner{ctx: ctx, build: Build{ID: "proj:1", Source: tt.src}}
			r.logSourceStatus("SUCCEEDED")

			assert.Equal(t, tt.want, bytes.Contains(buf.Bytes(), []byte("would report build status")))
		})
	}
}

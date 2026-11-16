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

func TestLogSourceStatusRedactsCredentials(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		location string
		target   string
	}{
		{name: "userinfo token", location: "https://ghp_SECRET@github.com/o/r.git", target: "https://ci.example/b"},
		{name: "user and password", location: "https://u:SECRET@github.com/o/r.git", target: "https://ci.example/b"},
		{name: "query token", location: "https://github.com/o/r.git", target: "https://ci.example/b?token=SECRET"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var buf bytes.Buffer

			ctx := logger.Save(t.Context(), slog.New(slog.NewTextHandler(&buf, nil)))
			src := &ProjectSource{
				Type: sourceTypeGitHub, Location: tt.location, ReportBuildStatus: true,
				BuildStatusConfig: &BuildStatusConfig{TargetURL: tt.target},
			}
			r := &buildRunner{ctx: ctx, build: Build{ID: "proj:1", Source: src}}
			r.logSourceStatus("SUCCEEDED")

			assert.Contains(t, buf.String(), "would report build status")
			assert.NotContains(t, buf.String(), "SECRET")
		})
	}
}

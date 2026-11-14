package gcpcloudbuild_test

import (
	"testing"

	"cloud.google.com/go/cloudbuild/apiv1/v2/cloudbuildpb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
	"github.com/blackbirdworks/gopherstack/services/gcpcloudbuild"
)

func TestRepoSourceRegex(t *testing.T) {
	t.Parallel()

	tests := []struct {
		rev     *cloudbuildpb.RepoSource
		name    string
		wantRev string
		wantErr string
	}{
		{
			name:    "branch regex",
			rev:     &cloudbuildpb.RepoSource{Revision: &cloudbuildpb.RepoSource_BranchName{BranchName: "^ma.n$"}},
			wantRev: "main",
		},
		{
			name:    "exact branch wins",
			rev:     &cloudbuildpb.RepoSource{Revision: &cloudbuildpb.RepoSource_BranchName{BranchName: "main"}},
			wantRev: "main",
		},
		{
			name:    "tag regex picks first sorted match",
			rev:     &cloudbuildpb.RepoSource{Revision: &cloudbuildpb.RepoSource_TagName{TagName: `^v\d+$`}},
			wantRev: "v1",
		},
		{
			name:    "exact tag wins over earlier match",
			rev:     &cloudbuildpb.RepoSource{Revision: &cloudbuildpb.RepoSource_TagName{TagName: "v2"}},
			wantRev: "v2",
		},
		{
			name:    "no match",
			rev:     &cloudbuildpb.RepoSource{Revision: &cloudbuildpb.RepoSource_BranchName{BranchName: "^release-"}},
			wantErr: `no ref matches "^release-"`,
		},
		{
			name:    "commit sha is not resolved",
			rev:     &cloudbuildpb.RepoSource{Revision: &cloudbuildpb.RepoSource_CommitSha{CommitSha: commit}},
			wantRev: commit,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var seen container.Spec

			e := newEnv(t, map[string]behavior{gitImage: fakeGit(&seen), "img": exits(0)})
			e.configure(func(s *gcpcloudbuild.Services) { s.RepoBase = "https://git.example.com" })

			tt.rev.RepoName = "web"

			got := e.run(t, gitBuild(&cloudbuildpb.Source{Source: &cloudbuildpb.Source_RepoSource{RepoSource: tt.rev}}))

			if tt.wantErr != "" {
				assert.Equal(t, cloudbuildpb.Build_FAILURE, got.GetStatus())
				assert.Contains(t, got.GetStatusDetail(), tt.wantErr)
				assert.Equal(t, cloudbuildpb.Build_FailureInfo_FETCH_SOURCE_FAILED, got.GetFailureInfo().GetType())

				return
			}

			require.Equal(t, cloudbuildpb.Build_SUCCESS, got.GetStatus(), got.GetStatusDetail())
			assert.Equal(t, tt.wantRev, seen.Cmd[4])
		})
	}
}

func TestRepoSourceBadRegexRejected(t *testing.T) {
	t.Parallel()

	e := newEnv(t, map[string]behavior{"img": exits(0)})

	src := &cloudbuildpb.RepoSource{
		RepoName: "web", Revision: &cloudbuildpb.RepoSource_BranchName{BranchName: "(unclosed"},
	}

	_, err := e.client.CreateBuild(t.Context(), &cloudbuildpb.CreateBuildRequest{
		Parent: parent,
		Build: func() *cloudbuildpb.Build {
			return gitBuild(&cloudbuildpb.Source{Source: &cloudbuildpb.Source_RepoSource{RepoSource: src}})
		}(),
	})
	require.Error(t, err)
	assert.Equal(t, codes.InvalidArgument, status.Code(err))
	assert.Contains(t, err.Error(), "invalid regular expression")
}

package codebuild

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDockerEngineArtifactNameShellExpansion(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		specName string
		wantKey  string
	}{
		{name: "literal", specName: "plain", wantKey: "artifacts/plain/f.txt"},
		{name: "env var and command", specName: "out-$FOO-$(echo sub)", wantKey: "artifacts/out-bar-sub/f.txt"},
		{name: "builtin var in path", specName: "b/$AWS_REGION/x", wantKey: "artifacts/b/us-east-1/x/f.txt"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			spec := "version: 0.2\nenv:\n  variables:\n    FOO: bar\nphases:\n  build:\n    commands:\n" +
				"      - echo hi > f.txt\nartifacts:\n  files: ['f.txt']\n  name: \"" + tt.specName + "\"\n"
			h := dockerEngineHarness(t)
			h.project(t, ProjectConfig{
				Source:    &ProjectSource{Type: "NO_SOURCE", Buildspec: spec},
				Artifacts: &ProjectArtifacts{Type: "S3", Location: "artifacts", OverrideArtifactName: true},
			})

			b := h.run(t, StartBuildConfig{})
			require.Equal(t, "SUCCEEDED", b.BuildStatus, h.logs.joined())
			assert.Contains(t, h.store.keys(), tt.wantKey)
		})
	}
}

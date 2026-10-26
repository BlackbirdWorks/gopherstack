package codebuild

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	codebuildsdk "github.com/aws/aws-sdk-go-v2/service/codebuild"
	cbtypes "github.com/aws/aws-sdk-go-v2/service/codebuild/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type keyedBuild struct {
	key, action string
	fallbacks   []string
}

func keyedSpec(b keyedBuild) string {
	var sb strings.Builder

	sb.WriteString("version: 0.2\nphases:\n  build:\n    commands: [echo BUILD_CMD]\ncache:\n")

	if b.key != "" {
		sb.WriteString("  key: " + b.key + "\n")
	}

	if b.action != "" {
		sb.WriteString("  action: " + b.action + "\n")
	}

	if len(b.fallbacks) > 0 {
		sb.WriteString("  fallback-keys:\n")

		for _, f := range b.fallbacks {
			sb.WriteString("    - " + f + "\n")
		}
	}

	sb.WriteString("  paths:\n    - 'deps/**/*'\n")

	return sb.String()
}

func TestEngineCacheKeyAndAction(t *testing.T) {
	t.Parallel()

	caches := map[string]*cbtypes.ProjectCache{
		"local": localCache(cbtypes.CacheModeLocalCustomCache),
		"s3":    {Type: cbtypes.CacheTypeS3, Location: aws.String("cachebucket/pfx")},
	}

	tests := []struct {
		name   string
		builds []keyedBuild
		want   []bool
	}{
		{"same_key", []keyedBuild{{key: "k1"}, {key: "k1"}}, []bool{false, true}},
		{"other_key_misses", []keyedBuild{{key: "k1"}, {key: "k2"}}, []bool{false, false}},
		{"key_is_exact", []keyedBuild{{key: "k-1"}, {key: "k"}}, []bool{false, false}},
		{
			"fallback_prefix",
			[]keyedBuild{{key: "npm-1"}, {key: "npm-2", fallbacks: []string{"npm-"}}},
			[]bool{false, true},
		},
		{
			"fallback_miss",
			[]keyedBuild{{key: "npm-1"}, {key: "pip-2", fallbacks: []string{"npm-x", "yarn-"}}},
			[]bool{false, false},
		},
		{
			"fallback_ignored_without_key",
			[]keyedBuild{{key: "npm-1"}, {fallbacks: []string{"npm-"}}},
			[]bool{false, false},
		},
		{"action_restore_never_saves", []keyedBuild{{key: "k1", action: "restore"}, {key: "k1"}}, []bool{false, false}},
		{"action_save_never_restores", []keyedBuild{{key: "k1"}, {key: "k1", action: "save"}}, []bool{false, false}},
		{
			"save_then_restore",
			[]keyedBuild{{key: "k1", action: "save"}, {key: "k1", action: "restore"}},
			[]bool{false, true},
		},
	}

	for cname, cache := range caches {
		for _, tt := range tests {
			t.Run(cname+"_"+tt.name, func(t *testing.T) {
				t.Parallel()

				obs, rule := cacheObserver()
				h := newHarnessWith(t, newFakeRuntime(rule), nil)
				c := sdkClientFor(t, h.b)
				createSDKProject(t, c, sdkProject{cache: cache, source: &cbtypes.ProjectSource{
					Type: cbtypes.SourceTypeNoSource, Buildspec: aws.String(keyedSpec(tt.builds[0])),
				}})

				for _, b := range tt.builds {
					out := startAndWait(
						t,
						h,
						c,
						&codebuildsdk.StartBuildInput{BuildspecOverride: aws.String(keyedSpec(b))},
					)
					require.Equal(t, cbtypes.StatusTypeSucceeded, out.BuildStatus, failedPhaseMessage(out))
				}

				obs.lock.RLock("read")
				defer obs.lock.RUnlock()

				assert.Equal(t, tt.want, obs.existed)
			})
		}
	}
}

func TestExpandCacheKey(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "a.lock"), []byte("one"), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "b.lock"), []byte("two"), 0o600))

	r := &buildRunner{workDir: dir, envMap: map[string]string{"SRC": "abc123", "OS": "linux"}}
	a := r.expandCacheKey("k-$(codebuild-hash-files a.lock)")
	both := r.expandCacheKey("k-$(codebuild-hash-files '*.lock')")

	tests := []struct {
		name, in, want string
	}{
		{"plain", "npm-key", "npm-key"},
		{"env", "npm-$SRC", "npm-abc123"},
		{"env_braces", "npm-${OS}-${SRC}", "npm-linux-abc123"},
		{"unknown_env_empty", "npm-$NOPE", "npm-"},
		{"hash_stable", "k-$(codebuild-hash-files a.lock)", a},
		{"hash_glob", "k-$(codebuild-hash-files '*.lock')", both},
		{"hash_no_match", "k-$(codebuild-hash-files missing.txt)", "k-"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, r.expandCacheKey(tt.in))
		})
	}

	assert.NotEqual(t, a, both)
	assert.Len(t, strings.TrimPrefix(a, "k-"), 64)
}

package guards_test

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	containerRemoveCall    = regexp.MustCompile(`\.ContainerRemove\(`)
	containerRemoveLiteral = regexp.MustCompile(
		`(?:\bContainerRemoveOptions|\b\w*container\w*\.RemoveOptions|\bdockertypes\.RemoveOptions)\{`,
	)
)

// containerRemovePassthrough lists wrappers that forward caller-supplied options verbatim.
func containerRemovePassthrough() map[string]string {
	return map[string]string{
		"pkgs/docker/docker.go":                  "realDockerClient forwards options",
		"pkgs/container/docker_runtime.go":       "realDockerClient forwards options",
		"internal/dockercompat/client/client.go": "compat client maps options field by field",
	}
}

func braceBody(src string, open int) string {
	open2, depth := open, 1
	for ; open2 < len(src) && depth > 0; open2++ {
		switch src[open2] {
		case '{', '(':
			depth++
		case '}', ')':
			depth--
		}
	}

	return src[open:open2]
}

// containerRemoveWithoutVolumes reports container removals that would orphan anonymous volumes.
func containerRemoveWithoutVolumes(src string) []string {
	var found []string

	for _, loc := range containerRemoveLiteral.FindAllStringIndex(src, -1) {
		if !strings.Contains(braceBody(src, loc[1]), "RemoveVolumes: true") {
			found = append(found, "container RemoveOptions without RemoveVolumes: true")
		}
	}

	for _, loc := range containerRemoveCall.FindAllStringIndex(src, -1) {
		if !strings.Contains(braceBody(src, loc[1]), "RemoveOptions{") {
			found = append(found, "ContainerRemove with non-literal options")
		}
	}

	return found
}

func TestContainerRemoveWithoutVolumes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		src  string
		want int
	}{
		{name: "force_only", src: `c.ContainerRemove(ctx, id, container.RemoveOptions{Force: true})`, want: 1},
		{
			name: "with_volumes",
			src:  `c.ContainerRemove(ctx, id, container.RemoveOptions{Force: true, RemoveVolumes: true})`,
			want: 0,
		},
		{name: "moby_force_only", src: `x := mobyclient.ContainerRemoveOptions{Force: true}`, want: 1},
		{
			name: "moby_with_volumes",
			src:  `x := mobyclient.ContainerRemoveOptions{Force: true, RemoveVolumes: true}`,
			want: 0,
		},
		{name: "variable_options", src: `c.ContainerRemove(ctx, id, opts)`, want: 1},
		{name: "image_options", src: `c.ImageRemove(ctx, id, image.RemoveOptions{Force: true})`, want: 0},
		{
			name: "multiline",
			src:  "d.api.ContainerRemove(\n ctx, id,\n mobyclient.ContainerRemoveOptions{Force: true},\n)",
			want: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Len(t, containerRemoveWithoutVolumes(tt.src), tt.want)
		})
	}
}

// TestContainerRemoveAlwaysRemovesVolumes keeps engine images that declare VOLUME from
// leaking anonymous volumes when their containers are removed.
func TestContainerRemoveAlwaysRemovesVolumes(t *testing.T) {
	t.Parallel()

	allow := containerRemovePassthrough()
	used := map[string]bool{}

	var offenders []string

	for _, root := range []string{"services", "pkgs", "internal"} {
		err := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
			if err != nil {
				return err
			}

			if d.IsDir() {
				if d.Name() != root && skipGuardDir(d.Name()) {
					return filepath.SkipDir
				}

				return nil
			}

			if !isGuardedSource(path) {
				return nil
			}

			src, readErr := os.ReadFile(path)
			if readErr != nil {
				return readErr
			}

			slashed := filepath.ToSlash(path)

			if uses := containerRemoveWithoutVolumes(string(src)); len(uses) > 0 {
				if _, ok := allow[slashed]; ok {
					used[slashed] = true

					return nil
				}

				offenders = append(offenders, slashed+": "+strings.Join(uses, ", "))
			}

			return nil
		})
		require.NoError(t, err)
	}

	for path := range allow {
		if !used[path] {
			offenders = append(offenders, path+": stale containerRemovePassthrough() entry")
		}
	}

	assert.Empty(t, offenders, "set RemoveVolumes: true on every container removal")
}

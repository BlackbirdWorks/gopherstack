package container_test

import (
	"os/exec"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
)

func TestDockerRuntime_StopAndRemoveDropsAnonymousVolume(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		image string
		env   []string
	}{
		{name: "postgres", image: "postgres:16", env: []string{"POSTGRES_PASSWORD=x"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if _, err := exec.LookPath("docker"); err != nil {
				t.Skip("docker CLI not available")
			}

			rt, err := container.NewRuntime(container.Config{})
			if err != nil {
				t.Skipf("container runtime unavailable: %v", err)
			}

			t.Cleanup(func() { _ = rt.Close() })

			if err = rt.Ping(t.Context()); err != nil {
				t.Skipf("container daemon unreachable: %v", err)
			}

			if ok, hasErr := rt.HasImage(t.Context(), tt.image); hasErr != nil || !ok {
				t.Skipf("image %s not present locally", tt.image)
			}

			id, err := rt.CreateAndStart(t.Context(), container.Spec{Image: tt.image, Env: tt.env})
			require.NoError(t, err)

			t.Cleanup(func() { _ = exec.Command("docker", "rm", "-fv", id).Run() })

			out, err := exec.CommandContext(t.Context(), "docker", "inspect", "-f",
				`{{range .Mounts}}{{if eq .Type "volume"}}{{.Name}} {{end}}{{end}}`, id).Output()
			require.NoError(t, err)

			volumes := strings.Fields(string(out))
			require.NotEmpty(t, volumes, "engine image must get an anonymous volume")

			t.Cleanup(func() {
				for _, v := range volumes {
					_ = exec.Command("docker", "volume", "rm", "-f", v).Run()
				}
			})

			require.NoError(t, rt.StopAndRemove(t.Context(), id))

			for _, v := range volumes {
				assert.Error(t, exec.CommandContext(t.Context(), "docker", "volume", "inspect", v).Run(),
					"volume %s must be removed with its container", v)
			}
		})
	}
}

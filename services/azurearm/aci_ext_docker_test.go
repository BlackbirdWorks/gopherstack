package azurearm_test

import (
	"context"
	"io"
	"net"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/containerinstance/armcontainerinstance/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const dockerWait = 60 * time.Second

func publishedFront(t *testing.T, g *armcontainerinstance.ContainerGroup) string {
	t.Helper()

	for _, e := range g.Properties.InstanceView.Events {
		if m := publishedRe.FindString(*e.Message); *e.Name == "PortPublished" && m != "" {
			return m
		}
	}

	t.Fatal("no PortPublished event")

	return ""
}

func TestACIDocker_InitContainersRunBeforeApps(t *testing.T) {
	t.Parallel()

	s := newACIDockerStack(t)

	_, err := s.createWith(t, "init", aciOpts{
		image:   "alpine:3.21",
		policy:  armcontainerinstance.ContainerGroupRestartPolicyNever,
		command: []string{"sh", "-c", "cat /work/msg; echo app-done"},
		mounts:  []*armcontainerinstance.VolumeMount{{Name: new("work"), MountPath: new("/work")}},
		volumes: []*armcontainerinstance.Volume{{Name: new("work"), EmptyDir: map[string]any{}}},
	}, func(g *armcontainerinstance.ContainerGroup) {
		g.Properties.InitContainers = []*armcontainerinstance.InitContainerDefinition{{
			Name: new("seed"),
			Properties: &armcontainerinstance.InitContainerPropertiesDefinition{
				Image:        new("alpine:3.21"),
				Command:      toPtrs([]string{"sh", "-c", "sleep 2; echo written-by-init > /work/msg"}),
				VolumeMounts: []*armcontainerinstance.VolumeMount{{Name: new("work"), MountPath: new("/work")}},
			},
		}}
	})
	require.NoError(t, err)

	g := s.waitState(t, "rg", "init", "Succeeded")

	logs, err := s.factory.NewContainersClient().ListLogs(t.Context(), "rg", "init", "main", nil)
	require.NoError(t, err)
	assert.Contains(t, *logs.Content, "written-by-init", "the app must see the init container's output")
	assert.Contains(t, *logs.Content, "app-done")

	cs := g.Properties.InitContainers[0].Properties.InstanceView.CurrentState
	assert.Equal(t, "Terminated", *cs.State)
	assert.Equal(t, int32(0), *cs.ExitCode)
}

func TestACIDocker_LivenessProbeRestartsContainer(t *testing.T) {
	t.Parallel()

	s := newACIDockerStack(t)

	_, err := s.createWith(t, "live", aciOpts{
		image:   "alpine:3.21",
		policy:  armcontainerinstance.ContainerGroupRestartPolicyAlways,
		command: []string{"sh", "-c", "touch /tmp/healthy; sleep 3; rm -f /tmp/healthy; sleep 600"},
	}, setProbe(&armcontainerinstance.ContainerProbe{
		Exec: execCmd("cat", "/tmp/healthy"), PeriodSeconds: new(int32(1)), FailureThreshold: new(int32(2)),
	}, nil))
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		iv := s.get(t, "rg", "live").Properties.Containers[0].Properties.InstanceView

		return *iv.RestartCount >= 1
	}, dockerWait, 200*time.Millisecond)

	events := s.get(t, "rg", "live").Properties.Containers[0].Properties.InstanceView.Events
	names := make([]string, 0, len(events))

	for _, e := range events {
		names = append(names, *e.Name)
	}

	assert.Contains(t, names, "Unhealthy")
	assert.Contains(t, names, "Killing")
}

func TestACIDocker_ReadinessProbeGatesTraffic(t *testing.T) {
	t.Parallel()

	tests := []struct {
		probe *armcontainerinstance.ContainerProbe
		name  string
	}{
		{&armcontainerinstance.ContainerProbe{
			HTTPGet: &armcontainerinstance.ContainerHTTPGet{
				Port: new(int32(8080)),
				Path: new("/"),
			},
			PeriodSeconds: new(int32(1)),
		}, "http"},
		{&armcontainerinstance.ContainerProbe{
			Exec: execCmd("cat", "/tmp/ready"), PeriodSeconds: new(int32(1)),
		}, "exec"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			s := newACIDockerStack(t)

			g, err := s.createWith(t, "ready", aciOpts{
				image:  "alpine:3.21",
				policy: armcontainerinstance.ContainerGroupRestartPolicyAlways,
				command: []string{
					"sh", "-c", `sleep 5; touch /tmp/ready; while true; do ` +
						`printf "HTTP/1.1 200 OK\r\nConnection: close\r\n\r\nserved-by-aci\n" | nc -l -p 8080; done`,
				},
				ip: &armcontainerinstance.IPAddress{
					Type:  new(armcontainerinstance.ContainerGroupIPAddressTypePublic),
					Ports: []*armcontainerinstance.Port{{Port: new(int32(8080))}},
				},
				ports: []*armcontainerinstance.ContainerPort{{Port: new(int32(8080))}},
			}, setProbe(nil, tc.probe))
			require.NoError(t, err)

			front := publishedFront(t, g)

			get := func() string {
				c, derr := net.DialTimeout("tcp", front, time.Second)
				if derr != nil {
					return ""
				}
				defer c.Close()

				_ = c.SetDeadline(time.Now().Add(2 * time.Second))
				_, _ = io.WriteString(c, "GET / HTTP/1.0\r\n\r\n")
				b, _ := io.ReadAll(c)

				return string(b)
			}

			assert.NotContains(t, get(), "served-by-aci", "traffic is gated while the readiness probe fails")
			if !assert.Eventually(t, func() bool { return strings.Contains(get(), "served-by-aci") },
				dockerWait, 250*time.Millisecond) {
				for _, e := range s.get(t, "rg", "ready").Properties.Containers[0].Properties.InstanceView.Events {
					t.Logf("%s %s: %s", *e.Type, *e.Name, *e.Message)
				}

				t.FailNow()
			}
		})
	}
}

func dockerRun(t *testing.T, args ...string) string {
	t.Helper()

	out, err := exec.CommandContext(t.Context(), "docker", args...).
		CombinedOutput()
	require.NoError(t, err, string(out))

	return strings.TrimSpace(string(out))
}

func TestACIDocker_GitRepoClone(t *testing.T) {
	t.Parallel()

	s := newACIDockerStack(t)

	setup := `set -e
git config --global user.email t@t; git config --global user.name t
git config --global --add safe.directory '*'
git init -q --bare /repos/r.git
git init -q /tmp/w && cd /tmp/w
echo v1-content > a.txt && git add a.txt && git commit -qm one && git tag v1
echo later > b.txt && git add b.txt && git commit -qm two
git push -q /repos/r.git HEAD:refs/heads/master --tags
git -C /repos/r.git update-server-info
chmod -R a+rwX /repos`
	repos := t.TempDir()
	name := "gopherstack-aci-gitd-" + strings.ReplaceAll(t.Name(), "/", "-")

	dockerRun(t, "run", "--rm", "-v", repos+":/repos", "--entrypoint", "sh", "alpine/git", "-c", setup)
	dockerRun(t, "run", "-d", "--rm", "--name", name, "-v", repos+":/repos:ro", "busybox:1.36",
		"httpd", "-f", "-p", "80", "-h", "/repos")
	t.Cleanup(func() {
		_ = exec.CommandContext(context.Background(), "docker", "rm", "-f", name).Run()
	})

	ip := dockerRun(t, "inspect", "-f", "{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}", name)
	require.NotEmpty(t, ip)

	require.Eventually(t, func() bool {
		c, err := net.DialTimeout("tcp", net.JoinHostPort(ip, "80"), time.Second)
		if err != nil {
			return false
		}

		_ = c.Close()

		return true
	}, dockerWait, 250*time.Millisecond)

	_, err := s.createWith(t, "git", aciOpts{
		image:   "alpine:3.21",
		policy:  armcontainerinstance.ContainerGroupRestartPolicyNever,
		command: []string{"sh", "-c", "cat /src/repo/a.txt; ls /src/repo"},
		mounts:  []*armcontainerinstance.VolumeMount{{Name: new("src"), MountPath: new("/src")}},
		volumes: []*armcontainerinstance.Volume{{
			Name: new("src"),
			GitRepo: &armcontainerinstance.GitRepoVolume{
				Repository: new("http://" + ip + "/r.git"), Directory: new("repo"), Revision: new("v1"),
			},
		}},
	}, nil)
	require.NoError(t, err)

	s.waitState(t, "rg", "git", "Succeeded")

	logs, err := s.factory.NewContainersClient().ListLogs(t.Context(), "rg", "git", "main", nil)
	require.NoError(t, err)
	assert.Contains(t, *logs.Content, "v1-content")
	assert.NotContains(t, *logs.Content, "b.txt", "revision v1 predates b.txt")
}

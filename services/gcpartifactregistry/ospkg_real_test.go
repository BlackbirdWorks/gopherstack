package gcpartifactregistry_test

import (
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"cloud.google.com/go/artifactregistry/apiv1/artifactregistrypb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	arapi "google.golang.org/api/artifactregistry/v1"
)

// buildRealDeb runs dpkg-deb, so the package is exactly what Debian tooling writes.
func buildRealDeb(t *testing.T, name, version, compression string) []byte {
	t.Helper()

	if _, err := exec.LookPath("dpkg-deb"); err != nil {
		t.Skip("dpkg-deb not installed")
	}

	root := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(root, "DEBIAN"), 0o755)) //nolint:gosec // dpkg-deb requires 0755
	require.NoError(t, os.MkdirAll(filepath.Join(root, "usr", "bin"), 0o750))
	require.NoError(t, os.WriteFile(filepath.Join(root, "usr", "bin", name), []byte("#!/bin/sh\necho "+name+"-ran\n"), 0o755)) //nolint:gosec // executable fixture

	control := "Package: " + name + "\nVersion: " + version + "\nArchitecture: all\nMaintainer: T <t@example.com>\n" +
		"Description: " + name + "\n real dpkg fixture\n"
	require.NoError(t, os.WriteFile(filepath.Join(root, "DEBIAN", "control"), []byte(control), 0o600))

	out := filepath.Join(t.TempDir(), name+".deb")

	cmd := exec.CommandContext(t.Context(), "dpkg-deb", "--root-owner-group", "-Z"+compression, "-b", root, out)
	if msg, err := cmd.CombinedOutput(); err != nil {
		if strings.Contains(string(msg), "compress") {
			t.Skipf("dpkg-deb -Z%s unsupported: %s", compression, msg)
		}

		require.NoError(t, err, string(msg))
	}

	data, err := os.ReadFile(out)
	require.NoError(t, err)

	return data
}

func TestAptParsesPackagesBuiltByDpkg(t *testing.T) {
	t.Parallel()

	for _, comp := range []string{"gzip", "xz", "zstd"} {
		t.Run(comp, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, "")
			e.createRepo(t, "dpkg", &artifactregistrypb.Repository{Format: artifactregistrypb.Repository_APT})

			deb := buildRealDeb(t, "realtool", "1:4.5~rc1-2", comp)
			svc := restService(t, e)

			res, err := svc.Projects.Locations.Repositories.AptArtifacts.Upload(repoName("dpkg"), &arapi.UploadAptArtifactRequest{}).
				Media(bytes.NewReader(deb)).Context(t.Context()).Do()
			require.NoError(t, err)
			assert.True(t, res.Operation.Done)

			_, packages := getBody(t, "http://127.0.0.1:"+e.port+"/apt/projects/"+project+"/dists/dpkg/main/binary-amd64/Packages")
			assert.Equal(t, "realtool", fieldOf(string(packages), "Package"))
			assert.Equal(t, "1:4.5~rc1-2", fieldOf(string(packages), "Version"))
			assert.Equal(t, "pool/dpkg/main/r/realtool/realtool_4.5~rc1-2_all.deb", fieldOf(string(packages), "Filename"),
				"the epoch is dropped from the file name")
		})
	}
}

func dockerRun(t *testing.T, image, script string) string {
	t.Helper()

	if _, err := exec.LookPath("docker"); err != nil {
		t.Skip("docker CLI not available")
	}

	if err := exec.CommandContext(t.Context(), "docker", "info").Run(); err != nil {
		t.Skip("docker daemon unavailable")
	}

	if err := exec.CommandContext(t.Context(), "docker", "pull", "-q", image).Run(); err != nil {
		t.Skipf("cannot pull %s: %v", image, err)
	}

	out, err := exec.CommandContext(t.Context(), "docker", "run", "--rm", "--network", "host", image, "sh", "-ec", script).CombinedOutput()
	require.NoError(t, err, string(out))

	return string(out)
}

func TestAptClientInstallsFromRepository(t *testing.T) {
	t.Parallel()

	e := newEnv(t, "")
	e.createRepo(t, "apt-live", &artifactregistrypb.Repository{Format: artifactregistrypb.Repository_APT})

	deb := buildRealDeb(t, "arhello", "2.0", "gzip")
	_, err := restService(t, e).Projects.Locations.Repositories.AptArtifacts.Upload(repoName("apt-live"), &arapi.UploadAptArtifactRequest{}).
		Media(bytes.NewReader(deb)).Context(t.Context()).Do()
	require.NoError(t, err)

	script := "echo 'deb [trusted=yes] http://127.0.0.1:" + e.port + "/apt/projects/" + project +
		" apt-live main' > /etc/apt/sources.list.d/ar.list && " +
		"apt-get update -qq && apt-get install -y -qq arhello >/dev/null && arhello"

	out := dockerRun(t, "debian:stable-slim", script)
	assert.Contains(t, strings.TrimSpace(out), "arhello-ran")
}

func TestYumClientInstallsFromRepository(t *testing.T) {
	t.Parallel()

	e := newEnv(t, "")
	e.createRepo(t, "yum-live", &artifactregistrypb.Repository{Format: artifactregistrypb.Repository_YUM})

	rpm := makeRPM(t, "arhello", "1.0", "1", "noarch")
	_, err := restService(t, e).Projects.Locations.Repositories.YumArtifacts.Upload(repoName("yum-live"), &arapi.UploadYumArtifactRequest{}).
		Media(bytes.NewReader(rpm)).Context(t.Context()).Do()
	require.NoError(t, err)

	script := "printf '[ar]\\nname=ar\\nbaseurl=http://127.0.0.1:" + e.port + "/yum/projects/" + project +
		"/yum-live\\nenabled=1\\ngpgcheck=0\\nrepo_gpgcheck=0\\n' > /etc/yum.repos.d/ar.repo && " +
		"dnf -y -q --disablerepo='*' --enablerepo=ar install arhello && arhello"

	out := dockerRun(t, "fedora:latest", script)
	assert.Contains(t, out, "arhello-ran")
}

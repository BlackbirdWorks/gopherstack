package gcprun

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"cloud.google.com/go/run/apiv2/runpb"

	"github.com/blackbirdworks/gopherstack/pkgs/gcpserve"
)

const (
	dirMode        = 0o755
	sharedDirMode  = 0o777
	defaultSecMode = 0o444
	secretPerm     = 0o777
	shmDir         = "/dev/shm"
)

var (
	errNoSecrets         = errors.New("secret manager is not available in this emulator")
	errUnsupportedVolume = errors.New("unsupported volume")
)

// SecretResolver returns the payload of a Secret Manager version resource name
// (projects/{p}/secrets/{s}/versions/{v}).
type SecretResolver = gcpserve.SecretResolver

// SetSecretResolver wires Secret Manager for secret environment variables and secret volumes.
func (b *InMemoryBackend) SetSecretResolver(fn SecretResolver) {
	b.mu.Lock("SetSecretResolver")
	defer b.mu.Unlock()

	b.secrets = fn
}

func (b *InMemoryBackend) secretValue(project, secret, version string) ([]byte, error) {
	b.mu.RLock("secretValue")
	fn := b.secrets
	b.mu.RUnlock()

	if fn == nil {
		return nil, errNoSecrets
	}

	if version == "" {
		version = "latest"
	}

	data, err := fn(gcpserve.SecretVersionName(project, secret, version))
	if err != nil {
		return nil, fmt.Errorf("secret %q version %q: %w", secret, version, err)
	}

	return data, nil
}

// revisionEnv returns the container's environment with secret references resolved.
func (b *InMemoryBackend) revisionEnv(project string, c *runpb.Container) ([]string, error) {
	env := make([]string, 0, len(c.GetEnv()))

	for _, e := range c.GetEnv() {
		val := e.GetValue()

		if ref := e.GetValueSource().GetSecretKeyRef(); ref != nil {
			data, err := b.secretValue(project, ref.GetSecret(), ref.GetVersion())
			if err != nil {
				return nil, err
			}

			val = string(data)
		}

		env = append(env, e.GetName()+"="+val)
	}

	return env, nil
}

// materializeVolumes creates the host directory behind every volume of the revision.
// It returns volume name to host path and the directories to remove at teardown.
func (b *InMemoryBackend) materializeVolumes(
	project string,
	vols []*runpb.Volume,
) (map[string]string, []string, error) {
	paths := make(map[string]string, len(vols))

	var dirs []string

	fail := func(err error) (map[string]string, []string, error) {
		removeDirs(dirs)

		return nil, nil, err
	}

	for _, v := range vols {
		var (
			dir string
			err error
		)

		switch src := v.GetVolumeType().(type) {
		case *runpb.Volume_EmptyDir:
			dir, err = emptyDir(src.EmptyDir)
		case *runpb.Volume_Secret:
			dir, err = b.secretDir(project, src.Secret)
		case *runpb.Volume_Gcs:
			err = fmt.Errorf("%w: volume %q is a Cloud Storage FUSE volume, which needs a FUSE mount inside the "+
				"container", errUnsupportedVolume, v.GetName())
		case *runpb.Volume_Nfs:
			err = fmt.Errorf("%w: volume %q is an NFS volume", errUnsupportedVolume, v.GetName())
		case *runpb.Volume_CloudSqlInstance:
			err = fmt.Errorf("%w: volume %q is a Cloud SQL volume", errUnsupportedVolume, v.GetName())
		default:
			err = fmt.Errorf("%w: volume %q has no source", errUnsupportedVolume, v.GetName())
		}

		if dir != "" {
			dirs = append(dirs, dir)
		}

		if err != nil {
			return fail(err)
		}

		paths[v.GetName()] = dir
	}

	return paths, dirs, nil
}

func removeDirs(dirs []string) {
	for _, d := range dirs {
		_ = os.RemoveAll(d)
	}
}

// emptyDir makes a directory shared by every container of the instance. Memory-medium volumes (and the default,
// which the API says is memory-backed today) live under /dev/shm when it exists.
func emptyDir(_ *runpb.EmptyDirVolumeSource) (string, error) {
	base := os.TempDir()
	if st, err := os.Stat(shmDir); err == nil && st.IsDir() {
		base = shmDir
	}

	dir, err := os.MkdirTemp(base, "gopherstack-run-vol-")
	if err != nil {
		return "", fmt.Errorf("create emptyDir: %w", err)
	}

	if err = os.Chmod(dir, sharedDirMode); err != nil {
		return dir, fmt.Errorf("chmod emptyDir: %w", err)
	}

	return dir, nil
}

// secretDir writes the secret versions of the volume as files.
func (b *InMemoryBackend) secretDir(project string, src *runpb.SecretVolumeSource) (string, error) {
	dir, err := os.MkdirTemp(os.TempDir(), "gopherstack-run-secret-")
	if err != nil {
		return "", fmt.Errorf("create secret volume: %w", err)
	}

	if err = os.Chmod(dir, dirMode); err != nil {
		return dir, fmt.Errorf("chmod secret volume: %w", err)
	}

	items := src.GetItems()
	if len(items) == 0 {
		items = []*runpb.VersionToPath{{Path: lastSegment(src.GetSecret()), Version: "latest"}}
	}

	for _, it := range items {
		data, serr := b.secretValue(project, src.GetSecret(), it.GetVersion())
		if serr != nil {
			return dir, serr
		}

		mode := os.FileMode(it.GetMode()) & secretPerm //nolint:gosec // proto int32 mode bits are bounded by the mask
		if mode == 0 {
			mode = os.FileMode(src.GetDefaultMode()) & secretPerm //nolint:gosec // bounded by the mask
		}

		if mode == 0 {
			mode = defaultSecMode
		}

		if err = gcpserve.WriteSecretFile(dir, it.GetPath(), data, mode); err != nil {
			return dir, err
		}
	}

	return dir, nil
}

// containerMounts returns the bind-mount specs of c: host directory (plus sub_path) to mount_path.
func containerMounts(c *runpb.Container, paths map[string]string, readOnly map[string]bool) ([]string, error) {
	mounts := make([]string, 0, len(c.GetVolumeMounts()))

	for _, m := range c.GetVolumeMounts() {
		host, ok := paths[m.GetName()]
		if !ok {
			return nil, fmt.Errorf(
				"%w: volume mount %q references an unknown volume",
				errUnsupportedVolume,
				m.GetName(),
			)
		}

		if sub := m.GetSubPath(); sub != "" {
			host = filepath.Join(host, filepath.Clean("/"+sub))
			if err := os.MkdirAll(host, sharedDirMode); err != nil {
				return nil, fmt.Errorf("create sub_path: %w", err)
			}
		}

		spec := host + ":" + m.GetMountPath()
		if readOnly[m.GetName()] {
			spec += ":ro"
		}

		mounts = append(mounts, spec)
	}

	return mounts, nil
}

func secretVolumes(vols []*runpb.Volume) map[string]bool {
	out := map[string]bool{}

	for _, v := range vols {
		if v.GetSecret() != nil {
			out[v.GetName()] = true
		}
	}

	return out
}

package gcpfunctions

import (
	"errors"
	"fmt"
	"os"
	"path"
	"regexp"
	"strings"

	"cloud.google.com/go/functions/apiv2/functionspb"
	"google.golang.org/grpc/status"

	"github.com/blackbirdworks/gopherstack/pkgs/gcpserve"
)

const (
	defaultSecretMode = 0o444
	secretDirPerm     = 0o755
)

var (
	errSecretRead      = errors.New("could not read secret")
	errNoSecretManager = errors.New("no Secret Manager is available in this emulator")
	versionRe          = regexp.MustCompile(`^([0-9]+|latest)$`)
)

// SetSecretResolver wires Secret Manager for secret environment variables and secret volumes.
func (b *InMemoryBackend) SetSecretResolver(fn gcpserve.SecretResolver) {
	b.mu.Lock("SetSecretResolver")
	defer b.mu.Unlock()

	b.secrets = fn
}

func reservedMountPath(p string) bool {
	for _, root := range []string{"/dev", "/proc", "/sys"} {
		if p == root || strings.HasPrefix(p, root+"/") {
			return true
		}
	}

	return false
}

func validateSecrets(sc *functionspb.ServiceConfig) error {
	if err := validateSecretEnv(sc); err != nil {
		return err
	}

	return validateSecretVolumes(sc.GetSecretVolumes())
}

func validateSecretEnv(sc *functionspb.ServiceConfig) error {
	keys := map[string]bool{}

	for k := range sc.GetEnvironmentVariables() {
		keys[k] = true
	}

	for _, s := range sc.GetSecretEnvironmentVariables() {
		switch {
		case !envNameRe.MatchString(s.GetKey()):
			return errInvalid("Secret environment variable name %q is invalid.", s.GetKey())
		case reservedEnv(s.GetKey()):
			return errInvalid("The following reserved env names were provided: %s.", s.GetKey())
		case keys[s.GetKey()]:
			return errInvalid("Environment variable %q is set more than once.", s.GetKey())
		}

		keys[s.GetKey()] = true

		if err := validateSecretRef(s.GetSecret(), s.GetVersion()); err != nil {
			return err
		}
	}

	return nil
}

func validateSecretVolumes(vols []*functionspb.SecretVolume) error {
	mounts := map[string]bool{}

	for _, v := range vols {
		mp := path.Clean(v.GetMountPath())

		switch {
		case !path.IsAbs(mp) || mp == "/" || reservedMountPath(mp) || mp == srcMount || mp == workspace:
			return errInvalid("Secret volume mount_path %q is not allowed.", v.GetMountPath())
		case mounts[mp]:
			return errInvalid("Secret volume mount_path %q is used more than once.", mp)
		}

		mounts[mp] = true

		if err := validateSecretRef(v.GetSecret(), ""); err != nil {
			return err
		}

		for _, ver := range v.GetVersions() {
			if err := validateSecretRef(v.GetSecret(), ver.GetVersion()); err != nil {
				return err
			}

			if ver.GetPath() == "" {
				return errInvalid("Secret volume %q needs a path for every version.", v.GetMountPath())
			}
		}
	}

	return nil
}

func validateSecretRef(secret, version string) error {
	if secret == "" || strings.Contains(secret, "/") {
		return errInvalid("Secret %q must be the secret's name, not its resource name.", secret)
	}

	if version != "" && !versionRe.MatchString(version) {
		return errInvalid("Secret %q version %q must be a version number or \"latest\".", secret, version)
	}

	return nil
}

// secretSet is everything a container needs from Secret Manager: environment entries, bind mounts and the
// scratch directories to remove with the instance.
type secretSet struct {
	env    []string
	mounts []string
	dirs   []string
}

func (s *secretSet) cleanup() {
	for _, d := range s.dirs {
		removeDir(d)
	}
}

func (b *InMemoryBackend) secretBytes(project, secret, version string) ([]byte, error) {
	b.mu.RLock("secretBytes")
	fn := b.secrets
	b.mu.RUnlock()

	if fn == nil {
		return nil, errNoSecretManager
	}

	name := gcpserve.SecretVersionName(project, secret, version)

	data, err := fn(name)
	if err != nil {
		return nil, fmt.Errorf("%w %s: %s", errSecretRead, name, status.Convert(err).Message())
	}

	return data, nil
}

// checkSecrets confirms every referenced secret version can be read; a failed deploy reports the first miss.
func (b *InMemoryBackend) checkSecrets(fn *functionspb.Function) error {
	sec, err := b.mountSecrets(fn)
	sec.cleanup()

	return err
}

// mountSecrets resolves secret environment variables and writes secret volumes under fresh directories.
func (b *InMemoryBackend) mountSecrets(fn *functionspb.Function) (*secretSet, error) {
	sc := fn.GetServiceConfig()
	set := &secretSet{}

	n, err := parseName(fn.GetName())
	if err != nil {
		return set, err
	}

	project := func(p string) string {
		if p != "" {
			return p
		}

		return n.project
	}

	for _, s := range sc.GetSecretEnvironmentVariables() {
		data, serr := b.secretBytes(project(s.GetProjectId()), s.GetSecret(), s.GetVersion())
		if serr != nil {
			return set, serr
		}

		set.env = append(set.env, s.GetKey()+"="+string(data))
	}

	for _, v := range sc.GetSecretVolumes() {
		dir, derr := b.secretVolume(project(v.GetProjectId()), v)
		if dir != "" {
			set.dirs = append(set.dirs, dir)
		}

		if derr != nil {
			return set, derr
		}

		set.mounts = append(set.mounts, dir+":"+path.Clean(v.GetMountPath())+":ro")
	}

	return set, nil
}

func (b *InMemoryBackend) secretVolume(project string, v *functionspb.SecretVolume) (string, error) {
	dir, err := os.MkdirTemp("", "gopherstack-gcf-secret-")
	if err != nil {
		return "", fmt.Errorf("create secret volume: %w", err)
	}

	if err = os.Chmod(dir, secretDirPerm); err != nil {
		return dir, fmt.Errorf("chmod secret volume: %w", err)
	}

	type item struct{ version, path string }

	items := make([]item, 0, len(v.GetVersions()))
	for _, ver := range v.GetVersions() {
		items = append(items, item{ver.GetVersion(), ver.GetPath()})
	}

	if len(items) == 0 {
		items = append(items, item{"latest", v.GetSecret()})
	}

	for _, it := range items {
		data, serr := b.secretBytes(project, v.GetSecret(), it.version)
		if serr != nil {
			return dir, serr
		}

		if err = gcpserve.WriteSecretFile(dir, it.path, data, defaultSecretMode); err != nil {
			return dir, err
		}
	}

	return dir, nil
}

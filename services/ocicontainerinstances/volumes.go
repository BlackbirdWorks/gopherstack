package ocicontainerinstances

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
)

const (
	scratchPerm = 0o777
	filePerm    = 0o666
	shmDir      = "/dev/shm"
)

// scratch holds the host directories backing one instance's volumes.
type scratch struct {
	root string
	dirs map[string]string
	dns  string
}

func newScratch() (*scratch, error) {
	root, err := os.MkdirTemp("", "gopherstack-oci-ci-")
	if err != nil {
		return nil, fmt.Errorf("create scratch dir: %w", err)
	}

	if err = os.Chmod(root, scratchPerm); err != nil {
		_ = os.RemoveAll(root)

		return nil, fmt.Errorf("chmod scratch dir: %w", err)
	}

	return &scratch{root: root, dirs: map[string]string{}}, nil
}

func (s *scratch) remove() {
	if s != nil {
		_ = os.RemoveAll(s.root)
	}
}

// fill creates each volume's directory and writes CONFIGFILE contents.
func (s *scratch) fill(vols []Volume, dns *DNSConfig) error {
	for i, v := range vols {
		dir := filepath.Join(s.root, "vol"+strconv.Itoa(i))
		if v.Type == volEmptyDir && v.BackingStore == backingMemory {
			if d, err := os.MkdirTemp(shmDir, "gopherstack-oci-ci-"); err == nil {
				dir = d
			}
		}

		if err := os.MkdirAll(dir, scratchPerm); err != nil {
			return fmt.Errorf("create volume %q: %w", v.Name, err)
		}

		_ = os.Chmod(dir, scratchPerm)
		s.dirs[v.Name] = dir

		if err := writeConfigs(dir, v.Configs); err != nil {
			return fmt.Errorf("volume %q: %w", v.Name, err)
		}
	}

	if dns != nil {
		s.dns = filepath.Join(s.root, "resolv.conf")

		if err := os.WriteFile(s.dns, []byte(resolvConf(dns)), filePerm); err != nil {
			return fmt.Errorf("write resolv.conf: %w", err)
		}
	}

	return nil
}

func (s *scratch) removeAll() {
	for _, d := range s.dirs {
		if strings.HasPrefix(d, shmDir) {
			_ = os.RemoveAll(d)
		}
	}

	s.remove()
}

func writeConfigs(dir string, cfgs []ConfigFile) error {
	if len(cfgs) == 0 {
		return nil
	}

	root, err := os.OpenRoot(dir)
	if err != nil {
		return fmt.Errorf("open volume dir: %w", err)
	}
	defer root.Close()

	for _, c := range cfgs {
		data, derr := base64.StdEncoding.DecodeString(c.Data)
		if derr != nil {
			return fmt.Errorf("decode %s: %w", c.FileName, derr)
		}

		rel := path.Join(c.Path, c.FileName)
		if d := path.Dir(rel); d != "." {
			if err = root.MkdirAll(d, scratchPerm); err != nil {
				return fmt.Errorf("create %s: %w", d, err)
			}
		}

		if err = root.WriteFile(rel, data, filePerm); err != nil {
			return fmt.Errorf("write %s: %w", rel, err)
		}
	}

	return nil
}

func resolvConf(d *DNSConfig) string {
	var sb strings.Builder

	for _, ns := range d.Nameservers {
		sb.WriteString("nameserver " + ns + "\n")
	}

	if len(d.Searches) > 0 {
		sb.WriteString("search " + strings.Join(d.Searches, " ") + "\n")
	}

	if len(d.Options) > 0 {
		sb.WriteString("options " + strings.Join(d.Options, " ") + "\n")
	}

	return sb.String()
}

// binds returns the docker bind mounts for a container's volume mounts and the instance DNS file.
func (s *scratch) binds(mounts []VolumeMount) []string {
	var out []string

	for _, m := range mounts {
		src := s.dirs[m.VolumeName]
		if m.SubPath != "" {
			src = filepath.Join(src, filepath.FromSlash(m.SubPath))
			_ = os.MkdirAll(src, scratchPerm)
			_ = os.Chmod(src, scratchPerm)
		}

		bind := src + ":" + m.MountPath
		if m.ReadOnly != nil && *m.ReadOnly {
			bind += ":ro"
		}

		out = append(out, bind)
	}

	if s.dns != "" {
		out = append(out, s.dns+":/etc/resolv.conf:ro")
	}

	return out
}

// securityUser maps a LINUX security context to docker's uid[:gid] user and the fsGroup supplementary group.
func securityUser(raw json.RawMessage) string {
	var sc struct {
		User  *int `json:"runAsUser"`
		Group *int `json:"runAsGroup"`
	}

	if len(raw) == 0 || json.Unmarshal(raw, &sc) != nil || sc.User == nil {
		return ""
	}

	u := strconv.Itoa(*sc.User)
	if sc.Group != nil {
		u += ":" + strconv.Itoa(*sc.Group)
	}

	return u
}

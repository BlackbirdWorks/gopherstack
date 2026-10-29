package eks

import (
	"fmt"
	"path"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/k3sengine"
)

const (
	// TagVolumeMount is the cluster tag that bind-mounts host paths into the k3s node container.
	TagVolumeMount = "gopherstack:volume-mount"
	// TagLBPorts is the cluster tag that publishes load-balancer ports from the k3s node container.
	TagLBPorts = "gopherstack:lb-ports"

	maxVolumeMounts = 16
	maxLBPorts      = 16
	maxPort         = 65535
	mountModeRO     = "ro"
	mountModeRW     = "rw"
	mountFieldsMin  = 2
	mountFieldsMax  = 3
)

var k3sTagPattern = regexp.MustCompile(`^v1\.(\d+)\.\d+[-+]k3s\d+$`)

// launchOptions are the per-cluster container settings resolved from cluster tags and server defaults.
type launchOptions struct {
	mounts  []string
	lbPorts []int
}

// PublishedPort is a container port published on a host port.
type PublishedPort struct {
	Host          string `json:"host"`
	ContainerPort int    `json:"containerPort"`
	HostPort      int    `json:"hostPort"`
}

func absCleanPath(kind, p string) (string, error) {
	if !path.IsAbs(p) {
		return "", fmt.Errorf("%w: %s path %q must be absolute", ErrValidation, kind, p)
	}

	for seg := range strings.SplitSeq(p, "/") {
		if seg == ".." {
			return "", fmt.Errorf("%w: %s path %q must not contain '..'", ErrValidation, kind, p)
		}
	}

	return path.Clean(p), nil
}

// ParseVolumeMounts parses "host:container[:ro|rw],..." into bind specs.
func ParseVolumeMounts(v string) ([]string, error) {
	if strings.TrimSpace(v) == "" {
		return nil, nil
	}

	items := strings.Split(v, ",")
	if len(items) > maxVolumeMounts {
		return nil, fmt.Errorf("%w: at most %d volume mounts are allowed", ErrValidation, maxVolumeMounts)
	}

	targets := make(map[string]bool, len(items))
	out := make([]string, 0, len(items))

	for _, item := range items {
		parts := strings.Split(strings.TrimSpace(item), ":")
		if len(parts) < mountFieldsMin || len(parts) > mountFieldsMax {
			return nil, fmt.Errorf("%w: volume mount %q must be host:container[:ro]", ErrValidation, item)
		}

		host, err := absCleanPath("host", parts[0])
		if err != nil {
			return nil, err
		}

		target, err := absCleanPath("container", parts[1])
		if err != nil {
			return nil, err
		}

		if target == "/" || targets[target] {
			return nil, fmt.Errorf("%w: invalid or duplicate container path %q", ErrValidation, parts[1])
		}

		targets[target] = true
		bind := host + ":" + target

		if len(parts) == mountFieldsMax {
			if parts[2] != mountModeRO && parts[2] != mountModeRW {
				return nil, fmt.Errorf("%w: volume mount mode %q must be ro or rw", ErrValidation, parts[2])
			}

			bind += ":" + parts[2]
		}

		out = append(out, bind)
	}

	return out, nil
}

// ParseLBPorts parses "8080,8443" into container ports to publish.
func ParseLBPorts(v string) ([]int, error) {
	if strings.TrimSpace(v) == "" {
		return nil, nil
	}

	items := strings.Split(v, ",")
	if len(items) > maxLBPorts {
		return nil, fmt.Errorf("%w: at most %d load balancer ports are allowed", ErrValidation, maxLBPorts)
	}

	seen := make(map[int]bool, len(items))
	out := make([]int, 0, len(items))

	for _, item := range items {
		p, err := strconv.Atoi(strings.TrimSpace(item))
		if err != nil || p < 1 || p > maxPort || p == k3sAPIPort {
			return nil, fmt.Errorf("%w: load balancer port %q must be 1-%d and not %d",
				ErrValidation, item, maxPort, k3sAPIPort)
		}

		if seen[p] {
			return nil, fmt.Errorf("%w: duplicate load balancer port %d", ErrValidation, p)
		}

		seen[p] = true
		out = append(out, p)
	}

	return out, nil
}

// ParseK3sImageTags parses "1.31=v1.31.5-k3s1,1.32=..." into a minor-to-tag pin map.
func ParseK3sImageTags(v string) (map[string]string, error) {
	out := make(map[string]string)

	if strings.TrimSpace(v) == "" {
		return out, nil
	}

	for item := range strings.SplitSeq(v, ",") {
		minor, tag, ok := strings.Cut(strings.TrimSpace(item), "=")
		if !ok {
			return nil, fmt.Errorf("%w: k3s tag pin %q must be <minor>=<tag>", ErrInvalidClusterEngineConfig, item)
		}

		out[minor] = tag
	}

	if err := validateK3sImageTags(out); err != nil {
		return nil, err
	}

	return out, nil
}

func validateK3sImageTags(tags map[string]string) error {
	for minor, tag := range tags {
		m := k3sTagPattern.FindStringSubmatch(tag)
		if m == nil || "1."+m[1] != minor {
			return fmt.Errorf("%w: k3s tag %q is not a v%s.x-k3sN tag", ErrInvalidClusterEngineConfig, tag, minor)
		}

		if _, ok := k8sMinor(minor); !ok {
			return fmt.Errorf("%w: unsupported Kubernetes minor %q", ErrInvalidClusterEngineConfig, minor)
		}
	}

	return nil
}

func validateLBPortList(ports []int) error {
	for _, p := range ports {
		if p < 1 || p > maxPort || p == k3sAPIPort {
			return fmt.Errorf("%w: load balancer port %d out of range", ErrInvalidClusterEngineConfig, p)
		}
	}

	return nil
}

// imageFor resolves the k3s image: full override, then per-minor pin, then built-in tags; otherwise unsupported.
func (e *clusterEngine) imageFor(version string) (string, error) {
	if e.cfg.Image != "" {
		return e.cfg.Image, nil
	}

	if key, ok := k3sengine.MinorKey(version); ok {
		if tag, pinned := e.cfg.ImageTags[key]; pinned {
			return K3sImageRepo + ":" + tag, nil
		}
	}

	if tag, ok := k3sengine.Tag(version); ok {
		return K3sImageRepo + ":" + tag, nil
	}

	return "", fmt.Errorf("%w: unsupported Kubernetes version %q for the cluster engine", ErrValidation, version)
}

// ParseVolumeMountAllow parses a comma list of absolute host path prefixes.
func ParseVolumeMountAllow(v string) ([]string, error) {
	var out []string

	for item := range strings.SplitSeq(v, ",") {
		if item = strings.TrimSpace(item); item != "" {
			out = append(out, item)
		}
	}

	return out, validateMountAllow(out)
}

func validateMountAllow(prefixes []string) error {
	for _, p := range prefixes {
		clean, err := absCleanPath("allowed", p)
		if err != nil || clean == "/" {
			return fmt.Errorf("%w: volume mount allowlist entry %q must be an absolute path other than /",
				ErrInvalidClusterEngineConfig, p)
		}
	}

	return nil
}

func insidePrefix(p, prefix string) bool {
	return p == prefix || strings.HasPrefix(p, prefix+string(filepath.Separator))
}

// checkMountsAllowed requires every host path to resolve (symlinks included) inside an allowed prefix.
func (e *clusterEngine) checkMountsAllowed(mounts []string) error {
	if len(mounts) == 0 {
		return nil
	}

	if len(e.cfg.VolumeMountAllow) == 0 {
		return fmt.Errorf("%w: host volume mounts are disabled; set EKS_VOLUME_MOUNT_ALLOW", ErrValidation)
	}

	for _, m := range mounts {
		host, _, _ := strings.Cut(m, ":")

		resolved, err := filepath.EvalSymlinks(filepath.Clean(host))
		if err != nil || !e.allowedHost(resolved) {
			return fmt.Errorf("%w: host path %q is not inside EKS_VOLUME_MOUNT_ALLOW", ErrValidation, host)
		}
	}

	return nil
}

func (e *clusterEngine) allowedHost(resolved string) bool {
	for _, a := range e.cfg.VolumeMountAllow {
		if ra, err := filepath.EvalSymlinks(filepath.Clean(a)); err == nil && insidePrefix(resolved, ra) {
			return true
		}
	}

	return false
}

// resolveLaunch validates the cluster's tags against the engine and returns its container options.
func (e *clusterEngine) resolveLaunch(version string, kv map[string]string) (launchOptions, string, error) {
	img, err := e.imageFor(version)
	if err != nil {
		return launchOptions{}, "", err
	}

	opts := launchOptions{lbPorts: e.cfg.LBPorts}

	if v, ok := kv[TagVolumeMount]; ok {
		if opts.mounts, err = ParseVolumeMounts(v); err != nil {
			return launchOptions{}, "", err
		}

		if err = e.checkMountsAllowed(opts.mounts); err != nil {
			return launchOptions{}, "", err
		}
	}

	if v, ok := kv[TagLBPorts]; ok {
		if opts.lbPorts, err = ParseLBPorts(v); err != nil {
			return launchOptions{}, "", err
		}
	}

	return opts, img, nil
}

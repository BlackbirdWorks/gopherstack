package lambda

import (
	"errors"
	"fmt"
	"io/fs"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
)

const (
	maxFileSystemConfigs = 1
	maxLocalMountPathLen = 160
)

var (
	localMountPathRE = regexp.MustCompile(`^/mnt/[a-zA-Z0-9-_.]+$`)
	efsAccessPointRE = regexp.MustCompile(
		`^arn:aws[a-zA-Z-]*:elasticfilesystem:[a-z0-9-]+:\d{12}:access-point/fsap-[a-f0-9]+$`)

	errEFSUnavailable = errors.New("EFS integration is not available")
)

// SetEFSResolver wires EFS access point validation and mounting.
func (b *InMemoryBackend) SetEFSResolver(r EFSResolver) {
	b.mu.Lock("SetEFSResolver")
	defer b.mu.Unlock()

	b.efsResolver = r
}

func (b *InMemoryBackend) efsResolverSnapshot() EFSResolver {
	b.mu.RLock("efsResolver")
	defer b.mu.RUnlock()

	return b.efsResolver
}

// CheckFileSystemConfigs validates each config against EFS when a resolver is wired.
func (b *InMemoryBackend) CheckFileSystemConfigs(cfgs []*FileSystemConfig, subnetIDs []string) error {
	r := b.efsResolverSnapshot()
	if r == nil {
		return nil
	}

	for _, cfg := range cfgs {
		if err := r.CheckAccessPoint(cfg.Arn, subnetIDs); err != nil {
			return err
		}
	}

	return nil
}

// fileSystemConfigProblem reports the first shape violation, or "" when valid.
func fileSystemConfigProblem(cfgs []*FileSystemConfig, vpc *VpcConfig) string {
	if len(cfgs) == 0 {
		return ""
	}

	if len(cfgs) > maxFileSystemConfigs {
		return fmt.Sprintf("Value at 'fileSystemConfigs' failed to satisfy constraint: "+
			"Member must have length less than or equal to %d", maxFileSystemConfigs)
	}

	if vpc == nil || len(vpc.SubnetIDs) == 0 {
		return "A VPC configuration is required to use a file system: provide VpcConfig with SubnetIds"
	}

	for _, cfg := range cfgs {
		if cfg == nil || !efsAccessPointRE.MatchString(cfg.Arn) {
			return "Value for 'fileSystemConfigs.Arn' must be the ARN of an Amazon EFS access point"
		}

		if len(cfg.LocalMountPath) > maxLocalMountPathLen || !localMountPathRE.MatchString(cfg.LocalMountPath) {
			return fmt.Sprintf("Value %q at 'fileSystemConfigs.localMountPath' failed to satisfy constraint: "+
				"Member must satisfy regular expression pattern: ^/mnt/[a-zA-Z0-9-_.]+$", cfg.LocalMountPath)
		}
	}

	return ""
}

// validateFileSystemConfigsInput enforces the FileSystemConfig rules and writes the error on failure.
func (h *Handler) validateFileSystemConfigsInput(c *echo.Context, cfgs []*FileSystemConfig, vpc *VpcConfig) bool {
	if msg := fileSystemConfigProblem(cfgs, vpc); msg != "" {
		_ = h.writeError(c, http.StatusBadRequest, "InvalidParameterValueException", msg)

		return false
	}

	if len(cfgs) == 0 {
		return true
	}

	chk, ok := h.Backend.(FileSystemConfigChecker)
	if !ok {
		return true
	}

	if err := chk.CheckFileSystemConfigs(cfgs, vpc.SubnetIDs); err != nil {
		_ = h.writeError(c, http.StatusBadRequest, "InvalidParameterValueException",
			"Error occurred while DescribeMountTargets. EFS Error: "+err.Error())

		return false
	}

	return true
}

// efsPlan is the container-side result of resolving a function's FileSystemConfigs: the bind
// mounts, and the access point's PosixUser as the container process identity. The runtime
// abstraction has no per-mount identity mapping, so the whole function process runs as the
// PosixUser (files it creates carry that uid/gid on the host backing directory).
type efsPlan struct {
	user   string
	binds  []string
	groups []string
}

func (p efsPlan) apply(spec *container.Spec) {
	spec.Mounts = append(spec.Mounts, p.binds...)

	if p.user != "" {
		spec.User = p.user
		spec.GroupAdd = p.groups
	}
}

// efsMounts resolves fn's FileSystemConfigs into an efsPlan.
func (b *InMemoryBackend) efsMounts(fn *FunctionConfiguration) (efsPlan, error) {
	if len(fn.FileSystemConfigs) == 0 {
		return efsPlan{}, nil
	}

	r := b.efsResolverSnapshot()
	if r == nil {
		return efsPlan{}, fmt.Errorf("%w: %w", ErrLambdaUnavailable, errEFSUnavailable)
	}

	plan := efsPlan{binds: make([]string, 0, len(fn.FileSystemConfigs))}

	for _, cfg := range fn.FileSystemConfigs {
		m, err := r.ResolveMount(cfg.Arn)
		if err != nil {
			return efsPlan{}, fmt.Errorf("%w: mount %s: %w", ErrLambdaUnavailable, cfg.Arn, err)
		}

		plan.binds = append(plan.binds, m.HostDir+":"+cfg.LocalMountPath)

		if m.HasPosixUser && plan.user == "" {
			plan.user = strconv.FormatInt(m.UID, 10) + ":" + strconv.FormatInt(m.GID, 10)

			for _, g := range m.SecondaryGIDs {
				plan.groups = append(plan.groups, strconv.FormatInt(g, 10))
			}
		}
	}

	return plan, nil
}

const (
	worldRead os.FileMode = 0o044
	worldExec os.FileMode = 0o011
	ownerExec os.FileMode = 0o100
)

// layerHostDir returns the host side of a "host:container[:ro]" bind string.
func layerHostDir(bind string) string {
	host, _, _ := strings.Cut(bind, ":")

	return host
}

// makeTreeWorldReadable lets a non-root PosixUser read the extracted code directory.
func makeTreeWorldReadable(root string) {
	_ = filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return nil //nolint:nilerr // best effort
		}

		info, infoErr := d.Info()
		if infoErr != nil {
			return nil //nolint:nilerr // best effort
		}

		extra := worldRead
		if d.IsDir() || info.Mode()&ownerExec != 0 {
			extra |= worldExec
		}

		_ = os.Chmod(p, info.Mode().Perm()|extra) // #nosec G122 -- emulator-owned temp tree

		return nil
	})
}

// validateFileSystemConfigsUpdate checks the effective FileSystemConfigs/VpcConfig after an update.
func (h *Handler) validateFileSystemConfigsUpdate(
	c *echo.Context, fn *FunctionConfiguration, input *UpdateFunctionConfigurationInput,
) bool {
	if input.FileSystemConfigs == nil && input.VpcConfig == nil {
		return true
	}

	cfgs, vpc := fn.FileSystemConfigs, fn.VpcConfig
	if input.FileSystemConfigs != nil {
		cfgs = input.FileSystemConfigs
	}

	if input.VpcConfig != nil {
		vpc = input.VpcConfig
	}

	return h.validateFileSystemConfigsInput(c, cfgs, vpc)
}

// validateUpdateAgainstCurrent runs the update checks that need the stored function.
func (h *Handler) validateUpdateAgainstCurrent(
	c *echo.Context, fn *FunctionConfiguration, input *UpdateFunctionConfigurationInput,
) bool {
	return h.validateDurableRuntimeUpdate(c, fn, input) && h.validateFileSystemConfigsUpdate(c, fn, input)
}

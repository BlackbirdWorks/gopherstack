package lambda

import (
	"errors"
	"fmt"
	"net/http"
	"regexp"

	"github.com/labstack/echo/v5"
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

// efsMountBinds returns the bind-mount strings for fn's FileSystemConfigs.
func (b *InMemoryBackend) efsMountBinds(fn *FunctionConfiguration) ([]string, error) {
	if len(fn.FileSystemConfigs) == 0 {
		return nil, nil
	}

	r := b.efsResolverSnapshot()
	if r == nil {
		return nil, fmt.Errorf("%w: %w", ErrLambdaUnavailable, errEFSUnavailable)
	}

	binds := make([]string, 0, len(fn.FileSystemConfigs))

	for _, cfg := range fn.FileSystemConfigs {
		m, err := r.ResolveMount(cfg.Arn)
		if err != nil {
			return nil, fmt.Errorf("%w: mount %s: %w", ErrLambdaUnavailable, cfg.Arn, err)
		}

		binds = append(binds, m.HostDir+":"+cfg.LocalMountPath)
	}

	return binds, nil
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

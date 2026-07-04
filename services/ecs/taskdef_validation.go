package ecs

import (
	"fmt"
	"regexp"
	"strings"
)

// ECS task-definition network modes.
const (
	networkModeBridge = "bridge"
	networkModeHost   = "host"
	networkModeAwsvpc = "awsvpc"
	networkModeNone   = "none"
)

// containerNamePattern matches the allowed characters in an ECS container name.
// AWS accepts up to 255 letters (uppercase and lowercase), numbers, underscores,
// and hyphens.
var containerNamePattern = regexp.MustCompile(`^[a-zA-Z0-9._-]+$`)

// validateRegisterTaskDefinition enforces the structural rules that real AWS ECS
// applies to RegisterTaskDefinition before a revision is created. Violations are
// reported as a ClientException, matching the AWS error code, except for the
// Fargate CPU/memory pairing which AWS surfaces as a ClientException too but is
// validated separately by validateFargateCPUMemory.
func validateRegisterTaskDefinition(input RegisterTaskDefinitionInput) error {
	if err := validateNetworkMode(input.NetworkMode); err != nil {
		return err
	}

	if err := validateContainerDefinitions(input.ContainerDefinitions, input.NetworkMode); err != nil {
		return err
	}

	return validateCompatibilities(input)
}

// validateNetworkMode rejects unknown network modes. An empty network mode is
// allowed and AWS defaults it to "bridge" on EC2.
func validateNetworkMode(mode string) error {
	switch mode {
	case "", networkModeBridge, networkModeHost, networkModeAwsvpc, networkModeNone:
		return nil
	default:
		return fmt.Errorf(
			"%w: network mode %q is not valid; valid values: bridge, host, awsvpc, none",
			ErrClient, mode,
		)
	}
}

// validateContainerDefinitions enforces the per-container rules AWS applies:
// at least one container, a unique well-formed name, and a non-empty image.
// For awsvpc mode, a port mapping's hostPort (when set) must equal its
// containerPort because the task ENI shares the container's network namespace.
func validateContainerDefinitions(defs []ContainerDefinition, networkMode string) error {
	if len(defs) == 0 {
		return fmt.Errorf("%w: container definitions should not be empty", ErrClient)
	}

	seen := make(map[string]struct{}, len(defs))

	for i := range defs {
		def := defs[i]

		switch {
		case def.Name == "":
			return fmt.Errorf("%w: container name is required", ErrClient)
		case !containerNamePattern.MatchString(def.Name):
			return fmt.Errorf(
				"%w: container name %q is invalid; up to 255 letters, numbers, hyphens, and underscores are allowed",
				ErrClient,
				def.Name,
			)
		case def.Image == "":
			return fmt.Errorf("%w: container %q must specify an image", ErrClient, def.Name)
		}

		if _, dup := seen[def.Name]; dup {
			return fmt.Errorf("%w: container name %q is used more than once", ErrClient, def.Name)
		}

		seen[def.Name] = struct{}{}

		if err := validatePortMappings(def, networkMode); err != nil {
			return err
		}
	}

	return nil
}

// validatePortMappings enforces the awsvpc constraint that a container's
// hostPort must match its containerPort.
func validatePortMappings(def ContainerDefinition, networkMode string) error {
	if networkMode != networkModeAwsvpc {
		return nil
	}

	for _, pm := range def.PortMappings {
		if pm.HostPort != 0 && pm.HostPort != pm.ContainerPort {
			return fmt.Errorf(
				"%w: when networkMode=awsvpc, the host ports and container ports in "+
					"container port mappings must match; container %q maps hostPort %d to containerPort %d",
				ErrClient, def.Name, pm.HostPort, pm.ContainerPort,
			)
		}
	}

	return nil
}

// validateCompatibilities enforces the rules that apply when a task definition
// requests Fargate compatibility: the network mode must be awsvpc and both a
// task-level CPU and memory value must be supplied.
func validateCompatibilities(input RegisterTaskDefinitionInput) error {
	requiresFargate := false

	for _, rc := range input.RequiresCompatibilities {
		if strings.EqualFold(rc, launchTypeFargate) {
			requiresFargate = true

			break
		}
	}

	if !requiresFargate {
		return nil
	}

	if input.NetworkMode != networkModeAwsvpc {
		return fmt.Errorf(
			"%w: networkMode awsvpc is required for tasks that require the Fargate launch type",
			ErrClient,
		)
	}

	if input.CPU == "" || input.Memory == "" {
		return fmt.Errorf(
			"%w: task-level CPU and memory are required for the Fargate launch type",
			ErrClient,
		)
	}

	return nil
}

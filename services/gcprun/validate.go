package gcprun

import (
	"slices"
	"strings"

	"cloud.google.com/go/run/apiv2/runpb"
)

// startOrder returns the containers in an order where every container follows the ones it depends_on;
// independent containers keep their declared order.
func startOrder(cs []*runpb.Container) ([]*runpb.Container, error) {
	done := map[string]bool{}
	out := make([]*runpb.Container, 0, len(cs))
	left := slices.Clone(cs)

	for len(left) > 0 {
		idx := slices.IndexFunc(left, func(c *runpb.Container) bool {
			return !slices.ContainsFunc(c.GetDependsOn(), func(d string) bool { return !done[d] })
		})
		if idx < 0 {
			return nil, errInvalid("Container dependencies (depends_on) form a cycle.")
		}

		c := left[idx]
		left = slices.Delete(left, idx, idx+1)
		out = append(out, c)

		if c.GetName() != "" {
			done[c.GetName()] = true
		}
	}

	return out, nil
}

func validateDependencies(cs []*runpb.Container) error {
	names := map[string]bool{}

	for _, c := range cs {
		if c.GetName() == "" {
			continue
		}

		if names[c.GetName()] {
			return errInvalid("Container name %q is used more than once.", c.GetName())
		}

		names[c.GetName()] = true
	}

	for _, c := range cs {
		for _, d := range c.GetDependsOn() {
			if !names[d] {
				return errInvalid("Container %q depends on %q, which is not a container of the revision.", c.GetName(), d)
			}

			if d == c.GetName() {
				return errInvalid("Container %q cannot depend on itself.", d)
			}
		}
	}

	_, err := startOrder(cs)

	return err
}

func validateVolumes(t *runpb.RevisionTemplate) error {
	vols := map[string]bool{}

	for _, v := range t.GetVolumes() {
		if v.GetName() == "" || vols[v.GetName()] {
			return errInvalid("Volume names must be set and unique.")
		}

		vols[v.GetName()] = true

		if v.GetVolumeType() == nil {
			return errInvalid("Volume %q needs a source.", v.GetName())
		}

		if s := v.GetSecret(); s != nil && s.GetSecret() == "" {
			return errInvalid("Volume %q: secret must be set.", v.GetName())
		}
	}

	for _, c := range t.GetContainers() {
		paths := map[string]bool{}

		for _, m := range c.GetVolumeMounts() {
			switch {
			case !vols[m.GetName()]:
				return errInvalid("Container %q mounts volume %q, which is not defined.", c.GetName(), m.GetName())
			case m.GetMountPath() == "" || strings.Contains(m.GetMountPath(), ":"):
				return errInvalid("Volume mount %q needs a mount_path without ':'.", m.GetName())
			case paths[m.GetMountPath()]:
				return errInvalid("Container %q mounts two volumes at %q.", c.GetName(), m.GetMountPath())
			}

			paths[m.GetMountPath()] = true
		}
	}

	return nil
}

func validateProbes(cs []*runpb.Container) error {
	for _, c := range cs {
		if err := validateProbe(c, c.GetStartupProbe(), "startup"); err != nil {
			return err
		}

		if err := validateProbe(c, c.GetLivenessProbe(), "liveness"); err != nil {
			return err
		}
	}

	return nil
}

func validateScaling(t *runpb.RevisionTemplate) error {
	sc := t.GetScaling()
	if sc.GetMinInstanceCount() < 0 || sc.GetMaxInstanceCount() < 0 {
		return errInvalid("Instance counts must not be negative.")
	}

	if sc.GetMaxInstanceCount() > 0 && sc.GetMinInstanceCount() > sc.GetMaxInstanceCount() {
		return errInvalid("min_instance_count must not exceed max_instance_count.")
	}

	return nil
}

// validateRuntimeShape checks the parts of a template that shape the running instance.
func validateRuntimeShape(t *runpb.RevisionTemplate) error {
	if err := validateDependencies(t.GetContainers()); err != nil {
		return err
	}

	if err := validateVolumes(t); err != nil {
		return err
	}

	if err := validateProbes(t.GetContainers()); err != nil {
		return err
	}

	return validateScaling(t)
}

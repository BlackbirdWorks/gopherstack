package gcpbatch

import (
	"strings"

	"cloud.google.com/go/batch/apiv1/batchpb"
)

const (
	maxRetries     = 10
	maxExitCode    = 255
	exitMaxRuntime = 50005
	minReserved    = 50001
	maxReserved    = 50006
)

func validateLabels(what string, labels map[string]string) error {
	if len(labels) > maxLabels {
		return errInvalid("At most %d %s are allowed.", maxLabels, what)
	}

	for k, v := range labels {
		if !labelKeyRe.MatchString(k) {
			return errInvalid("Invalid %s key %q.", what, k)
		}

		if strings.HasPrefix(k, "goog-") || strings.HasPrefix(k, "google-") {
			return errInvalid("%s key %q uses a reserved prefix.", what, k)
		}

		if !labelValRe.MatchString(v) {
			return errInvalid("Invalid value %q for %s %q.", v, what, k)
		}
	}

	return nil
}

func validateEnv(where string, e *batchpb.Environment) error {
	if e == nil {
		return nil
	}

	if e.GetEncryptedVariables() != nil {
		return errInvalid("%s: encrypted_variables are not supported; use secret_variables.", where)
	}

	for k := range e.GetVariables() {
		if !envNameRe.MatchString(k) {
			return errInvalid("%s: invalid environment variable name %q.", where, k)
		}
	}

	for k, v := range e.GetSecretVariables() {
		if !envNameRe.MatchString(k) {
			return errInvalid("%s: invalid environment variable name %q.", where, k)
		}

		if !secretRe.MatchString(v) {
			return errInvalid(
				"%s: secret variable %q must name projects/{project}/secrets/{secret}/versions/{version}.",
				where,
				k,
			)
		}
	}

	return nil
}

func validateRunnable(i int, r *batchpb.Runnable) error {
	where := "runnables[" + itoa(i) + "]"

	switch x := r.GetExecutable().(type) {
	case *batchpb.Runnable_Container_:
		if x.Container.GetImageUri() == "" {
			return errInvalid("%s.container.image_uri is required.", where)
		}
	case *batchpb.Runnable_Script_:
		if x.Script.GetText() == "" && x.Script.GetPath() == "" {
			return errInvalid("%s.script needs text or path.", where)
		}
	case *batchpb.Runnable_Barrier_:
	default:
		return errInvalid("%s: one of container, script or barrier is required.", where)
	}

	if r.GetTimeout() != nil && r.GetTimeout().AsDuration() < 0 {
		return errInvalid("%s.timeout must not be negative.", where)
	}

	if r.GetBackground() && r.GetAlwaysRun() {
		return errInvalid("%s: background and always_run cannot both be set.", where)
	}

	return validateEnv(where+".environment", r.GetEnvironment())
}

func validatePolicies(ps []*batchpb.LifecyclePolicy) error {
	if len(ps) > 1 {
		return errInvalid("Only one lifecycle policy is supported.")
	}

	for _, p := range ps {
		if p.GetAction() == batchpb.LifecyclePolicy_ACTION_UNSPECIFIED {
			return errInvalid("lifecycle_policies.action is required.")
		}

		codes := p.GetActionCondition().GetExitCodes()
		if len(codes) == 0 {
			return errInvalid("lifecycle_policies.action_condition.exit_codes is required.")
		}

		for _, c := range codes {
			if c < 0 || (c > maxExitCode && (c < minReserved || c > maxReserved)) {
				return errInvalid("Invalid lifecycle policy exit code %d.", c)
			}
		}
	}

	return nil
}

func validateVolumes(vs []*batchpb.Volume) error {
	for i, v := range vs {
		if v.GetMountPath() == "" || !strings.HasPrefix(v.GetMountPath(), "/") {
			return errInvalid("volumes[%d].mount_path must be an absolute path.", i)
		}

		if v.GetSource() == nil {
			return errInvalid("volumes[%d]: one of nfs, gcs or device_name is required.", i)
		}
	}

	return nil
}

func validateSpec(s *batchpb.TaskSpec) error {
	if s == nil {
		return errInvalid("task_groups[0].task_spec is required.")
	}

	if len(s.GetRunnables()) == 0 {
		return errInvalid("task_spec.runnables must contain at least one runnable.")
	}

	for i, r := range s.GetRunnables() {
		if err := validateRunnable(i, r); err != nil {
			return err
		}
	}

	if s.GetMaxRetryCount() < 0 || s.GetMaxRetryCount() > maxRetries {
		return errInvalid("task_spec.max_retry_count must be in [0, %d].", maxRetries)
	}

	if s.GetMaxRunDuration() != nil && s.GetMaxRunDuration().AsDuration() < 0 {
		return errInvalid("task_spec.max_run_duration must not be negative.")
	}

	if c := s.GetComputeResource(); c.GetCpuMilli() < 0 || c.GetMemoryMib() < 0 || c.GetBootDiskMib() < 0 {
		return errInvalid("task_spec.compute_resource values must not be negative.")
	}

	if err := validatePolicies(s.GetLifecyclePolicies()); err != nil {
		return err
	}

	if err := validateVolumes(s.GetVolumes()); err != nil {
		return err
	}

	return validateEnv("task_spec.environment", s.GetEnvironment())
}

func validateGroup(g *batchpb.TaskGroup) error {
	if err := validateSpec(g.GetTaskSpec()); err != nil {
		return err
	}

	if g.GetTaskCount() < 0 || g.GetParallelism() < 0 {
		return errInvalid("task_count and parallelism must not be negative.")
	}

	if g.GetSchedulingPolicy() == batchpb.TaskGroup_IN_ORDER && g.GetParallelism() > 1 {
		return errInvalid("parallelism must be 1 when scheduling_policy is IN_ORDER.")
	}

	for i, e := range g.GetTaskEnvironments() {
		if err := validateEnv("task_environments["+itoa(i)+"]", e); err != nil {
			return err
		}
	}

	return nil
}

func validateAllocation(a *batchpb.AllocationPolicy) error {
	if a == nil {
		return nil
	}

	if err := validateLabels("allocation_policy label", a.GetLabels()); err != nil {
		return err
	}

	for _, l := range a.GetLocation().GetAllowedLocations() {
		if !strings.HasPrefix(l, "regions/") && !strings.HasPrefix(l, "zones/") {
			return errInvalid(
				"allocation_policy.location.allowed_locations entry %q must be regions/... or zones/....",
				l,
			)
		}
	}

	if sa := a.GetServiceAccount().GetEmail(); sa != "" && !saEmailRe.MatchString(sa) {
		return errInvalid("allocation_policy.service_account.email %q is not a service account email.", sa)
	}

	return nil
}

func validateLogs(p *batchpb.LogsPolicy) error {
	if p.GetDestination() == batchpb.LogsPolicy_PATH && p.GetLogsPath() == "" {
		return errInvalid("logs_policy.logs_path is required when destination is PATH.")
	}

	return nil
}

func validateJob(j *batchpb.Job) error {
	if j == nil {
		return errInvalid("job is required.")
	}

	switch n := len(j.GetTaskGroups()); {
	case n == 0:
		return errInvalid("job.task_groups is required.")
	case n > 1:
		return errInvalid("Only one TaskGroup is supported now.")
	}

	if j.GetPriority() < 0 || j.GetPriority() >= maxPriority {
		return errInvalid("job.priority must be in [0, %d).", maxPriority)
	}

	if err := validateLabels("label", j.GetLabels()); err != nil {
		return err
	}

	if err := validateGroup(j.GetTaskGroups()[0]); err != nil {
		return err
	}

	if err := validateAllocation(j.GetAllocationPolicy()); err != nil {
		return err
	}

	return validateLogs(j.GetLogsPolicy())
}

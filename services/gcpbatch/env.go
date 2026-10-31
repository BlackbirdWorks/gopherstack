package gcpbatch

import (
	"errors"
	"fmt"
	"maps"
	"slices"
	"strconv"

	"cloud.google.com/go/batch/apiv1/batchpb"
)

const (
	envTaskIndex = "BATCH_TASK_INDEX"
	envTaskCount = "BATCH_TASK_COUNT"
	envHostsFile = "BATCH_HOSTS_FILE"
	// HostsFilePath is where BATCH_HOSTS_FILE points when require_hosts_file is set.
	HostsFilePath = "/etc/cloudbatch-taskgroup-hosts"
)

var errNoSecrets = errors.New("secret manager is not available to this job")

// envLayers returns the environments that apply to task i, lowest precedence first: task spec, then the task's
// own task_environments entry, then the runnable's.
func envLayers(g *batchpb.TaskGroup, i int, r *batchpb.Runnable) []*batchpb.Environment {
	spec := g.GetTaskSpec()
	deprecated := spec.GetEnvironments() //nolint:staticcheck // still honoured, below task_spec.environment
	layers := []*batchpb.Environment{{Variables: deprecated}, spec.GetEnvironment()}

	if envs := g.GetTaskEnvironments(); i < len(envs) {
		layers = append(layers, envs[i])
	}

	return append(layers, r.GetEnvironment())
}

// resolveEnv flattens the layers into variables, reading secret_variables from Secret Manager. Within a layer a
// secret variable overrides a plain one of the same name. Error text names the secret, never its value.
func (b *InMemoryBackend) resolveEnv(svc Services, layers []*batchpb.Environment) (map[string]string, error) {
	out := map[string]string{}

	for _, l := range layers {
		maps.Copy(out, l.GetVariables())

		for _, name := range slices.Sorted(maps.Keys(l.GetSecretVariables())) {
			ref := l.GetSecretVariables()[name]
			if svc.Secrets == nil {
				return nil, fmt.Errorf("%w (variable %s)", errNoSecrets, name)
			}

			v, err := svc.Secrets.AccessSecret(ref)
			if err != nil {
				return nil, fmt.Errorf("access secret %s for variable %s: %w", ref, name, err)
			}

			out[name] = string(v)
		}
	}

	return out, nil
}

// predefined are the variables Batch sets on every runnable of a task.
func predefined(g *batchpb.TaskGroup, i, count int, vars map[string]string) {
	vars[envTaskIndex] = strconv.Itoa(i)
	vars[envTaskCount] = strconv.Itoa(count)

	if g.GetRequireHostsFile() {
		vars[envHostsFile] = HostsFilePath
	}
}

func envList(vars map[string]string) []string {
	out := make([]string, 0, len(vars))
	for _, k := range slices.Sorted(maps.Keys(vars)) {
		out = append(out, k+"="+vars[k])
	}

	return out
}

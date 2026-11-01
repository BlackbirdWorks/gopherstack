package batch

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"maps"
	"regexp"
	"strconv"
	"strings"
)

const (
	resourceTypeVCPU   = "VCPU"
	resourceTypeMemory = "MEMORY"
	taskIDBytes        = 16
	maxSessionNameLen  = 64
)

var (
	refParamRe = regexp.MustCompile(`Ref::[A-Za-z0-9_.-]+`)
	envRefRe   = regexp.MustCompile(`\$\(([A-Za-z_][A-Za-z0-9_]*)\)`)
)

func newTaskID() string {
	buf := make([]byte, taskIDBytes)
	_, _ = rand.Read(buf)

	return hex.EncodeToString(buf)
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}

	return s[:n]
}

func envStrings(kvs []KeyValuePair) []string {
	out := make([]string, 0, len(kvs))
	for _, kv := range kvs {
		out = append(out, kv.Name+"="+kv.Value)
	}

	return out
}

// containerResources returns the vCPU and memory (MiB) request, preferring resourceRequirements.
func containerResources(cd *ContainerDetail) (float64, int64) {
	vcpus, mem := float64(cd.Vcpus), int64(cd.Memory)

	for _, rr := range cd.ResourceRequirements {
		switch strings.ToUpper(rr.Type) {
		case resourceTypeVCPU:
			if v, err := strconv.ParseFloat(rr.Value, 64); err == nil {
				vcpus = v
			}
		case resourceTypeMemory:
			if v, err := strconv.ParseInt(rr.Value, 10, 64); err == nil {
				mem = v
			}
		}
	}

	return vcpus, mem
}

// expandCommand substitutes Ref::name from the job definition's parameters
// overlaid with the job's, then $(NAME) from the job environment.
func expandCommand(cmd []string, defParams, jobParams map[string]string, env []KeyValuePair) []string {
	if len(cmd) == 0 {
		return nil
	}

	params := make(map[string]string, len(defParams)+len(jobParams))
	maps.Copy(params, defParams)

	maps.Copy(params, jobParams)

	vars := make(map[string]string, len(env))
	for _, kv := range env {
		vars[kv.Name] = kv.Value
	}

	out := make([]string, len(cmd))
	for i, arg := range cmd {
		arg = refParamRe.ReplaceAllStringFunc(arg, func(m string) string {
			if v, ok := params[strings.TrimPrefix(m, "Ref::")]; ok {
				return v
			}

			return m
		})
		out[i] = envRefRe.ReplaceAllStringFunc(arg, func(m string) string {
			if v, ok := vars[m[2:len(m)-1]]; ok {
				return v
			}

			return m
		})
	}

	return out
}

// runtimeEnv is the emulator-provided environment: region, endpoint and job-role credentials.
func runtimeEnv(rs *runState, issuer JobRoleCredentialIssuer, endpoint string) ([]string, error) {
	return runtimeEnvFor(rs, rs.jobRoleArn, issuer, endpoint)
}

func runtimeEnvFor(rs *runState, roleArn string, issuer JobRoleCredentialIssuer, endpoint string) ([]string, error) {
	env := []string{"AWS_REGION=" + rs.region, "AWS_DEFAULT_REGION=" + rs.region}

	if endpoint != "" {
		env = append(env, "AWS_ENDPOINT_URL="+endpoint)
	}

	if roleArn != "" && issuer != nil {
		session := truncate("AWSBatch-"+strings.ReplaceAll(rs.jobID, ":", "-"), maxSessionNameLen)

		creds, err := issuer.IssueJobCredentials(roleArn, session)
		if err != nil {
			return nil, err
		}

		env = append(env,
			"AWS_ACCESS_KEY_ID="+creds.AccessKeyID,
			"AWS_SECRET_ACCESS_KEY="+creds.SecretAccessKey,
			"AWS_SESSION_TOKEN="+creds.SessionToken,
		)
	}

	return env, nil
}

// batchEnv is the AWS_BATCH_* environment, appended last so job definitions cannot shadow it.
func batchEnv(rs *runState) []string { return batchEnvFor(rs, rs.jobID) }

func batchEnvFor(rs *runState, jobID string) []string {
	env := []string{
		"AWS_BATCH_JOB_ID=" + jobID,
		"AWS_BATCH_JQ_NAME=" + rs.queueName,
		fmt.Sprintf("AWS_BATCH_JOB_ATTEMPT=%d", rs.attempt),
	}

	if rs.ceName != "" {
		env = append(env, "AWS_BATCH_CE_NAME="+rs.ceName)
	}

	if rs.arrayIndex != nil {
		env = append(env, fmt.Sprintf("AWS_BATCH_JOB_ARRAY_INDEX=%d", *rs.arrayIndex))
	}

	return env
}

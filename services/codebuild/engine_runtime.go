package codebuild

import (
	"context"
	"encoding/base64"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"time"
)

const (
	codeBuildServicePrincipal = "codebuild.amazonaws.com"
	sessionPrefix             = "AWSCodeBuild-"
	awsEnvPrefix              = "AWS_"
)

var envNameRe = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

// BuildCredentials are temporary service-role credentials injected into a build container.
type BuildCredentials struct {
	Expires         time.Time
	AccessKeyID     string
	SecretAccessKey string
	SessionToken    string
}

// CredentialIssuer mints temporary credentials for a build's service role.
type CredentialIssuer interface {
	IssueBuildCredentials(ctx context.Context, roleArn, sessionName string) (BuildCredentials, error)
}

// ServicePrincipal is the principal the service role's trust policy must allow.
func ServicePrincipal() string { return codeBuildServicePrincipal }

// runtimeEnv returns the emulator endpoint and service-role credentials for the build container,
// plus the secret values that must be masked in logs.
func (r *buildRunner) runtimeEnv() ([]EnvironmentVariable, []string, error) {
	var out []EnvironmentVariable

	if ep := strings.TrimSuffix(r.svc.Endpoint, "/"); ep != "" {
		out = append(out, EnvironmentVariable{Name: "AWS_ENDPOINT_URL", Value: ep, Type: envTypePlaintext})
	}

	if r.svc.Credentials == nil || r.build.ServiceRole == "" {
		return out, nil, nil
	}

	creds, err := r.svc.Credentials.IssueBuildCredentials(r.ctx, r.build.ServiceRole, sessionPrefix+r.buildUUID())
	if err != nil {
		return nil, nil, fmt.Errorf("unable to assume service role %s: %w", r.build.ServiceRole, err)
	}

	out = append(out,
		EnvironmentVariable{Name: "AWS_ACCESS_KEY_ID", Value: creds.AccessKeyID, Type: envTypePlaintext},
		EnvironmentVariable{Name: "AWS_SECRET_ACCESS_KEY", Value: creds.SecretAccessKey, Type: envTypePlaintext},
		EnvironmentVariable{Name: "AWS_SESSION_TOKEN", Value: creds.SessionToken, Type: envTypePlaintext},
	)

	return out, []string{creds.AccessKeyID, creds.SecretAccessKey, creds.SessionToken}, nil
}

// grantWorkspace lets non-root run-as users read and write the workspace and temp state.
func (r *buildRunner) grantWorkspace() {
	_, _ = r.rt.Exec(r.ctx, r.containerID, []string{"sh", "-c", "chmod -R a+rwX " + shQuote(r.srcDir)})
}

// announceRuntimes logs the install phase's runtime-versions selection.
func (r *buildRunner) announceRuntimes(ph specPhase) {
	if len(ph.RuntimeVersions) == 0 {
		return
	}

	image := ""
	if r.build.Environment != nil {
		image = r.build.Environment.Image
	}

	if !strings.HasPrefix(image, awsStandardImagePfx) {
		r.log.write(r.ctx, marker("Skipping install of runtimes. Runtime version selection is not supported by this build image"))

		return
	}

	names := make([]string, 0, len(ph.RuntimeVersions))
	for n := range ph.RuntimeVersions {
		names = append(names, n)
	}

	sort.Strings(names)

	for _, n := range names {
		v := expandEnvRefs(fmt.Sprint(ph.RuntimeVersions[n]), r.envMap)
		r.log.write(r.ctx, marker("Selecting runtime %s: %s", n, v))
	}
}

var envRefRe = regexp.MustCompile(`\$\{?([A-Za-z_][A-Za-z0-9_]*)\}?`)

func expandEnvRefs(s string, env map[string]string) string {
	return envRefRe.ReplaceAllStringFunc(s, func(m string) string {
		name := strings.Trim(m, "${}")
		if v, ok := env[name]; ok {
			return v
		}

		return m
	})
}

// exportableNames returns the env.exported-variables names AWS allows: not AWS_-prefixed and not
// sourced from Parameter Store or Secrets Manager.
func (r *buildRunner) exportableNames(secretNames map[string]bool) []string {
	if r.spec.Env == nil {
		return nil
	}

	var out []string

	for _, n := range r.spec.Env.ExportedVars {
		if !envNameRe.MatchString(n) || strings.HasPrefix(n, awsEnvPrefix) || secretNames[n] {
			continue
		}

		out = append(out, n)
	}

	return out
}

// collectExports reads the exported variables from the container and records them on the build.
func (r *buildRunner) collectExports() {
	names := r.exportableNames(r.secretNames)
	if len(names) == 0 || r.ctx.Err() != nil {
		return
	}

	script := "[ -f " + cbEnvFile + " ] && . " + cbEnvFile + "\nfor n in " + strings.Join(names, " ") + `; do
  eval "set=\${$n+x}"; [ "$set" = x ] || continue
  eval "v=\${$n}"
  printf '%s=%s\n' "$n" "$(printf %s "$v" | base64 | tr -d '\n')"
done
`

	res, err := r.rt.Exec(r.ctx, r.containerID, shellInvocation(script, r.spec.linuxShell(), ""))
	if err != nil || res.ExitCode != 0 {
		r.log.write(r.ctx, marker("Failed to read exported variables"))

		return
	}

	var vars []ExportedEnvironmentVariable

	for line := range strings.SplitSeq(res.Stdout, "\n") {
		name, enc, ok := strings.Cut(strings.TrimSpace(line), "=")
		if !ok {
			continue
		}

		raw, derr := base64.StdEncoding.DecodeString(enc)
		if derr != nil {
			continue
		}

		vars = append(vars, ExportedEnvironmentVariable{Name: name, Value: string(raw)})
	}

	if len(vars) == 0 {
		return
	}

	r.b.mutateBuild(r.build.ID, func(bd *Build) { bd.ExportedEnvironmentVariables = vars })
}

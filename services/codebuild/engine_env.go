package codebuild

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
)

var errEnvResolve = errors.New("cannot resolve environment variable")

const (
	envTypePlaintext   = "PLAINTEXT"
	envTypeParameter   = "PARAMETER_STORE"
	envTypeSecret      = "SECRETS_MANAGER"
	arnFieldsBeforeRef = 7
	secretFieldStage   = 1
	secretFieldVersion = 2
	millisPerSecond    = 1000
)

// splitSecretRef parses "secret-id[:json-key[:version-stage[:version-id]]]"; secret-id may be an ARN.
func splitSecretRef(ref string) (string, []string) {
	if strings.HasPrefix(ref, "arn:") {
		parts := strings.SplitN(ref, ":", arnFieldsBeforeRef+1)
		if len(parts) <= arnFieldsBeforeRef {
			return ref, nil
		}

		id := strings.Join(parts[:arnFieldsBeforeRef], ":")

		return id, strings.Split(parts[arnFieldsBeforeRef], ":")
	}

	parts := strings.Split(ref, ":")

	return parts[0], parts[1:]
}

func (r *buildRunner) resolveSecret(ctx context.Context, ref string) (string, error) {
	if r.svc.Values == nil {
		return "", fmt.Errorf("%w: Secrets Manager backend is not wired", errEnvResolve)
	}

	id, rest := splitSecretRef(ref)

	field := func(i int) string {
		if i < len(rest) {
			return rest[i]
		}

		return ""
	}

	val, err := r.svc.Values.SecretValue(ctx, r.region, id, field(secretFieldStage), field(secretFieldVersion))
	if err != nil {
		return "", fmt.Errorf("%w: secret %s: %w", errEnvResolve, id, err)
	}

	key := field(0)
	if key == "" {
		return val, nil
	}

	var obj map[string]any
	if jerr := json.Unmarshal([]byte(val), &obj); jerr != nil {
		return "", fmt.Errorf("%w: secret %s is not JSON: %w", errEnvResolve, id, jerr)
	}

	v, ok := obj[key]
	if !ok {
		return "", fmt.Errorf("%w: secret %s has no key %q", errEnvResolve, id, key)
	}

	if s, isStr := v.(string); isStr {
		return s, nil
	}

	return fmt.Sprint(v), nil
}

// resolveEnv resolves every variable to a plain value; masked holds the values to hide in logs.
func (r *buildRunner) resolveEnv(
	ctx context.Context, vars []EnvironmentVariable,
) ([]EnvironmentVariable, []string, error) {
	out := make([]EnvironmentVariable, 0, len(vars))

	var masked []string

	for _, v := range vars {
		val := v.Value

		switch v.Type {
		case envTypeParameter:
			if r.svc.Values == nil {
				return nil, nil, fmt.Errorf("%w: SSM backend is not wired", errEnvResolve)
			}

			p, err := r.svc.Values.Parameter(ctx, r.region, v.Value)
			if err != nil {
				return nil, nil, fmt.Errorf("%w: parameter %s: %w", errEnvResolve, v.Value, err)
			}

			val = p
			masked = append(masked, p)
		case envTypeSecret:
			s, err := r.resolveSecret(ctx, v.Value)
			if err != nil {
				return nil, nil, err
			}

			val = s
			masked = append(masked, s)
		}

		out = append(out, EnvironmentVariable{Name: v.Name, Value: val, Type: envTypePlaintext})
	}

	return out, masked, nil
}

// builtinEnv returns the CODEBUILD_* variables every build sees.
func (r *buildRunner) builtinEnv(srcDir string) []EnvironmentVariable {
	b := r.build
	pairs := [][2]string{
		{"CODEBUILD_BUILD_ID", b.ID},
		{"CODEBUILD_BUILD_ARN", b.Arn},
		{"CODEBUILD_BUILD_NUMBER", strconv.FormatInt(b.BuildNumber, 10)},
		{"CODEBUILD_SRC_DIR", srcDir},
		{"CODEBUILD_RESOLVED_SOURCE_VERSION", b.ResolvedSourceVersion},
		{"CODEBUILD_SOURCE_VERSION", b.SourceVersion},
		{"CODEBUILD_INITIATOR", b.Initiator},
		{"CODEBUILD_KMS_KEY_ID", b.EncryptionKey},
		{"CODEBUILD_START_TIME", strconv.FormatInt(int64(b.StartTime*millisPerSecond), 10)},
		{"AWS_REGION", r.region},
		{"AWS_DEFAULT_REGION", r.region},
		{"CODEBUILD_CI", "true"},
	}

	out := make([]EnvironmentVariable, len(pairs))
	for i, p := range pairs {
		out[i] = EnvironmentVariable{Name: p[0], Value: p[1], Type: envTypePlaintext}
	}

	return out
}

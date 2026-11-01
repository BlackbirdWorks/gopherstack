package ecs

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
)

const (
	sourceASM = "asm"
	sourceSSM = "ssm"

	secretsManagerARNParts = 7
	secretARNMinParts      = 6
)

var errSecretReference = errors.New("invalid secret reference")

// SecretsResolver reads secret values for container "secrets" from Secrets Manager and SSM Parameter Store.
type SecretsResolver interface {
	// SecretString returns a secret's string value; empty versionStage and versionID mean AWSCURRENT.
	SecretString(ctx context.Context, region, secretID, versionStage, versionID string) (string, error)
	// ParameterValue returns a parameter's decrypted value.
	ParameterValue(ctx context.Context, region, name string) (string, error)
}

// SetSecretsResolver wires Secrets Manager and SSM so container secrets resolve at launch.
func (b *InMemoryBackend) SetSecretsResolver(s SecretsResolver) {
	b.mu.Lock("SetSecretsResolver")
	defer b.mu.Unlock()

	b.secrets = s
}

func (b *InMemoryBackend) secretsResolver() SecretsResolver {
	b.mu.RLock("secretsResolver")
	defer b.mu.RUnlock()

	return b.secrets
}

type secretRef struct {
	region         string
	id             string
	jsonKey        string
	versionStage   string
	versionID      string
	secretsManager bool
}

// parseSecretRef understands the valueFrom forms in the ECS task definition parameters docs: a Secrets Manager
// ARN with optional :json-key:version-stage:version-id, an SSM parameter ARN, or a bare SSM parameter name.
func parseSecretRef(valueFrom, defaultRegion string) (secretRef, error) {
	if !strings.HasPrefix(valueFrom, "arn:") {
		return secretRef{region: defaultRegion, id: valueFrom}, nil
	}

	parts := strings.Split(valueFrom, ":")
	if len(parts) < secretARNMinParts {
		return secretRef{}, fmt.Errorf("%w: %q", errSecretReference, valueFrom)
	}

	switch parts[2] {
	case "secretsmanager":
		if len(parts) < secretsManagerARNParts {
			return secretRef{}, fmt.Errorf("%w: %q", errSecretReference, valueFrom)
		}

		ref := secretRef{
			secretsManager: true,
			region:         parts[3],
			id:             strings.Join(parts[:secretsManagerARNParts], ":"),
		}

		fields := parts[secretsManagerARNParts:]
		for i, dst := range []*string{&ref.jsonKey, &ref.versionStage, &ref.versionID} {
			if i < len(fields) {
				*dst = fields[i]
			}
		}

		return ref, nil
	case sourceSSM:
		name, ok := strings.CutPrefix(strings.Join(parts[5:], ":"), "parameter")
		if !ok {
			return secretRef{}, fmt.Errorf("%w: %q", errSecretReference, valueFrom)
		}

		return secretRef{region: parts[3], id: name}, nil
	default:
		return secretRef{}, fmt.Errorf(
			"%w: %q is neither a Secrets Manager nor an SSM ARN",
			errSecretReference,
			valueFrom,
		)
	}
}

// resolveSecret fetches valueFrom; failures use ECS's ResourceInitializationError stoppedReason shape.
func (b *InMemoryBackend) resolveSecret(ctx context.Context, valueFrom string) (string, error) {
	ref, err := parseSecretRef(valueFrom, b.region)
	if err != nil {
		return "", secretLaunchError(sourceASM, valueFrom, err)
	}

	resolver := b.secretsResolver()
	if resolver == nil {
		return "", secretLaunchError(secretSource(ref), valueFrom, errSecretsUnavailable)
	}

	if !ref.secretsManager {
		val, perr := resolveParameter(ctx, resolver, ref)
		if perr != nil {
			return "", secretLaunchError(sourceSSM, valueFrom, perr)
		}

		return val, nil
	}

	val, err := resolver.SecretString(ctx, ref.region, ref.id, ref.versionStage, ref.versionID)
	if err != nil {
		return "", secretLaunchError(sourceASM, valueFrom, err)
	}

	if ref.jsonKey == "" {
		return val, nil
	}

	extracted, err := jsonSecretField(val, ref.jsonKey)
	if err != nil {
		return "", secretLaunchError(sourceASM, valueFrom, err)
	}

	return extracted, nil
}

var errSecretsUnavailable = errors.New("secrets backend is not wired")

func secretSource(ref secretRef) string {
	if ref.secretsManager {
		return sourceASM
	}

	return sourceSSM
}

// resolveParameter tries the name as written in the ARN, then with a leading slash toggled, since an ARN does
// not say whether the parameter name started with one.
func resolveParameter(ctx context.Context, resolver SecretsResolver, ref secretRef) (string, error) {
	val, err := resolver.ParameterValue(ctx, ref.region, ref.id)
	if err == nil {
		return val, nil
	}

	alt, hadSlash := strings.CutPrefix(ref.id, "/")
	if !hadSlash {
		alt = "/" + ref.id
	}

	if alt != "" && alt != ref.id {
		if altVal, altErr := resolver.ParameterValue(ctx, ref.region, alt); altErr == nil {
			return altVal, nil
		}
	}

	return "", err
}

func jsonSecretField(secret, key string) (string, error) {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal([]byte(secret), &fields); err != nil {
		return "", fmt.Errorf("secret value is not a JSON object: %w", err)
	}

	raw, ok := fields[key]
	if !ok {
		return "", fmt.Errorf("%w: json key %q not found in secret", errSecretReference, key)
	}

	var s string
	if json.Unmarshal(raw, &s) == nil {
		return s, nil
	}

	return string(raw), nil
}

func secretLaunchError(source, valueFrom string, err error) error {
	return &taskLaunchError{
		Reason: fmt.Sprintf(
			"ResourceInitializationError: unable to pull secrets or registry auth: execution resource retrieval "+
				"failed: unable to retrieve secret from %s: service call has been retried 1 time(s): failed to "+
				"fetch secret %s from %s: %v",
			source, valueFrom, secretSourceName(source), err),
		Err: err,
	}
}

func secretSourceName(source string) string {
	if source == sourceASM {
		return "secrets manager"
	}

	return sourceSSM
}

// taskLaunchError carries the stoppedReason of a task that failed to start.
type taskLaunchError struct {
	Err    error
	Reason string
}

func (e *taskLaunchError) Error() string { return e.Reason }

func (e *taskLaunchError) Unwrap() error { return e.Err }

const (
	stopCodeUserInitiated             = "UserInitiated"
	stopCodeEssentialContainerExited  = "EssentialContainerExited"
	stopCodeTaskFailedToStart         = "TaskFailedToStart"
	stopCodeServiceSchedulerInitiated = "ServiceSchedulerInitiated"
)

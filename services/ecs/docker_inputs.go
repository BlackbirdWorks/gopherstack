package ecs

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/registryauth"
)

const (
	s3ARNParts        = 6
	utf8BOM           = "\xef\xbb\xbf"
	envFileTypeS3     = "s3"
	logDriverFirelens = "awsfirelens"
)

var (
	errRegistryCredentials = errors.New(
		"repository credentials secret must be a JSON object with username and password")
	errEnvFileReference    = errors.New("invalid environment file reference")
	errEnvFilesUnavailable = errors.New("s3 backend is not wired")
	errFirelensUnsupported = errors.New("firelens log routing is not supported by the Docker runtime")
)

// EnvFileStore reads the S3 objects named by container environmentFiles.
type EnvFileStore interface {
	// ObjectBytes returns the object's body.
	ObjectBytes(ctx context.Context, bucket, key string) ([]byte, error)
}

// SetEnvFileStore wires S3 so container environmentFiles resolve at launch.
func (b *InMemoryBackend) SetEnvFileStore(s EnvFileStore) {
	b.mu.Lock("SetEnvFileStore")
	defer b.mu.Unlock()

	b.envFiles = s
}

func (b *InMemoryBackend) envFileStore() EnvFileStore {
	b.mu.RLock("envFileStore")
	defer b.mu.RUnlock()

	return b.envFiles
}

// launchInputRunner is implemented by runners that apply repositoryCredentials and environmentFiles.
type launchInputRunner interface {
	SetRegistryAuthResolver(fn func(ctx context.Context, cd ContainerDefinition) (string, error))
	SetEnvFileResolver(fn func(ctx context.Context, cd ContainerDefinition) ([]string, error))
}

// resolveRegistryAuth returns the base64 registry auth for cd's image, or "" without repositoryCredentials.
func (b *InMemoryBackend) resolveRegistryAuth(ctx context.Context, cd ContainerDefinition) (string, error) {
	if cd.RepositoryCredentials == nil || cd.RepositoryCredentials.CredentialsParameter == "" {
		return "", nil
	}

	param := cd.RepositoryCredentials.CredentialsParameter

	region, id := b.region, param
	if strings.HasPrefix(param, "arn:") {
		ref, err := parseSecretRef(param, b.region)
		if err != nil || !ref.secretsManager {
			return "", secretLaunchError(sourceASM, param, errors.Join(errSecretReference, err))
		}

		region, id = ref.region, ref.id
	}

	resolver := b.secretsResolver()
	if resolver == nil {
		return "", secretLaunchError(sourceASM, param, errSecretsUnavailable)
	}

	raw, err := resolver.SecretString(ctx, region, id, "", "")
	if err != nil {
		return "", secretLaunchError(sourceASM, param, err)
	}

	var creds struct {
		Username string `json:"username"`
		Password string `json:"password"`
	}

	if json.Unmarshal([]byte(raw), &creds) != nil || creds.Username == "" || creds.Password == "" {
		return "", secretLaunchError(sourceASM, param, errRegistryCredentials)
	}

	return registryauth.EncodeCredential(registryauth.HostOf(cd.Image), creds.Username, creds.Password), nil
}

// resolveEnvFiles returns KEY=VALUE pairs from cd's environmentFiles; the first file defining a key wins.
func (b *InMemoryBackend) resolveEnvFiles(ctx context.Context, cd ContainerDefinition) ([]string, error) {
	if len(cd.EnvironmentFiles) == 0 {
		return nil, nil
	}

	store := b.envFileStore()
	seen := map[string]bool{}

	var out []string

	for _, f := range cd.EnvironmentFiles {
		bucket, key, err := parseEnvFileARN(f)
		if err != nil {
			return nil, envFileLaunchError(f.Value, err)
		}

		if store == nil {
			return nil, envFileLaunchError(f.Value, errEnvFilesUnavailable)
		}

		body, err := store.ObjectBytes(ctx, bucket, key)
		if err != nil {
			return nil, envFileLaunchError(f.Value, err)
		}

		for _, kv := range parseEnvFile(string(body)) {
			name, _, _ := strings.Cut(kv, "=")
			if seen[name] {
				continue
			}

			seen[name] = true

			out = append(out, kv)
		}
	}

	return out, nil
}

func parseEnvFileARN(f EnvironmentFile) (string, string, error) {
	if f.Type != envFileTypeS3 {
		return "", "", fmt.Errorf("%w: type %q is not s3", errEnvFileReference, f.Type)
	}

	parts := strings.SplitN(f.Value, ":", s3ARNParts)
	ok := len(parts) == s3ARNParts && parts[0] == "arn" && parts[2] == envFileTypeS3
	bucket, key, found := strings.Cut(parts[len(parts)-1], "/")

	if !ok || !found || bucket == "" || key == "" {
		return "", "", fmt.Errorf("%w: %q is not an S3 object ARN", errEnvFileReference, f.Value)
	}

	return bucket, key, nil
}

// parseEnvFile reads VARIABLE=VALUE lines; values are verbatim (no quote handling) and "#" lines are comments.
func parseEnvFile(body string) []string {
	body = strings.TrimPrefix(body, utf8BOM)

	var out []string

	for line := range strings.SplitSeq(body, "\n") {
		line = strings.TrimLeft(strings.TrimSuffix(line, "\r"), " \t")
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}

		if name, _, ok := strings.Cut(line, "="); ok && name != "" {
			out = append(out, line)
		}
	}

	return out
}

func envFileLaunchError(arn string, err error) error {
	return &taskLaunchError{
		Reason: fmt.Sprintf(
			"ResourceInitializationError: failed to download env files: failed to get environment file %s: %v",
			arn, err),
		Err: err,
	}
}

// checkFirelens rejects task definitions that need FireLens log routing, which the Docker runtime cannot do.
func checkFirelens(defs []ContainerDefinition) error {
	for _, cd := range defs {
		usesDriver := cd.LogConfiguration != nil && cd.LogConfiguration.LogDriver == logDriverFirelens
		if cd.FirelensConfiguration != nil || usesDriver {
			return &taskLaunchError{
				Reason: fmt.Sprintf("Task failed to start: container %q: %v", cd.Name, errFirelensUnsupported),
				Err:    errFirelensUnsupported,
			}
		}
	}

	return nil
}

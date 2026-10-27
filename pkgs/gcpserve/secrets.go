package gcpserve

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

const secretDirMode = 0o755

// ErrSecretPath means a secret volume file path escapes its volume.
var ErrSecretPath = errors.New("secret volume path must be relative and stay inside the volume")

// SecretResolver returns the payload of a Secret Manager version resource name
// (projects/{p}/secrets/{s}/versions/{v}).
type SecretResolver func(versionName string) ([]byte, error)

// SecretVersionName builds the version resource name for a secret given by short name or full resource name.
// An empty version means "latest".
func SecretVersionName(project, secret, version string) string {
	if version == "" {
		version = "latest"
	}

	name := secret
	if !strings.HasPrefix(secret, "projects/") {
		name = "projects/" + project + "/secrets/" + secret
	}

	return name + "/versions/" + version
}

// WriteSecretFile writes data to rel inside dir with mode, rejecting paths that leave dir.
func WriteSecretFile(dir, rel string, data []byte, mode os.FileMode) error {
	clean := filepath.Clean(rel)
	if clean == "." || filepath.IsAbs(clean) || strings.HasPrefix(clean, "..") {
		return fmt.Errorf("%w: %q", ErrSecretPath, rel)
	}

	full := filepath.Join(dir, clean)
	if err := os.MkdirAll(filepath.Dir(full), secretDirMode); err != nil {
		return fmt.Errorf("create secret volume path: %w", err)
	}

	if err := os.WriteFile(full, data, mode); err != nil {
		return fmt.Errorf("write secret volume file: %w", err)
	}

	return os.Chmod(full, mode) //nolint:wrapcheck // a plain chmod failure message is clear
}

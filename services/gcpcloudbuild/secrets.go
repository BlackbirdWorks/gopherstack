package gcpcloudbuild

import (
	"errors"
	"fmt"
	"sort"
)

var (
	errNoSecrets = errors.New("secret manager is not available to this build")
	errNoKMS     = errors.New("cloud KMS is not available to this build")
	errDupSecret = errors.New("secret environment variable is defined more than once")
)

// resolveSecrets reads availableSecrets.secretManager and decrypts the KMS-encrypted secrets (availableSecrets.inline
// and the deprecated secrets field) into environment values for steps that list them in secretEnv, and masks the
// values in the build log.
func (r *runner) resolveSecrets() error {
	b := r.build
	r.secrets = map[string]string{}

	if err := r.resolveSecretManager(); err != nil {
		return err
	}

	for _, s := range b.GetAvailableSecrets().GetInline() {
		if err := r.decryptAll(s.GetKmsKeyName(), s.GetEnvMap()); err != nil {
			return err
		}
	}

	for _, s := range b.GetSecrets() {
		if err := r.decryptAll(s.GetKmsKeyName(), s.GetSecretEnv()); err != nil {
			return err
		}
	}

	return nil
}

func (r *runner) resolveSecretManager() error {
	sm := r.build.GetAvailableSecrets().GetSecretManager()
	if len(sm) == 0 {
		return nil
	}

	if r.svc.Secrets == nil {
		return errNoSecrets
	}

	for _, s := range sm {
		v, err := r.svc.Secrets.AccessSecret(s.GetVersionName())
		if err != nil {
			return fmt.Errorf("access %s: %w", s.GetVersionName(), err)
		}

		if err = r.addSecret(s.GetEnv(), string(v)); err != nil {
			return err
		}
	}

	return nil
}

func (r *runner) decryptAll(key string, env map[string][]byte) error {
	if len(env) == 0 {
		return nil
	}

	if r.svc.KMS == nil {
		return errNoKMS
	}

	names := make([]string, 0, len(env))
	for n := range env {
		names = append(names, n)
	}

	sort.Strings(names)

	for _, n := range names {
		pt, err := r.svc.KMS.Decrypt(key, env[n])
		if err != nil {
			return fmt.Errorf("decrypt %s with %s: %w", n, key, err)
		}

		if err = r.addSecret(n, string(pt)); err != nil {
			return err
		}
	}

	return nil
}

func (r *runner) addSecret(env, value string) error {
	if _, dup := r.secrets[env]; dup {
		return fmt.Errorf("%w: %s", errDupSecret, env)
	}

	r.secrets[env] = value
	r.log.mask(value)

	return nil
}

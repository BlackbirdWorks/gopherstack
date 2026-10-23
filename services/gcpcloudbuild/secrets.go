package gcpcloudbuild

import (
	"errors"
	"fmt"
)

var (
	errNoSecrets  = errors.New("secret manager is not available to this build")
	errKMSSecrets = errors.New(
		"KMS-encrypted secrets are not supported; use availableSecrets.secretManager",
	)
)

// resolveSecrets reads availableSecrets.secretManager into environment values for steps that list them in
// secretEnv, and masks the values in the build log.
func (r *runner) resolveSecrets() error {
	b := r.build

	if len(b.GetSecrets()) > 0 || len(b.GetAvailableSecrets().GetInline()) > 0 {
		return errKMSSecrets
	}

	sm := b.GetAvailableSecrets().GetSecretManager()
	if len(sm) == 0 {
		return nil
	}

	if r.svc.Secrets == nil {
		return errNoSecrets
	}

	r.secrets = make(map[string]string, len(sm))

	for _, s := range sm {
		v, err := r.svc.Secrets.AccessSecret(s.GetVersionName())
		if err != nil {
			return fmt.Errorf("access %s: %w", s.GetVersionName(), err)
		}

		r.secrets[s.GetEnv()] = string(v)
		r.log.mask(string(v))
	}

	return nil
}

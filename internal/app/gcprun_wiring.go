package app

import (
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	gcprunbackend "github.com/blackbirdworks/gopherstack/services/gcprun"
	gcpsecretmanagerbackend "github.com/blackbirdworks/gopherstack/services/gcpsecretmanager"
)

// wireGCPRun lets Cloud Run read secret environment variables and secret volumes from Secret Manager.
func wireGCPRun(byName map[string]service.Registerable) {
	run, ok := byName["GCPRun"].(*gcprunbackend.Handler)
	if !ok {
		return
	}

	if sm, isSM := byName["GCPSecretManager"].(*gcpsecretmanagerbackend.Handler); isSM {
		run.Backend.SetSecretResolver(func(versionName string) ([]byte, error) {
			resp, err := sm.Backend.AccessSecretVersion(versionName)
			if err != nil {
				return nil, fmt.Errorf("access %s: %w", versionName, err)
			}

			return resp.GetPayload().GetData(), nil
		})
	}
}

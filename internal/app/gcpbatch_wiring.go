package app

import (
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	gcpbatchbackend "github.com/blackbirdworks/gopherstack/services/gcpbatch"
	gcploggingbackend "github.com/blackbirdworks/gopherstack/services/gcplogging"
	gcpsecretmanagerbackend "github.com/blackbirdworks/gopherstack/services/gcpsecretmanager"
)

// wireGCPBatch connects Batch to Secret Manager (secret_variables) and Cloud Logging (CLOUD_LOGGING logs policy).
func wireGCPBatch(byName map[string]service.Registerable) {
	bt, ok := byName["GCPBatch"].(*gcpbatchbackend.Handler)
	if !ok {
		return
	}

	var svc gcpbatchbackend.Services

	if sm, isSM := byName["GCPSecretManager"].(*gcpsecretmanagerbackend.Handler); isSM {
		svc.Secrets = cloudBuildSecrets{b: sm.Backend}
	}

	if lg, isLG := byName["GCPLogging"].(*gcploggingbackend.Handler); isLG {
		svc.Logs = lg.Backend
	}

	bt.Backend.SetServices(svc)
}

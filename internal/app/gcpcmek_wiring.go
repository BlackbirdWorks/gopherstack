package app

import (
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	gcpeventarcbackend "github.com/blackbirdworks/gopherstack/services/gcpeventarc"
	gcpkmsbackend "github.com/blackbirdworks/gopherstack/services/gcpkms"
	gcploggingbackend "github.com/blackbirdworks/gopherstack/services/gcplogging"
	gcpredisbackend "github.com/blackbirdworks/gopherstack/services/gcpredis"
	gcpsecretmanagerbackend "github.com/blackbirdworks/gopherstack/services/gcpsecretmanager"
	gcpspannerbackend "github.com/blackbirdworks/gopherstack/services/gcpspanner"
	gcpstoragebackend "github.com/blackbirdworks/gopherstack/services/gcpstorage"
	gcpworkflowsbackend "github.com/blackbirdworks/gopherstack/services/gcpworkflows"
)

// wireGCPCMEK makes customer-managed encryption keys on GCP services resolve against the Cloud KMS emulator.
func wireGCPCMEK(byName map[string]service.Registerable) {
	kms, ok := byName["GCPKMS"].(*gcpkmsbackend.Handler)
	if !ok || kms.Backend == nil {
		return
	}

	if h, found := byName["GCPSecretManager"].(*gcpsecretmanagerbackend.Handler); found {
		h.Backend.SetKeyChecker(kms.Backend)
	}

	if h, found := byName["GCPLogging"].(*gcploggingbackend.Handler); found {
		h.Backend.SetKeyChecker(kms.Backend)
	}

	if h, found := byName["GCPEventarc"].(*gcpeventarcbackend.Handler); found {
		h.Backend.SetKeyChecker(kms.Backend)
	}

	if h, found := byName["GCPWorkflows"].(*gcpworkflowsbackend.Handler); found {
		h.Backend.SetKeyChecker(kms.Backend)
	}

	if h, found := byName["GCPRedis"].(*gcpredisbackend.Handler); found {
		h.Backend.SetKeyChecker(kms.Backend)
	}

	if h, found := byName["GCPSpanner"].(*gcpspannerbackend.Handler); found {
		h.Backend.SetKeyChecker(kms.Backend)
	}

	if h, found := byName["GCPStorage"].(*gcpstoragebackend.Handler); found {
		h.Backend.SetKMSChecker(kms.Backend)
	}
}

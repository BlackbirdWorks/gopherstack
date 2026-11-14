package app

import (
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	appintegrationsbackend "github.com/blackbirdworks/gopherstack/services/appintegrations"
	agentcorebackend "github.com/blackbirdworks/gopherstack/services/bedrockagentcorecontrol"
	codegurureviewerbackend "github.com/blackbirdworks/gopherstack/services/codegurureviewer"
	kmsbackend "github.com/blackbirdworks/gopherstack/services/kms"
	rdsbackend "github.com/blackbirdworks/gopherstack/services/rds"
)

// wireAWSKeyLookups makes customer-managed KMS key references on AWS services resolve against the KMS emulator.
func wireAWSKeyLookups(byName map[string]service.Registerable) {
	kmsH, ok := byName["KMS"].(*kmsbackend.Handler)
	if !ok || kmsH.Backend == nil {
		return
	}

	keys := kmsbackend.NewKeyChecker(kmsH.Backend)

	if h, found := byName["AppIntegrations"].(*appintegrationsbackend.Handler); found {
		h.Backend.SetKeyChecker(keys)
	}

	if h, found := byName["CodeGuruReviewer"].(*codegurureviewerbackend.Handler); found {
		h.Backend.SetKeyChecker(keys)
	}

	if h, found := byName["BedrockAgentCoreControl"].(*agentcorebackend.Handler); found {
		h.Backend.SetKeyChecker(keys)
	}

	if h, found := byName["RDS"].(*rdsbackend.Handler); found {
		h.SetKeyChecker(keys)
	}
}

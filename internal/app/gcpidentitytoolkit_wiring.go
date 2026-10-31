package app

import (
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	gcpiambackend "github.com/blackbirdworks/gopherstack/services/gcpiam"
	gcpidtkbackend "github.com/blackbirdworks/gopherstack/services/gcpidentitytoolkit"
)

// wireGCPIdentityToolkit lets Identity Platform verify RS256 custom tokens against Cloud IAM service account keys.
func wireGCPIdentityToolkit(byName map[string]service.Registerable) {
	idp, ok := byName["GCPIdentityToolkit"].(*gcpidtkbackend.Handler)
	if !ok {
		return
	}

	if iam, isIAM := byName["GCPIAM"].(*gcpiambackend.Handler); isIAM {
		idp.Backend.SetCustomTokenKeys(iam.Backend.VerificationKeys)
	}
}

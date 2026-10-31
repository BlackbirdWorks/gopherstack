package app

import (
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	cognitoidpbackend "github.com/blackbirdworks/gopherstack/services/cognitoidp"
	verifiedpermissionsbackend "github.com/blackbirdworks/gopherstack/services/verifiedpermissions"
)

// wireVerifiedPermissionsCognito gives Verified Permissions in-process access to user pool signing keys.
func wireVerifiedPermissionsCognito(vpReg, idpReg service.Registerable) {
	vpH, ok := vpReg.(*verifiedpermissionsbackend.Handler)
	if !ok || vpH.TokenVerifier() == nil {
		return
	}

	if idpH, idpOk := idpReg.(*cognitoidpbackend.Handler); idpOk {
		vpH.TokenVerifier().SetCognitoKeys(idpH)
	}
}

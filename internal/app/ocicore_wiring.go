package app

import (
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	ocicorebackend "github.com/blackbirdworks/gopherstack/services/ocicore"
	ocivaultbackend "github.com/blackbirdworks/gopherstack/services/ocivault"
)

// wireOCICoreVault lets OCI Core validate volume and backup kmsKeyId references against OCI Vault keys.
func wireOCICoreVault(byName map[string]service.Registerable) {
	coreH, cok := byName["OCICore"].(*ocicorebackend.Handler)
	vaultH, vok := byName["OCIVault"].(*ocivaultbackend.Handler)

	if cok && vok {
		coreH.Backend.SetKeyChecker(vaultH.Backend.KeyState)
	}
}

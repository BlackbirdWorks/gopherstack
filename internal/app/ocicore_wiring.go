package app

import (
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	ocicorebackend "github.com/blackbirdworks/gopherstack/services/ocicore"
	ocifilestoragebackend "github.com/blackbirdworks/gopherstack/services/ocifilestorage"
	ocifunctionsbackend "github.com/blackbirdworks/gopherstack/services/ocifunctions"
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

// wireOCIKeyLookups lets OCI File Storage and OCI Functions validate kmsKeyId references against OCI Vault keys.
func wireOCIKeyLookups(byName map[string]service.Registerable) {
	vaultH, ok := byName["OCIVault"].(*ocivaultbackend.Handler)
	if !ok {
		return
	}

	if h, found := byName["OCIFileStorage"].(*ocifilestoragebackend.Handler); found {
		h.Backend.SetKeyChecker(vaultH.Backend.KeyState)
	}

	if h, found := byName["OCIFunctions"].(*ocifunctionsbackend.Handler); found {
		h.Backend.SetKeyChecker(vaultH.Backend.KeyState)
	}
}

package app

import (
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	gcpsqlbackend "github.com/blackbirdworks/gopherstack/services/gcpsql"
	gcpstoragebackend "github.com/blackbirdworks/gopherstack/services/gcpstorage"
)

// wireGCPSQL connects Cloud SQL export and import to the GCS emulator.
func wireGCPSQL(byName map[string]service.Registerable) {
	sqlH, ok := byName["GCPSQL"].(*gcpsqlbackend.Handler)
	if !ok {
		return
	}

	if gcs, isGCS := byName["GCPStorage"].(*gcpstoragebackend.Handler); isGCS {
		sqlH.Backend.SetObjectStore(gcsObjectStore{b: gcs.Backend})
	}
}

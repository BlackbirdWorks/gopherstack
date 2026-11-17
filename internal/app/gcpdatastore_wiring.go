package app

import (
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	gcpdatastorebackend "github.com/blackbirdworks/gopherstack/services/gcpdatastore"
	gcpstoragebackend "github.com/blackbirdworks/gopherstack/services/gcpstorage"
)

// wireGCPDatastore connects Datastore export and import to the GCS emulator.
func wireGCPDatastore(byName map[string]service.Registerable) {
	ds, ok := byName["GCPDatastore"].(*gcpdatastorebackend.Handler)
	if !ok {
		return
	}

	if gcs, isGCS := byName["GCPStorage"].(*gcpstoragebackend.Handler); isGCS {
		ds.Backend.SetObjectStore(gcsObjectStore{b: gcs.Backend})
	}
}

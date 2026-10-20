package app

import (
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	gcpfunctionsbackend "github.com/blackbirdworks/gopherstack/services/gcpfunctions"
	gcppubsubbackend "github.com/blackbirdworks/gopherstack/services/gcppubsub"
	gcpstoragebackend "github.com/blackbirdworks/gopherstack/services/gcpstorage"
)

// gcsObjectReader lets Cloud Functions read source archives out of the in-process GCS emulator.
type gcsObjectReader struct {
	b *gcpstoragebackend.InMemoryBackend
}

func (r gcsObjectReader) ReadObject(bucket, object string) ([]byte, error) {
	o, err := r.b.GetObject(bucket, object, 0, gcpstoragebackend.Preconds{})
	if err != nil {
		return nil, fmt.Errorf("read gs://%s/%s: %w", bucket, object, err)
	}

	return o.Data, nil
}

// wireGCPFunctions connects Cloud Functions to Pub/Sub (event triggers) and GCS (storage_source archives).
func wireGCPFunctions(byName map[string]service.Registerable) {
	fns, ok := byName["GCPFunctions"].(*gcpfunctionsbackend.Handler)
	if !ok {
		return
	}

	if ps, isPS := byName["GCPPubSub"].(*gcppubsubbackend.Handler); isPS {
		fns.Backend.SetEventBus(ps.Backend)
	}

	if gcs, isGCS := byName["GCPStorage"].(*gcpstoragebackend.Handler); isGCS {
		fns.Backend.SetObjectReader(gcsObjectReader{b: gcs.Backend})
	}
}

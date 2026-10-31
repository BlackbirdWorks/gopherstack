package app

import (
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	gcpfirestorebackend "github.com/blackbirdworks/gopherstack/services/gcpfirestore"
	gcpfunctionsbackend "github.com/blackbirdworks/gopherstack/services/gcpfunctions"
	gcppubsubbackend "github.com/blackbirdworks/gopherstack/services/gcppubsub"
	gcpsecretmanagerbackend "github.com/blackbirdworks/gopherstack/services/gcpsecretmanager"
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

// BucketExists lets storage triggers be checked against the GCS emulator.
func (r gcsObjectReader) BucketExists(bucket string) bool {
	_, err := r.b.GetBucket(bucket, gcpstoragebackend.Preconds{})

	return err == nil
}

// wireGCPFunctions connects Cloud Functions to Pub/Sub, GCS and Firestore (event triggers and storage_source
// archives) and Secret Manager (secret environment variables and volumes).
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
		fns.Backend.SetBucketLookup(gcsObjectReader{b: gcs.Backend})
		gcs.Backend.ObserveObjects(func(ev gcpstoragebackend.ObjectEvent) {
			fns.Backend.HandleStorageEvent(gcpfunctionsbackend.StorageEvent{
				Type: string(ev.Type), Bucket: ev.Object.Bucket, Name: ev.Object.Name, Data: ev.Data(), Time: ev.Time,
			})
		})
	}

	if fs, isFS := byName["GCPFirestore"].(*gcpfirestorebackend.Handler); isFS {
		fs.Backend.ObserveDocuments(func(ev gcpfirestorebackend.DocumentEvent) {
			fns.Backend.HandleDocumentEvent(gcpfunctionsbackend.DocumentEvent{
				Kind: string(ev.Kind), Old: ev.Old, New: ev.New, UpdateMask: ev.UpdateMask, Time: ev.Time,
			})
		})
	}

	if sm, isSM := byName["GCPSecretManager"].(*gcpsecretmanagerbackend.Handler); isSM {
		fns.Backend.SetSecretResolver(func(versionName string) ([]byte, error) {
			resp, err := sm.Backend.AccessSecretVersion(versionName)
			if err != nil {
				return nil, err //nolint:wrapcheck // the status message is shown to the user as is
			}

			return resp.GetPayload().GetData(), nil
		})
	}
}

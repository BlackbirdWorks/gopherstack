package gcpredis

import (
	"strings"

	"cloud.google.com/go/longrunning/autogen/longrunningpb"
	"cloud.google.com/go/redis/apiv1/redispb"
	"google.golang.org/protobuf/proto"
)

const gcsScheme = "gs://"

// parseGCSURI splits gs://bucket/object.
func parseGCSURI(uri string) (string, string, error) {
	rest, ok := strings.CutPrefix(uri, gcsScheme)
	if !ok {
		return "", "", errInvalid("%q is not a gs://bucket/object URI.", uri)
	}

	bucket, object, ok := strings.Cut(rest, "/")
	if !ok || bucket == "" || object == "" {
		return "", "", errInvalid("%q is not a gs://bucket/object URI.", uri)
	}

	return bucket, object, nil
}

// ImportInstance replaces an instance's data with an RDB file read from Cloud Storage.
func (b *InMemoryBackend) ImportInstance(in *redispb.ImportInstanceRequest) (*longrunningpb.Operation, error) {
	bucket, object, err := parseGCSURI(in.GetInputConfig().GetGcsSource().GetUri())
	if err != nil {
		return nil, err
	}

	b.mu.Lock("ImportInstance")
	rec, n, err := b.readyLocked(in.GetName())

	if err == nil {
		rec.pb.State = redispb.Instance_IMPORTING
	}
	b.mu.Unlock()

	if err != nil {
		return nil, err
	}

	settle := b.settle(n.full(), nil)

	return b.begin(n.parent(), "import", n.full(), func() (proto.Message, error) {
		loadErr := b.loadRDB(rec, bucket, object)
		resp, settleErr := settle()

		if loadErr != nil {
			return nil, loadErr
		}

		return resp, settleErr
	})
}

func (b *InMemoryBackend) loadRDB(rec *instRec, bucket, object string) error {
	b.mu.RLock("loadRDB")
	objects, eng := b.objects, rec.engine
	b.mu.RUnlock()

	if objects == nil {
		return errPrecondition("Cloud Storage is not available to this emulator instance.")
	}

	data, err := objects.ReadObject(bucket, object)
	if err != nil {
		return errNotFound("Cannot read %s%s/%s: %v", gcsScheme, bucket, object, err)
	}

	if err = eng.ImportRDB(data); err != nil {
		return errInvalid("Cannot import %s%s/%s: %v", gcsScheme, bucket, object, err)
	}

	return nil
}

// ExportInstance writes an instance's data to Cloud Storage as an RDB file.
func (b *InMemoryBackend) ExportInstance(in *redispb.ExportInstanceRequest) (*longrunningpb.Operation, error) {
	bucket, object, err := parseGCSURI(in.GetOutputConfig().GetGcsDestination().GetUri())
	if err != nil {
		return nil, err
	}

	b.mu.Lock("ExportInstance")
	rec, n, readyErr := b.readyLocked(in.GetName())

	if readyErr == nil {
		rec.exports++
	}
	b.mu.Unlock()

	if readyErr != nil {
		return nil, readyErr
	}

	return b.begin(n.parent(), "export", n.full(), func() (proto.Message, error) {
		defer b.endExport(rec)

		if storeErr := b.storeRDB(rec, bucket, object); storeErr != nil {
			return nil, storeErr
		}

		return b.GetInstance(n.full())
	})
}

func (b *InMemoryBackend) endExport(rec *instRec) {
	b.mu.Lock("endExport")
	defer b.mu.Unlock()

	rec.exports--
}

func (b *InMemoryBackend) storeRDB(rec *instRec, bucket, object string) error {
	b.mu.RLock("storeRDB")
	objects, eng := b.objects, rec.engine
	b.mu.RUnlock()

	if objects == nil {
		return errPrecondition("Cloud Storage is not available to this emulator instance.")
	}

	data, err := eng.ExportRDB()
	if err != nil {
		return errPrecondition("Cannot export instance data: %v", err)
	}

	if err = objects.WriteObject(bucket, object, data); err != nil {
		return errNotFound("Cannot write %s%s/%s: %v", gcsScheme, bucket, object, err)
	}

	return nil
}

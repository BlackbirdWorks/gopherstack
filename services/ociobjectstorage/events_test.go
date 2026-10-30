package ociobjectstorage_test

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	oci "github.com/blackbirdworks/gopherstack/services/ociobjectstorage"
)

func TestObjectEvents(t *testing.T) {
	t.Parallel()

	ns := oci.DefaultNamespace
	meta, pre := oci.ObjectMeta{}, oci.Preconds{}

	tests := []struct {
		name    string
		enabled *bool
		run     func(t *testing.T, b *oci.InMemoryBackend)
		want    []string
	}{
		{"put create then update", new(true), func(t *testing.T, b *oci.InMemoryBackend) {
			t.Helper()

			_, err := b.PutObject(ns, "bk", "o", []byte("1"), meta, pre, "")
			require.NoError(t, err)
			_, err = b.PutObject(ns, "bk", "o", []byte("2"), meta, pre, "")
			require.NoError(t, err)
		}, []string{oci.EventCreateObject, oci.EventUpdateObject}},
		{"delete", new(true), func(t *testing.T, b *oci.InMemoryBackend) {
			t.Helper()

			_, err := b.PutObject(ns, "bk", "o", []byte("1"), meta, pre, "")
			require.NoError(t, err)
			_, err = b.DeleteObject(ns, "bk", "o", "", "")
			require.NoError(t, err)
		}, []string{oci.EventCreateObject, oci.EventDeleteObject}},
		{"failed delete emits nothing", new(true), func(t *testing.T, b *oci.InMemoryBackend) {
			t.Helper()

			_, err := b.DeleteObject(ns, "bk", "missing", "", "")
			require.Error(t, err)
		}, nil},
		{"batch delete", new(true), func(t *testing.T, b *oci.InMemoryBackend) {
			t.Helper()

			_, err := b.PutObject(ns, "bk", "o", []byte("1"), meta, pre, "")
			require.NoError(t, err)
			_, _, err = b.BatchDelete(ns, "bk", []oci.BatchItem{{ObjectName: "o"}, {ObjectName: "nope"}})
			require.NoError(t, err)
		}, []string{oci.EventCreateObject, oci.EventDeleteObject}},
		{"copy", new(true), func(t *testing.T, b *oci.InMemoryBackend) {
			t.Helper()

			_, err := b.PutObject(ns, "bk", "o", []byte("1"), meta, pre, "")
			require.NoError(t, err)
			_, err = b.CopyObject(ns, "bk", oci.CopyInput{
				SourceObjectName:  "o",
				DestinationRegion: oci.Region,
				DestinationNS:     ns,
				DestinationBucket: "bk",
				DestinationObject: "c",
			})
			require.NoError(t, err)
		}, []string{oci.EventCreateObject, oci.EventCreateObject}},
		{"multipart commit", new(true), func(t *testing.T, b *oci.InMemoryBackend) {
			t.Helper()

			u, err := b.CreateUpload(ns, "bk", oci.CreateUploadInput{Object: "big"}, pre)
			require.NoError(t, err)
			etag, _, err := b.UploadPart(ns, "bk", "big", u.ID, 1, []byte("x"), "")
			require.NoError(t, err)
			_, err = b.CommitUpload(ns, "bk", "big", u.ID, oci.CommitInput{
				PartsToCommit: []oci.CommitPart{{Etag: etag, PartNum: 1}},
			}, pre)
			require.NoError(t, err)
		}, []string{oci.EventCreateObject}},
		{"flag off", nil, func(t *testing.T, b *oci.InMemoryBackend) {
			t.Helper()

			_, err := b.PutObject(ns, "bk", "o", []byte("1"), meta, pre, "")
			require.NoError(t, err)
		}, nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := oci.NewInMemoryBackend()

			var (
				mu  sync.Mutex
				got []oci.ObjectEvent
			)

			b.SetObjectEventSink(func(e oci.ObjectEvent) {
				mu.Lock()
				got = append(got, e)
				mu.Unlock()
			})

			_, err := b.CreateBucket(ns, oci.CreateBucketInput{
				Name: "bk", CompartmentID: "ocid1.compartment.oc1..c", ObjectEventsEnabled: tt.enabled,
			}, "")
			require.NoError(t, err)

			tt.run(t, b)

			mu.Lock()
			defer mu.Unlock()

			types := make([]string, 0, len(got))
			for _, e := range got {
				types = append(types, e.EventType)
				assert.Equal(t, "bk", e.BucketName)
				assert.Equal(t, "ocid1.compartment.oc1..c", e.CompartmentID)
				assert.NotEmpty(t, e.BucketID)
				assert.Equal(t, ns, e.Namespace)
			}

			if tt.want == nil {
				assert.Empty(t, types)

				return
			}

			assert.Equal(t, tt.want, types)
		})
	}
}

package ociobjectstorage_test

import (
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/oracle/oci-go-sdk/v65/objectstorage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	oci "github.com/blackbirdworks/gopherstack/services/ociobjectstorage"
)

func TestInvalidStorageTierCode(t *testing.T) {
	t.Parallel()

	tests := []struct {
		call func(t *testing.T) error
		name string
	}{
		{name: "create bucket", call: func(*testing.T) error {
			_, err := oci.NewInMemoryBackend().CreateBucket(oci.DefaultNamespace,
				oci.CreateBucketInput{Name: "tb", CompartmentID: testCompartment, StorageTier: "Cold"}, "")

			return err
		}},
		{name: "update tier", call: func(t *testing.T) error {
			t.Helper()

			b := oci.NewInMemoryBackend()
			_, err := b.CreateBucket(
				oci.DefaultNamespace,
				oci.CreateBucketInput{Name: "tb", CompartmentID: testCompartment},
				"",
			)
			require.NoError(t, err)
			require.NoError(t, b.PutBlob(oci.DefaultNamespace, "tb", "o", []byte("x")))

			return b.UpdateStorageTier(oci.DefaultNamespace, "tb", "o", "", "Cold")
		}},
		{name: "put object", call: func(t *testing.T) error {
			t.Helper()

			e := newEnv(t, envOpts{})
			e.mkBucket(t, "bk", nil)
			_, err := e.client.PutObject(t.Context(), objectstorage.PutObjectRequest{
				NamespaceName: new(e.ns), BucketName: new("bk"), ObjectName: new("o"),
				ContentLength: new(int64(1)), PutObjectBody: io.NopCloser(strings.NewReader("x")),
				StorageTier: objectstorage.PutObjectStorageTierEnum("Cold"),
			})
			if se, ok := errAsService(err); ok {
				return &oci.APIError{Code: se.GetCode(), Status: se.GetHTTPStatusCode()}
			}

			return err
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var ae *oci.APIError

			require.ErrorAs(t, tt.call(t), &ae)
			assert.Equal(t, "InvalidStorageTier", ae.Code)
			assert.Equal(t, http.StatusBadRequest, ae.Status)
		})
	}
}

func errAsService(err error) (interface {
	GetCode() string
	GetHTTPStatusCode() int
}, bool) {
	var se interface {
		GetCode() string
		GetHTTPStatusCode() int
	}

	return se, errors.As(err, &se)
}

package ocifilestorage_test

import (
	"net/http"
	"testing"

	"github.com/oracle/oci-go-sdk/v65/filestorage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	svc "github.com/blackbirdworks/gopherstack/services/ocifilestorage"
)

func vaultKeys(id string) (string, bool) {
	switch id {
	case "ocid1.key.oc1..enabled":
		return "ENABLED", true
	case "ocid1.key.oc1..disabled":
		return "DISABLED", true
	}

	return "", false
}

func TestFileSystemKmsKey(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		key        string
		wantCode   string
		wantStatus int
		unwired    bool
	}{
		{name: "enabled", key: "ocid1.key.oc1..enabled"},
		{name: "oracle managed"},
		{
			name:       "missing",
			key:        "ocid1.key.oc1..none",
			wantStatus: http.StatusNotFound,
			wantCode:   "NotAuthorizedOrNotFound",
		},
		{name: "disabled", key: "ocid1.key.oc1..disabled", wantStatus: http.StatusConflict, wantCode: "IncorrectState"},
		{name: "unwired", key: "ocid1.key.oc1..none", unwired: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, envOpts{configure: func(b *svc.InMemoryBackend) {
				if !tt.unwired {
					b.SetKeyChecker(vaultKeys)
				}
			}})

			create := func(key string) (filestorage.CreateFileSystemResponse, error) {
				d := filestorage.CreateFileSystemRequest{
					AvailabilityDomain: new(testAD), CompartmentId: new(testCompartment), DisplayName: new("fs"),
				}
				if key != "" {
					d.KmsKeyId = new(key)
				}

				return e.client.CreateFileSystem(t.Context(), d)
			}

			_, err := create(tt.key)
			if tt.wantStatus != 0 {
				assertStatus(t, err, tt.wantStatus, tt.wantCode)

				return
			}

			require.NoError(t, err)

			if tt.key == "" || tt.unwired {
				return
			}

			base := e.createFS(t, nil)

			_, err = e.client.UpdateFileSystem(t.Context(), filestorage.UpdateFileSystemRequest{
				FileSystemId: base.Id,
				KmsKeyId:     new(tt.key),
			})
			require.NoError(t, err)
		})
	}
}

func TestFileSystemUpdateKmsKeyRejected(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{configure: func(b *svc.InMemoryBackend) { b.SetKeyChecker(vaultKeys) }})
	fs := e.createFS(t, nil)

	_, err := e.client.UpdateFileSystem(t.Context(), filestorage.UpdateFileSystemRequest{
		FileSystemId: fs.Id,
		KmsKeyId:     new("ocid1.key.oc1..none"),
	})
	assertStatus(t, err, http.StatusNotFound, "NotAuthorizedOrNotFound")
	assert.Empty(t, e.getFS(t, fs.Id).KmsKeyId)
}

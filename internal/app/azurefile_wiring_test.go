package app

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/azureblob"
	"github.com/blackbirdworks/gopherstack/services/azurefile"
	"github.com/blackbirdworks/gopherstack/services/azurestoragevhost"
)

func TestWireAzureFile(t *testing.T) {
	t.Parallel()

	tests := []struct {
		byName func() map[string]service.Registerable
		check  func(t *testing.T, byName map[string]service.Registerable)
		name   string
	}{
		{
			name: "wires blob reader and vhost",
			byName: func() map[string]service.Registerable {
				blob := azureblob.NewHandler(azureblob.NewInMemoryBackend())
				blob.Port = 12345

				return map[string]service.Registerable{
					"AzureFile":         azurefile.NewHandler(azurefile.NewInMemoryBackend()),
					"AzureBlob":         blob,
					"AzureStorageVHost": azurestoragevhost.NewHandler(),
				}
			},
			check: func(t *testing.T, byName map[string]service.Registerable) {
				t.Helper()

				fileH, ok := byName["AzureFile"].(*azurefile.Handler)
				require.True(t, ok)
				assert.NotNil(t, fileH.Blobs)
				assert.Equal(t, 12345, fileH.BlobPort)

				vhost, ok := byName["AzureStorageVHost"].(*azurestoragevhost.Handler)
				require.True(t, ok)
				assert.Same(t, fileH, vhost.File)
			},
		},
		{
			name: "missing services are tolerated",
			byName: func() map[string]service.Registerable {
				return map[string]service.Registerable{
					"AzureFile": azurefile.NewHandler(azurefile.NewInMemoryBackend()),
				}
			},
			check: func(t *testing.T, byName map[string]service.Registerable) {
				t.Helper()

				fileH, ok := byName["AzureFile"].(*azurefile.Handler)
				require.True(t, ok)
				assert.Nil(t, fileH.Blobs)
			},
		},
		{
			name:   "no file service is a no-op",
			byName: func() map[string]service.Registerable { return map[string]service.Registerable{} },
			check:  func(_ *testing.T, _ map[string]service.Registerable) {},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			byName := tt.byName()
			wireAzureFile(byName)
			tt.check(t, byName)
		})
	}
}

func TestAzureBlobReader(t *testing.T) {
	t.Parallel()

	backend := azureblob.NewInMemoryBackend()
	_, err := backend.CreateContainerWith("cont", azureblob.ContainerOptions{})
	require.NoError(t, err)
	_, err = backend.PutBlobWith("cont", "b.txt", []byte("payload"), azureblob.BlobAttrs{ContentType: "text/plain"})
	require.NoError(t, err)

	tests := []struct {
		reader   azureBlobReader
		name     string
		blob     string
		wantData string
		wantErr  bool
	}{
		{name: "existing blob", reader: azureBlobReader{backend: backend}, blob: "b.txt", wantData: "payload"},
		{name: "missing blob", reader: azureBlobReader{backend: backend}, blob: "nope", wantErr: true},
		{name: "no backend", reader: azureBlobReader{}, blob: "b.txt", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			data, ctype, readErr := tt.reader.ReadBlob("cont", tt.blob, "")
			if tt.wantErr {
				require.Error(t, readErr)

				return
			}

			require.NoError(t, readErr)
			assert.Equal(t, tt.wantData, string(data))
			assert.Equal(t, "text/plain", ctype)
		})
	}
}

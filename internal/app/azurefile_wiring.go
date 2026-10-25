package app

import (
	"errors"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/azureblob"
	"github.com/blackbirdworks/gopherstack/services/azurefile"
	"github.com/blackbirdworks/gopherstack/services/azurestoragevhost"
)

// wireAzureFile gives Azure Files its Blob copy-source reader and routes the
// virtual-hosted ".file." Host label on the shared storage listener to it.
func wireAzureFile(byName map[string]service.Registerable) {
	fileH, ok := byName["AzureFile"].(*azurefile.Handler)
	if !ok {
		return
	}

	if blobH, blobOk := byName["AzureBlob"].(*azureblob.Handler); blobOk {
		fileH.Blobs = azureBlobReader{backend: blobH.Backend}
		fileH.BlobPort = blobH.Port
	}

	if vhostH, vhostOk := byName["AzureStorageVHost"].(*azurestoragevhost.Handler); vhostOk {
		vhostH.File = fileH
	}
}

// azureBlobReader exposes blob bytes to Azure Files copy operations.
type azureBlobReader struct {
	backend azureblob.StorageBackend
}

var errBlobBackendUnavailable = errors.New("azure blob backend unavailable")

func (r azureBlobReader) ReadBlob(container, blob, snapshot string) ([]byte, string, error) {
	if r.backend == nil {
		return nil, "", errBlobBackendUnavailable
	}

	info, data, err := r.backend.GetBlobAt(container, blob, snapshot)
	if err != nil {
		return nil, "", err
	}

	return data, info.ContentType, nil
}

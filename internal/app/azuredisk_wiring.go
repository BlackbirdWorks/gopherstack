package app

import (
	"errors"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/azurearm"
	"github.com/blackbirdworks/gopherstack/services/azureblob"
)

const vhdFooterBytes = 512

// wireAzureDisks gives ARM's Microsoft.Compute provider the Blob data plane that disk Import reads.
func wireAzureDisks(byName map[string]service.Registerable) {
	armH, ok := byName["AzureARM"].(*azurearm.Handler)
	if !ok {
		return
	}

	if blobH, blobOk := byName["AzureBlob"].(*azureblob.Handler); blobOk {
		armH.Registry.SetDiskBlobs(azureDiskBlobs{handler: blobH})
	}
}

// azureDiskBlobs adapts the Blob backend to services/azurearm.DiskBlobs.
type azureDiskBlobs struct {
	handler *azureblob.Handler
}

var _ azurearm.DiskBlobs = azureDiskBlobs{}

func (d azureDiskBlobs) ProbeBlob(ref azurearm.BlobRef) (azurearm.BlobProbe, error) {
	if d.handler == nil || d.handler.Backend == nil {
		return azurearm.BlobProbe{}, errBlobBackendUnavailable
	}

	if err := d.handler.AuthorizeRead(ref.Account, ref.Container, ref.Blob, ref.Query); err != nil {
		return azurearm.BlobProbe{}, fmt.Errorf("%w: %w", azurearm.ErrDiskBlobDenied, err)
	}

	info, tail, err := d.handler.Backend.ReadTail(ref.Container, ref.Blob, vhdFooterBytes)
	if errors.Is(err, azureblob.ErrBlobNotFound) || errors.Is(err, azureblob.ErrContainerNotFound) {
		return azurearm.BlobProbe{}, azurearm.ErrDiskBlobNotFound
	}

	if err != nil {
		return azurearm.BlobProbe{}, err
	}

	return azurearm.BlobProbe{
		PageBlob: info.BlobType == azureblob.BlobTypePage, Size: info.ContentLength, Footer: tail,
	}, nil
}

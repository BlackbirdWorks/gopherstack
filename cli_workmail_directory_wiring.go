package main

import (
	"context"
	"errors"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	directoryservicebackend "github.com/blackbirdworks/gopherstack/services/directoryservice"
	workmailbackend "github.com/blackbirdworks/gopherstack/services/workmail"
)

// workmailDirectoryDeleter adapts Directory Service to workmail.DirectoryDeleter.
type workmailDirectoryDeleter struct {
	backend *directoryservicebackend.InMemoryBackend
}

func (d workmailDirectoryDeleter) DeleteDirectory(region, directoryID string) error {
	err := d.backend.DeleteDirectory(directoryservicebackend.WithRegion(context.Background(), region), directoryID)
	if errors.Is(err, directoryservicebackend.ErrDirectoryNotFound) {
		return nil
	}

	return err
}

// wireWorkMailDirectory lets WorkMail DeleteOrganization delete the organization's directory.
func wireWorkMailDirectory(byName map[string]service.Registerable) {
	wmH, ok := byName["WorkMail"].(*workmailbackend.Handler)
	if !ok {
		return
	}

	wmBk, ok := wmH.Backend.(*workmailbackend.InMemoryBackend)
	if !ok {
		return
	}

	dsH, ok := byName["DirectoryService"].(*directoryservicebackend.Handler)
	if !ok {
		return
	}

	dsBk, ok := dsH.Backend.(*directoryservicebackend.InMemoryBackend)
	if !ok {
		return
	}

	wmBk.SetDirectoryDeleter(workmailDirectoryDeleter{backend: dsBk})
}

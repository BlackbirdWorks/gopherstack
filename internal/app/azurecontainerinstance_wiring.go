package app

import (
	"github.com/blackbirdworks/gopherstack/services/azurearm"
	"github.com/blackbirdworks/gopherstack/services/azurefile"
)

// aciFileShares lets Container Instances azureFile volumes read and write Azure Files shares.
type aciFileShares struct {
	backend *azurefile.InMemoryBackend
}

func (a aciFileShares) ExportShare(account, share string) (azurearm.ACIShareTree, error) {
	tree, err := a.backend.ExportShare(account, share)
	if err != nil {
		return azurearm.ACIShareTree{}, err
	}

	return azurearm.ACIShareTree{Files: tree.Files, Dirs: tree.Dirs}, nil
}

func (a aciFileShares) ApplyShare(account, share string, tree azurearm.ACIShareTree) error {
	return a.backend.ApplyShare(account, share, azurefile.ShareTree{Files: tree.Files, Dirs: tree.Dirs})
}

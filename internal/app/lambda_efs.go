package app

import (
	"errors"
	"path/filepath"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	efsbackend "github.com/blackbirdworks/gopherstack/services/efs"
	lambdabackend "github.com/blackbirdworks/gopherstack/services/lambda"
)

var errLambdaEFSUnavailable = errors.New("efs backend unavailable")

// lambdaEFSResolver adapts EFS to lambda.EFSResolver. The EFS backend is
// resolved at call time, never captured in Provider.Init.
type lambdaEFSResolver struct {
	byName map[string]service.Registerable
}

func (a *lambdaEFSResolver) backend() (*efsbackend.InMemoryBackend, error) {
	h, ok := a.byName["EFS"].(*efsbackend.Handler)
	if !ok {
		return nil, errLambdaEFSUnavailable
	}

	return h.Backend, nil
}

func (a *lambdaEFSResolver) CheckAccessPoint(accessPointARN string, subnetIDs []string) error {
	bk, err := a.backend()
	if err != nil {
		return err
	}

	return bk.CheckMountTarget(accessPointARN, subnetIDs)
}

func (a *lambdaEFSResolver) ResolveMount(accessPointARN string) (lambdabackend.EFSMount, error) {
	bk, err := a.backend()
	if err != nil {
		return lambdabackend.EFSMount{}, err
	}

	m, err := bk.ResolveAccessPointMount(accessPointARN)
	if err != nil {
		return lambdabackend.EFSMount{}, err
	}

	return lambdabackend.EFSMount{
		HostDir: m.HostDir, UID: m.UID, GID: m.GID, SecondaryGIDs: m.SecondaryGIDs, HasPosixUser: m.HasPosixUser,
	}, nil
}

// wireLambdaEFS backs EFS file systems with host directories and lets Lambda
// bind-mount access points into function containers.
func wireLambdaEFS(cli *CLI, byName map[string]service.Registerable) {
	lambdaH, ok := byName["Lambda"].(*lambdabackend.Handler)
	if !ok {
		return
	}

	lambdaBk, ok := lambdaH.Backend.(*lambdabackend.InMemoryBackend)
	if !ok {
		return
	}

	lambdaBk.SetEFSResolver(&lambdaEFSResolver{byName: byName})

	if cli == nil || !cli.Persist {
		return
	}

	efsH, ok := byName["EFS"].(*efsbackend.Handler)
	if !ok {
		return
	}

	efsH.Backend.SetStorageRoot(filepath.Join(cli.resolvedDataDir(), "efs"))
}

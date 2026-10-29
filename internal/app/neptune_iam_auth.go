package app

import (
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	iambackend "github.com/blackbirdworks/gopherstack/services/iam"
	"github.com/blackbirdworks/gopherstack/services/neptune"
	stsbackend "github.com/blackbirdworks/gopherstack/services/sts"
)

// wireNeptuneIAMAuth connects Neptune data-plane IAM database authentication to IAM and STS.
func wireNeptuneIAMAuth(services []service.Registerable, enforceIAM bool) {
	var (
		nepH *neptune.Handler
		iamH *iambackend.Handler
		stsH *stsbackend.Handler
	)

	for _, svc := range services {
		switch h := svc.(type) {
		case *neptune.Handler:
			nepH = h
		case *iambackend.Handler:
			iamH = h
		case *stsbackend.Handler:
			stsH = h
		}
	}

	if nepH == nil {
		return
	}

	bk, ok := nepH.Backend.(interface{ SetIAMAuth(neptune.IAMAuthResolver) })
	if !ok {
		return
	}

	bk.SetIAMAuth(func() neptune.IAMAuthConfig { return neptuneIAMAuthConfig(iamH, stsH, enforceIAM) })
}

func neptuneIAMAuthConfig(iamH *iambackend.Handler, stsH *stsbackend.Handler, enforceIAM bool) neptune.IAMAuthConfig {
	cfg := neptune.IAMAuthConfig{Enforce: enforceIAM}

	if iamH != nil {
		if ids, ok := iamH.Backend.(neptune.IAMAuthIdentityProvider); ok {
			cfg.Identities = ids
		}
	}

	if stsH != nil {
		if ss, ok := stsH.Backend.(neptune.IAMAuthSessionProvider); ok {
			cfg.Sessions = ss
		}
	}

	return cfg
}

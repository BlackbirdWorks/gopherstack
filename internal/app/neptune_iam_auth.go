package app

import (
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	iambackend "github.com/blackbirdworks/gopherstack/services/iam"
	"github.com/blackbirdworks/gopherstack/services/neptune"
	s3backend "github.com/blackbirdworks/gopherstack/services/s3"
	stsbackend "github.com/blackbirdworks/gopherstack/services/sts"
)

// wireNeptuneIAMAuth connects Neptune data-plane IAM database authentication to IAM and STS, and the RDF bulk
// loader / SPARQL LOAD to S3.
func wireNeptuneIAMAuth(services []service.Registerable, enforceIAM bool) {
	var (
		nepH *neptune.Handler
		iamH *iambackend.Handler
		stsH *stsbackend.Handler
		s3H  *s3backend.S3Handler
	)

	for _, svc := range services {
		switch h := svc.(type) {
		case *neptune.Handler:
			nepH = h
		case *iambackend.Handler:
			iamH = h
		case *stsbackend.Handler:
			stsH = h
		case *s3backend.S3Handler:
			s3H = h
		}
	}

	if nepH == nil {
		return
	}

	if nbk, ok := nepH.Backend.(interface{ SetS3Backend(neptune.S3Reader) }); ok && s3H != nil {
		if s3bk, isS3 := s3H.Backend.(neptune.S3Reader); isS3 {
			nbk.SetS3Backend(s3bk)
		}
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

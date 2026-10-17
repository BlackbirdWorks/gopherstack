package app

import (
	"net/http"
	"path/filepath"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	iambackend "github.com/blackbirdworks/gopherstack/services/iam"
	"github.com/blackbirdworks/gopherstack/services/rds"
	stsbackend "github.com/blackbirdworks/gopherstack/services/sts"
)

const rdsCAPath = "/_gopherstack/rds/ca.pem"

// GetRDSCADir persists the docker-mode RDS CA under the data dir when --persist is on (rds.EngineCADirConfig).
func (c *CLI) GetRDSCADir() string {
	if !c.Persist {
		return ""
	}

	return filepath.Join(c.resolvedDataDir(), "rds-ca")
}

// wireRDSIAMAuth connects RDS IAM database authentication to IAM and STS and serves the engine CA at rdsCAPath.
func wireRDSIAMAuth(e *echo.Echo, services []service.Registerable, enforceIAM bool) {
	var (
		rdsH *rds.Handler
		iamH *iambackend.Handler
		stsH *stsbackend.Handler
	)

	for _, svc := range services {
		switch h := svc.(type) {
		case *rds.Handler:
			rdsH = h
		case *iambackend.Handler:
			iamH = h
		case *stsbackend.Handler:
			stsH = h
		}
	}

	if rdsH == nil {
		return
	}

	rdsH.Backend.SetIAMAuth(func() rds.IAMAuthConfig { return rdsIAMAuthConfig(iamH, stsH, enforceIAM) })

	e.GET(rdsCAPath, func(c *echo.Context) error {
		pem := rdsH.Backend.EngineCAPEM()
		if pem == nil {
			return c.String(http.StatusNotFound, "RDS docker engine is not enabled")
		}

		return c.Blob(http.StatusOK, "application/x-pem-file", pem)
	})
}

func rdsIAMAuthConfig(iamH *iambackend.Handler, stsH *stsbackend.Handler, enforceIAM bool) rds.IAMAuthConfig {
	cfg := rds.IAMAuthConfig{Enforce: enforceIAM}

	if iamH != nil {
		if ids, ok := iamH.Backend.(rds.IAMAuthIdentityProvider); ok {
			cfg.Identities = ids
		}
	}

	if stsH != nil {
		if ss, ok := stsH.Backend.(rds.IAMAuthSessionProvider); ok {
			cfg.Sessions = ss
		}
	}

	return cfg
}
